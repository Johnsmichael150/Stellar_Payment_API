import { pool, isRetryablePoolError } from "../lib/db.js";
import { logger } from "./logger.js";

// Retry configuration for RLS operations
const RLS_RETRY_ATTEMPTS = Number.parseInt(
  process.env.RLS_RETRY_ATTEMPTS || "3",
  10,
);
const RLS_RETRY_DELAY_MS = Number.parseInt(
  process.env.RLS_RETRY_DELAY_MS || "100",
  10,
);
const RLS_RETRY_MAX_DELAY_MS = Number.parseInt(
  process.env.RLS_RETRY_MAX_DELAY_MS || "5000",
  10,
);

/**
 * Sleep utility for retry delays
 */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Calculate exponential backoff delay with jitter
 *
 * @param {number} attempt - Current attempt number (0-based)
 * @param {number} baseDelayMs - Base delay in milliseconds
 * @param {number} maxDelayMs - Maximum delay in milliseconds
 * @returns {number} - Delay in milliseconds
 */
function calculateBackoffDelay(attempt, baseDelayMs, maxDelayMs) {
  const exponentialDelay = baseDelayMs * Math.pow(2, attempt);
  const jitter = Math.random() * 0.1 * exponentialDelay; // Add 10% jitter
  const delay = Math.min(exponentialDelay + jitter, maxDelayMs);
  return Math.floor(delay);
}

/**
 * Executes a callback within a transaction where the merchant context is set.
 * This ensures Row Level Security (RLS) is enforced for direct pg pool queries.
 */
export async function withMerchantContext(merchantId, callback) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    if (merchantId) {
      await client.query("SET LOCAL app.current_merchant_id = $1", [
        merchantId,
      ]);
    }
    const result = await callback(client);
    await client.query("COMMIT");
    return result;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Executes a callback within a transaction with merchant context and automatic retry.
 *
 * Uses exponential backoff with jitter for retry delays. Each retry opens a new
 * transaction on a fresh client to avoid issues with mid-transaction connection drops.
 *
 * @param {string} merchantId - The merchant ID for RLS context
 * @param {Function} callback - Function to execute with the client
 * @param {Object} options - Retry options
 * @param {number} options.retryAttempts - Maximum number of retry attempts
 * @param {number} options.retryDelayMs - Base delay for exponential backoff
 * @param {number} options.retryMaxDelayMs - Maximum delay for exponential backoff
 * @param {string} options.label - Label for logging
 * @returns {Promise<*>} - Result of the callback
 */
export async function withMerchantContextRetry(
  merchantId,
  callback,
  {
    retryAttempts = RLS_RETRY_ATTEMPTS,
    retryDelayMs = RLS_RETRY_DELAY_MS,
    retryMaxDelayMs = RLS_RETRY_MAX_DELAY_MS,
    label = "rls-query",
  } = {},
) {
  let lastError;

  for (let attempt = 0; attempt <= retryAttempts; attempt += 1) {
    try {
      return await withMerchantContext(merchantId, callback);
    } catch (err) {
      lastError = err;
      const shouldRetry = attempt < retryAttempts && isRetryablePoolError(err);

      if (!shouldRetry) {
        logger.error(
          {
            err: err.message,
            code: err.code,
            label,
            attempt: attempt + 1,
            maxAttempts: retryAttempts + 1,
            retryable: false,
          },
          "RLS query failed (non-retryable)",
        );
        throw err;
      }

      const delayMs = calculateBackoffDelay(attempt, retryDelayMs, retryMaxDelayMs);

      logger.warn(
        {
          err: err.message,
          code: err.code,
          label,
          attempt: attempt + 1,
          maxAttempts: retryAttempts + 1,
          delayMs,
          nextRetryIn: delayMs,
        },
        "RLS query failed, retrying with exponential backoff",
      );

      await sleep(delayMs);
    }
  }

  logger.error(
    {
      err: lastError?.message,
      code: lastError?.code,
      label,
      attempts: retryAttempts + 1,
      exhausted: true,
    },
    "RLS query retry attempts exhausted",
  );

  throw lastError;
}

/**
 * Distributed advisory lock management for Row-Level Security
 *
 * PostgreSQL advisory locks provide distributed concurrency control across
 * multiple application instances. Locks are automatically released when the
 * session ends, providing safety against orphaned locks.
 */

const LOCK_TIMEOUT_MS = Number.parseInt(
  process.env.RLS_LOCK_TIMEOUT_MS || "5000",
  10,
);

/**
 * Acquire an advisory lock for a specific merchant resource.
 *
 * @param {string} merchantId - The merchant ID to lock
 * @param {string} resourceType - Type of resource (e.g., 'payment', 'merchant')
 * @param {Object} options - Lock options
 * @param {number} options.timeout - Lock acquisition timeout in milliseconds
 * @returns {Promise<boolean>} - True if lock was acquired, false otherwise
 */
export async function acquireAdvisoryLock(
  merchantId,
  resourceType,
  { timeout = LOCK_TIMEOUT_MS } = {},
) {
  const lockKey = generateLockKey(merchantId, resourceType);
  const startTime = Date.now();

  while (Date.now() - startTime < timeout) {
    try {
      const { rows } = await pool.query(
        "SELECT pg_try_advisory_lock($1::bigint) as acquired",
        [lockKey],
      );

      if (rows[0].acquired) {
        logger.info(
          { merchantId, resourceType, lockKey },
          "Advisory lock acquired",
        );
        return true;
      }

      // Wait before retrying (exponential backoff)
      const elapsed = Date.now() - startTime;
      const backoffMs = Math.min(100 * Math.pow(2, Math.floor(elapsed / 1000)), 1000);
      await new Promise((resolve) => setTimeout(resolve, backoffMs));
    } catch (err) {
      logger.error(
        { err: err.message, merchantId, resourceType },
        "Failed to acquire advisory lock",
      );
      return false;
    }
  }

  logger.warn(
    { merchantId, resourceType, timeout },
    "Advisory lock acquisition timed out",
  );
  return false;
}

/**
 * Release an advisory lock for a specific merchant resource.
 *
 * @param {string} merchantId - The merchant ID to unlock
 * @param {string} resourceType - Type of resource
 * @returns {Promise<boolean>} - True if lock was released, false otherwise
 */
export async function releaseAdvisoryLock(merchantId, resourceType) {
  const lockKey = generateLockKey(merchantId, resourceType);

  try {
    const { rows } = await pool.query(
      "SELECT pg_advisory_unlock($1::bigint) as released",
      [lockKey],
    );

    if (rows[0].released) {
      logger.info(
        { merchantId, resourceType, lockKey },
        "Advisory lock released",
      );
      return true;
    }

    logger.warn(
      { merchantId, resourceType, lockKey },
      "Advisory lock was not held by this session",
    );
    return false;
  } catch (err) {
    logger.error(
      { err: err.message, merchantId, resourceType },
      "Failed to release advisory lock",
    );
    return false;
  }
}

/**
 * Execute a callback with an advisory lock held.
 *
 * This provides distributed concurrency control for critical sections
 * that need to be serialized across multiple application instances.
 *
 * @param {string} merchantId - The merchant ID to lock
 * @param {string} resourceType - Type of resource
 * @param {Function} callback - Function to execute while holding the lock
 * @param {Object} options - Lock options
 * @returns {Promise<*>} - Result of the callback
 */
export async function withAdvisoryLock(
  merchantId,
  resourceType,
  callback,
  options = {},
) {
  const acquired = await acquireAdvisoryLock(merchantId, resourceType, options);

  if (!acquired) {
    throw new Error(
      `Failed to acquire advisory lock for merchant ${merchantId} on ${resourceType}`,
    );
  }

  try {
    return await callback();
  } finally {
    await releaseAdvisoryLock(merchantId, resourceType);
  }
}

/**
 * Execute a callback with both merchant context and advisory lock.
 *
 * This combines RLS enforcement with distributed concurrency control,
 * ensuring both security and serialization across instances.
 *
 * @param {string} merchantId - The merchant ID
 * @param {string} resourceType - Type of resource for locking
 * @param {Function} callback - Function to execute
 * @param {Object} options - Lock options
 * @returns {Promise<*>} - Result of the callback
 */
export async function withMerchantContextAndLock(
  merchantId,
  resourceType,
  callback,
  options = {},
) {
  return withAdvisoryLock(merchantId, resourceType, async () => {
    return withMerchantContext(merchantId, callback);
  }, options);
}

/**
 * Check if an advisory lock is currently held.
 *
 * @param {string} merchantId - The merchant ID
 * @param {string} resourceType - Type of resource
 * @returns {Promise<boolean>} - True if lock is held, false otherwise
 */
export async function isAdvisoryLockHeld(merchantId, resourceType) {
  const lockKey = generateLockKey(merchantId, resourceType);

  try {
    const { rows } = await pool.query(
      "SELECT pg_advisory_lock_is_held($1::bigint) as held",
      [lockKey],
    );

    return rows[0].held;
  } catch (err) {
    logger.error(
      { err: err.message, merchantId, resourceType },
      "Failed to check advisory lock status",
    );
    return false;
  }
}

/**
 * Generate a consistent lock key from merchant ID and resource type.
 *
 * Uses hash function to convert string identifiers to numeric lock keys
 * for PostgreSQL advisory locks.
 *
 * @param {string} merchantId - The merchant ID
 * @param {string} resourceType - Type of resource
 * @returns {number} - Numeric lock key
 */
function generateLockKey(merchantId, resourceType) {
  const combined = `${merchantId}:${resourceType}`;
  let hash = 0;

  for (let i = 0; i < combined.length; i++) {
    const char = combined.charCodeAt(i);
    hash = (hash << 5) - hash + char;
    hash = hash & hash; // Convert to 32-bit integer
  }

  return Math.abs(hash);
}

/**
 * Get current advisory lock statistics for monitoring.
 *
 * @returns {Promise<Object>} - Lock statistics
 */
export async function getLockStats() {
  try {
    const { rows } = await pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE locktype = 'advisory') as advisory_locks,
        COUNT(*) FILTER (WHERE locktype = 'relation') as relation_locks,
        COUNT(*) FILTER (WHERE locktype = 'transactionid') as transaction_locks
      FROM pg_locks
      WHERE pid = pg_backend_pid()
    `);

    return {
      advisoryLocks: Number.parseInt(rows[0].advisory_locks || "0", 10),
      relationLocks: Number.parseInt(rows[0].relation_locks || "0", 10),
      transactionLocks: Number.parseInt(rows[0].transaction_locks || "0", 10),
    };
  } catch (err) {
    logger.error({ err: err.message }, "Failed to get lock stats");
    return {
      advisoryLocks: 0,
      relationLocks: 0,
      transactionLocks: 0,
    };
  }
}
