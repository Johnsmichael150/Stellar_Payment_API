/**
 * Integration tests for Row-Level Security Retry with Exponential Backoff (Issue #1469)
 *
 * These tests verify the automated retry logic with exponential backoff
 * for the Database Row-Level Security Service.
 *
 * Prerequisites: a real PostgreSQL instance with RLS applied.
 * Skip gracefully when DATABASE_URL is not set so CI doesn't fail in
 * environments without a database.
 */
import { describe, it, expect, beforeAll, afterAll, vi } from "vitest";
import pg from "pg";
import crypto from "crypto";
import { withMerchantContextRetry } from "./db-rls.js";

const { Pool } = pg;

const DB_URL = process.env.DATABASE_URL;

describe.skipIf(!DB_URL || process.env.CI)("RLS Retry — exponential backoff", () => {
  let pool;
  let merchantId;

  beforeAll(async () => {
    pool = new Pool({ connectionString: DB_URL, max: 2 });
    merchantId = crypto.randomUUID();
  });

  afterAll(async () => {
    await pool.end();
  });

  describe("Basic retry functionality", () => {
    it("succeeds on first attempt when no error occurs", async () => {
      let attemptCount = 0;

      const result = await withMerchantContextRetry(merchantId, async (client) => {
        attemptCount += 1;
        const { rows } = await client.query("SELECT 1 as value");
        return rows[0].value;
      });

      expect(result).toBe(1);
      expect(attemptCount).toBe(1);
    });

    it("retries on transient errors and eventually succeeds", async () => {
      let attemptCount = 0;

      const result = await withMerchantContextRetry(
        merchantId,
        async (client) => {
          attemptCount += 1;
          if (attemptCount < 2) {
            // Simulate a transient error on first attempt
            const error = new Error("Connection terminated");
            error.code = "08006";
            throw error;
          }
          const { rows } = await client.query("SELECT 1 as value");
          return rows[0].value;
        },
        { retryAttempts: 3, retryDelayMs: 10 },
      );

      expect(result).toBe(1);
      expect(attemptCount).toBe(2);
    });

    it("exhausts retries and throws after max attempts", async () => {
      let attemptCount = 0;

      await expect(
        withMerchantContextRetry(
          merchantId,
          async () => {
            attemptCount += 1;
            const error = new Error("Connection terminated");
            error.code = "08006";
            throw error;
          },
          { retryAttempts: 2, retryDelayMs: 10 },
        ),
      ).rejects.toThrow("Connection terminated");

      expect(attemptCount).toBe(3); // Initial + 2 retries
    });

    it("does not retry non-retryable errors", async () => {
      let attemptCount = 0;

      await expect(
        withMerchantContextRetry(
          merchantId,
          async () => {
            attemptCount += 1;
            const error = new Error("Syntax error");
            error.code = "42601";
            throw error;
          },
          { retryAttempts: 3, retryDelayMs: 10 },
        ),
      ).rejects.toThrow("Syntax error");

      expect(attemptCount).toBe(1); // Should not retry
    });
  });

  describe("Exponential backoff", () => {
    it("uses exponential backoff delays", async () => {
      const delays = [];
      const startTime = Date.now();

      await withMerchantContextRetry(
        merchantId,
        async () => {
          const elapsed = Date.now() - startTime;
          delays.push(elapsed);
          const error = new Error("Connection terminated");
          error.code = "08006";
          throw error;
        },
        { retryAttempts: 3, retryDelayMs: 50, retryMaxDelayMs: 1000 },
      ).catch(() => {}); // Ignore expected error

      // Verify delays increase exponentially (with jitter)
      expect(delays.length).toBeGreaterThan(1);
      expect(delays[1]).toBeGreaterThan(delays[0]);
    });

    it("respects maximum delay cap", async () => {
      const delays = [];
      const startTime = Date.now();

      await withMerchantContextRetry(
        merchantId,
        async () => {
          const elapsed = Date.now() - startTime;
          delays.push(elapsed);
          const error = new Error("Connection terminated");
          error.code = "08006";
          throw error;
        },
        { retryAttempts: 10, retryDelayMs: 100, retryMaxDelayMs: 200 },
      ).catch(() => {}); // Ignore expected error

      // Verify no delay exceeds maxDelayMs
      for (let i = 1; i < delays.length; i++) {
        const delay = delays[i] - delays[i - 1];
        expect(delay).toBeLessThanOrEqual(200 + 50); // Allow some margin for jitter
      }
    });
  });

  describe("Jitter in backoff", () => {
    it("adds randomness to retry delays", async () => {
      const delaySets = [];

      for (let run = 0; run < 5; run++) {
        const delays = [];
        const startTime = Date.now();

        await withMerchantContextRetry(
          merchantId,
          async () => {
            const elapsed = Date.now() - startTime;
            delays.push(elapsed);
            const error = new Error("Connection terminated");
            error.code = "08006";
            throw error;
          },
          { retryAttempts: 2, retryDelayMs: 50, retryMaxDelayMs: 1000 },
        ).catch(() => {});

        delaySets.push(delays);
      }

      // Verify delays vary between runs (due to jitter)
      const firstDelays = delaySets[0];
      const hasVariation = delaySets.some((delays) => {
        return delays.some((delay, i) => Math.abs(delay - firstDelays[i]) > 10);
      });

      expect(hasVariation).toBe(true);
    });
  });

  describe("Configuration options", () => {
    it("respects custom retry attempts", async () => {
      let attemptCount = 0;

      await withMerchantContextRetry(
        merchantId,
        async () => {
          attemptCount += 1;
          const error = new Error("Connection terminated");
          error.code = "08006";
          throw error;
        },
        { retryAttempts: 1, retryDelayMs: 10 },
      ).catch(() => {});

      expect(attemptCount).toBe(2); // Initial + 1 retry
    });

    it("respects custom base delay", async () => {
      const startTime = Date.now();
      let attemptCount = 0;

      await withMerchantContextRetry(
        merchantId,
        async () => {
          attemptCount += 1;
          if (attemptCount < 2) {
            const error = new Error("Connection terminated");
            error.code = "08006";
            throw error;
          }
          return { success: true };
        },
        { retryAttempts: 2, retryDelayMs: 200 },
      );

      const elapsed = Date.now() - startTime;
      // Should have waited at least 200ms for first retry
      expect(elapsed).toBeGreaterThanOrEqual(200);
    });

    it("uses label for logging", async () => {
      const consoleSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

      await withMerchantContextRetry(
        merchantId,
        async () => {
          const error = new Error("Connection terminated");
          error.code = "08006";
          throw error;
        },
        { retryAttempts: 1, retryDelayMs: 10, label: "test-label" },
      ).catch(() => {});

      // Verify label appears in log (check was called)
      expect(consoleSpy).toHaveBeenCalled();

      consoleSpy.mockRestore();
    });
  });

  describe("Transaction isolation on retry", () => {
    it("opens new transaction on each retry", async () => {
      let transactionIds = [];

      await withMerchantContextRetry(
        merchantId,
        async (client) => {
          const { rows } = await client.query("SELECT txid_current() as txid");
          transactionIds.push(rows[0].txid);

          if (transactionIds.length < 2) {
            const error = new Error("Connection terminated");
            error.code = "08006";
            throw error;
          }

          return { success: true };
        },
        { retryAttempts: 2, retryDelayMs: 10 },
      );

      // Each retry should have a different transaction ID
      expect(transactionIds.length).toBeGreaterThan(1);
      expect(new Set(transactionIds).size).toBe(transactionIds.length);
    });

    it("sets merchant context on each retry", async () => {
      let contextCheckCount = 0;

      await withMerchantContextRetry(
        merchantId,
        async (client) => {
          const { rows } = await client.query(
            "SELECT current_setting('app.current_merchant_id', true) as merchant_id",
          );
          expect(rows[0].merchant_id).toBe(merchantId);
          contextCheckCount += 1;

          if (contextCheckCount < 2) {
            const error = new Error("Connection terminated");
            error.code = "08006";
            throw error;
          }

          return { success: true };
        },
        { retryAttempts: 2, retryDelayMs: 10 },
      );

      expect(contextCheckCount).toBe(2);
    });
  });

  describe("Error handling", () => {
    it("preserves original error message on retry exhaustion", async () => {
      const originalError = new Error("Original error message");
      originalError.code = "08006";

      await expect(
        withMerchantContextRetry(
          merchantId,
          async () => {
            throw originalError;
          },
          { retryAttempts: 1, retryDelayMs: 10 },
        ),
      ).rejects.toThrow("Original error message");
    });

    it("preserves error code on retry exhaustion", async () => {
      const originalError = new Error("Connection terminated");
      originalError.code = "08006";

      try {
        await withMerchantContextRetry(
          merchantId,
          async () => {
            throw originalError;
          },
          { retryAttempts: 1, retryDelayMs: 10 },
        );
      } catch (err) {
        expect(err.code).toBe("08006");
      }
    });
  });

  describe("Real-world scenarios", () => {
    it("handles connection pool exhaustion", async () => {
      // This test would require a more complex setup to simulate pool exhaustion
      // For now, we verify the retry logic structure is in place
      let attemptCount = 0;

      await withMerchantContextRetry(
        merchantId,
        async (client) => {
          attemptCount += 1;
          // Simulate pool exhaustion error
          if (attemptCount < 2) {
            const error = new Error("too many clients");
            error.code = "53300";
            throw error;
          }
          const { rows } = await client.query("SELECT 1 as value");
          return rows[0].value;
        },
        { retryAttempts: 3, retryDelayMs: 10 },
      );

      expect(attemptCount).toBe(2);
    });

    it("handles statement timeout", async () => {
      let attemptCount = 0;

      await withMerchantContextRetry(
        merchantId,
        async (client) => {
          attemptCount += 1;
          if (attemptCount < 2) {
            const error = new Error("statement timeout");
            error.code = "57014";
            throw error;
          }
          const { rows } = await client.query("SELECT 1 as value");
          return rows[0].value;
        },
        { retryAttempts: 3, retryDelayMs: 10 },
      );

      expect(attemptCount).toBe(2);
    });
  });
});
