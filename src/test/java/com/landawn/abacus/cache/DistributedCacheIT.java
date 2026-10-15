/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static com.landawn.abacus.cache.DistributedCacheBreakerAccess.failedCounter;
import static com.landawn.abacus.cache.DistributedCacheBreakerAccess.setBreakerState;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DistributedCacheIT {

    private static final String SERVER_URL = com.landawn.abacus.cache.MemcachedTestSupport.serverUrl();
    private static final String DISTRIBUTED_CACHE_LOGGER = "com.landawn.abacus.cache.DistributedCache";

    /** Long-lived client used only to flush the server between tests and to host constructor-validation. */
    private static SpyMemcached<String> flushClient;

    @BeforeAll
    static void connect() {
        flushClient = new SpyMemcached<>(SERVER_URL);
    }

    @AfterAll
    static void disconnect() {
        if (flushClient != null) {
            flushClient.disconnect();
        }
    }

    @BeforeEach
    void flush() {
        flushClient.flushAll();
    }

    // Construction

    /**
     * Regression test for the unvalidated key prefix: a prefix containing a space (or any
     * non-printable-ASCII character) would make every generated key invalid at the server — every
     * write throwing and every read silently feeding the circuit breaker. It is now rejected at
     * construction time.
     */
    @Test
    void constructorInvalidKeyPrefixThrowsIAE() {
        final SpyMemcached<String> client = newClient();
        try {
            assertThrows(IllegalArgumentException.class, () -> new DistributedCache<>(client, "my app: ", 100, 1000));
            assertThrows(IllegalArgumentException.class, () -> new DistributedCache<>(client, "café:", 100, 1000));
            // A printable-ASCII prefix is accepted.
            final DistributedCache<String, String> cache = new DistributedCache<>(client, "myapp:", 100, 1000);
            try {
                assertTrue(cache.put("k", "v", 60_000, 0));
                assertEquals("v", cache.getOrNull("k"));
            } finally {
                cache.close();
            }
        } finally {
            client.disconnect();
        }
    }

    // Reads

    /**
     * A read failure from the backend is swallowed and surfaced as a cache miss (null). The failure is
     * real: the underlying client is shut down, so {@code get} throws instantly.
     */
    @Test
    void getOrNullFailingBackendIsSwallowedAsNull() {
        final SpyMemcached<String> client = newClient();
        // High threshold + long retry window so the breaker stays closed and the failing get actually
        // reaches the (shut-down) client.
        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 5, 10_000);
        try {
            client.disconnect(); // every subsequent get() now throws IllegalStateException
            assertNull(cache.getOrNull("k"));
        } finally {
            cache.close();
        }
    }

    /**
     * Regression test for breaker poisoning by deterministic validation errors.
     *
     * <p>Base64 encoding expands keys by 4/3, so a long natural key produces a generated key over
     * memcached's 250-character limit, which the client rejects with {@link IllegalArgumentException}
     * on every call. Previously that IAE was swallowed as a cache miss AND counted toward the
     * availability breaker — a hot invalid key could open the circuit and blank reads for ALL keys.
     * The IAE is now rethrown and leaves the breaker state untouched.
     */
    @Test
    void getOrNullOverlongKeyThrowsIAEWithoutTouchingBreaker() throws Exception {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient(), "", 5, 60_000);
        try {
            final String overlongKey = "k".repeat(300); // Base64-encodes to ~400 chars, over the 250 limit

            assertThrows(IllegalArgumentException.class, () -> cache.getOrNull(overlongKey));
            assertEquals(0, failedCounter(cache), "a client-side validation error must not count as an availability failure");

            // Normal keys keep working and the breaker is still closed.
            assertTrue(cache.put("ok", "v", 60_000, 0));
            assertEquals("v", cache.getOrNull("ok"));
        } finally {
            cache.close();
        }
    }

    /**
     * A read failure is swallowed (treated as a cache miss) and logged at debug. Raising the logger
     * level to DEBUG exercises the debug-logging line inside the read-failure catch block.
     */
    @Test
    void getOrNullReadFailureLoggedAtDebug() {
        Configurator.setLevel(DISTRIBUTED_CACHE_LOGGER, Level.DEBUG);
        try {
            final SpyMemcached<String> client = newClient();
            // High threshold + long retry window so the breaker stays closed and the failing get reaches
            // the (shut-down) client and the debug-logging branch.
            final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 5, 10_000);
            try {
                client.disconnect();
                assertNull(cache.getOrNull("k"));
            } finally {
                cache.close();
            }
        } finally {
            Configurator.setLevel(DISTRIBUTED_CACHE_LOGGER, Level.ERROR);
        }
    }

    // Writes

    @Test
    void putAndGetOrNull() {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());
        try {
            assertTrue(cache.put("k1", "v1", 60_000, 0));
            assertEquals("v1", cache.getOrNull("k1"));
        } finally {
            cache.close();
        }
    }

    @Test
    void putWithKeyPrefix() {
        final SpyMemcached<String> client = newClient();
        final DistributedCache<String, String> cache = new DistributedCache<>(client, "myapp:");
        try {
            assertTrue(cache.put("k1", "v1", 60_000, 0));
            // Round-trips through the cache...
            assertEquals("v1", cache.getOrNull("k1"));
            // ...and the prefix is actually applied to the key stored on the server.
            final String storedKey = cache.generateKey("k1");
            assertTrue(storedKey.startsWith("myapp:"));
            assertEquals("v1", client.get(storedKey));
        } finally {
            cache.close();
        }
    }

    // Removals

    @Test
    void clearRemovesEntries() {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());
        try {
            cache.put("k", "v", 0, 0);
            cache.clear();
            assertNull(cache.getOrNull("k"));
        } finally {
            cache.close();
        }
    }

    @Test
    void removeDeletesEntry() {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());
        try {
            cache.put("k", "v", 60_000, 0);
            cache.remove("k");
            assertNull(cache.getOrNull("k"));
        } finally {
            cache.close();
        }
    }

    // Membership And Views

    @Test
    void containsKeyReportsPresence() {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());
        try {
            cache.put("k", "v", 60_000, 0);
            assertTrue(cache.containsKey("k"));
            assertFalse(cache.containsKey("missing"));
        } finally {
            cache.close();
        }
    }

    // Validation And Failures

    /**
     * Guards the failure counter against unbounded growth: with {@code retryDelay == 0} the breaker
     * never fast-fails, so each failing get passes through the (clamped) increment branch. Driving many
     * failures past the threshold must leave the counter pinned at the cap rather than overflowing.
     */
    @Test
    void circuitBreakerFailureCounterDoesNotGrowUnbounded() throws Exception {
        final int cap = 2;
        final SpyMemcached<String> client = newClient();
        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", cap, 0);
        try {
            client.disconnect();
            for (int i = 0; i < 50; i++) {
                assertNull(cache.getOrNull("k"));
            }
            assertEquals(cap, failedCounter(cache), "counter must stay clamped at the cap, never overflow");
        } finally {
            cache.close();
        }
    }

    // Concurrency

    /**
     * The failure counter must never exceed {@code maxFailuresBeforeCircuitOpen}, even under concurrent failing
     * reads. With {@code retryDelay == 0} the breaker never short-circuits, so every concurrent get
     * flows through the capped increment path. Failures are real (shut-down client) and instant.
     */
    @Test
    void circuitBreakerFailureCounterNeverExceedsCapUnderConcurrency() throws Exception {
        final int cap = 5;
        final SpyMemcached<String> client = newClient();
        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", cap, 0);
        try {
            client.disconnect(); // all reads now fail instantly

            final int threadCount = 16;
            final ExecutorService pool = Executors.newFixedThreadPool(threadCount);
            final CountDownLatch start = new CountDownLatch(1);
            final List<Future<?>> futures = new ArrayList<>();

            for (int t = 0; t < threadCount; t++) {
                futures.add(pool.submit(() -> {
                    assertTrue(start.await(10, TimeUnit.SECONDS));
                    for (int i = 0; i < 2000; i++) {
                        cache.getOrNull("k");
                    }
                    return null;
                }));
            }

            start.countDown();
            for (final Future<?> f : futures) {
                f.get();
            }
            pool.shutdown();

            assertEquals(cap, failedCounter(cache), "failure counter must be capped at maxFailuresBeforeCircuitOpen under concurrency");
        } finally {
            cache.close();
        }
    }

    // Other Behavior

    /**
     * With a genuinely-present value on the server, forcing the breaker into the open state (via its
     * internal counters) makes {@code getOrNull} short-circuit to {@code null} despite the value being
     * available; once the retry window elapses the read succeeds again and the counter resets.
     */
    @Test
    void circuitBreakerOpensThenRecovers() throws Exception {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient(), "", 1, 60_000);
        try {
            assertTrue(cache.put("k", "v", 60_000, 0));
            assertEquals("v", cache.getOrNull("k"));

            // Open the circuit: counter at threshold, last failure just now. The breaker measures
            // elapsed time with System.nanoTime() (monotonic), not the wall clock.
            setBreakerState(cache, 1, System.nanoTime());
            assertNull(cache.getOrNull("k"), "an open circuit must short-circuit to null even though the value is present");

            // Let the retry window elapse: backdate the last failure past the 60s window so the
            // read reaches the server again and the breaker closes.
            setBreakerState(cache, 1, System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(60_001));
            assertEquals("v", cache.getOrNull("k"));
            assertEquals(0, failedCounter(cache), "a successful read must reset the failure counter");
        } finally {
            cache.close();
        }
    }

    /**
     * Regression test for the monotonic-clock circuit breaker: the fail-fast window must close by
     * genuinely elapsed time. (Previously the window was measured with {@code System.currentTimeMillis()},
     * so a backward wall-clock adjustment extended the read blackout by the size of the jump; the breaker
     * now uses {@code System.nanoTime()}, which cannot move backwards.)
     */
    @Test
    void circuitBreakerRecoversAfterRealElapsedDelay() throws Exception {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient(), "", 1, 100);
        try {
            assertTrue(cache.put("k", "v", 60_000, 0));

            setBreakerState(cache, 1, System.nanoTime()); // open: last failure "just now", 100ms window
            assertNull(cache.getOrNull("k"), "circuit must be open immediately after the failure");

            Thread.sleep(150); // let the 100ms retry window genuinely elapse

            assertEquals("v", cache.getOrNull("k"), "circuit must close once the retry delay has elapsed");
            assertEquals(0, failedCounter(cache));
        } finally {
            cache.close();
        }
    }

    private static SpyMemcached<String> newClient() {
        return new SpyMemcached<>(SERVER_URL);
    }
}
