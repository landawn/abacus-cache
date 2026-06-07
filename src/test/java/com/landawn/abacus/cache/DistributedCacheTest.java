/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.TestBase;

@Tag("2025")
public class DistributedCacheTest extends TestBase {

    /**
     * In-memory backing for the DistributedCacheClient interface so DistributedCache can be
     * exercised without a real server. The "fail" toggle lets tests verify circuit-breaker behavior.
     */
    private static final class InMemoryClient implements DistributedCacheClient<String> {
        final Map<String, String> store = new ConcurrentHashMap<>();
        final AtomicInteger getCalls = new AtomicInteger();
        final AtomicInteger flushCalls = new AtomicInteger();
        final AtomicInteger disconnectCalls = new AtomicInteger();
        volatile boolean failGet = false;

        @Override
        public String serverUrl() {
            return "in-memory";
        }

        @Override
        public String get(final String key) {
            getCalls.incrementAndGet();
            if (failGet) {
                throw new RuntimeException("simulated failure");
            }
            return store.get(key);
        }

        @Override
        public Map<String, String> getBulk(final String... keys) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> getBulk(final Collection<String> keys) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean set(final String key, final String value, final long liveTime) {
            store.put(key, value);
            return true;
        }

        @Override
        public boolean delete(final String key) {
            store.remove(key);
            return true;
        }

        @Override
        public long incr(final String key) {
            return 0;
        }

        @Override
        public long incr(final String key, final int delta) {
            return 0;
        }

        @Override
        public long decr(final String key) {
            return 0;
        }

        @Override
        public long decr(final String key, final int delta) {
            return 0;
        }

        @Override
        public void flushAll() {
            flushCalls.incrementAndGet();
            store.clear();
        }

        @Override
        public void disconnect() {
            disconnectCalls.incrementAndGet();
        }
    }

    @Test
    public void testConstructor_EdgeCase_NullClient() {
        assertThrows(IllegalArgumentException.class, () -> new DistributedCache<>(null));
    }

    @Test
    public void testConstructor_EdgeCase_NegativeMaxFailed() {
        assertThrows(IllegalArgumentException.class, () -> new DistributedCache<>(new InMemoryClient(), "p:", -1, 1000));
    }

    @Test
    public void testConstructor_EdgeCase_NegativeRetryDelay() {
        assertThrows(IllegalArgumentException.class, () -> new DistributedCache<>(new InMemoryClient(), "p:", 100, -1));
    }

    @Test
    public void testPutAndGetOrNull() {
        final InMemoryClient client = new InMemoryClient();
        try (DistributedCache<String, String> cache = new DistributedCache<>(client)) {
            assertTrue(cache.put("k1", "v1", 1000, 0));
            assertEquals("v1", cache.getOrNull("k1"));
        }
    }

    @Test
    public void testPut_WithKeyPrefix() {
        final InMemoryClient client = new InMemoryClient();
        try (DistributedCache<String, String> cache = new DistributedCache<>(client, "myapp:")) {
            assertTrue(cache.put("k1", "v1", 1000, 0));
            // Verify it round-trips
            assertEquals("v1", cache.getOrNull("k1"));
            // And confirm the prefix is applied to the stored key.
            assertEquals(1, client.store.size());
            assertTrue(client.store.keySet().iterator().next().startsWith("myapp:"));
        }
    }

    @Test
    public void testRemove() {
        final InMemoryClient client = new InMemoryClient();
        try (DistributedCache<String, String> cache = new DistributedCache<>(client)) {
            cache.put("k", "v", 1000, 0);
            cache.remove("k");
            assertNull(cache.getOrNull("k"));
        }
    }

    @Test
    public void testContainsKey() {
        final InMemoryClient client = new InMemoryClient();
        try (DistributedCache<String, String> cache = new DistributedCache<>(client)) {
            cache.put("k", "v", 1000, 0);
            assertTrue(cache.containsKey("k"));
            assertFalse(cache.containsKey("missing"));
        }
    }

    @Test
    public void testClear() {
        final InMemoryClient client = new InMemoryClient();
        try (DistributedCache<String, String> cache = new DistributedCache<>(client)) {
            cache.put("k", "v", 0, 0);
            cache.clear();
            assertEquals(1, client.flushCalls.get());
            assertNull(cache.getOrNull("k"));
        }
    }

    @Test
    public void testKeySet_Unsupported() {
        final InMemoryClient client = new InMemoryClient();
        try (DistributedCache<String, String> cache = new DistributedCache<>(client)) {
            assertThrows(UnsupportedOperationException.class, cache::keySet);
        }
    }

    @Test
    public void testSize_Unsupported() {
        final InMemoryClient client = new InMemoryClient();
        try (DistributedCache<String, String> cache = new DistributedCache<>(client)) {
            assertThrows(UnsupportedOperationException.class, cache::size);
        }
    }

    @Test
    public void testClose_IsIdempotent() {
        final InMemoryClient client = new InMemoryClient();
        final DistributedCache<String, String> cache = new DistributedCache<>(client);
        cache.close();
        cache.close();
        assertTrue(cache.isClosed());
        assertEquals(1, client.disconnectCalls.get(), "disconnect should run exactly once");
    }

    @Test
    public void testOperations_AfterClose_Throw() {
        final InMemoryClient client = new InMemoryClient();
        final DistributedCache<String, String> cache = new DistributedCache<>(client);
        cache.close();
        assertThrows(IllegalStateException.class, () -> cache.getOrNull("k"));
        assertThrows(IllegalStateException.class, () -> cache.put("k", "v", 0, 0));
        assertThrows(IllegalStateException.class, () -> cache.remove("k"));
        assertThrows(IllegalStateException.class, cache::clear);
    }

    @Test
    public void testGenerateKey_EdgeCase_NullKey() {
        final InMemoryClient client = new InMemoryClient();
        try (DistributedCache<String, String> cache = new DistributedCache<>(client)) {
            assertThrows(IllegalArgumentException.class, () -> cache.getOrNull(null));
        }
    }

    /**
     * After maxFailed consecutive failures within the retry window, getOrNull short-circuits
     * to null without touching the client.
     */
    @Test
    public void testCircuitBreaker_OpensAfterMaxFailures() {
        final InMemoryClient client = new InMemoryClient();
        client.failGet = true;
        try (DistributedCache<String, String> cache = new DistributedCache<>(client, "", 2, 10_000)) {
            assertNull(cache.getOrNull("k"));
            assertNull(cache.getOrNull("k"));
            final int callsBefore = client.getCalls.get();
            // Circuit should now be open — next call must not reach the client.
            assertNull(cache.getOrNull("k"));
            assertEquals(callsBefore, client.getCalls.get());
        }
    }

    /**
     * The documented contract of {@link DistributedCache#getOrNull(Object)} is
     * "{@code @throws IllegalArgumentException if the key is null}", with no carve-out for the
     * circuit-breaker-open state. Before the fix, a null key was only rejected inside
     * generateKey(...), which runs AFTER the circuit-breaker fast path — so once the circuit was
     * open, getOrNull(null) silently returned null instead of throwing. This verifies the null key
     * is rejected even when the circuit is open.
     */
    @Test
    public void testGetOrNull_NullKey_ThrowsEvenWhenCircuitOpen() {
        final InMemoryClient client = new InMemoryClient();
        client.failGet = true;
        // maxFailed=1, long retry window so the circuit stays open after the first failure.
        try (DistributedCache<String, String> cache = new DistributedCache<>(client, "", 1, 60_000)) {
            // Drive one failure to open the circuit.
            assertNull(cache.getOrNull("k"));
            // Circuit is now open; a null key must still throw rather than short-circuit to null.
            assertThrows(IllegalArgumentException.class, () -> cache.getOrNull(null));
        }
    }

    /**
     * Guards the circuit-breaker failure counter against unbounded growth. With {@code retryDelay == 0}
     * the breaker never fast-fails (the time window is always elapsed), so every failing get reaches the
     * client and flows through the failure-increment path. Driving many failures past the threshold
     * exercises the clamp that stops incrementing once {@code failedCounter >= maxFailedNumForRetry};
     * without that clamp the counter would eventually overflow to a negative value and silently disable
     * the breaker. The breaker must still recover cleanly once the backend comes back.
     */
    @Test
    public void testCircuitBreaker_FailureCounterDoesNotGrowUnbounded() {
        final InMemoryClient client = new InMemoryClient();
        client.failGet = true;
        try (DistributedCache<String, String> cache = new DistributedCache<>(client, "", 2, 0)) {
            // Every call reaches the client (retryDelay==0 means the circuit never short-circuits),
            // so each failure passes through the (clamped) increment branch — well past the threshold.
            for (int i = 0; i < 50; i++) {
                assertNull(cache.getOrNull("k"));
            }
            assertEquals(50, client.getCalls.get(), "every failing get should reach the client when retryDelay==0");

            // Recovery still works: counter resets to 0 on the next success.
            client.failGet = false;
            client.store.put(cache.generateKey("k"), "ok");
            assertEquals("ok", cache.getOrNull("k"));
        }
    }

    /**
     * The circuit-breaker failure counter must never exceed {@code maxFailedNumForRetry}, even under
     * concurrent failing reads. The increment uses an atomic compare-and-cap ({@code getAndUpdate})
     * rather than a separate {@code get()}-then-{@code incrementAndGet()}; the latter is a
     * check-then-act race in which many threads can all observe {@code (cap - 1)}, all pass the guard,
     * and overshoot the cap. With {@code retryDelay == 0} the breaker never short-circuits, so every
     * concurrent get flows through the capped increment path.
     */
    @Test
    public void testCircuitBreaker_FailureCounterNeverExceedsCapUnderConcurrency() throws Exception {
        final InMemoryClient client = new InMemoryClient();
        client.failGet = true;
        final int cap = 5;

        try (DistributedCache<String, String> cache = new DistributedCache<>(client, "", cap, 0)) {
            final int threadCount = 16;
            final ExecutorService pool = Executors.newFixedThreadPool(threadCount);
            final CountDownLatch start = new CountDownLatch(1);
            final List<Future<?>> futures = new ArrayList<>();

            for (int t = 0; t < threadCount; t++) {
                futures.add(pool.submit(() -> {
                    start.await();
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

            final Field field = DistributedCache.class.getDeclaredField("failedCounter");
            field.setAccessible(true);
            final AtomicInteger counter = (AtomicInteger) field.get(cache);

            assertEquals(cap, counter.get(), "failure counter must be capped at maxFailedNumForRetry under concurrency");
        }
    }

    @Test
    public void testCircuitBreaker_ClosesOnSuccess() {
        final InMemoryClient client = new InMemoryClient();
        client.failGet = true;
        try (DistributedCache<String, String> cache = new DistributedCache<>(client, "", 1, 100)) {
            assertNull(cache.getOrNull("k"));
            // Recover and store a value.
            client.failGet = false;
            client.store.put(cache.generateKey("k"), "ok");
            // Wait long enough so the retry window has elapsed.
            try {
                Thread.sleep(150);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            assertEquals("ok", cache.getOrNull("k"));
        }
    }
}
