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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
