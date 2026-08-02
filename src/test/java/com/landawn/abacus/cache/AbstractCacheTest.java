/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.TestBase;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.Properties;
import com.landawn.abacus.util.u.Optional;

/**
 * Covers the {@link AbstractCache} async wrappers and property helpers via the concrete
 * {@link LocalCache} subclass.
 */
@Tag("2025")
public class AbstractCacheTest extends TestBase {

    private LocalCache<String, String> newCache() {
        return new LocalCache<>(100, 0);
    }

    @Test
    public void testCacheContract_DeclaresCloseWithoutBeingAutoCloseable() throws NoSuchMethodException {
        assertFalse(AutoCloseable.class.isAssignableFrom(Cache.class));
        assertFalse(java.io.Closeable.class.isAssignableFrom(Cache.class));
        assertEquals(void.class, Cache.class.getDeclaredMethod("close").getReturnType());
    }

    @Test
    public void testGet() {
        final LocalCache<String, String> cache = newCache();
        try {
            cache.put("k", "v");
            final Optional<String> got = cache.get("k");
            assertTrue(got.isPresent());
            assertEquals("v", got.get());
        } finally {
            cache.close();
        }
    }

    @Test
    public void testGet_EdgeCase_Missing() {
        final LocalCache<String, String> cache = newCache();
        try {
            assertFalse(cache.get("missing").isPresent());
        } finally {
            cache.close();
        }
    }

    @Test
    public void testPutTwoArg_UsesDefaults() {
        final LocalCache<String, String> cache = newCache();
        try {
            // Defaults should be applied; verify the entry is stored.
            assertTrue(cache.put("k", "v"));
            assertEquals("v", cache.getOrNull("k"));
        } finally {
            cache.close();
        }
    }

    // Async operations
    @Test
    public void testAsyncGet() throws Exception {
        final LocalCache<String, String> cache = newCache();
        try {
            cache.put("k", "v");
            final ContinuableFuture<Optional<String>> f = cache.asyncGet("k");
            final Optional<String> opt = f.get();
            assertTrue(opt.isPresent());
            assertEquals("v", opt.get());
        } finally {
            cache.close();
        }
    }

    @Test
    public void testAsyncGetOrNull() throws Exception {
        final LocalCache<String, String> cache = newCache();
        try {
            cache.put("k", "v");
            assertEquals("v", cache.asyncGetOrNull("k").get());
        } finally {
            cache.close();
        }
    }

    @Test
    public void testAsyncGetOrNull_EdgeCase_Missing() throws Exception {
        final LocalCache<String, String> cache = newCache();
        try {
            assertNull(cache.asyncGetOrNull("none").get());
        } finally {
            cache.close();
        }
    }

    @Test
    public void testAsyncPut() throws Exception {
        final LocalCache<String, String> cache = newCache();
        try {
            assertTrue(cache.asyncPut("k", "v").get());
            assertEquals("v", cache.getOrNull("k"));
        } finally {
            cache.close();
        }
    }

    @Test
    public void testAsyncPutWithTimes() throws Exception {
        final LocalCache<String, String> cache = newCache();
        try {
            assertTrue(cache.asyncPut("k", "v", 5000, 5000).get());
            assertEquals("v", cache.getOrNull("k"));
        } finally {
            cache.close();
        }
    }

    @Test
    public void testAsyncRemove() throws Exception {
        final LocalCache<String, String> cache = newCache();
        try {
            cache.put("k", "v");
            assertNull(cache.asyncRemove("k").get());
            assertNull(cache.getOrNull("k"));
        } finally {
            cache.close();
        }
    }

    @Test
    public void testAsyncContainsKey() throws Exception {
        final LocalCache<String, String> cache = newCache();
        try {
            cache.put("k", "v");
            assertTrue(cache.asyncContainsKey("k").get());
            assertFalse(cache.asyncContainsKey("missing").get());
        } finally {
            cache.close();
        }
    }

    // Properties bag
    @Test
    public void testGetProperties() {
        final LocalCache<String, String> cache = newCache();
        try {
            final Properties<String, Object> props = cache.getProperties();
            assertNotNull(props);
            // Returns the same instance on subsequent calls.
            assertTrue(props == cache.getProperties());
        } finally {
            cache.close();
        }
    }

    @Test
    public void testProperties_FunctionalArguments_EdgeCase_NullRejected() {
        final LocalCache<String, String> cache = newCache();
        try {
            final Properties<String, Object> props = cache.getProperties();

            assertThrows(IllegalArgumentException.class, () -> props.forEach(null));
            assertThrows(IllegalArgumentException.class, () -> props.replaceAll(null));
            assertThrows(IllegalArgumentException.class, () -> props.computeIfAbsent("key", null));
            assertThrows(IllegalArgumentException.class, () -> props.computeIfPresent("key", null));
            assertThrows(IllegalArgumentException.class, () -> props.compute("key", null));
            assertThrows(IllegalArgumentException.class, () -> props.merge("key", "value", null));
        } finally {
            cache.close();
        }
    }

    @Test
    public void testSetAndGetProperty() {
        final LocalCache<String, String> cache = newCache();
        try {
            assertNull(cache.setProperty("name", "alpha"));
            final String name = cache.getProperty("name");
            assertEquals("alpha", name);
        } finally {
            cache.close();
        }
    }

    @Test
    public void testSetProperty_ReturnsPreviousValue() {
        final LocalCache<String, String> cache = newCache();
        try {
            cache.setProperty("name", "v1");
            final String prev = cache.setProperty("name", "v2");
            assertEquals("v1", prev);
            assertEquals("v2", cache.getProperty("name"));
        } finally {
            cache.close();
        }
    }

    @Test
    public void testRemoveProperty() {
        final LocalCache<String, String> cache = newCache();
        try {
            cache.setProperty("foo", "bar");
            final String removed = cache.removeProperty("foo");
            assertEquals("bar", removed);
            assertNull(cache.getProperty("foo"));
        } finally {
            cache.close();
        }
    }

    @Test
    public void testRemoveProperty_EdgeCase_Missing() {
        final LocalCache<String, String> cache = newCache();
        try {
            assertNull(cache.removeProperty("never-set"));
        } finally {
            cache.close();
        }
    }

    @Test
    public void testProperties_DontAffectCacheEntries() {
        final LocalCache<String, String> cache = newCache();
        try {
            cache.put("a", "1");
            cache.setProperty("a", "this-is-a-property");
            // Cache entries and properties live in separate spaces.
            assertEquals("1", cache.getOrNull("a"));
            assertEquals("this-is-a-property", cache.getProperty("a"));
            assertDoesNotThrow(() -> cache.removeProperty("a"));
            assertEquals("1", cache.getOrNull("a"));
        } finally {
            cache.close();
        }
    }

    /**
     * Property mutators are lifecycle-guarded like the data operations: mutating the property bag
     * of a closed cache is a lifecycle bug and fails fast with {@link IllegalStateException}.
     * Property reads remain usable after close, like other configuration accessors.
     */
    @Test
    public void testPropertyMutators_EdgeCase_AfterClose_throwISE_whileReadsStayUsable() {
        final LocalCache<String, String> cache = newCache();
        cache.setProperty("region", "us-east");
        cache.close();

        assertThrows(IllegalStateException.class, () -> cache.setProperty("region", "eu-west"));
        assertThrows(IllegalStateException.class, () -> cache.removeProperty("region"));

        // Reads are configuration accessors and stay usable; the failed mutations changed nothing.
        assertEquals("us-east", cache.getProperty("region"));
        assertEquals("us-east", cache.getProperties().get("region"));
    }

    // --- Fix: shared async executor must use daemon threads -------------------------------------
    // Regression test: the shared executor previously created non-daemon core threads that never
    // timed out, so a JVM that had used any async cache operation could not exit normally.

    @Test
    public void testAsyncOperations_RunOnDaemonThreads() throws Exception {
        final Boolean daemon = AbstractCache.asyncExecutor.execute(() -> Thread.currentThread().isDaemon()).get();
        assertTrue(daemon, "async cache tasks must run on daemon threads");
    }

    // --- Fix: property bag must be safe for concurrent mutation ---------------------------------
    // Regression test: the property bag was previously backed by an unsynchronized LinkedHashMap,
    // so concurrent setProperty calls could lose updates or corrupt the map.

    @Test
    public void testProperties_ConcurrentMutation_IsThreadSafe() throws Exception {
        final LocalCache<String, String> cache = newCache();
        try {
            final int threads = 8;
            final int perThread = 250;
            final ExecutorService pool = Executors.newFixedThreadPool(threads);
            try {
                final CountDownLatch start = new CountDownLatch(1);
                final List<Future<?>> futures = new ArrayList<>();
                for (int t = 0; t < threads; t++) {
                    final int id = t;
                    futures.add(pool.submit(() -> {
                        start.await();
                        for (int i = 0; i < perThread; i++) {
                            cache.setProperty("p-" + id + "-" + i, id * perThread + i);
                        }
                        return null;
                    }));
                }
                start.countDown();
                for (final Future<?> f : futures) {
                    f.get(30, TimeUnit.SECONDS);
                }
            } finally {
                pool.shutdownNow();
            }

            assertEquals(threads * perThread, cache.getProperties().size());
            for (int t = 0; t < threads; t++) {
                for (int i = 0; i < perThread; i++) {
                    final Integer value = cache.getProperty("p-" + t + "-" + i);
                    assertNotNull(value);
                    assertEquals(t * perThread + i, value.intValue());
                }
            }
        } finally {
            cache.close();
        }
    }

    /**
     * Regression for the synchronized-wrapper gap: {@link Properties} implements
     * {@code computeIfAbsent} as separate {@code get}/{@code put} calls unless the cache's wrapper
     * delegates it to the synchronized backing map. With separate calls every worker can run the
     * mapping function for the same absent key; the atomic implementation runs it exactly once.
     */
    @Test
    public void testProperties_ComputeIfAbsent_IsAtomic() throws Exception {
        final LocalCache<String, String> cache = newCache();
        try {
            final int threads = 16;
            final ExecutorService pool = Executors.newFixedThreadPool(threads);
            final CountDownLatch ready = new CountDownLatch(threads);
            final CountDownLatch start = new CountDownLatch(1);
            final CountDownLatch firstMappingStarted = new CountDownLatch(1);
            final CountDownLatch releaseMapping = new CountDownLatch(1);
            final AtomicInteger mappingCalls = new AtomicInteger();
            final List<Future<String>> futures = new ArrayList<>();

            try {
                for (int i = 0; i < threads; i++) {
                    futures.add(pool.submit(() -> {
                        ready.countDown();
                        start.await();

                        return cache.getProperties().computeIfAbsent("shared", key -> {
                            mappingCalls.incrementAndGet();
                            firstMappingStarted.countDown();

                            try {
                                releaseMapping.await();
                            } catch (final InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException(e);
                            }

                            return "computed";
                        }).toString();
                    }));
                }

                assertTrue(ready.await(10, TimeUnit.SECONDS));
                start.countDown();
                assertTrue(firstMappingStarted.await(10, TimeUnit.SECONDS));

                // Give every already-started worker an opportunity to reach computeIfAbsent. In
                // the fixed implementation they block on one map mutex and cannot enter the mapper.
                Thread.sleep(100);
                releaseMapping.countDown();

                for (final Future<String> future : futures) {
                    assertEquals("computed", future.get(10, TimeUnit.SECONDS));
                }

                assertEquals(1, mappingCalls.get(), "the mapping function must run once for one absent key");
            } finally {
                releaseMapping.countDown();
                pool.shutdownNow();
            }
        } finally {
            cache.close();
        }
    }

    // --- Default put(K, V) delegation contract --------------------------------------------------
    // Verifies that AbstractCache.put(k, v) forwards the configured defaults to
    // put(k, v, liveTime, maxIdleTime) in the correct argument positions (no swap).

    @Test
    public void testPutTwoArg_DelegatesDefaultLiveAndIdleTimes_NoSwap() {
        final long liveTime = 111_000L;
        final long idleTime = 222_000L;
        final RecordingCache<String, String> cache = new RecordingCache<>(liveTime, idleTime);
        try {
            assertTrue(cache.put("k", "v"));
            assertEquals(liveTime, cache.lastLiveTime, "defaultLiveTime must be forwarded as liveTime");
            assertEquals(idleTime, cache.lastMaxIdleTime, "defaultMaxIdleTime must be forwarded as maxIdleTime");
        } finally {
            cache.close();
        }
    }

    @Test
    public void testPutTwoArg_UsesInterfaceDefaultsWhenConstructedWithoutArgs() {
        final RecordingCache<String, String> cache = new RecordingCache<>();
        try {
            assertTrue(cache.put("k", "v"));
            assertEquals(Cache.DEFAULT_LIVE_TIME, cache.lastLiveTime);
            assertEquals(Cache.DEFAULT_MAX_IDLE_TIME, cache.lastMaxIdleTime);
        } finally {
            cache.close();
        }
    }

    @Test
    public void testGet_WrapsGetOrNull() {
        final RecordingCache<String, String> cache = new RecordingCache<>();
        try {
            cache.put("k", "v");
            assertTrue(cache.get("k").isPresent());
            assertEquals("v", cache.get("k").get());
            assertFalse(cache.get("missing").isPresent());
        } finally {
            cache.close();
        }
    }

    /**
     * Minimal in-memory {@link AbstractCache} used to assert the base-class delegation logic
     * directly, without relying on {@link LocalCache} internals. It records the expiration
     * arguments seen by the four-arg {@link #put(Object, Object, long, long)}.
     */
    private static final class RecordingCache<K, V> extends AbstractCache<K, V> {
        private final Map<K, V> store = new HashMap<>();
        long lastLiveTime = Long.MIN_VALUE;
        long lastMaxIdleTime = Long.MIN_VALUE;
        private boolean closed = false;

        RecordingCache() {
            super();
        }

        RecordingCache(final long defaultLiveTime, final long defaultMaxIdleTime) {
            super(defaultLiveTime, defaultMaxIdleTime);
        }

        @Override
        public V getOrNull(final K key) {
            return store.get(key);
        }

        @Override
        public boolean put(final K key, final V value, final long liveTime, final long maxIdleTime) {
            lastLiveTime = liveTime;
            lastMaxIdleTime = maxIdleTime;
            store.put(key, value);
            return true;
        }

        @Override
        public void remove(final K key) {
            store.remove(key);
        }

        @Override
        public boolean containsKey(final K key) {
            return store.containsKey(key);
        }

        @Override
        public Set<K> keySet() {
            return store.keySet();
        }

        @Override
        public int size() {
            return store.size();
        }

        @Override
        public void clear() {
            store.clear();
        }

        @Override
        public void close() {
            closed = true;
            store.clear();
        }

        @Override
        public boolean isClosed() {
            return closed;
        }
    }
}
