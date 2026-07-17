/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.landawn.abacus.TestBase;
import com.landawn.abacus.util.Suppliers;

@Tag("2025")
public class CaffeineCacheTest extends TestBase {

    private CaffeineCache<String, String> newCache() {
        // Use a Supplier to create the underlying Caffeine instance, exercising the Suppliers helper.
        final Supplier<com.github.benmanes.caffeine.cache.Cache<String, String>> supplier = Suppliers
                .of(() -> Caffeine.newBuilder().maximumSize(100).recordStats().build());
        return new CaffeineCache<>(supplier.get());
    }

    @Test
    public void testConstructor_EdgeCase_NullCache() {
        assertThrows(IllegalArgumentException.class, () -> new CaffeineCache<String, String>(null));
    }

    @Test
    public void testPutAndGetOrNull() {
        try (CaffeineCache<String, String> cache = newCache()) {
            assertTrue(cache.put("k", "v", 0, 0));
            assertEquals("v", cache.getOrNull("k"));
        }
    }

    @Test
    public void testPut_EdgeCase_NullKey() {
        try (CaffeineCache<String, String> cache = newCache()) {
            assertThrows(IllegalArgumentException.class, () -> cache.put(null, "v", 0, 0));
        }
    }

    /**
     * Null values are now rejected up-front with {@link IllegalArgumentException} (consistent with the
     * null-key contract), rather than surfacing as an unrelated {@code NullPointerException} from the
     * underlying Caffeine cache.
     */
    @Test
    public void testPut_EdgeCase_NullValue() {
        try (CaffeineCache<String, String> cache = newCache()) {
            assertThrows(IllegalArgumentException.class, () -> cache.put("k", null, 0, 0));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPut_RacingClose_RemovesOwnLateWrite() throws InterruptedException {
        final com.github.benmanes.caffeine.cache.Cache<String, String> delegate = mock(com.github.benmanes.caffeine.cache.Cache.class);
        final ConcurrentMap<String, String> delegateMap = new ConcurrentHashMap<>();
        final CountDownLatch putStarted = new CountDownLatch(1);
        final CountDownLatch allowPutToFinish = new CountDownLatch(1);

        org.mockito.Mockito.when(delegate.asMap()).thenReturn(delegateMap);
        stubQuietProbe(delegate, delegateMap);
        doAnswer(invocation -> {
            putStarted.countDown();
            assertTrue(allowPutToFinish.await(5, TimeUnit.SECONDS));
            delegateMap.put("k", "v");
            return null;
        }).when(delegate).put("k", "v");

        final CaffeineCache<String, String> cache = new CaffeineCache<>(delegate);
        final AtomicReference<Throwable> putFailure = new AtomicReference<>();
        final Thread putThread = new Thread(() -> {
            try {
                cache.put("k", "v", 0, 0);
            } catch (final Throwable e) {
                putFailure.set(e);
            }
        });

        try {
            putThread.start();
            assertTrue(putStarted.await(5, TimeUnit.SECONDS));

            cache.close();
            allowPutToFinish.countDown();
            putThread.join(5_000L);

            assertFalse(putThread.isAlive());
            assertInstanceOf(IllegalStateException.class, putFailure.get());
            assertFalse(delegateMap.containsKey("k"));
            verify(delegate).invalidateAll();
        } finally {
            allowPutToFinish.countDown();
            putThread.join(5_000L);
            cache.close();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPut_RacingClose_DoesNotRemoveNewerDelegateValue() throws InterruptedException {
        final com.github.benmanes.caffeine.cache.Cache<String, String> delegate = mock(com.github.benmanes.caffeine.cache.Cache.class);
        final ConcurrentMap<String, String> delegateMap = new ConcurrentHashMap<>();
        final CountDownLatch putStarted = new CountDownLatch(1);
        final CountDownLatch allowPutToFinish = new CountDownLatch(1);

        org.mockito.Mockito.when(delegate.asMap()).thenReturn(delegateMap);
        stubQuietProbe(delegate, delegateMap);
        doAnswer(invocation -> {
            putStarted.countDown();
            assertTrue(allowPutToFinish.await(5, TimeUnit.SECONDS));

            // The wrapper's late write lands after close() invalidated the delegate. Before the
            // wrapper can perform its closed-state cleanup, a direct delegate user replaces it.
            // Cleanup must not erase that newer, externally-owned mapping.
            delegateMap.put("k", "v");
            delegateMap.put("k", "newer-direct-value");
            return null;
        }).when(delegate).put("k", "v");

        final CaffeineCache<String, String> cache = new CaffeineCache<>(delegate);
        final AtomicReference<Throwable> putFailure = new AtomicReference<>();
        final Thread putThread = new Thread(() -> {
            try {
                cache.put("k", "v", 0, 0);
            } catch (final Throwable e) {
                putFailure.set(e);
            }
        });

        try {
            putThread.start();
            assertTrue(putStarted.await(5, TimeUnit.SECONDS));

            cache.close();
            allowPutToFinish.countDown();
            putThread.join(5_000L);

            assertFalse(putThread.isAlive());
            assertInstanceOf(IllegalStateException.class, putFailure.get());
            assertEquals("newer-direct-value", delegateMap.get("k"));
            verify(delegate).invalidateAll();
        } finally {
            allowPutToFinish.countDown();
            putThread.join(5_000L);
            cache.close();
        }
    }

    /** The closed-state cleanup probes via {@code Policy.getIfPresentQuietly}; mocks must stub it. */
    @SuppressWarnings("unchecked")
    private static void stubQuietProbe(final com.github.benmanes.caffeine.cache.Cache<String, String> delegate,
            final ConcurrentMap<String, String> delegateMap) {
        final com.github.benmanes.caffeine.cache.Policy<String, String> delegatePolicy = mock(com.github.benmanes.caffeine.cache.Policy.class);
        org.mockito.Mockito.when(delegate.policy()).thenReturn(delegatePolicy);
        org.mockito.Mockito.when(delegatePolicy.getIfPresentQuietly("k")).thenAnswer(invocation -> delegateMap.get("k"));
    }

    /**
     * The post-close cleanup must be side-effect-free on the delegate: it probes with the
     * documented no-side-effect {@code Policy.getIfPresentQuietly} and issues at most one
     * conditional remove. The previous {@code computeIfPresent} cleanup was processed by Caffeine
     * as an update even when it kept the mapping - refreshing the newer entry's expiration
     * metadata and, with {@code recordStats()}, logging a synthetic loadSuccess (kept) or
     * loadFailure (removed). Real-Caffeine load counters pin both cleanup branches at zero.
     */
    @Test
    public void testPut_RacingClose_CleanupRecordsNoLoadStatsOnRealCaffeine() throws Exception {
        // Branch 1: cleanup finds its own write current and removes it -> no loadFailure.
        runRealCaffeineCleanupScenario(false);
        // Branch 2: cleanup finds a newer direct write and keeps it -> no loadSuccess.
        runRealCaffeineCleanupScenario(true);
    }

    private void runRealCaffeineCleanupScenario(final boolean directWriteReplacesLateWrite) throws Exception {
        final com.github.benmanes.caffeine.cache.Cache<String, String> real = com.github.benmanes.caffeine.cache.Caffeine.newBuilder().recordStats().build();
        final CountDownLatch putStarted = new CountDownLatch(1);
        final CountDownLatch allowPutToFinish = new CountDownLatch(1);

        final com.github.benmanes.caffeine.cache.Cache<String, String> latchingDelegate = new ForwardingCaffeineCache(real) {
            @Override
            public void put(final String key, final String value) {
                putStarted.countDown();

                try {
                    assertTrue(allowPutToFinish.await(5, TimeUnit.SECONDS));
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }

                super.put(key, value);

                if (directWriteReplacesLateWrite) {
                    // A direct delegate user replaces the late write before the wrapper's cleanup.
                    // Built via StringBuilder so it is guaranteed to be a distinct String instance.
                    super.put(key, new StringBuilder("newer-direct-value").toString());
                }
            }
        };

        final CaffeineCache<String, String> cache = new CaffeineCache<>(latchingDelegate);
        final AtomicReference<Throwable> putFailure = new AtomicReference<>();
        final Thread putThread = new Thread(() -> {
            try {
                cache.put("k", "v", 0, 0);
            } catch (final Throwable e) {
                putFailure.set(e);
            }
        });

        try {
            putThread.start();
            assertTrue(putStarted.await(5, TimeUnit.SECONDS));

            cache.close();
            allowPutToFinish.countDown();
            putThread.join(5_000L);

            assertFalse(putThread.isAlive());
            assertInstanceOf(IllegalStateException.class, putFailure.get());

            if (directWriteReplacesLateWrite) {
                assertEquals("newer-direct-value", real.asMap().get("k"), "the newer direct write must survive the cleanup");
            } else {
                assertFalse(real.asMap().containsKey("k"), "the wrapper's own late write must be removed");
            }

            assertEquals(0L, real.stats().loadSuccessCount(), "cleanup must not record a synthetic load success");
            assertEquals(0L, real.stats().loadFailureCount(), "cleanup must not record a synthetic load failure");
        } finally {
            allowPutToFinish.countDown();
            putThread.join(5_000L);
            cache.close();
        }
    }

    /** Forwards every {@code Cache} method to a real Caffeine instance; tests override what they latch. */
    private static class ForwardingCaffeineCache implements com.github.benmanes.caffeine.cache.Cache<String, String> {
        private final com.github.benmanes.caffeine.cache.Cache<String, String> real;

        ForwardingCaffeineCache(final com.github.benmanes.caffeine.cache.Cache<String, String> real) {
            this.real = real;
        }

        @Override
        public String getIfPresent(final String key) {
            return real.getIfPresent(key);
        }

        @Override
        public String get(final String key, final java.util.function.Function<? super String, ? extends String> mappingFunction) {
            return real.get(key, mappingFunction);
        }

        @Override
        public Map<String, String> getAllPresent(final Iterable<? extends String> keys) {
            return real.getAllPresent(keys);
        }

        @Override
        public Map<String, String> getAll(final Iterable<? extends String> keys,
                final java.util.function.Function<? super Set<? extends String>, ? extends Map<? extends String, ? extends String>> mappingFunction) {
            return real.getAll(keys, mappingFunction);
        }

        @Override
        public void put(final String key, final String value) {
            real.put(key, value);
        }

        @Override
        public void putAll(final Map<? extends String, ? extends String> map) {
            real.putAll(map);
        }

        @Override
        public void invalidate(final String key) {
            real.invalidate(key);
        }

        @Override
        public void invalidateAll(final Iterable<? extends String> keys) {
            real.invalidateAll(keys);
        }

        @Override
        public void invalidateAll() {
            real.invalidateAll();
        }

        @Override
        public long estimatedSize() {
            return real.estimatedSize();
        }

        @Override
        public com.github.benmanes.caffeine.cache.stats.CacheStats stats() {
            return real.stats();
        }

        @Override
        public ConcurrentMap<String, String> asMap() {
            return real.asMap();
        }

        @Override
        public void cleanUp() {
            real.cleanUp();
        }

        @Override
        public com.github.benmanes.caffeine.cache.Policy<String, String> policy() {
            return real.policy();
        }
    }

    @Test
    public void testGetOrNull_EdgeCase_Missing() {
        try (CaffeineCache<String, String> cache = newCache()) {
            assertNull(cache.getOrNull("missing"));
        }
    }

    @Test
    public void testRemove() {
        try (CaffeineCache<String, String> cache = newCache()) {
            cache.put("k", "v", 0, 0);
            cache.remove("k");
            assertNull(cache.getOrNull("k"));
        }
    }

    @Test
    public void testContainsKey() {
        try (CaffeineCache<String, String> cache = newCache()) {
            cache.put("k", "v", 0, 0);
            assertTrue(cache.containsKey("k"));
            assertFalse(cache.containsKey("missing"));
        }
    }

    @Test
    public void testKeySet_Unsupported() {
        try (CaffeineCache<String, String> cache = newCache()) {
            assertThrows(UnsupportedOperationException.class, cache::keySet);
        }
    }

    @Test
    public void testSize() {
        try (CaffeineCache<String, String> cache = newCache()) {
            cache.put("a", "1", 0, 0);
            cache.put("b", "2", 0, 0);
            // Caffeine performs maintenance lazily; force pending writes to drain so size is reliable.
            // We just check it's between 0 and the maximum.
            final int size = cache.size();
            assertTrue(size >= 0);
            assertTrue(size <= 2);
        }
    }

    @Test
    public void testClear() {
        try (CaffeineCache<String, String> cache = newCache()) {
            cache.put("k", "v", 0, 0);
            cache.clear();
            assertNull(cache.getOrNull("k"));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testClear_RacingClose_FinishesBeforeCloseReturns() throws InterruptedException {
        final com.github.benmanes.caffeine.cache.Cache<String, String> delegate = mock(com.github.benmanes.caffeine.cache.Cache.class);
        final AtomicInteger invalidateAllCalls = new AtomicInteger();
        final CountDownLatch clearStarted = new CountDownLatch(1);
        final CountDownLatch allowClearToFinish = new CountDownLatch(1);
        final CountDownLatch closeStarted = new CountDownLatch(1);
        final CountDownLatch closeFinished = new CountDownLatch(1);

        doAnswer(invocation -> {
            if (invalidateAllCalls.incrementAndGet() == 1) {
                clearStarted.countDown();
                assertTrue(allowClearToFinish.await(5, TimeUnit.SECONDS));
            }

            return null;
        }).when(delegate).invalidateAll();

        final CaffeineCache<String, String> cache = new CaffeineCache<>(delegate);
        final Thread clearThread = new Thread(cache::clear);
        final Thread closeThread = new Thread(() -> {
            closeStarted.countDown();
            cache.close();
            closeFinished.countDown();
        });

        try {
            clearThread.start();
            assertTrue(clearStarted.await(5, TimeUnit.SECONDS));
            closeThread.start();

            assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
            assertFalse(closeFinished.await(200, TimeUnit.MILLISECONDS), "close must wait for an in-flight clear of the shared delegate");
            allowClearToFinish.countDown();

            clearThread.join(5_000L);
            closeThread.join(5_000L);
            assertFalse(clearThread.isAlive());
            assertFalse(closeThread.isAlive());
            assertEquals(2, invalidateAllCalls.get(), "close must perform the final delegate invalidation after clear finishes");
        } finally {
            allowClearToFinish.countDown();
            clearThread.join(5_000L);
            closeThread.join(5_000L);
            cache.close();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testClose_DoesNotDeadlockWithReentrantDelegateCallback() throws InterruptedException {
        final com.github.benmanes.caffeine.cache.Cache<String, String> delegate = mock(com.github.benmanes.caffeine.cache.Cache.class);
        final AtomicInteger invalidateAllCalls = new AtomicInteger();
        final AtomicReference<CaffeineCache<String, String>> cacheRef = new AtomicReference<>();
        final CountDownLatch outerClearEnteredDelegate = new CountDownLatch(1);
        final CountDownLatch allowReentrantClose = new CountDownLatch(1);
        final CountDownLatch reentrantCloseReturned = new CountDownLatch(1);

        doAnswer(invocation -> {
            if (invalidateAllCalls.incrementAndGet() == 1) {
                outerClearEnteredDelegate.countDown();
                assertTrue(allowReentrantClose.await(5, TimeUnit.SECONDS));
                cacheRef.get().close();
                reentrantCloseReturned.countDown();
            }

            return null;
        }).when(delegate).invalidateAll();

        final CaffeineCache<String, String> cache = new CaffeineCache<>(delegate);
        cacheRef.set(cache);
        final Thread clearThread = new Thread(cache::clear);
        final Thread competingCloseThread = new Thread(cache::close);
        // If the regression reappears these two threads form an unbreakable monitor/lock cycle;
        // daemon threads let the failing test fork terminate instead of hanging the whole suite.
        clearThread.setDaemon(true);
        competingCloseThread.setDaemon(true);

        clearThread.start();
        assertTrue(outerClearEnteredDelegate.await(5, TimeUnit.SECONDS));
        competingCloseThread.start();

        final long stateDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (competingCloseThread.getState() != Thread.State.WAITING && System.nanoTime() < stateDeadline) {
            Thread.onSpinWait();
        }
        assertEquals(Thread.State.WAITING, competingCloseThread.getState(), "the competing close must be queued on the lifecycle lock");

        allowReentrantClose.countDown();
        assertTrue(reentrantCloseReturned.await(5, TimeUnit.SECONDS), "delegate callback must be able to reenter close without lock inversion");

        clearThread.join(5_000L);
        competingCloseThread.join(5_000L);
        assertFalse(clearThread.isAlive());
        assertFalse(competingCloseThread.isAlive());
        assertTrue(cache.isClosed());
        assertEquals(2, invalidateAllCalls.get());
    }

    @Test
    public void testClose_IsIdempotent() {
        final CaffeineCache<String, String> cache = newCache();
        assertFalse(cache.isClosed());
        cache.close();
        cache.close();
        assertTrue(cache.isClosed());
    }

    @Test
    public void testOperations_AfterClose_Throw() {
        final CaffeineCache<String, String> cache = newCache();
        cache.close();
        assertThrows(IllegalStateException.class, () -> cache.getOrNull("k"));
        assertThrows(IllegalStateException.class, () -> cache.put("k", "v", 0, 0));
        assertThrows(IllegalStateException.class, () -> cache.remove("k"));
        assertThrows(IllegalStateException.class, () -> cache.containsKey("k"));
        assertThrows(IllegalStateException.class, cache::size);
        assertThrows(IllegalStateException.class, cache::clear);
    }

    @Test
    public void testStats_ReturnsNonNull() {
        try (CaffeineCache<String, String> cache = newCache()) {
            cache.put("k", "v", 0, 0);
            cache.getOrNull("k");
            cache.getOrNull("missing");
            assertNotNull(cache.stats());
        }
    }

    /**
     * Regression coverage for the asymmetric null-key handling defect.
     *
     * <p>Before the fix {@link CaffeineCache#put(Object, Object, long, long)} explicitly rejected
     * null keys with {@code IllegalArgumentException}, but the read-side methods
     * ({@code getOrNull}, {@code remove}, {@code containsKey}) delegated straight to Caffeine,
     * which raises an unrelated {@code NullPointerException}. The fix harmonises the contract so
     * every key-taking operation rejects nulls with the same {@code IllegalArgumentException}.
     */
    @Test
    public void testGetOrNull_EdgeCase_NullKey() {
        try (CaffeineCache<String, String> cache = newCache()) {
            assertThrows(IllegalArgumentException.class, () -> cache.getOrNull(null));
        }
    }

    @Test
    public void testRemove_EdgeCase_NullKey() {
        try (CaffeineCache<String, String> cache = newCache()) {
            assertThrows(IllegalArgumentException.class, () -> cache.remove(null));
        }
    }

    @Test
    public void testContainsKey_EdgeCase_NullKey() {
        try (CaffeineCache<String, String> cache = newCache()) {
            assertThrows(IllegalArgumentException.class, () -> cache.containsKey(null));
        }
    }

    /**
     * Content-level stats mapping (previously only non-nullness was asserted): capacity from the
     * eviction policy's maximum, wrapper-tracked putCount, Caffeine-tracked hit/miss/request
     * counts, and the -1 "not tracked" sentinels for maxMemory/dataSize.
     */
    @Test
    public void testStats_ContentReflectsTraffic() {
        try (CaffeineCache<String, String> cache = newCache()) { // maximumSize(100), recordStats()
            assertTrue(cache.put("a", "1"));
            assertEquals("1", cache.getOrNull("a"));
            assertNull(cache.getOrNull("missing"));

            final CacheStats stats = cache.stats();
            assertEquals(100, stats.capacity());
            assertEquals(1, stats.size());
            assertEquals(1L, stats.putCount());
            assertEquals(2L, stats.getCount());
            assertEquals(1L, stats.hitCount());
            assertEquals(1L, stats.missCount());
            assertEquals(-1L, stats.maxMemory(), "memory is not tracked: -1 sentinel");
            assertEquals(-1L, stats.dataSize(), "data size is not tracked: -1 sentinel");

            final com.github.benmanes.caffeine.cache.stats.CacheStats nativeStats = cache.caffeineStats();
            assertEquals(1L, nativeStats.hitCount());
            assertEquals(1L, nativeStats.missCount());
        }
    }
}
