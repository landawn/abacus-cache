/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.pool.KeyedObjectPool;
import com.landawn.abacus.pool.PoolStats;
import com.landawn.abacus.pool.PoolableAdapter;

class LocalCacheTest {

    // Construction

    @Test
    void constructorInvalidCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new LocalCache<String, String>(0, 0));
        assertThrows(IllegalArgumentException.class, () -> new LocalCache<String, String>(-1, 0));
    }

    @Test
    void constructorNegativeEvictDelay() {
        assertThrows(IllegalArgumentException.class, () -> new LocalCache<String, String>(100, -1));
    }

    @Test
    void constructorNullPool() {
        assertThrows(IllegalArgumentException.class, () -> new LocalCache<String, String>(0L, 0L, null));
    }

    @Test
    void constructorWithCustomPool() {
        final com.landawn.abacus.pool.KeyedObjectPool<String, com.landawn.abacus.pool.PoolableAdapter<String>> pool = com.landawn.abacus.pool.PoolFactory
                .createKeyedObjectPool(50, 0);
        final LocalCache<String, String> cache = new LocalCache<>(60_000L, 30_000L, pool);
        try {
            assertTrue(cache.put("k", "v"));
            assertEquals("v", cache.getOrNull("k"));
        } finally {
            cache.close();
        }
    }

    // Reads

    /**
     * Regression coverage for the asymmetric null-key handling defect.
     *
     * <p>Before the fix {@link LocalCache#put(Object, Object, long, long)} threw
     * {@code IllegalArgumentException("Key cannot be null")} on a null key, but the read-side
     * methods ({@link LocalCache#getOrNull(Object)},
     * {@link LocalCache#remove(Object)}, and {@link LocalCache#containsKey(Object)})
     * delegated straight to the pool, where a null key triggered a raw NPE from the underlying
     * pool. Callers therefore got two different exception types depending on which method was
     * called. The fix harmonizes the contract: all four reject null keys with
     * {@code IllegalArgumentException} up front.
     */
    @Test
    void getOrNullNullKeyThrowsIllegalArgumentException() {
        final LocalCache<String, String> cache = new LocalCache<>(100, 0);
        try {
            assertThrows(IllegalArgumentException.class, () -> cache.getOrNull(null));
        } finally {
            cache.close();
        }
    }

    // Writes

    /**
     * Pins the dependency's overflow-safe expiration arithmetic: createdTime + Long.MAX_VALUE must
     * not wrap around and make a "never expires" entry look expired immediately.
     */
    @Test
    void putMaxValueTimesDoNotOverflowIntoExpiry() {
        final LocalCache<String, String> cache = new LocalCache<>(100, 0);
        try {
            assertTrue(cache.put("k", "v", Long.MAX_VALUE, Long.MAX_VALUE));
            assertTrue(cache.containsKey("k"));
            assertEquals("v", cache.getOrNull("k"));
            assertEquals(0L, cache.stats().evictionCount());
        } finally {
            cache.close();
        }
    }

    /** A non-positive liveTime disables only the TTL: a positive maxIdleTime must still expire the entry. */
    @Test
    void putNonPositiveLiveTimeIdleTimeoutStillApplies() throws Exception {
        final LocalCache<String, String> cache = new LocalCache<>(100, 0);
        try {
            assertTrue(cache.put("k", "v", 0, 50));

            com.landawn.abacus.TestSupport.awaitCondition("idle entry to expire", () -> !cache.containsKey("k"));

            assertNull(cache.getOrNull("k"));
            assertEquals(0, cache.size());
        } finally {
            cache.close();
        }
    }

    @Test
    void putNullKey() {
        final LocalCache<String, String> cache = new LocalCache<>(100, 0);
        try {
            assertThrows(IllegalArgumentException.class, () -> cache.put(null, "v", 1000, 1000));
        } finally {
            cache.close();
        }
    }

    /** Null values are rejected up-front with IAE, as documented (settled validation policy). */
    @Test
    void putNullValue() {
        final LocalCache<String, String> cache = new LocalCache<>(100, 0);
        try {
            assertThrows(IllegalArgumentException.class, () -> cache.put("k", null));
            assertThrows(IllegalArgumentException.class, () -> cache.put("k", null, 1000, 1000));
        } finally {
            cache.close();
        }
    }

    /**
     * Pins the documented replacement semantics against the default pool: replacing an existing key
     * in a full cache detaches the old mapping first, so the replacement is admitted without a
     * capacity rejection and without evicting an unrelated entry.
     */
    @Test
    void putReplaceExistingKeyAtFullCapacitySucceedsWithoutEvictingOthers() {
        final LocalCache<String, String> cache = new LocalCache<>(2, 0);
        try {
            assertTrue(cache.put("a", "1"));
            assertTrue(cache.put("b", "2"));

            assertTrue(cache.put("a", "1b"));

            assertEquals(2, cache.size());
            assertEquals("1b", cache.getOrNull("a"));
            assertEquals("2", cache.getOrNull("b"));
            assertEquals(0L, cache.stats().evictionCount());
            assertEquals(3L, cache.stats().putCount());
        } finally {
            cache.close();
        }
    }

    @Test
    void putWithNegativeTimesDoesNotThrow() {
        final LocalCache<String, String> cache = new LocalCache<>(100, 0);
        try {
            final boolean stored = cache.put("k", "v", -1, -1);

            assertTrue(stored);
            assertEquals("v", cache.getOrNull("k"));
        } finally {
            cache.close();
        }
    }

    @Test
    void putWithPositiveTimesStillWorks() {
        final LocalCache<String, String> cache = new LocalCache<>(100, 0);
        try {
            final boolean stored = cache.put("user:123", "John", 3600000, 1800000);

            assertTrue(stored);
            assertEquals("John", cache.getOrNull("user:123"));
        } finally {
            cache.close();
        }
    }

    /**
     * Regression test for the LocalCache.put(key, value, liveTime, maxIdleTime) bug.
     *
     * <p>The {@code Cache}/{@code LocalCache} contract explicitly documents that a {@code liveTime}
     * or {@code maxIdleTime} of {@code 0} or negative means "no expiration" (see the Javadoc and
     * the example {@code cache.put("config:app", configData, 0, 0)}).
     *
     * <p>Before the fix, {@code put} passed the raw values straight to
     * {@code Poolable.wrap(value, liveTime, maxIdleTime)}, whose underlying {@code ActivityPrint}
     * throws {@code IllegalArgumentException} when either value is not strictly positive. So the
     * documented "permanent entry" use case threw instead of storing the entry.
     */
    @Test
    void putWithZeroLiveTimeAndIdleTimeDoesNotThrow() {
        final LocalCache<String, String> cache = new LocalCache<>(100, 0);
        try {
            // Documented "permanent entry" usage - must NOT throw and must store the value.
            final boolean stored = cache.put("config:app", "configData", 0, 0);

            assertTrue(stored);
            assertEquals("configData", cache.getOrNull("config:app"));
            assertTrue(cache.containsKey("config:app"));
        } finally {
            cache.close();
        }
    }

    @Test
    void putWithZeroLiveTimeButPositiveIdleTime() {
        final LocalCache<String, String> cache = new LocalCache<>(100, 0);
        try {
            // No TTL, but a 5s idle timeout - must not throw.
            final boolean stored = cache.put("temp:data", "data", 0, 5000);

            assertTrue(stored);
            assertEquals("data", cache.getOrNull("temp:data"));
        } finally {
            cache.close();
        }
    }

    // Removals

    @Test
    void clearRemovesEntries() {
        final LocalCache<String, String> cache = new LocalCache<>(100, 0);
        try {
            cache.put("a", "1");
            cache.put("b", "2");
            cache.clear();
            assertEquals(0, cache.size());
        } finally {
            cache.close();
        }
    }

    @Test
    void defaultPutAndRemove() {
        final LocalCache<String, String> cache = new LocalCache<>(100, 0);
        try {
            assertTrue(cache.put("a", "1"));
            assertEquals("1", cache.getOrNull("a"));

            cache.remove("a");
            assertNull(cache.getOrNull("a"));
            assertEquals(0, cache.size());
        } finally {
            cache.close();
        }
    }

    @Test
    void removeNullKeyThrowsIllegalArgumentException() {
        final LocalCache<String, String> cache = new LocalCache<>(100, 0);
        try {
            assertThrows(IllegalArgumentException.class, () -> cache.remove(null));
        } finally {
            cache.close();
        }
    }

    // Membership and views

    @Test
    void containsKeyNullKeyThrowsIllegalArgumentException() {
        final LocalCache<String, String> cache = new LocalCache<>(100, 0);
        try {
            assertThrows(IllegalArgumentException.class, () -> cache.containsKey(null));
        } finally {
            cache.close();
        }
    }

    @Test
    void containsKeyReportsPresence() {
        final LocalCache<String, String> cache = new LocalCache<>(100, 0);
        try {
            cache.put("a", "1");
            assertTrue(cache.containsKey("a"));
            assertFalse(cache.containsKey("missing"));
        } finally {
            cache.close();
        }
    }

    @Test
    void containsKeyReturnsFalseForExpiredEntry() throws Exception {
        final LocalCache<String, String> cache = new LocalCache<>(100, 0);
        try {
            assertTrue(cache.put("a", "1", 5, 0));

            com.landawn.abacus.TestSupport.awaitCondition("TTL entry to expire", () -> !cache.containsKey("a"));

            assertFalse(cache.containsKey("a"));
            assertNull(cache.getOrNull("a"));
        } finally {
            cache.close();
        }
    }

    @Test
    void keySetAndSize() {
        final LocalCache<String, String> cache = new LocalCache<>(100, 0);
        try {
            cache.put("a", "1");
            cache.put("b", "2");
            assertEquals(2, cache.size());
            final Set<String> keys = cache.keySet();
            assertEquals(2, keys.size());
            assertTrue(keys.contains("a"));
            assertTrue(keys.contains("b"));
        } finally {
            cache.close();
        }
    }

    // Statistics

    /**
     * Regression test for the stats() crash on drifted memory accounting.
     *
     * <p>A custom or otherwise non-standard pool can report drifted memory accounting. Current
     * {@link PoolStats} constructors validate these fields, so a mock is used to exercise the
     * adapter's defensive normalization directly. Before the fix, {@code LocalCache.stats()}
     * forwarded the raw negative values into {@code CacheStats}, whose compact constructor threw
     * {@code IllegalArgumentException} — so a pure monitoring call crashed. The fix clamps
     * {@code maxMemory}/{@code dataSize} to the {@code -1} "not tracked" sentinel.
     */
    @Test
    @SuppressWarnings("unchecked")
    void statsNegativePoolMemoryAccountingClampedInsteadOfThrowing() {
        final KeyedObjectPool<String, PoolableAdapter<String>> pool = mock(KeyedObjectPool.class);
        final PoolStats poolStats = mock(PoolStats.class);
        when(poolStats.capacity()).thenReturn(100);
        when(poolStats.size()).thenReturn(1);
        when(poolStats.putCount()).thenReturn(2L);
        when(poolStats.getCount()).thenReturn(3L);
        when(poolStats.hitCount()).thenReturn(2L);
        when(poolStats.missCount()).thenReturn(1L);
        when(poolStats.evictionCount()).thenReturn(0L);
        when(poolStats.maxMemory()).thenReturn(-7L);
        when(poolStats.dataSize()).thenReturn(-5L);
        when(pool.stats()).thenReturn(poolStats);

        final LocalCache<String, String> cache = new LocalCache<>(60_000L, 30_000L, pool);
        try {
            final CacheStats stats = assertDoesNotThrow(cache::stats);
            assertEquals(100, stats.capacity());
            assertEquals(-1, stats.maxMemory());
            assertEquals(-1, stats.dataSize());
        } finally {
            cache.close();
        }
    }

    @Test
    void statsReportsCounters() {
        final LocalCache<String, String> cache = new LocalCache<>(100, 0);
        try {
            cache.put("a", "1");
            cache.getOrNull("a");
            cache.getOrNull("missing");
            final com.landawn.abacus.cache.CacheStats stats = cache.stats();
            assertNotNull(stats);
            assertEquals(100, stats.capacity());
            assertEquals(1, stats.size());
            assertEquals(1, stats.putCount());
            assertEquals(2, stats.getCount());
            assertEquals(1, stats.hitCount());
            assertEquals(1, stats.missCount());
        } finally {
            cache.close();
        }
    }

    // Lifecycle

    @Test
    void isClosedReflectsLifecycle() {
        final LocalCache<String, String> cache = new LocalCache<>(100, 0);
        assertFalse(cache.isClosed());
        cache.close();
        assertTrue(cache.isClosed());
    }

    /** The state check runs before argument validation: a closed cache reports ISE even for a null key/value. */
    @Test
    void keyOperationsAfterCloseNullArgsThrowIllegalStateExceptionFirst() {
        final LocalCache<String, String> cache = new LocalCache<>(100, 0);
        cache.close();

        assertThrows(IllegalStateException.class, () -> cache.getOrNull(null));
        assertThrows(IllegalStateException.class, () -> cache.put(null, null, 1000, 1000));
        assertThrows(IllegalStateException.class, () -> cache.remove(null));
        assertThrows(IllegalStateException.class, () -> cache.containsKey(null));
    }

    /**
     * Every data operation documents {@code @throws IllegalStateException} after close. Key-based
     * operations check the state in LocalCache itself; keySet/size/clear/stats rely on the backing
     * pool, so pin both paths here against dependency changes.
     */
    @Test
    void operationsAfterCloseThrowIllegalStateException() {
        final LocalCache<String, String> cache = new LocalCache<>(100, 0);
        cache.put("k", "v");
        cache.close();

        assertThrows(IllegalStateException.class, () -> cache.getOrNull("k"));
        assertThrows(IllegalStateException.class, () -> cache.put("k", "v2"));
        assertThrows(IllegalStateException.class, () -> cache.remove("k"));
        assertThrows(IllegalStateException.class, () -> cache.containsKey("k"));
        assertThrows(IllegalStateException.class, cache::keySet);
        assertThrows(IllegalStateException.class, cache::size);
        assertThrows(IllegalStateException.class, cache::clear);
        assertThrows(IllegalStateException.class, cache::stats);
    }

}
