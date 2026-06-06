/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.cache.LocalCache;

@Tag("2025")
public class LocalCacheTest {

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
    public void testPutWithZeroLiveTimeAndIdleTimeDoesNotThrow() {
        try (LocalCache<String, String> cache = new LocalCache<>(100, 0)) {
            // Documented "permanent entry" usage - must NOT throw and must store the value.
            final boolean stored = cache.put("config:app", "configData", 0, 0);

            assertTrue(stored);
            assertEquals("configData", cache.getOrNull("config:app"));
            assertTrue(cache.containsKey("config:app"));
        }
    }

    @Test
    public void testPutWithNegativeTimesDoesNotThrow() {
        try (LocalCache<String, String> cache = new LocalCache<>(100, 0)) {
            final boolean stored = cache.put("k", "v", -1, -1);

            assertTrue(stored);
            assertEquals("v", cache.getOrNull("k"));
        }
    }

    @Test
    public void testPutWithZeroLiveTimeButPositiveIdleTime() {
        try (LocalCache<String, String> cache = new LocalCache<>(100, 0)) {
            // No TTL, but a 5s idle timeout - must not throw.
            final boolean stored = cache.put("temp:data", "data", 0, 5000);

            assertTrue(stored);
            assertEquals("data", cache.getOrNull("temp:data"));
        }
    }

    @Test
    public void testPutWithPositiveTimesStillWorks() {
        try (LocalCache<String, String> cache = new LocalCache<>(100, 0)) {
            final boolean stored = cache.put("user:123", "John", 3600000, 1800000);

            assertTrue(stored);
            assertEquals("John", cache.getOrNull("user:123"));
        }
    }

    @Test
    public void testDefaultPutAndRemove() {
        try (LocalCache<String, String> cache = new LocalCache<>(100, 0)) {
            assertTrue(cache.put("a", "1"));
            assertEquals("1", cache.getOrNull("a"));

            cache.remove("a");
            assertNull(cache.getOrNull("a"));
            assertEquals(0, cache.size());
        }
    }

    @org.junit.jupiter.api.Test
    public void testConstructor_EdgeCase_InvalidCapacity() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> new LocalCache<String, String>(0, 0));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> new LocalCache<String, String>(-1, 0));
    }

    @org.junit.jupiter.api.Test
    public void testConstructor_EdgeCase_NegativeEvictDelay() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> new LocalCache<String, String>(100, -1));
    }

    @org.junit.jupiter.api.Test
    public void testConstructor_EdgeCase_NullPool() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> new LocalCache<String, String>(0L, 0L, null));
    }

    @org.junit.jupiter.api.Test
    public void testPut_EdgeCase_NullKey() {
        try (LocalCache<String, String> cache = new LocalCache<>(100, 0)) {
            org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> cache.put(null, "v", 1000, 1000));
        }
    }

    @org.junit.jupiter.api.Test
    public void testContainsKey() {
        try (LocalCache<String, String> cache = new LocalCache<>(100, 0)) {
            cache.put("a", "1");
            org.junit.jupiter.api.Assertions.assertTrue(cache.containsKey("a"));
            org.junit.jupiter.api.Assertions.assertFalse(cache.containsKey("missing"));
        }
    }

    @org.junit.jupiter.api.Test
    public void testContainsKey_returnsFalseForExpiredEntry() throws Exception {
        try (LocalCache<String, String> cache = new LocalCache<>(100, 0)) {
            assertTrue(cache.put("a", "1", 5, 0));

            Thread.sleep(25);

            assertFalse(cache.containsKey("a"));
            assertNull(cache.getOrNull("a"));
        }
    }

    @org.junit.jupiter.api.Test
    public void testKeySet_AndSize() {
        try (LocalCache<String, String> cache = new LocalCache<>(100, 0)) {
            cache.put("a", "1");
            cache.put("b", "2");
            org.junit.jupiter.api.Assertions.assertEquals(2, cache.size());
            final java.util.Set<String> keys = cache.keySet();
            org.junit.jupiter.api.Assertions.assertEquals(2, keys.size());
            org.junit.jupiter.api.Assertions.assertTrue(keys.contains("a"));
            org.junit.jupiter.api.Assertions.assertTrue(keys.contains("b"));
        }
    }

    @org.junit.jupiter.api.Test
    public void testClear() {
        try (LocalCache<String, String> cache = new LocalCache<>(100, 0)) {
            cache.put("a", "1");
            cache.put("b", "2");
            cache.clear();
            org.junit.jupiter.api.Assertions.assertEquals(0, cache.size());
        }
    }

    @org.junit.jupiter.api.Test
    public void testIsClosed() {
        final LocalCache<String, String> cache = new LocalCache<>(100, 0);
        org.junit.jupiter.api.Assertions.assertFalse(cache.isClosed());
        cache.close();
        org.junit.jupiter.api.Assertions.assertTrue(cache.isClosed());
    }

    @org.junit.jupiter.api.Test
    public void testStats() {
        try (LocalCache<String, String> cache = new LocalCache<>(100, 0)) {
            cache.put("a", "1");
            cache.getOrNull("a");
            cache.getOrNull("missing");
            final com.landawn.abacus.cache.CacheStats stats = cache.stats();
            org.junit.jupiter.api.Assertions.assertNotNull(stats);
            org.junit.jupiter.api.Assertions.assertEquals(100, stats.capacity());
        }
    }

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
    @org.junit.jupiter.api.Test
    public void testGetOrNull_EdgeCase_NullKeyThrowsIAE() {
        try (LocalCache<String, String> cache = new LocalCache<>(100, 0)) {
            org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> cache.getOrNull(null));
        }
    }

    @org.junit.jupiter.api.Test
    public void testRemove_EdgeCase_NullKeyThrowsIAE() {
        try (LocalCache<String, String> cache = new LocalCache<>(100, 0)) {
            org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> cache.remove(null));
        }
    }

    @org.junit.jupiter.api.Test
    public void testContainsKey_EdgeCase_NullKeyThrowsIAE() {
        try (LocalCache<String, String> cache = new LocalCache<>(100, 0)) {
            org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> cache.containsKey(null));
        }
    }

    @org.junit.jupiter.api.Test
    public void testConstructor_WithCustomPool() {
        final com.landawn.abacus.pool.KeyedObjectPool<String, com.landawn.abacus.pool.PoolableAdapter<String>> pool = com.landawn.abacus.pool.PoolFactory
                .createKeyedObjectPool(50, 0);
        try (LocalCache<String, String> cache = new LocalCache<>(60_000L, 30_000L, pool)) {
            assertTrue(cache.put("k", "v"));
            assertEquals("v", cache.getOrNull("k"));
        }
    }
}
