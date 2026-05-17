/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
