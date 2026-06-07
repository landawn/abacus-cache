/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.TestBase;

/**
 * Coverage for the package-private convenience constructors of {@link OffHeapCache}. These delegate
 * to the full constructor with progressively more defaults filled in; the public API is the Builder,
 * so they are only reachable from a same-package test.
 */
@Tag("2025")
public class OffHeapCacheTest extends TestBase {

    /** {@code OffHeapCache(capacityInMB)} delegates to the (capacity, evictDelay=3000) constructor. */
    @Test
    public void testConstructor_capacityOnly() {
        try (OffHeapCache<String, byte[]> cache = new OffHeapCache<>(1)) {
            assertFalse(cache.isClosed());
            final byte[] value = { 1, 2, 3, 4 };
            assertTrue(cache.put("k", value));
            assertArrayEquals(value, cache.getOrNull("k"));
        }
    }

    /** {@code OffHeapCache(capacityInMB, evictDelay)} delegates to the default-TTL constructor. */
    @Test
    public void testConstructor_capacityAndEvictDelay() {
        try (OffHeapCache<String, byte[]> cache = new OffHeapCache<>(1, 0L)) {
            final byte[] value = { 5, 6, 7 };
            assertTrue(cache.put("k", value));
            assertArrayEquals(value, cache.getOrNull("k"));
            assertNotNull(cache.stats());
        }
    }

    /** {@code OffHeapCache(capacityInMB, evictDelay, defaultLiveTime, defaultMaxIdleTime)} full basic form. */
    @Test
    public void testConstructor_capacityEvictDelayLiveTimeIdleTime() {
        try (OffHeapCache<String, byte[]> cache = new OffHeapCache<>(1, 0L, 60_000L, 60_000L)) {
            final byte[] value = { 8, 9 };
            assertTrue(cache.put("k", value));
            assertArrayEquals(value, cache.getOrNull("k"));
        }
    }
}
