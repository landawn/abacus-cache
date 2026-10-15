/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class CacheStatsTest {

    // Statistics

    @Test
    void ratesCounterSumDoesNotOverflow() {
        final CacheStats stats = new CacheStats(0, 0, 0L, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, 0L, 0L, 0L);

        assertEquals(0.5D, stats.hitRate());
        assertEquals(0.5D, stats.missRate());
    }

    @Test
    void ratesOrdinaryAsymmetricCounts() {
        final CacheStats stats = new CacheStats(10, 2, 4L, 4L, 3L, 1L, 0L, 0L, 0L);
        assertEquals(0.75D, stats.hitRate());
        assertEquals(0.25D, stats.missRate());
    }

    @Test
    void ratesZeroRequestsReturnZero() {
        // Documented zero-safe guard: no requests recorded -> both rates are 0.0, not NaN.
        final CacheStats stats = new CacheStats(10, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
        assertEquals(0.0D, stats.hitRate());
        assertEquals(0.0D, stats.missRate());
    }

    @Test
    void recordAccessorsReturnConstructorValues() {
        final CacheStats stats = new CacheStats(100, 42, 10L, 30L, 25L, 5L, 3L, 1024L, 512L);
        assertEquals(100, stats.capacity());
        assertEquals(42, stats.size());
        assertEquals(10L, stats.putCount());
        assertEquals(30L, stats.getCount());
        assertEquals(25L, stats.hitCount());
        assertEquals(5L, stats.missCount());
        assertEquals(3L, stats.evictionCount());
        assertEquals(1024L, stats.maxMemory());
        assertEquals(512L, stats.dataSize());
    }

    @Test
    void recordAllZeros() {
        final CacheStats zero = new CacheStats(0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
        assertEquals(0, zero.capacity());
        assertEquals(0, zero.size());
        assertEquals(0L, zero.putCount());
    }

    @Test
    void recordMaxMemoryAndDataSizeNotTrackedSentinelAccepted() {
        // -1 is reserved as the "not tracked" sentinel for maxMemory and dataSize, returned by
        // the underlying KeyedObjectPool when memory accounting is disabled.
        final CacheStats stats = new CacheStats(10, 5, 1L, 2L, 1L, 1L, 0L, -1L, -1L);
        assertEquals(-1L, stats.maxMemory());
        assertEquals(-1L, stats.dataSize());
    }

    @Test
    void recordNegativeCapacityRejected() {
        assertThrows(IllegalArgumentException.class, () -> new CacheStats(-1, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L));
    }

    @Test
    void recordNegativeDataSizeBelowSentinelRejected() {
        assertThrows(IllegalArgumentException.class, () -> new CacheStats(0, 0, 0L, 0L, 0L, 0L, 0L, 0L, -2L));
    }

    @Test
    void recordNegativeEvictionCountRejected() {
        assertThrows(IllegalArgumentException.class, () -> new CacheStats(0, 0, 0L, 0L, 0L, 0L, -1L, 0L, 0L));
    }

    @Test
    void recordNegativeGetCountRejected() {
        assertThrows(IllegalArgumentException.class, () -> new CacheStats(0, 0, 0L, -1L, 0L, 0L, 0L, 0L, 0L));
    }

    @Test
    void recordNegativeHitCountRejected() {
        assertThrows(IllegalArgumentException.class, () -> new CacheStats(0, 0, 0L, 0L, -1L, 0L, 0L, 0L, 0L));
    }

    @Test
    void recordNegativeMaxMemoryBelowSentinelRejected() {
        // -1 is the documented "not tracked" sentinel; anything more-negative is invalid.
        assertThrows(IllegalArgumentException.class, () -> new CacheStats(0, 0, 0L, 0L, 0L, 0L, 0L, -2L, 0L));
    }

    @Test
    void recordNegativeMissCountRejected() {
        assertThrows(IllegalArgumentException.class, () -> new CacheStats(0, 0, 0L, 0L, 0L, -1L, 0L, 0L, 0L));
    }

    @Test
    void recordNegativePutCountRejected() {
        assertThrows(IllegalArgumentException.class, () -> new CacheStats(0, 0, -1L, 0L, 0L, 0L, 0L, 0L, 0L));
    }

    @Test
    void recordNegativeSizeRejected() {
        assertThrows(IllegalArgumentException.class, () -> new CacheStats(0, -1, 0L, 0L, 0L, 0L, 0L, 0L, 0L));
    }

    // Other Behavior

    @Test
    void equalsDifferentValues() {
        final CacheStats a = new CacheStats(1, 2, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        final CacheStats b = new CacheStats(1, 2, 3L, 4L, 5L, 6L, 7L, 8L, 10L);
        assertNotEquals(a, b);
    }

    @Test
    void equalsSameValues() {
        final CacheStats a = new CacheStats(1, 2, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        final CacheStats b = new CacheStats(1, 2, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void toStringIncludesRecordComponents() {
        // Record auto-generates toString; verify it includes the component values.
        final CacheStats stats = new CacheStats(100, 42, 10L, 30L, 25L, 5L, 3L, 1024L, 512L);
        final String s = stats.toString();
        assertNotNull(s);
        assertTrue(s.contains("100"));
        assertTrue(s.contains("42"));
        assertTrue(s.contains("1024"));
    }
}
