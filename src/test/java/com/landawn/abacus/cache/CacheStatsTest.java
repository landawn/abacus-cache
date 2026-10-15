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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class CacheStatsTest {

    // Construction and validation

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

    @ParameterizedTest(name = "{0} rejects {2}")
    @CsvSource({ "capacity,0,-1", "size,1,-1", "putCount,2,-1", "getCount,3,-1", "hitCount,4,-1", "missCount,5,-1", "evictionCount,6,-1", "maxMemory,7,-2",
            "dataSize,8,-2" })
    void recordRejectsInvalidNumericComponents(final String component, final int index, final long invalid) {
        final long[] values = new long[9];
        values[index] = invalid;

        assertThrows(IllegalArgumentException.class,
                () -> new CacheStats((int) values[0], (int) values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8]), component);
    }

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

    // Value semantics

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
