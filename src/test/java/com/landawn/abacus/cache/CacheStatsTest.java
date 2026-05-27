/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.TestBase;

@Tag("2025")
public class CacheStatsTest extends TestBase {

    @Test
    public void testRecord_AccessorsReturnConstructorValues() {
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
    public void testEquals_SameValues() {
        final CacheStats a = new CacheStats(1, 2, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        final CacheStats b = new CacheStats(1, 2, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testEquals_DifferentValues() {
        final CacheStats a = new CacheStats(1, 2, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        final CacheStats b = new CacheStats(1, 2, 3L, 4L, 5L, 6L, 7L, 8L, 10L);
        assertNotEquals(a, b);
    }

    @Test
    public void testToString() {
        // Record auto-generates toString; verify it includes the component values.
        final CacheStats stats = new CacheStats(100, 42, 10L, 30L, 25L, 5L, 3L, 1024L, 512L);
        final String s = stats.toString();
        assertNotNull(s);
        assertTrue(s.contains("100"));
        assertTrue(s.contains("42"));
        assertTrue(s.contains("1024"));
    }

    @Test
    public void testRecord_EdgeCase_AllZeros() {
        final CacheStats zero = new CacheStats(0, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L);
        assertEquals(0, zero.capacity());
        assertEquals(0, zero.size());
        assertEquals(0L, zero.putCount());
    }

    @Test
    public void testRecord_EdgeCase_NegativeCapacityRejected() {
        assertThrows(IllegalArgumentException.class, () -> new CacheStats(-1, 0, 0L, 0L, 0L, 0L, 0L, 0L, 0L));
    }

    @Test
    public void testRecord_EdgeCase_NegativeSizeRejected() {
        assertThrows(IllegalArgumentException.class, () -> new CacheStats(0, -1, 0L, 0L, 0L, 0L, 0L, 0L, 0L));
    }

    @Test
    public void testRecord_EdgeCase_NegativePutCountRejected() {
        assertThrows(IllegalArgumentException.class, () -> new CacheStats(0, 0, -1L, 0L, 0L, 0L, 0L, 0L, 0L));
    }

    @Test
    public void testRecord_EdgeCase_NegativeGetCountRejected() {
        assertThrows(IllegalArgumentException.class, () -> new CacheStats(0, 0, 0L, -1L, 0L, 0L, 0L, 0L, 0L));
    }

    @Test
    public void testRecord_EdgeCase_NegativeHitCountRejected() {
        assertThrows(IllegalArgumentException.class, () -> new CacheStats(0, 0, 0L, 0L, -1L, 0L, 0L, 0L, 0L));
    }

    @Test
    public void testRecord_EdgeCase_NegativeMissCountRejected() {
        assertThrows(IllegalArgumentException.class, () -> new CacheStats(0, 0, 0L, 0L, 0L, -1L, 0L, 0L, 0L));
    }

    @Test
    public void testRecord_EdgeCase_NegativeEvictionCountRejected() {
        assertThrows(IllegalArgumentException.class, () -> new CacheStats(0, 0, 0L, 0L, 0L, 0L, -1L, 0L, 0L));
    }

    @Test
    public void testRecord_EdgeCase_NegativeMaxMemoryBelowSentinelRejected() {
        // -1 is the documented "not tracked" sentinel; anything more-negative is invalid.
        assertThrows(IllegalArgumentException.class, () -> new CacheStats(0, 0, 0L, 0L, 0L, 0L, 0L, -2L, 0L));
    }

    @Test
    public void testRecord_EdgeCase_NegativeDataSizeBelowSentinelRejected() {
        assertThrows(IllegalArgumentException.class, () -> new CacheStats(0, 0, 0L, 0L, 0L, 0L, 0L, 0L, -2L));
    }

    @Test
    public void testRecord_EdgeCase_MaxMemoryAndDataSize_NotTrackedSentinelAccepted() {
        // -1 is reserved as the "not tracked" sentinel for maxMemory and dataSize, returned by
        // the underlying KeyedObjectPool when memory accounting is disabled.
        final CacheStats stats = new CacheStats(10, 5, 1L, 2L, 1L, 1L, 0L, -1L, -1L);
        assertEquals(-1L, stats.maxMemory());
        assertEquals(-1L, stats.dataSize());
    }
}
