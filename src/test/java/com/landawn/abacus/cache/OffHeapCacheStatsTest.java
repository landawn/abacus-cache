/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.cache.OffHeapCacheStats;
import com.landawn.abacus.cache.OffHeapCacheStats.MinMaxAvg;

@Tag("2025")
public class OffHeapCacheStatsTest {

    /**
     * {@link MinMaxAvg} tracks disk-I/O timings (milliseconds), which can never be negative, so the
     * record now rejects a negative {@code min}, {@code max}, or {@code avg} with
     * {@link IllegalArgumentException}.
     */
    @Test
    public void testMinMaxAvgRejectsNegativeValues() {
        assertThrows(IllegalArgumentException.class, () -> new MinMaxAvg(-1, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new MinMaxAvg(0, -1, 0));
        assertThrows(IllegalArgumentException.class, () -> new MinMaxAvg(0, 0, -1));
    }

    /**
     * Regression: {@link MinMaxAvg} documents its values as non-negative, finite milliseconds, but the
     * previous {@code N.checkArgNotNegative} guard let {@code NaN} and {@code +Infinity} through —
     * every comparison with {@code NaN} is {@code false}, so {@code NaN < 0} never fired. The compact
     * constructor now rejects non-finite values with {@link IllegalArgumentException} so an invalid
     * computed statistic fails fast instead of silently violating the invariant.
     */
    @Test
    public void testMinMaxAvgRejectsNonFiniteValues() {
        assertThrows(IllegalArgumentException.class, () -> new MinMaxAvg(Double.NaN, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new MinMaxAvg(0, Double.NaN, 0));
        assertThrows(IllegalArgumentException.class, () -> new MinMaxAvg(0, 0, Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> new MinMaxAvg(Double.POSITIVE_INFINITY, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new MinMaxAvg(0, Double.POSITIVE_INFINITY, 0));
        assertThrows(IllegalArgumentException.class, () -> new MinMaxAvg(0, 0, Double.POSITIVE_INFINITY));
    }

    /**
     * Regression coverage for the missing non-negative validation on
     * {@link OffHeapCacheStats}'s numeric components. The Javadoc has long stated that every
     * counter, size, and memory metric must be non-negative, but the canonical constructor
     * previously enforced this only for {@code writeToDiskTimeStats}, {@code readFromDiskTimeStats},
     * and {@code occupiedSlots}. The compact constructor now rejects any negative numeric input.
     */
    @Test
    public void testRecord_NegativeComponentRejected() {
        final MinMaxAvg z = new MinMaxAvg(0, 0, 0);
        // capacity negative
        assertThrows(IllegalArgumentException.class, () -> new OffHeapCacheStats(-1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, z, z, 0, Map.of()));
        // size negative
        assertThrows(IllegalArgumentException.class, () -> new OffHeapCacheStats(0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, z, z, 0, Map.of()));
        // sizeOnDisk negative
        assertThrows(IllegalArgumentException.class, () -> new OffHeapCacheStats(0, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, z, z, 0, Map.of()));
        // putCount negative
        assertThrows(IllegalArgumentException.class, () -> new OffHeapCacheStats(0, 0, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, z, z, 0, Map.of()));
        // missCount negative
        assertThrows(IllegalArgumentException.class, () -> new OffHeapCacheStats(0, 0, 0, 0, 0, 0, 0, 0, -1, 0, 0, 0, 0, 0, 0, z, z, 0, Map.of()));
        // segmentSize negative
        assertThrows(IllegalArgumentException.class, () -> new OffHeapCacheStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, z, z, -1, Map.of()));
        // dataSizeOnDisk negative
        assertThrows(IllegalArgumentException.class, () -> new OffHeapCacheStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, z, z, 0, Map.of()));
    }

    @Test
    public void testRecord_AllZerosOk() {
        final MinMaxAvg z = new MinMaxAvg(0, 0, 0);
        final OffHeapCacheStats stats = new OffHeapCacheStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, z, z, 0, Map.of());
        assertEquals(0, stats.capacity());
        assertEquals(0, stats.segmentSize());
        assertTrue(stats.occupiedSlots().isEmpty());
    }
}
