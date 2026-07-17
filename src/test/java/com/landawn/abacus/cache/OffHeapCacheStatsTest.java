/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
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

    @Test
    public void testMinMaxAvgRejectsInconsistentOrdering() {
        assertThrows(IllegalArgumentException.class, () -> new MinMaxAvg(2, 1, 1.5));
        assertThrows(IllegalArgumentException.class, () -> new MinMaxAvg(1, 3, 0.5));
        assertThrows(IllegalArgumentException.class, () -> new MinMaxAvg(1, 3, 3.5));
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

    @Test
    public void testRecordValidatesAndDefensivelyCopiesOccupiedSlots() {
        final MinMaxAvg z = new MinMaxAvg(0, 0, 0);
        final Map<Integer, Integer> perSegment = new LinkedHashMap<>();
        perSegment.put(2, 3);
        final Map<Integer, Map<Integer, Integer>> slots = new LinkedHashMap<>();
        slots.put(64, perSegment);

        final OffHeapCacheStats stats = new OffHeapCacheStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, z, z, 1_048_576, slots);
        perSegment.put(4, 5);
        slots.put(128, Map.of(1, 1));

        assertEquals(Map.of(64, Map.of(2, 3)), stats.occupiedSlots());
        assertThrows(UnsupportedOperationException.class, () -> stats.occupiedSlots().put(128, Map.of()));
        assertThrows(UnsupportedOperationException.class, () -> stats.occupiedSlots().get(64).put(4, 5));

        assertThrows(IllegalArgumentException.class, () -> new OffHeapCacheStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, z, z, 1, Map.of(0, Map.of())));
        assertThrows(IllegalArgumentException.class,
                () -> new OffHeapCacheStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, z, z, 1, Map.of(64, Map.of(-1, 0))));
        assertThrows(IllegalArgumentException.class,
                () -> new OffHeapCacheStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, z, z, 1, Map.of(64, Map.of(0, -1))));
    }

    @Test
    public void testRates_EdgeCase_CounterSumDoesNotOverflow() {
        final MinMaxAvg z = new MinMaxAvg(0, 0, 0);
        final OffHeapCacheStats stats = new OffHeapCacheStats(0, 0, 0, 0, 0, Long.MAX_VALUE, Long.MAX_VALUE, 0, Long.MAX_VALUE, 0, 0, 0, 0, 0, 0, z, z, 0,
                Map.of());

        assertEquals(0.5D, stats.hitRate());
        assertEquals(0.5D, stats.missRate());
    }

    /**
     * The documented (deliberate) NPE-for-null-components convention: reference components are
     * rejected with NullPointerException via Objects.requireNonNull, unlike the IAE used for
     * out-of-range numerics.
     */
    @Test
    public void testRecord_EdgeCase_NullComponents_ThrowNPE() {
        final MinMaxAvg z = new MinMaxAvg(0, 0, 0);

        assertThrows(NullPointerException.class, () -> new OffHeapCacheStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null, z, 1, Map.of()));
        assertThrows(NullPointerException.class, () -> new OffHeapCacheStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, z, null, 1, Map.of()));
        assertThrows(NullPointerException.class, () -> new OffHeapCacheStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, z, z, 1, null));

        final Map<Integer, Map<Integer, Integer>> nullNested = new java.util.HashMap<>();
        nullNested.put(64, null);
        assertThrows(NullPointerException.class, () -> new OffHeapCacheStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, z, z, 1, nullNested));
    }
}
