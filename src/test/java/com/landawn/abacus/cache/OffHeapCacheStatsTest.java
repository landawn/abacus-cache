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
import com.landawn.abacus.cache.OffHeapCacheStats.OccupiedSlot;

@Tag("2025")
public class OffHeapCacheStatsTest {

    /**
     * Regression test for the immutability defect in {@link OccupiedSlot}.
     *
     * <p>Before the fix, {@code OccupiedSlot} stored the caller-supplied {@code Map}
     * reference directly with no defensive copy, so mutating the original map after
     * construction would corrupt the record's state, and the exposed map was mutable.
     * After the fix, the record holds an unmodifiable defensive copy.
     */
    @Test
    public void testOccupiedSlotDefensiveCopyAndImmutability() {
        final Map<Integer, Integer> source = new LinkedHashMap<>();
        source.put(0, 5);
        source.put(1, 3);

        final OccupiedSlot slot = new OccupiedSlot(1024, source);

        // Mutating the source after construction must NOT affect the record (defensive copy).
        source.put(2, 99);
        source.clear();

        assertEquals(2, slot.occupiedSlots().size());
        assertEquals(5, slot.occupiedSlots().get(0));
        assertEquals(3, slot.occupiedSlots().get(1));

        // The exposed map must be unmodifiable.
        assertThrows(UnsupportedOperationException.class, () -> slot.occupiedSlots().put(7, 7));
        assertEquals(1024, slot.sizeOfSlot());
    }

    @Test
    public void testOccupiedSlotRejectsNullMap() {
        assertThrows(NullPointerException.class, () -> new OccupiedSlot(64, null));
    }

    @Test
    public void testOccupiedSlotEmptyMap() {
        final OccupiedSlot slot = new OccupiedSlot(64, new LinkedHashMap<>());
        assertTrue(slot.occupiedSlots().isEmpty());
        assertThrows(UnsupportedOperationException.class, () -> slot.occupiedSlots().put(1, 1));
    }

    /**
     * A negative {@code sizeOfSlot} is rejected with {@link IllegalArgumentException}, consistent with
     * the numeric-component validation in the enclosing {@link OffHeapCacheStats} record (a null map is
     * still rejected with {@link NullPointerException}).
     */
    @Test
    public void testOccupiedSlotRejectsNegativeSize() {
        assertThrows(IllegalArgumentException.class, () -> new OccupiedSlot(-1, new LinkedHashMap<>()));
    }

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
