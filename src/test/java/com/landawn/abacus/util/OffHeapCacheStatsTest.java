/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
}
