/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * Covers the deprecated {@link ChronicleMap} compatibility shim. It delegates to
 * {@link LocalCache}, so we exercise each constructor and confirm round-trip semantics.
 */
@SuppressWarnings("deprecation")
class ChronicleMapTest {

    // Construction

    @Test
    void defaultConstructor() {
        final ChronicleMap<String, String> map = new ChronicleMap<>();

        try {
            assertNotNull(map);
            assertTrue(map.put("k", "v"));
            assertEquals("v", map.getOrNull("k"));
        } finally {
            map.close();
        }
    }

    @Test
    void constructorFullySpecified() {
        final ChronicleMap<String, String> map = new ChronicleMap<>(128, 0L, 60_000L, 30_000L);

        try {
            assertTrue(map.put("session", "abc"));
            assertEquals("abc", map.getOrNull("session"));
        } finally {
            map.close();
        }
    }

    @Test
    void constructorInvalidCapacity() {
        assertThrows(IllegalArgumentException.class, () -> new ChronicleMap<String, String>(0, 0L));
    }

    /** A negative evictDelay is rejected (documented; unlike the off-heap caches, which treat it as "disabled"). */
    @Test
    void constructorNegativeEvictDelayRejected() {
        assertThrows(IllegalArgumentException.class, () -> new ChronicleMap<String, String>(1_024, -1L));
    }

    @Test
    void constructorWithCapacityAndEvictDelay() {
        final ChronicleMap<String, Integer> map = new ChronicleMap<>(64, 0L);

        try {
            assertTrue(map.put("count", 42));
            assertEquals(Integer.valueOf(42), map.getOrNull("count"));
        } finally {
            map.close();
        }
    }

    // Removals

    @Test
    void removeDeletesEntry() {
        final ChronicleMap<String, String> map = new ChronicleMap<>();

        try {
            map.put("k", "v");
            map.remove("k");
            assertNull(map.getOrNull("k"));
        } finally {
            map.close();
        }
    }
}
