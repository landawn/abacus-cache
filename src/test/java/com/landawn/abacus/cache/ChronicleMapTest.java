/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.TestBase;

/**
 * Covers the deprecated {@link ChronicleMap} compatibility shim. It delegates to
 * {@link LocalCache}, so we exercise each constructor and confirm round-trip semantics.
 */
@SuppressWarnings("deprecation")
@Tag("2025")
public class ChronicleMapTest extends TestBase {

    @Test
    public void testDefaultConstructor() {
        try (ChronicleMap<String, String> map = new ChronicleMap<>()) {
            assertNotNull(map);
            assertTrue(map.put("k", "v"));
            assertEquals("v", map.getOrNull("k"));
        }
    }

    @Test
    public void testConstructor_WithCapacityAndEvictDelay() {
        try (ChronicleMap<String, Integer> map = new ChronicleMap<>(64, 0L)) {
            assertTrue(map.put("count", 42));
            assertEquals(Integer.valueOf(42), map.getOrNull("count"));
        }
    }

    @Test
    public void testConstructor_FullySpecified() {
        try (ChronicleMap<String, String> map = new ChronicleMap<>(128, 0L, 60_000L, 30_000L)) {
            assertTrue(map.put("session", "abc"));
            assertEquals("abc", map.getOrNull("session"));
        }
    }

    @Test
    public void testConstructor_EdgeCase_InvalidCapacity() {
        try {
            new ChronicleMap<String, String>(0, 0L);
            org.junit.jupiter.api.Assertions.fail("expected IllegalArgumentException");
        } catch (final IllegalArgumentException expected) {
            // ok
        }
    }

    @Test
    public void testRemove() {
        try (ChronicleMap<String, String> map = new ChronicleMap<>()) {
            map.put("k", "v");
            map.remove("k");
            assertNull(map.getOrNull("k"));
        }
    }
}
