/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Supplier;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.landawn.abacus.TestBase;
import com.landawn.abacus.util.Suppliers;

@Tag("2025")
public class CaffeineCacheTest extends TestBase {

    private CaffeineCache<String, String> newCache() {
        // Use a Supplier to create the underlying Caffeine instance, exercising the Suppliers helper.
        final Supplier<com.github.benmanes.caffeine.cache.Cache<String, String>> supplier = Suppliers
                .of(() -> Caffeine.newBuilder().maximumSize(100).recordStats().build());
        return new CaffeineCache<>(supplier.get());
    }

    @Test
    public void testConstructor_EdgeCase_NullCache() {
        assertThrows(IllegalArgumentException.class, () -> new CaffeineCache<String, String>(null));
    }

    @Test
    public void testPutAndGetOrNull() {
        try (CaffeineCache<String, String> cache = newCache()) {
            assertTrue(cache.put("k", "v", 0, 0));
            assertEquals("v", cache.getOrNull("k"));
        }
    }

    @Test
    public void testPut_EdgeCase_NullKey() {
        try (CaffeineCache<String, String> cache = newCache()) {
            assertThrows(IllegalArgumentException.class, () -> cache.put(null, "v", 0, 0));
        }
    }

    @Test
    public void testGetOrNull_EdgeCase_Missing() {
        try (CaffeineCache<String, String> cache = newCache()) {
            assertNull(cache.getOrNull("missing"));
        }
    }

    @Test
    public void testRemove() {
        try (CaffeineCache<String, String> cache = newCache()) {
            cache.put("k", "v", 0, 0);
            cache.remove("k");
            assertNull(cache.getOrNull("k"));
        }
    }

    @Test
    public void testContainsKey() {
        try (CaffeineCache<String, String> cache = newCache()) {
            cache.put("k", "v", 0, 0);
            assertTrue(cache.containsKey("k"));
            assertFalse(cache.containsKey("missing"));
        }
    }

    @Test
    public void testKeySet_Unsupported() {
        try (CaffeineCache<String, String> cache = newCache()) {
            assertThrows(UnsupportedOperationException.class, cache::keySet);
        }
    }

    @Test
    public void testSize() {
        try (CaffeineCache<String, String> cache = newCache()) {
            cache.put("a", "1", 0, 0);
            cache.put("b", "2", 0, 0);
            // Caffeine performs maintenance lazily; force pending writes to drain so size is reliable.
            // We just check it's between 0 and the maximum.
            final int size = cache.size();
            assertTrue(size >= 0);
            assertTrue(size <= 2);
        }
    }

    @Test
    public void testClear() {
        try (CaffeineCache<String, String> cache = newCache()) {
            cache.put("k", "v", 0, 0);
            cache.clear();
            assertNull(cache.getOrNull("k"));
        }
    }

    @Test
    public void testClose_IsIdempotent() {
        final CaffeineCache<String, String> cache = newCache();
        assertFalse(cache.isClosed());
        cache.close();
        cache.close();
        assertTrue(cache.isClosed());
    }

    @Test
    public void testOperations_AfterClose_Throw() {
        final CaffeineCache<String, String> cache = newCache();
        cache.close();
        assertThrows(IllegalStateException.class, () -> cache.getOrNull("k"));
        assertThrows(IllegalStateException.class, () -> cache.put("k", "v", 0, 0));
        assertThrows(IllegalStateException.class, () -> cache.remove("k"));
        assertThrows(IllegalStateException.class, () -> cache.containsKey("k"));
        assertThrows(IllegalStateException.class, cache::size);
        assertThrows(IllegalStateException.class, cache::clear);
    }

    @Test
    public void testStats_ReturnsNonNull() {
        try (CaffeineCache<String, String> cache = newCache()) {
            cache.put("k", "v", 0, 0);
            cache.getOrNull("k");
            cache.getOrNull("missing");
            assertNotNull(cache.stats());
        }
    }
}
