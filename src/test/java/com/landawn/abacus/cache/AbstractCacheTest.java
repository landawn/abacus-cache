/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.TestBase;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.Properties;
import com.landawn.abacus.util.u.Optional;

/**
 * Covers the {@link AbstractCache} async wrappers and property helpers via the concrete
 * {@link LocalCache} subclass.
 */
@Tag("2025")
public class AbstractCacheTest extends TestBase {

    private LocalCache<String, String> newCache() {
        return new LocalCache<>(100, 0);
    }

    @Test
    public void testGet() {
        try (LocalCache<String, String> cache = newCache()) {
            cache.put("k", "v");
            final Optional<String> got = cache.get("k");
            assertTrue(got.isPresent());
            assertEquals("v", got.get());
        }
    }

    @Test
    public void testGet_EdgeCase_Missing() {
        try (LocalCache<String, String> cache = newCache()) {
            assertFalse(cache.get("missing").isPresent());
        }
    }

    @Test
    public void testPutTwoArg_UsesDefaults() {
        try (LocalCache<String, String> cache = newCache()) {
            // Defaults should be applied; verify the entry is stored.
            assertTrue(cache.put("k", "v"));
            assertEquals("v", cache.getOrNull("k"));
        }
    }

    // Async operations
    @Test
    public void testAsyncGet() throws Exception {
        try (LocalCache<String, String> cache = newCache()) {
            cache.put("k", "v");
            final ContinuableFuture<Optional<String>> f = cache.asyncGet("k");
            final Optional<String> opt = f.get();
            assertTrue(opt.isPresent());
            assertEquals("v", opt.get());
        }
    }

    @Test
    public void testAsyncGetOrNull() throws Exception {
        try (LocalCache<String, String> cache = newCache()) {
            cache.put("k", "v");
            assertEquals("v", cache.asyncGetOrNull("k").get());
        }
    }

    @Test
    public void testAsyncGetOrNull_EdgeCase_Missing() throws Exception {
        try (LocalCache<String, String> cache = newCache()) {
            assertNull(cache.asyncGetOrNull("none").get());
        }
    }

    @Test
    public void testAsyncPut() throws Exception {
        try (LocalCache<String, String> cache = newCache()) {
            assertTrue(cache.asyncPut("k", "v").get());
            assertEquals("v", cache.getOrNull("k"));
        }
    }

    @Test
    public void testAsyncPutWithTimes() throws Exception {
        try (LocalCache<String, String> cache = newCache()) {
            assertTrue(cache.asyncPut("k", "v", 5000, 5000).get());
            assertEquals("v", cache.getOrNull("k"));
        }
    }

    @Test
    public void testAsyncRemove() throws Exception {
        try (LocalCache<String, String> cache = newCache()) {
            cache.put("k", "v");
            assertNull(cache.asyncRemove("k").get());
            assertNull(cache.getOrNull("k"));
        }
    }

    @Test
    public void testAsyncContainsKey() throws Exception {
        try (LocalCache<String, String> cache = newCache()) {
            cache.put("k", "v");
            assertTrue(cache.asyncContainsKey("k").get());
            assertFalse(cache.asyncContainsKey("missing").get());
        }
    }

    // Properties bag
    @Test
    public void testGetProperties() {
        try (LocalCache<String, String> cache = newCache()) {
            final Properties<String, Object> props = cache.getProperties();
            assertNotNull(props);
            // Returns the same instance on subsequent calls.
            assertTrue(props == cache.getProperties());
        }
    }

    @Test
    public void testSetAndGetProperty() {
        try (LocalCache<String, String> cache = newCache()) {
            assertNull(cache.setProperty("name", "alpha"));
            final String name = cache.getProperty("name");
            assertEquals("alpha", name);
        }
    }

    @Test
    public void testSetProperty_ReturnsPreviousValue() {
        try (LocalCache<String, String> cache = newCache()) {
            cache.setProperty("name", "v1");
            final String prev = cache.setProperty("name", "v2");
            assertEquals("v1", prev);
            assertEquals("v2", cache.getProperty("name"));
        }
    }

    @Test
    public void testRemoveProperty() {
        try (LocalCache<String, String> cache = newCache()) {
            cache.setProperty("foo", "bar");
            final String removed = cache.removeProperty("foo");
            assertEquals("bar", removed);
            assertNull(cache.getProperty("foo"));
        }
    }

    @Test
    public void testRemoveProperty_EdgeCase_Missing() {
        try (LocalCache<String, String> cache = newCache()) {
            assertNull(cache.removeProperty("never-set"));
        }
    }

    @Test
    public void testProperties_DontAffectCacheEntries() {
        try (LocalCache<String, String> cache = newCache()) {
            cache.put("a", "1");
            cache.setProperty("a", "this-is-a-property");
            // Cache entries and properties live in separate spaces.
            assertEquals("1", cache.getOrNull("a"));
            assertEquals("this-is-a-property", cache.getProperty("a"));
            assertDoesNotThrow(() -> cache.removeProperty("a"));
            assertEquals("1", cache.getOrNull("a"));
        }
    }
}
