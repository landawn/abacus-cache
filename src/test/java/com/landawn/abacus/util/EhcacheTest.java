/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.cache.Ehcache;

@Tag("2025")
public class EhcacheTest {

    private static org.ehcache.Cache<String, String> newUnderlyingCache(final CacheManager cacheManager) {
        return cacheManager.createCache("c" + System.nanoTime(),
                CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class, ResourcePoolsBuilder.heap(100)));
    }

    /**
     * Regression test for the Ehcache.close() bug.
     *
     * <p>The Javadoc of {@link Ehcache#close()} explicitly states: "This method only marks the
     * wrapper as closed; it does not close or dispose the underlying Ehcache instance. The
     * underlying cache manager is responsible for managing the lifecycle of Ehcache instances."
     *
     * <p>Before the fix, close() called {@code cacheImpl.clear()}, destroying all entries of the
     * underlying (externally-owned, possibly shared) Ehcache instance. This test verifies the
     * underlying cache data survives closing the wrapper.
     */
    @Test
    public void testCloseDoesNotClearUnderlyingCache() {
        final CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final org.ehcache.Cache<String, String> underlying = newUnderlyingCache(cacheManager);

            final Ehcache<String, String> wrapper = new Ehcache<>(underlying);
            wrapper.put("k1", "v1", 0, 0);
            wrapper.put("k2", "v2", 0, 0);

            assertEquals("v1", underlying.get("k1"));

            wrapper.close();
            assertTrue(wrapper.isClosed());

            // The underlying cache (owned by the CacheManager, not the wrapper) must retain its data.
            assertEquals("v1", underlying.get("k1"));
            assertEquals("v2", underlying.get("k2"));
        } finally {
            cacheManager.close();
        }
    }

    /**
     * Sanity check: a second close() is idempotent and still does not clear the underlying cache.
     */
    @Test
    public void testCloseIsIdempotentAndNonDestructive() {
        final CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final org.ehcache.Cache<String, String> underlying = newUnderlyingCache(cacheManager);

            final Ehcache<String, String> wrapper = new Ehcache<>(underlying);
            wrapper.put("a", "1", 0, 0);

            wrapper.close();
            wrapper.close(); // idempotent, no exception

            assertTrue(wrapper.isClosed());
            assertEquals("1", underlying.get("a"));
        } finally {
            cacheManager.close();
        }
    }

    /**
     * Confirms clear() still works (it must clear), distinguishing intended clear() from close().
     */
    @Test
    public void testClearStillClears() {
        final CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final org.ehcache.Cache<String, String> underlying = newUnderlyingCache(cacheManager);

            final Ehcache<String, String> wrapper = new Ehcache<>(underlying);
            wrapper.put("x", "y", 0, 0);
            assertEquals("y", underlying.get("x"));

            wrapper.clear();

            assertFalse(wrapper.isClosed());
            assertEquals(null, underlying.get("x"));
        } finally {
            cacheManager.close();
        }
    }

    @Test
    public void testConstructor_EdgeCase_NullCache() {
        assertThrows(IllegalArgumentException.class, () -> new Ehcache<String, String>(null));
    }

    @Test
    public void testGetOrNull() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            wrapper.put("k", "v", 0, 0);
            assertEquals("v", wrapper.getOrNull("k"));
            assertNull(wrapper.getOrNull("missing"));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testPut_EdgeCase_NullKey() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            assertThrows(IllegalArgumentException.class, () -> wrapper.put(null, "v", 0, 0));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testRemove() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            wrapper.put("k", "v", 0, 0);
            wrapper.remove("k");
            assertNull(wrapper.getOrNull("k"));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testContainsKey() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            wrapper.put("k", "v", 0, 0);
            assertTrue(wrapper.containsKey("k"));
            assertFalse(wrapper.containsKey("missing"));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testPutIfAbsent() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            assertNull(wrapper.putIfAbsent("k", "v1"));
            assertEquals("v1", wrapper.putIfAbsent("k", "v2"));
            assertEquals("v1", wrapper.getOrNull("k"));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testPutIfAbsent_EdgeCase_NullKey() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            assertThrows(IllegalArgumentException.class, () -> wrapper.putIfAbsent(null, "v"));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testGetAll() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            wrapper.put("a", "1", 0, 0);
            wrapper.put("b", "2", 0, 0);

            final Set<String> keys = new HashSet<>();
            keys.add("a");
            keys.add("b");
            final Map<String, String> got = wrapper.getAll(keys);
            assertEquals("1", got.get("a"));
            assertEquals("2", got.get("b"));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testGetAll_EdgeCase_NullKeys() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            assertThrows(IllegalArgumentException.class, () -> wrapper.getAll(null));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testPutAll() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            final Map<String, String> entries = new HashMap<>();
            entries.put("a", "1");
            entries.put("b", "2");
            wrapper.putAll(entries);
            assertEquals("1", wrapper.getOrNull("a"));
            assertEquals("2", wrapper.getOrNull("b"));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testPutAll_EdgeCase_NullEntries() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            assertThrows(IllegalArgumentException.class, () -> wrapper.putAll(null));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testRemoveAll() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            wrapper.put("a", "1", 0, 0);
            wrapper.put("b", "2", 0, 0);
            final Set<String> keys = new HashSet<>();
            keys.add("a");
            keys.add("b");
            wrapper.removeAll(keys);
            assertNull(wrapper.getOrNull("a"));
            assertNull(wrapper.getOrNull("b"));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testRemoveAll_EdgeCase_NullKeys() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            assertThrows(IllegalArgumentException.class, () -> wrapper.removeAll(null));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testKeySet_Unsupported() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            assertThrows(UnsupportedOperationException.class, wrapper::keySet);
        } finally {
            cm.close();
        }
    }

    @Test
    public void testSize_Unsupported() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            assertThrows(UnsupportedOperationException.class, wrapper::size);
        } finally {
            cm.close();
        }
    }

    @Test
    public void testOperations_AfterClose_Throw() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            wrapper.close();
            assertThrows(IllegalStateException.class, () -> wrapper.getOrNull("k"));
            assertThrows(IllegalStateException.class, () -> wrapper.put("k", "v", 0, 0));
            assertThrows(IllegalStateException.class, () -> wrapper.remove("k"));
            assertThrows(IllegalStateException.class, () -> wrapper.containsKey("k"));
            assertThrows(IllegalStateException.class, wrapper::clear);
            assertThrows(IllegalStateException.class, () -> wrapper.putIfAbsent("k", "v"));
            assertThrows(IllegalStateException.class, () -> wrapper.getAll(new HashSet<>()));
            assertThrows(IllegalStateException.class, () -> wrapper.putAll(new HashMap<>()));
            assertThrows(IllegalStateException.class, () -> wrapper.removeAll(new HashSet<>()));
        } finally {
            cm.close();
        }
    }
}
