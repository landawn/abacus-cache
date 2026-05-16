/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
}
