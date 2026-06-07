/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.TestBase;

/**
 * Coverage for the package-private convenience constructors of {@link OffHeapCache25} (the
 * Foreign-Memory-API backed off-heap cache) and the {@code allocate()} arena-cleanup-on-failure path.
 * The public API is the Builder, so the convenience constructors are only reachable from a
 * same-package test.
 */
@Tag("2025")
public class OffHeapCache25Test extends TestBase {

    /** {@code OffHeapCache25(capacityInMB)} delegates to the (capacity, evictDelay=3000) constructor. */
    @Test
    public void testConstructor_capacityOnly() {
        try (OffHeapCache25<String, byte[]> cache = new OffHeapCache25<>(1)) {
            assertFalse(cache.isClosed());
            final byte[] value = { 1, 2, 3, 4 };
            assertTrue(cache.put("k", value));
            assertArrayEquals(value, cache.getOrNull("k"));
        }
    }

    /** {@code OffHeapCache25(capacityInMB, evictDelay)} delegates to the default-TTL constructor. */
    @Test
    public void testConstructor_capacityAndEvictDelay() {
        try (OffHeapCache25<String, byte[]> cache = new OffHeapCache25<>(1, 0L)) {
            final byte[] value = { 5, 6, 7 };
            assertTrue(cache.put("k", value));
            assertArrayEquals(value, cache.getOrNull("k"));
            assertNotNull(cache.stats());
        }
    }

    /** {@code OffHeapCache25(capacityInMB, evictDelay, defaultLiveTime, defaultMaxIdleTime)} full basic form. */
    @Test
    public void testConstructor_capacityEvictDelayLiveTimeIdleTime() {
        try (OffHeapCache25<String, byte[]> cache = new OffHeapCache25<>(1, 0L, 60_000L, 60_000L)) {
            final byte[] value = { 8, 9 };
            assertTrue(cache.put("k", value));
            assertArrayEquals(value, cache.getOrNull("k"));
        }
    }

    /**
     * When the shared {@link java.lang.foreign.Arena} cannot satisfy the requested allocation, the
     * arena must be closed and the failure rethrown rather than leaking the arena. A capacity of
     * {@code Integer.MAX_VALUE} MB (~2 PB) is impossible to allocate, so construction fails inside
     * {@code allocate()} and exercises the cleanup branch.
     */
    @Test
    public void testAllocate_failureClosesArenaAndRethrows() {
        assertThrows(Throwable.class, () -> {
            try (OffHeapCache25<String, byte[]> cache = new OffHeapCache25<>(Integer.MAX_VALUE)) {
                // unreachable: allocation of ~2 PB must fail during construction.
                cache.put("k", new byte[1]);
            }
        });
    }
}
