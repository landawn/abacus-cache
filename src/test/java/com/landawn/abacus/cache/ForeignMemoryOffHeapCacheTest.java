/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ForeignMemoryOffHeapCacheTest {

    // Construction

    @Test
    void builderFunctionalArgumentsNullRejected() {
        final ForeignMemoryOffHeapCache.Builder<String, Account> builder = ForeignMemoryOffHeapCache.builder();

        assertThrows(IllegalArgumentException.class, () -> builder.serializer(null));
        assertThrows(IllegalArgumentException.class, () -> builder.deserializer(null));
        assertThrows(IllegalArgumentException.class, () -> builder.testerForLoadingItemFromDiskToMemory(null));
        assertThrows(IllegalArgumentException.class, () -> builder.storeSelector(null));
    }

    /** {@code ForeignMemoryOffHeapCache(capacityInMB, evictDelay)} delegates to the default-TTL constructor. */
    @Test
    void constructorCapacityAndEvictDelay() {
        final ForeignMemoryOffHeapCache<String, byte[]> cache = new ForeignMemoryOffHeapCache<>(1, 0L);

        try {
            final byte[] value = { 5, 6, 7 };
            assertTrue(cache.put("k", value));
            assertArrayEquals(value, cache.getOrNull("k"));
            assertNotNull(cache.stats());
        } finally {
            cache.close();
        }
    }

    /** {@code ForeignMemoryOffHeapCache(capacityInMB, evictDelay, defaultLiveTime, defaultMaxIdleTime)} full basic form. */
    @Test
    void constructorCapacityEvictDelayLiveTimeIdleTime() {
        final ForeignMemoryOffHeapCache<String, byte[]> cache = new ForeignMemoryOffHeapCache<>(1, 0L, 60_000L, 60_000L);

        try {
            final byte[] value = { 8, 9 };
            assertTrue(cache.put("k", value));
            assertArrayEquals(value, cache.getOrNull("k"));
        } finally {
            cache.close();
        }
    }

    /** {@code ForeignMemoryOffHeapCache(capacityInMB)} delegates to the (capacity, evictDelay=3000) constructor. */
    @Test
    void constructorCapacityOnly() {
        final ForeignMemoryOffHeapCache<String, byte[]> cache = new ForeignMemoryOffHeapCache<>(1);

        try {
            assertFalse(cache.isClosed());
            final byte[] value = { 1, 2, 3, 4 };
            assertTrue(cache.put("k", value));
            assertArrayEquals(value, cache.getOrNull("k"));
        } finally {
            cache.close();
        }
    }

    // Validation And Failures

    /**
     * Exercises the failure branch of {@code allocate()} directly: {@code Arena.allocate} rejects
     * an overflowing size with {@link OutOfMemoryError} and a negative size with
     * {@link IllegalArgumentException}. The failure must propagate unchanged (the fresh arena is
     * closed, not self-suppressed) and must not disturb the live arena/segment already backing the
     * cache.
     */
    @Test
    void allocateDirectFailureRethrowsAndLeavesLiveSegmentUsable() {
        final ForeignMemoryOffHeapCache<String, byte[]> cache = ForeignMemoryOffHeapCache.<String, byte[]> builder().capacityInMB(1).build();

        try {
            final byte[] value = { 1, 2, 3 };
            assertTrue(cache.put("k", value));

            final OutOfMemoryError oom = assertThrows(OutOfMemoryError.class, () -> cache.allocate(Long.MAX_VALUE));
            assertEquals(0, oom.getSuppressed().length);
            assertThrows(IllegalArgumentException.class, () -> cache.allocate(-1L));

            assertArrayEquals(value, cache.getOrNull("k"));
            assertTrue(cache.put("k2", new byte[] { 4, 5 }));
            assertArrayEquals(new byte[] { 4, 5 }, cache.getOrNull("k2"));
        } finally {
            cache.close();
        }
    }

    /**
     * A capacity of {@code Integer.MAX_VALUE} MB (~2 PB) is impossible to satisfy, so construction
     * must fail rather than return a half-built cache. (It currently fails while sizing the
     * base-class segment table, before {@code allocate()} is reached; the arena-cleanup branch of
     * {@code allocate()} itself is covered by
     * {@link #allocateDirectFailureRethrowsAndLeavesLiveSegmentUsable()}.)
     */
    @Test
    void allocateFailureClosesArenaAndRethrows() {
        assertThrows(OutOfMemoryError.class, () -> {
            final ForeignMemoryOffHeapCache<String, byte[]> cache = new ForeignMemoryOffHeapCache<>(Integer.MAX_VALUE);

            try {
                // unreachable: allocation of ~2 PB must fail during construction.
                cache.put("k", new byte[1]);
            } finally {
                cache.close();
            }
        });
    }

    /**
     * Documented full-constructor contract: unlike {@code Builder.build()}, the constructor performs no
     * {@code 0}-to-default substitution for {@code maxBlockSize}, so {@code 0} is rejected. The
     * invalid argument is detected before the arena is created, so the supplied store stays open
     * and owned by the caller.
     */
    @Test
    void fullConstructorZeroMaxBlockSizeRejectedWithoutClosingStore() {
        final java.util.concurrent.atomic.AtomicBoolean storeClosed = new java.util.concurrent.atomic.AtomicBoolean();
        final OffHeapStore<String> store = new OffHeapStore<>() {
            @Override
            public byte[] get(final String key) {
                return null;
            }

            @Override
            public boolean put(final String key, final byte[] value) {
                return false;
            }

            @Override
            public boolean remove(final String key) {
                return false;
            }

            @Override
            public void close() {
                storeClosed.set(true);
            }
        };

        assertThrows(IllegalArgumentException.class,
                () -> new ForeignMemoryOffHeapCache<String, byte[]>(1, 0, 0, 0, 0, 0f, null, null, store, false, null, null));
        assertFalse(storeClosed.get());
    }
}
