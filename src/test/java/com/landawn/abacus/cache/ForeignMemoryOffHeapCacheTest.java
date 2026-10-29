/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

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

    @Test
    void builderUsesTheConfiguredSerializationCallbacks() {
        final AtomicInteger writes = new AtomicInteger();
        final AtomicInteger reads = new AtomicInteger();
        final ForeignMemoryOffHeapCache<String, String> cache = ForeignMemoryOffHeapCache.<String, String> builder()
                .capacityInMB(1)
                .serializer((value, output) -> {
                    writes.incrementAndGet();
                    final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                    output.write(bytes, 0, bytes.length);
                })
                .deserializer((bytes, type) -> {
                    reads.incrementAndGet();
                    return new String(bytes, StandardCharsets.UTF_8);
                })
                .build();
        try {
            assertTrue(cache.put("key", "custom payload"));
            assertEquals("custom payload", cache.getOrNull("key"));
            assertEquals(1, writes.get());
            assertEquals(1, reads.get());
        } finally {
            cache.close();
        }
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

    /**
     * Documented full-constructor contract: unlike {@code Builder.build()}, the constructor performs no
     * {@code 0}-to-default substitution for {@code maxBlockSize}, so {@code 0} is rejected. The
     * invalid argument is detected before the arena is created, so the supplied store stays open
     * and owned by the caller.
     */
    @Test
    void fullConstructorZeroMaxBlockSizeRejectedWithoutClosingStore() {
        final AtomicBoolean storeClosed = new AtomicBoolean();
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

    // Allocation and cleanup

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

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void allocationFailureRetainsItsIdentityWhenArenaCleanupAlsoFails(final boolean reusedFailure) {
        final ForeignMemoryOffHeapCache<String, byte[]> cache = ForeignMemoryOffHeapCache.<String, byte[]> builder().capacityInMB(1).build();
        final IllegalStateException allocationFailure = new IllegalStateException("allocation failed");
        final IllegalStateException cleanupFailure = reusedFailure ? allocationFailure : new IllegalStateException("arena close failed");
        try (Arena actual = Arena.ofShared()) {
            final Arena failing = Mockito.spy(actual);
            Mockito.doThrow(allocationFailure).when(failing).allocate(64L);
            Mockito.doThrow(cleanupFailure).when(failing).close();
            try (MockedStatic<Arena> arenas = Mockito.mockStatic(Arena.class)) {
                arenas.when(Arena::ofShared).thenReturn(failing);
                assertSame(allocationFailure, assertThrows(IllegalStateException.class, () -> cache.allocate(64)));
            }
            assertEquals(reusedFailure ? 0 : 1, allocationFailure.getSuppressed().length);
            if (!reusedFailure) {
                assertSame(cleanupFailure, allocationFailure.getSuppressed()[0]);
            }
            assertTrue(cache.put("key", new byte[] { 1, 2 }));
            assertArrayEquals(new byte[] { 1, 2 }, cache.getOrNull("key"));
        } finally {
            cache.close();
        }
    }

    @Test
    void deallocationBeforeArenaInitializationIsHarmless() {
        final ForeignMemoryOffHeapCache<?, ?> uninitialized = Mockito.mock(ForeignMemoryOffHeapCache.class, Mockito.CALLS_REAL_METHODS);
        assertDoesNotThrow(uninitialized::deallocate);
    }

    /** {@code close()} releases the arena, and a repeated {@code close()} must not try to close it again. */
    @Test
    void closeReleasesTheArenaExactlyOnce() throws Exception {
        final ForeignMemoryOffHeapCache<String, byte[]> cache = ForeignMemoryOffHeapCache.<String, byte[]> builder().capacityInMB(1).build();
        final Field arenaField = ForeignMemoryOffHeapCache.class.getDeclaredField("arena");
        arenaField.setAccessible(true);
        final Arena arena = (Arena) arenaField.get(cache);

        try {
            assertTrue(arena.scope().isAlive());
        } finally {
            cache.close();
        }

        assertFalse(arena.scope().isAlive());
        assertDoesNotThrow(cache::close);
    }

    /**
     * Fills a two-segment region up to its last 64-byte slot: 16384 slots per segment is the largest
     * slot index the 14-bit slot handle encodes, and the final copy ends exactly at the end of the
     * segment. The bounds-checked copy rejects any offset past the region, and distinct per-entry
     * payloads expose any overlapping or misplaced slot.
     */
    @Test
    void regionFilledToItsLastSlotRoundTripsEveryEntry() {
        final ForeignMemoryOffHeapCache<String, byte[]> cache = ForeignMemoryOffHeapCache.<String, byte[]> builder().capacityInMB(2).build();
        final int slotCount = 2 * AbstractOffHeapCache.SEGMENT_SIZE / AbstractOffHeapCache.MIN_BLOCK_SIZE;

        try {
            for (int i = 0; i < slotCount; i++) {
                assertTrue(cache.put("k" + i, distinctSlotPayload(i)), "slot " + i);
            }

            final OffHeapCacheStats stats = cache.stats();
            assertEquals(stats.allocatedMemory(), stats.occupiedMemory());
            assertEquals(Map.of(64, Map.of(0, slotCount / 2, 1, slotCount / 2)), stats.occupiedSlots());

            for (int i = 0; i < slotCount; i++) {
                assertArrayEquals(distinctSlotPayload(i), cache.getOrNull("k" + i), "slot " + i);
            }
        } finally {
            cache.close();
        }
    }

    /** A 64-byte payload whose first four bytes encode {@code i}, so every entry's bytes differ. */
    private static byte[] distinctSlotPayload(final int i) {
        final byte[] bytes = new byte[AbstractOffHeapCache.MIN_BLOCK_SIZE];

        for (int j = 0; j < bytes.length; j++) {
            bytes[j] = (byte) ((i >>> (j % 4) * 8) ^ j);
        }

        return bytes;
    }

}
