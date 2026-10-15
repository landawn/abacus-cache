/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static com.landawn.abacus.TestSupport.awaitCondition;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.pool.ActivityPrint;
import com.landawn.abacus.type.ByteBufferType;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.N;

class AbstractOffHeapCacheTest {

    @Nested
    class ConstructionAndNativeAccess {

        @ParameterizedTest
        @CsvSource({ "0,false,false", "10000,true,true", "10000,true,false" })
        void failedShutdownHookRegistrationReleasesAllAcquiredResources(final long evictDelay, final boolean withStore, final boolean storeCloseFails)
                throws Exception {
            Class.forName(AbstractOffHeapCache.class.getName());
            final Logger logger = mock(Logger.class);
            final Runtime runtime = mock(Runtime.class);
            final IllegalStateException registrationFailure = new IllegalStateException("JVM shutting down");
            final IllegalStateException storeFailure = new IllegalStateException("store close failed");
            doThrow(registrationFailure).when(runtime).addShutdownHook(any(Thread.class));
            final OffHeapStore<String> store = withStore ? mock(OffHeapStore.class) : null;
            if (storeCloseFails) {
                doThrow(storeFailure).when(store).close();
            }
            final AtomicInteger allocations = new AtomicInteger();
            final AtomicInteger releases = new AtomicInteger();
            try (MockedStatic<Runtime> runtimes = Mockito.mockStatic(Runtime.class)) {
                runtimes.when(Runtime::getRuntime).thenReturn(runtime);
                final IllegalStateException failure = assertThrows(IllegalStateException.class,
                        () -> new AbstractOffHeapCache<String, byte[]>(1, 1024, evictDelay, 60_000, 0, 0f, 0, null, null, store, false, null, null, logger) {
                            @Override
                            protected long allocate(final long bytes) {
                                allocations.incrementAndGet();
                                return 0;
                            }

                            @Override
                            protected void deallocate() {
                                releases.incrementAndGet();
                            }

                            @Override
                            protected void copyToMemory(final long pointer, final byte[] bytes, final int offset, final int length) {
                                // No entry is installed before shutdown-hook registration.
                            }

                            @Override
                            protected void copyFromMemory(final long pointer, final byte[] bytes, final int offset, final int length) {
                                // Construction failure cannot read an entry.
                            }
                        });
                assertSame(registrationFailure, failure);
                assertEquals(1, allocations.get());
                assertEquals(1, releases.get());
                assertEquals(storeCloseFails ? 1 : 0, failure.getSuppressed().length);
                if (withStore) {
                    if (storeCloseFails) {
                        assertSame(storeFailure, failure.getSuppressed()[0]);
                    }
                    verify(store).close();
                }
            }
        }

        @ParameterizedTest
        @ValueSource(ints = { 1023, 1048577 })
        void rejectsBlockSizesOutsideTheAllocatorRange(final int maxBlockSize) {
            assertThrows(IllegalArgumentException.class, () -> OffHeapCache.builder().capacityInMB(1).maxBlockSizeInBytes(maxBlockSize).build());
        }

        @ParameterizedTest
        @ValueSource(floats = { -0.1f, 1.1f, Float.NaN })
        void rejectsVacatingFactorsOutsideTheValidFraction(final float factor) {
            assertThrows(IllegalArgumentException.class, () -> OffHeapCache.builder().capacityInMB(1).vacatingFactor(factor).build());
        }

        @Test
        void nativeCopyHooksRejectNullArraysBeforeCopying() {
            for (final boolean useForeignMemory : new boolean[] { false, true }) {
                final AbstractOffHeapCache<String, byte[]> cache = useForeignMemory
                        ? ForeignMemoryOffHeapCache.<String, byte[]> builder().capacityInMB(1).build()
                        : OffHeapCache.<String, byte[]> builder().capacityInMB(1).build();
                try {
                    final byte[] expected = { 21 };
                    cache.copyToMemory(cache.baseAddress, expected, 0, expected.length);

                    // Zero-length copies never access a native address even if either null guard regresses.
                    final IllegalArgumentException missingSource = assertThrows(IllegalArgumentException.class,
                            () -> cache.copyToMemory(cache.baseAddress, null, 0, 0));
                    assertTrue(missingSource.getMessage().contains("srcBytes"));
                    final IllegalArgumentException missingDestination = assertThrows(IllegalArgumentException.class,
                            () -> cache.copyFromMemory(cache.baseAddress, null, 0, 0));
                    assertTrue(missingDestination.getMessage().contains("bytes"));

                    cache.copyToMemory(cache.baseAddress, new byte[0], 0, 0);
                    cache.copyFromMemory(cache.baseAddress, new byte[0], 0, 0);
                    final byte[] actual = new byte[expected.length];
                    cache.copyFromMemory(cache.baseAddress, actual, 0, actual.length);
                    assertArrayEquals(expected, actual);
                } finally {
                    cache.close();
                }
            }
        }

        /**
         * A {@code null} logger is rejected eagerly with {@link IllegalArgumentException} before any
         * native memory is allocated, while {@code null} serializer, deserializer, store, promotion
         * tester, and store selector remain accepted as "use the default / feature disabled".
         */
        @Test
        void constructorRejectsNullLoggerBeforeAllocationButAcceptsNullOptionalCallbacks() {
            final AtomicBoolean allocated = new AtomicBoolean();

            assertThrows(IllegalArgumentException.class, () -> new AbstractOffHeapCache<String, byte[]>(1, AbstractOffHeapCache.DEFAULT_MAX_BLOCK_SIZE, 0,
                    60_000L, 60_000L, AbstractOffHeapCache.DEFAULT_VACATING_FACTOR, 0, null, null, null, false, null, null, null) {
                @Override
                protected long allocate(final long capacityInBytes) {
                    allocated.set(true);
                    return 0L;
                }

                @Override
                protected void deallocate() {
                }

                @Override
                protected void copyToMemory(final long startPtr, final byte[] bytes, final int srcOffset, final int len) {
                }

                @Override
                protected void copyFromMemory(final long startPtr, final byte[] bytes, final int destOffset, final int len) {
                }
            });

            assertFalse(allocated.get(), "validation must fail before native memory is allocated");

            final GuardedOffHeapCache cache = new GuardedOffHeapCache();
            try {
                assertNotNull(cache.serializer);
                assertNotNull(cache.deserializer);
                assertNull(cache.offHeapStore);
                assertNull(cache.testerForLoadingItemFromDiskToMemory);
                assertNull(cache.storeSelector);
            } finally {
                cache.close();
            }
        }
    }

    @Nested
    class Allocation {

        @Test
        void statisticsSaturateTheCombinedReadCountWithoutOverflow() throws Exception {
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder().capacityInMB(1).build();
            try {
                for (final String counterName : new String[] { "hitCount", "missCount" }) {
                    final Field counter = AbstractOffHeapCache.class.getDeclaredField(counterName);
                    counter.setAccessible(true);
                    ((LongAdder) counter.get(cache)).add(Long.MAX_VALUE);
                }
                final OffHeapCacheStats stats = cache.stats();
                assertEquals(Long.MAX_VALUE, stats.getCount());
                assertEquals(Long.MAX_VALUE, stats.hitCount());
                assertEquals(Long.MAX_VALUE, stats.missCount());
                assertEquals(0.5, stats.hitRate());
                assertEquals(0.5, stats.missRate());
            } finally {
                cache.close();
            }
        }

        /**
         * The {@code stats()} occupied-slot reporting reads each segment's occupied-slot count under
         * the allocator lock. This verifies the reported per-segment occupied-slot total matches the
         * number of in-memory entries.
         */
        @Test
        void statsOccupiedSlotsReflectInMemoryEntries() {
            final OffHeapCache<String, String> cache = OffHeapCache.<String, String> builder()
                    .capacityInMB(16)
                    .evictDelay(0)
                    .defaultLiveTime(600_000)
                    .defaultMaxIdleTime(600_000)
                    .build();
            try {
                for (int i = 0; i < 50; i++) {
                    assertTrue(cache.put("k" + i, "v" + i));
                }

                final OffHeapCacheStats stats = cache.stats();
                assertEquals(50, stats.size(), "in-memory entry count");
                assertEquals(50, totalOccupiedSlots(stats), "sum of per-segment occupied slots should equal the entry count");
            } finally {
                cache.close();
            }
        }

        /**
         * {@code clear()} removes every entry and then reclaims the now-empty segments; afterwards no
         * occupied slots may remain in the stats snapshot.
         */
        @Test
        void clearReleasesAllSegments() {
            final OffHeapCache<String, String> cache = OffHeapCache.<String, String> builder()
                    .capacityInMB(16)
                    .evictDelay(0)
                    .defaultLiveTime(600_000)
                    .defaultMaxIdleTime(600_000)
                    .build();
            try {
                for (int i = 0; i < 50; i++) {
                    assertTrue(cache.put("k" + i, "v" + i));
                }
                assertTrue(totalOccupiedSlots(cache.stats()) > 0);

                cache.clear();

                final OffHeapCacheStats stats = cache.stats();
                assertEquals(0, stats.size(), "cache should be empty after clear()");
                assertEquals(0, totalOccupiedSlots(stats), "all segments should be released after clear()");
            } finally {
                cache.close();
            }
        }

        /**
         * Exercises allocation across multiple slot-size classes, each with its own vacancy queue in
         * {@code segmentQueues}. Those queues are created on a class's first use, under the allocator
         * lock (not via the unsafe lazy double-checked locking of an old version), so writes spanning
         * several size classes must all succeed and round-trip.
         */
        @Test
        void allocationAcrossMultipleSizeClasses() {
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                    .capacityInMB(32)
                    .evictDelay(0)
                    .defaultLiveTime(600_000)
                    .defaultMaxIdleTime(600_000)
                    .build();
            try {
                // A handful of distinct slot-size classes (each maps to its own segment queue). The count
                // is kept well under the number of available 1MB segments so capacity is not exhausted.
                final int[] sizes = { 50, 200, 800, 2000, 5000, 8000 };

                for (int s = 0; s < sizes.length; s++) {
                    for (int j = 0; j < 3; j++) {
                        assertTrue(cache.put("k" + s + "_" + j, new byte[sizes[s]]), "put should succeed for size " + sizes[s]);
                    }
                }

                for (int s = 0; s < sizes.length; s++) {
                    for (int j = 0; j < 3; j++) {
                        final byte[] value = cache.getOrNull("k" + s + "_" + j);
                        assertEquals(sizes[s], value.length, "value should round-trip with its original length");
                    }
                }
            } finally {
                cache.close();
            }
        }

        /**
         * The chunk-count arithmetic must not overflow int for serialized sizes near
         * {@code Integer.MAX_VALUE} (legal in a sufficiently large cache): computing
         * {@code size + maxBlockSize - 1} in int arithmetic wraps negative, which made {@code put}
         * fail with {@code NegativeArraySizeException} instead of storing the value.
         */
        @Test
        void chunkCountDoesNotOverflowForHugeSizes() throws Exception {
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder().capacityInMB(1).evictDelay(0).build();
            try {
                final Method chunkCountOf = AbstractOffHeapCache.class.getDeclaredMethod("chunkCountOf", int.class);
                chunkCountOf.setAccessible(true);

                final int hugeSize = Integer.MAX_VALUE - 2;
                final int expectedChunks = (int) (((long) hugeSize + AbstractOffHeapCache.DEFAULT_MAX_BLOCK_SIZE - 1)
                        / AbstractOffHeapCache.DEFAULT_MAX_BLOCK_SIZE);

                assertEquals(expectedChunks, chunkCountOf.invoke(cache, hugeSize));
            } finally {
                cache.close();
            }
        }

        /**
         * A failed multi-class allocation followed by a successful disk fallback must not leave its
         * now-empty segment dedicated to the attempted class; the very next differently sized memory
         * put must be able to reuse it without waiting for an asynchronous vacate pass.
         */
        @Test
        void partialAllocationRollbackImmediatelyReclaimsEmptySegments() {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();

            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                    .capacityInMB(2)
                    .maxBlockSizeInBytes(512 * 1024)
                    .evictDelay(0)
                    .offHeapStore(newInMemoryStore(backing))
                    .storeSelector((key, value, size) -> "spill".equals(key) ? 0 : 1)
                    .build();
            try {
                // Segment 0 is dedicated to the 128-byte class, so only segment 1 is free.
                assertTrue(cache.put("filler", new byte[100]));

                // One 512 KiB chunk dedicates the remaining free segment; the final one-byte chunk
                // needs the 64-byte class, which has no segment and no free segment left, so memory
                // placement rolls back and the value spills to disk. (The value could fit an empty
                // two-segment cache, so the memory attempt is genuinely made.)
                assertTrue(cache.put("spill", new byte[512 * 1024 + 1]));
                assertEquals(1L, cache.stats().sizeOnDisk());
                // Allocation also reclaims empty segments on demand, so pin the rollback's own
                // reclamation directly: the attempted class must not keep the emptied segment.
                assertFalse(cache.stats().occupiedSlots().containsKey(512 * 1024), "the rollback must return the emptied segment to the free pool");

                assertTrue(cache.put("small", new byte[] { 7 }), "the rollback must make the emptied segment immediately reusable");
                assertArrayEquals(new byte[] { 7 }, cache.getOrNull("small"));
                assertArrayEquals(new byte[100], cache.getOrNull("filler"));
            } finally {
                cache.close();
            }
        }

        /**
         * A segment emptied by an explicit {@code remove()} stays dedicated to its slot class until it
         * is reclaimed. When a differently sized put finds no free segment, the allocator must reclaim
         * that empty segment on demand rather than fail the put - which, without a disk store, would
         * also schedule a vacate pass that evicts unrelated live entries.
         */
        @Test
        void allocationReclaimsEmptySegmentOfAnotherClassOnDemand() throws InterruptedException {
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder().capacityInMB(2).evictDelay(0).build();
            try {
                assertTrue(cache.put("live", new byte[100])); // segment 0: 128-byte class
                assertTrue(cache.put("removed", new byte[100 * 8192])); // segment 1: 8192-byte class
                cache.remove("removed"); // segment 1 is now empty but still dedicated to the 8192-byte class

                assertTrue(cache.put("other-class", new byte[1000]), "the empty segment must be re-dedicated to the 1024-byte class on demand");
                assertArrayEquals(new byte[1000], cache.getOrNull("other-class"));

                // Give an (erroneously) scheduled vacate pass time to run before checking for collateral eviction.
                Thread.sleep(200);
                assertArrayEquals(new byte[100], cache.getOrNull("live"), "no live entry may be evicted when an empty segment was available");
                assertEquals(0L, cache.stats().evictionCount());
            } finally {
                cache.close();
            }
        }

        /**
         * The on-demand reclaim is driven by a count of empty-but-dedicated segments, which must stay
         * exact across allocation, release, reuse of an empty segment by its own class, and reclamation.
         */
        @Test
        void emptyDedicatedSegmentCountStaysExact() throws Exception {
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder().capacityInMB(2).evictDelay(0).build();
            final Field countField = AbstractOffHeapCache.class.getDeclaredField("emptyDedicatedSegmentCount");
            countField.setAccessible(true);
            try {
                assertTrue(cache.put("a", new byte[100]));
                assertTrue(cache.put("b", new byte[1000]));
                assertEquals(0, countField.getInt(cache));

                cache.remove("a");
                assertEquals(1, countField.getInt(cache));

                assertTrue(cache.put("a2", new byte[100])); // same class reuses its own empty segment
                assertEquals(0, countField.getInt(cache));

                cache.remove("a2");
                cache.remove("b");
                assertEquals(2, countField.getInt(cache));

                cache.clear();
                assertEquals(0, countField.getInt(cache));
                assertTrue(cache.stats().occupiedSlots().isEmpty());
            } finally {
                cache.close();
            }
        }

        /** Failed entry construction/copy must release and reclaim all unpublished slots. */
        @Test
        void initialCopyFailureDoesNotStrandSegment() {
            final FailingCopyOffHeapCache cache = new FailingCopyOffHeapCache();
            try {
                assertThrows(IllegalStateException.class, () -> cache.put("large", new byte[AbstractOffHeapCache.DEFAULT_MAX_BLOCK_SIZE]));
                assertEquals(0, cache.size());

                assertTrue(cache.put("small", new byte[] { 1 }), "a different size class must reuse the segment immediately after the failed copy");
                assertArrayEquals(new byte[] { 1 }, cache.getOrNull("small"));
            } finally {
                cache.close();
            }
        }

        /**
         * An undeclared checked exception from a key's {@code hashCode} (as code written in Kotlin, or
         * using sneaky throws, can produce) aborts the put's {@code compute} before the new entry is
         * installed. The freshly allocated slots must still be released, exactly as they are for a
         * {@link RuntimeException} or {@link Error}.
         */
        @Test
        void putReleasesFreshSlotsWhenKeyHashCodeThrowsUndeclaredCheckedException() {
            final OffHeapCache<ThrowingHashKey, byte[]> cache = OffHeapCache.<ThrowingHashKey, byte[]> builder().capacityInMB(1).evictDelay(0).build();
            try {
                final ThrowingHashKey key = new ThrowingHashKey();
                // The put's entries.compute is the first operation to hash the key.
                key.throwOnNextHashCalls(1, new IOException("forced hashCode failure"));

                assertThrows(IOException.class, () -> cache.put(key, new byte[] { 1 }));
                assertEquals(0, key.remainingFailures());
                assertTrue(cache.stats().occupiedSlots().isEmpty(), "an aborted put must release its freshly allocated slots");
                assertEquals(0, cache.size());
                assertEquals(0L, cache.stats().putCount());
            } finally {
                cache.close();
            }
        }
    }

    @Nested
    class Serialization {

        // The memory/disk wrappers special-case byte[] and ByteBuffer values before ever calling the
        // configured deserializer, so the raw-type branches of the package-private default DESERIALIZER
        // are only reachable by invoking it directly. These two tests exercise those branches.

        /** {@code DESERIALIZER} returns the supplied array unchanged for a primitive {@code byte[]} type. */
        @Test
        void deserializerPrimitiveByteArray() {
            final byte[] raw = { 1, 2, 3, 4, 5 };
            final Type<?> type = N.typeOf(byte[].class);

            final Object result = AbstractOffHeapCache.DESERIALIZER.apply(raw, type);

            assertSame(raw, result, "primitive byte[] must be returned as-is without copying");
        }

        /** Every buffer type reconstructs as a writable heap buffer, including direct implementation classes. */
        @Test
        void deserializerByteBuffer() {
            final byte[] raw = "byte-buffer-payload".getBytes();
            for (final Class<?> bufferType : new Class<?>[] { ByteBuffer.class, MappedByteBuffer.class, ByteBuffer.allocateDirect(1).getClass(),
                    ByteBuffer.allocateDirect(1).asReadOnlyBuffer().getClass() }) {
                final Object result = AbstractOffHeapCache.DESERIALIZER.apply(raw, N.typeOf(bufferType));
                assertTrue(result instanceof ByteBuffer, "ByteBuffer type must deserialize to a ByteBuffer");
                assertHeapBufferContent(raw, (ByteBuffer) result);
            }
        }

        // Exercise the ByteBuffer serialization branch in put() and the ByteBuffer read branch for
        // each entry form (single-slot, multi-slot, and disk-backed).

        @Test
        void mappedByteBufferMemoryReadsReturnHeapBuffers(@TempDir final Path directory) throws Exception {
            assertMappedByteBufferRoundTrip(directory, false);
        }

        @Test
        void directByteBufferMemoryReadsReturnHeapBuffers() {
            assertDirectByteBufferRoundTrip(false, false);
        }

        @Test
        void heapByteBufferStillReconstructsOnHeapForBothBackends() {
            for (final boolean useForeignMemory : new boolean[] { false, true }) {
                final AbstractOffHeapCache<String, ByteBuffer> cache = useForeignMemory
                        ? ForeignMemoryOffHeapCache.<String, ByteBuffer> builder().capacityInMB(1).build()
                        : OffHeapCache.<String, ByteBuffer> builder().capacityInMB(1).build();
                try {
                    final byte[] expected = { 4, 5, 6 };
                    assertTrue(cache.put("heap", bufferOf(expected)));
                    final ByteBuffer result = cache.getOrNull("heap");
                    assertHeapBufferContent(expected, result);
                } finally {
                    cache.close();
                }
            }
        }

        /** Small ByteBuffer value: stored in a single slot and read back from memory. */
        @Test
        void byteBufferValueSingleSlotRoundtrip() {
            final byte[] data = "hello-single-slot-byte-buffer".getBytes();
            final OffHeapCache<String, ByteBuffer> cache = OffHeapCache.<String, ByteBuffer> builder()
                    .capacityInMB(8)
                    .evictDelay(0)
                    .defaultLiveTime(600_000)
                    .defaultMaxIdleTime(600_000)
                    .build();
            try {
                assertTrue(cache.put("k", bufferOf(data)));

                final ByteBuffer out = cache.getOrNull("k");
                assertNotNull(out);
                assertArrayEquals(data, ByteBufferType.byteArrayOf(out));
            } finally {
                cache.close();
            }
        }

        /**
         * {@code ByteBufferType.byteArrayOf} temporarily moves its argument's position to zero, which
         * invalidates the argument's mark. A cache put is a read-only operation from the caller's point
         * of view, so the cache now extracts bytes from a duplicate and preserves position, limit, and
         * mark on the supplied buffer.
         */
        @Test
        void byteBufferPutPreservesCallerStateAndMark() {
            final byte[] data = "marked-byte-buffer".getBytes();
            final ByteBuffer input = ByteBuffer.allocate(data.length);
            input.put(data);
            input.position(3);
            input.mark();
            input.position(data.length);

            final OffHeapCache<String, ByteBuffer> cache = OffHeapCache.<String, ByteBuffer> builder().capacityInMB(1).evictDelay(0).build();
            try {
                assertTrue(cache.put("k", input));
                assertEquals(data.length, input.position(), "put must preserve the caller's position");
                assertEquals(data.length, input.limit(), "put must preserve the caller's limit");

                input.reset(); // Before the fix this threw InvalidMarkException.
                assertEquals(3, input.position(), "put must preserve the caller's mark");
                assertArrayEquals(data, ByteBufferType.byteArrayOf(cache.getOrNull("k")));
            } finally {
                cache.close();
            }
        }

        /** Large ByteBuffer value (> maxBlockSize): split across slots and reassembled on read. */
        @Test
        void byteBufferValueMultiSlotRoundtrip() {
            final byte[] data = new byte[20_000];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) (i * 31 + 7);
            }
            final OffHeapCache<String, ByteBuffer> cache = OffHeapCache.<String, ByteBuffer> builder()
                    .capacityInMB(16)
                    .evictDelay(0)
                    .defaultLiveTime(600_000)
                    .defaultMaxIdleTime(600_000)
                    .build();
            try {
                assertTrue(cache.put("big", bufferOf(data)));

                final ByteBuffer out = cache.getOrNull("big");
                assertNotNull(out);
                assertArrayEquals(data, ByteBufferType.byteArrayOf(out));
            } finally {
                cache.close();
            }
        }

        /**
         * A zero-length {@code byte[]} value is a legal cache value that exercises the size-0 path of
         * the slot math ({@code slotSizeOfChunk} maps a 0-byte chunk to one {@code MIN_BLOCK_SIZE}
         * slot): the entry must occupy a slot, read back as a non-null empty array (a hit, never
         * mistaken for a miss), and release its slot on removal.
         */
        @Test
        void zeroLengthByteArrayRoundTripInMemory() {
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder().capacityInMB(1).evictDelay(0).build();
            try {
                assertTrue(cache.put("empty", new byte[0]));

                assertTrue(cache.containsKey("empty"));
                final byte[] out = cache.getOrNull("empty");
                assertNotNull(out, "a zero-length value must read back as a hit, not a miss");
                assertEquals(0, out.length);

                final OffHeapCacheStats stats = cache.stats();
                assertEquals(1, stats.size());
                assertEquals(1, totalOccupiedSlots(stats), "a zero-length value still occupies one minimum-size slot");
                assertEquals(0L, stats.dataSize(), "a zero-length value contributes no payload bytes");
                assertEquals(1L, stats.hitCount());

                cache.remove("empty");
                assertEquals(0, totalOccupiedSlots(cache.stats()), "removing the zero-length entry must release its slot");
            } finally {
                cache.close();
            }
        }

        /**
         * A {@link ByteBuffer} at position 0 stores zero payload bytes (the cache stores {@code [0,
         * position)}): it must round-trip as a non-null, zero-capacity buffer rather than a miss.
         */
        @Test
        void zeroPositionByteBufferRoundTrip() {
            final OffHeapCache<String, ByteBuffer> cache = OffHeapCache.<String, ByteBuffer> builder().capacityInMB(1).evictDelay(0).build();
            try {
                assertTrue(cache.put("empty", ByteBuffer.allocate(16))); // position 0 -> zero bytes stored

                final ByteBuffer out = cache.getOrNull("empty");
                assertNotNull(out, "a zero-byte ByteBuffer value must read back as a hit, not a miss");
                assertEquals(0, ByteBufferType.byteArrayOf(out).length);
            } finally {
                cache.close();
            }
        }

        @Test
        void byteBufferEncodingPreservesWrittenBytes() {
            final ByteBufferType bbType = (ByteBufferType) ((Type<?>) com.landawn.abacus.type.TypeFactory.getType(ByteBufferType.BYTE_BUFFER));
            ByteBuffer bb = ByteBuffer.allocate(1024);
            bb.put("abc".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            bb.put("123".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            byte[] bytes = ByteBufferType.byteArrayOf(bb);

            assertEquals("abc123", new String(bytes, java.nio.charset.StandardCharsets.UTF_8));

            bb = ByteBuffer.wrap("abc123".getBytes(java.nio.charset.StandardCharsets.UTF_8), 6, 0);
            bytes = ByteBufferType.byteArrayOf(bb);

            assertEquals("abc123", new String(bytes, java.nio.charset.StandardCharsets.UTF_8));

            bb = ByteBuffer.wrap(N.EMPTY_BYTE_ARRAY);
            assertEquals("", bbType.stringOf(bb));

            bb = ByteBuffer.wrap(N.EMPTY_BYTE_ARRAY, 0, 0);
            assertEquals("", bbType.stringOf(bb));
        }
    }

    @Nested
    class DiskStorage {

        @Test
        void diskTimingIgnoresMemoryReadsAndRejectedStoreWrites() {
            final OffHeapStore<String> store = mock(OffHeapStore.class);
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                    .capacityInMB(1)
                    .offHeapStore(store)
                    .statsTimeOnDisk(true)
                    .storeSelector((key, value, size) -> "memory".equals(key) ? 1 : 2)
                    .build();
            try {
                assertTrue(cache.put("memory", new byte[] { 1 }));
                assertArrayEquals(new byte[] { 1 }, cache.getOrNull("memory"));
                assertFalse(cache.put("disk", new byte[] { 2 }));
                cache.remove("absent");
                assertEquals(1, cache.size());
                assertEquals(1L, cache.stats().putCount());
                assertEquals(0L, cache.stats().putCountToDisk());
                assertEquals(new OffHeapCacheStats.MinMaxAvg(0, 0, 0), cache.stats().readFromDiskTimeStats());
                assertEquals(new OffHeapCacheStats.MinMaxAvg(0, 0, 0), cache.stats().writeToDiskTimeStats());
                verify(store).put("disk", new byte[] { 2 });
            } finally {
                cache.close();
            }
        }

        @Test
        void corruptedStoreValueIsRejectedAndTheMappingCanRecover() {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                    .capacityInMB(1)
                    .offHeapStore(newInMemoryStore(backing))
                    .storeSelector((key, value, size) -> 2)
                    .build();
            try {
                assertTrue(cache.put("key", new byte[] { 1, 2, 3 }));
                backing.put("key", new byte[] { 9 });
                final IllegalStateException failure = assertThrows(IllegalStateException.class, () -> cache.getOrNull("key"));
                assertTrue(failure.getMessage().contains("does not match expected size"));
                backing.put("key", new byte[] { 1, 2, 3 });
                assertArrayEquals(new byte[] { 1, 2, 3 }, cache.getOrNull("key"));
            } finally {
                cache.close();
            }
        }

        /** Invalid custom routing results are programming errors, not undocumented aliases. */
        @ParameterizedTest
        @NullSource
        @ValueSource(ints = { -1, 3 })
        void storeSelectorRejectsUnknownAndNullResults(final Integer selected) {
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                    .capacityInMB(1)
                    .storeSelector((key, value, size) -> selected)
                    .build();
            try {
                assertThrows(IllegalArgumentException.class, () -> cache.put("key", new byte[] { 1 }));
                assertEquals(0, cache.size());
            } finally {
                cache.close();
            }
        }

        /**
         * A failure raised before the per-key atomic replacement begins (here: an exceptional
         * {@code hashCode} thrown by the map lookup itself) must fail the put BEFORE the key-only
         * backing store is mutated; the prior memory entry must survive and the store must remain
         * untouched.
         */
        @Test
        void diskOwnershipFailurePrecedesBackingStoreMutation() {
            final ExceptionalHashKey key = new ExceptionalHashKey();
            final AtomicBoolean storePutCalled = new AtomicBoolean();
            final OffHeapStore<ExceptionalHashKey> store = new OffHeapStore<>() {
                @Override
                public byte[] get(final ExceptionalHashKey ignored) {
                    return null;
                }

                @Override
                public boolean put(final ExceptionalHashKey ignored, final byte[] value) {
                    storePutCalled.set(true);
                    return true;
                }

                @Override
                public boolean remove(final ExceptionalHashKey ignored) {
                    return false;
                }
            };

            final OffHeapCache<ExceptionalHashKey, byte[]> cache = OffHeapCache.<ExceptionalHashKey, byte[]> builder()
                    .capacityInMB(1)
                    .offHeapStore(store)
                    .storeSelector((ignored, value, size) -> value.length == 1 ? 1 : 2)
                    .build();
            try {
                final byte[] prior = { 1 };
                assertTrue(cache.put(key, prior));

                // The disk-routed replacement's first (and only) hashCode call is the entry map's
                // atomic compute; a throwing hashCode fails the put before its lambda - and therefore
                // before any store mutation - runs.
                key.throwOnHashCall(1);
                final IllegalStateException failure = assertThrows(IllegalStateException.class, () -> cache.put(key, new byte[] { 2, 3 }));

                assertTrue(failure.getMessage().contains("forced hash failure"));
                assertFalse(storePutCalled.get(), "the backing bytes must not be mutated when ownership registration fails");
                assertArrayEquals(prior, cache.getOrNull(key), "the previous memory entry must be restored");
                assertEquals(0L, cache.stats().sizeOnDisk());
            } finally {
                cache.close();
            }
        }

        /**
         * With {@code statsTimeOnDisk} enabled, the disk timing statistics cover the
         * {@link OffHeapStore} call. A store whose operations take a known minimum time must produce
         * observations at least that large (only a lower bound is asserted, so the test is robust to
         * scheduling delays), with {@code min <= avg <= max} holding.
         */
        @Test
        void diskTimingStatsReflectStoreIo() {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();
            final OffHeapStore<String> slowStore = new OffHeapStore<>() {
                private void sleep() {
                    try {
                        Thread.sleep(15L);
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                @Override
                public byte[] get(final String key) {
                    sleep();
                    return backing.get(key);
                }

                @Override
                public boolean put(final String key, final byte[] value) {
                    sleep();
                    backing.put(key, value);
                    return true;
                }

                @Override
                public boolean remove(final String key) {
                    return backing.remove(key) != null;
                }
            };

            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                    .capacityInMB(1)
                    .evictDelay(0)
                    .offHeapStore(slowStore)
                    .storeSelector((k, v, size) -> 2)
                    .statsTimeOnDisk(true)
                    .build();
            try {
                assertTrue(cache.put("key", new byte[] { 1, 2, 3 }));
                assertArrayEquals(new byte[] { 1, 2, 3 }, cache.getOrNull("key"));

                final OffHeapCacheStats stats = cache.stats();
                assertTrue(stats.writeToDiskTimeStats().min() >= 10.0D, "write timing must cover the store write: " + stats.writeToDiskTimeStats());
                assertTrue(stats.readFromDiskTimeStats().min() >= 10.0D, "read timing must cover the store read: " + stats.readFromDiskTimeStats());
                assertTrue(stats.writeToDiskTimeStats().min() <= stats.writeToDiskTimeStats().avg()
                        && stats.writeToDiskTimeStats().avg() <= stats.writeToDiskTimeStats().max());
            } finally {
                cache.close();
            }
        }

        /**
         * A disk lookup that finds an already-freed entry (the state a getOrNull racing an eviction
         * observes just after its map lookup) must read as a miss WITHOUT touching the store: the
         * freed entry's store bytes may already belong to nobody (or, under a same-key replacement,
         * to a newer entry).
         */
        @Test
        void freedDiskEntryReadsAsMissWithoutTouchingStore() throws Exception {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();
            final AtomicInteger storeReads = new AtomicInteger();
            final OffHeapStore<String> store = new OffHeapStore<>() {
                @Override
                public byte[] get(final String key) {
                    storeReads.incrementAndGet();
                    return backing.get(key);
                }

                @Override
                public boolean put(final String key, final byte[] value) {
                    backing.put(key, value);
                    return true;
                }

                @Override
                public boolean remove(final String key) {
                    return backing.remove(key) != null;
                }
            };

            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                    .capacityInMB(1)
                    .evictDelay(0)
                    .offHeapStore(store)
                    .storeSelector((k, v, size) -> 2)
                    .build();
            try {
                assertTrue(cache.put("key", new byte[] { 1 }));
                assertArrayEquals(new byte[] { 1 }, cache.getOrNull("key"));
                assertEquals(1, storeReads.get());

                // Mark the disk entry freed in place while the map still holds it.
                final AbstractOffHeapCache.Entry<byte[]> entry = entriesOf(cache).get("key");
                assertNotNull(entry);
                entry.freed = true;

                assertNull(cache.getOrNull("key"), "a freed disk entry must read as a miss");
                assertEquals(1, storeReads.get(), "the store-less early return must not touch the store");
            } finally {
                cache.close();
            }
        }

        /** Disk-routed ByteBuffer value: read back from the store and deserialized. */
        @Test
        void byteBufferValueDiskStoreRoundtrip() {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();
            final byte[] data = "disk-resident-byte-buffer-payload".getBytes();
            final OffHeapCache<String, ByteBuffer> cache = OffHeapCache.<String, ByteBuffer> builder()
                    .capacityInMB(8)
                    .evictDelay(0)
                    .defaultLiveTime(600_000)
                    .defaultMaxIdleTime(600_000)
                    .offHeapStore(newInMemoryStore(backing))
                    .storeSelector((k, v, size) -> 2) // disk only
                    .build();
            try {
                assertTrue(cache.put("k", bufferOf(data)));
                assertEquals(1L, cache.stats().sizeOnDisk());

                final ByteBuffer out = cache.getOrNull("k");
                assertNotNull(out);
                assertArrayEquals(data, ByteBufferType.byteArrayOf(out));
            } finally {
                cache.close();
            }
        }

        /** A store may retain and return its own array; disk reads must never expose that mutable array. */
        @Test
        void diskReadReturnsDefensiveCopy() {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();
            final byte[] expected = { 1, 2, 3, 4 };

            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                    .capacityInMB(1)
                    .evictDelay(0)
                    .offHeapStore(newInMemoryStore(backing))
                    .storeSelector((k, v, size) -> 2)
                    .build();
            try {
                assertTrue(cache.put("k", expected));

                final byte[] firstRead = cache.getOrNull("k");
                firstRead[0] = 99;

                assertArrayEquals(expected, cache.getOrNull("k"), "mutating a returned value must not alter the disk-resident cache entry");
                assertArrayEquals(expected, backing.get("k"), "the store's retained array must remain private");
            } finally {
                cache.close();
            }
        }

        /** A failed custom deserialization is not a successful disk hit. */
        @Test
        void throwingDeserializerDoesNotIncrementDiskHitCount() {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();

            final OffHeapCache<String, String> cache = OffHeapCache.<String, String> builder()
                    .capacityInMB(1)
                    .evictDelay(0)
                    .offHeapStore(newInMemoryStore(backing))
                    .storeSelector((k, v, size) -> 2)
                    .statsTimeOnDisk(true)
                    .serializer((value, output) -> {
                        final byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        output.write(bytes, 0, bytes.length);
                    })
                    .deserializer((bytes, type) -> {
                        throw new IllegalStateException("cannot deserialize");
                    })
                    .build();
            try {
                assertTrue(cache.put("k", "value"));
                assertThrows(IllegalStateException.class, () -> cache.getOrNull("k"));
                assertEquals(0L, cache.stats().hitCountFromDisk());
            } finally {
                cache.close();
            }
        }

        /**
         * A same-key disk replacement spans both the store and the entry map. The whole transition
         * runs inside the map's per-key atomic compute, so a second put cannot overwrite the bytes
         * while the first put is paused retiring its prior entry - the final mapping and the store
         * bytes always belong to the same put.
         */
        @Test
        void concurrentSameKeyDiskPutsAreAtomicAcrossStoreAndPool() throws Exception {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();
            final AtomicBoolean routeToDisk = new AtomicBoolean(false);
            final AtomicInteger diskWriteCount = new AtomicInteger();
            final CountDownLatch firstDiskWrite = new CountDownLatch(1);
            final CountDownLatch secondDiskWrite = new CountDownLatch(1);
            final CountDownLatch priorWrapperLocked = new CountDownLatch(1);
            final CountDownLatch releasePriorWrapper = new CountDownLatch(1);
            final CountDownLatch secondPutStarted = new CountDownLatch(1);
            final ExecutorService executor = Executors.newFixedThreadPool(2);

            final OffHeapStore<String> store = new OffHeapStore<>() {
                @Override
                public boolean put(final String key, final byte[] value) {
                    backing.put(key, value);
                    if (diskWriteCount.incrementAndGet() == 1) {
                        firstDiskWrite.countDown();
                    } else {
                        secondDiskWrite.countDown();
                    }
                    return true;
                }

                @Override
                public byte[] get(final String key) {
                    return backing.get(key);
                }

                @Override
                public boolean remove(final String key) {
                    return backing.remove(key) != null;
                }
            };

            try {
                final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                        .capacityInMB(1)
                        .evictDelay(0)
                        .offHeapStore(store)
                        .storeSelector((k, v, size) -> routeToDisk.get() ? 2 : 1)
                        .build();

                try {
                    assertTrue(cache.put("k", new byte[] { 0 }));
                    final AbstractOffHeapCache.Entry<byte[]> priorWrapper = entriesOf(cache).get("k");

                    final Thread monitorHolder = new Thread(() -> {
                        synchronized (priorWrapper) {
                            priorWrapperLocked.countDown();
                            try {
                                assertTrue(releasePriorWrapper.await(10, TimeUnit.SECONDS));
                            } catch (final InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }, "offheap-test-prior-wrapper-holder");
                    monitorHolder.start();
                    assertTrue(priorWrapperLocked.await(5, TimeUnit.SECONDS));
                    routeToDisk.set(true);

                    final byte[] first = { 1, 1, 1 };
                    final byte[] second = { 2, 2, 2, 2 };
                    final Future<Boolean> firstResult = executor.submit(() -> cache.put("k", first));
                    assertTrue(firstDiskWrite.await(5, TimeUnit.SECONDS));
                    final Future<Boolean> secondResult = executor.submit(() -> {
                        secondPutStarted.countDown();
                        return cache.put("k", second);
                    });
                    assertTrue(secondPutStarted.await(5, TimeUnit.SECONDS));

                    final boolean secondWriteOverlappedFirst;
                    try {
                        secondWriteOverlappedFirst = secondDiskWrite.await(750, TimeUnit.MILLISECONDS);
                    } finally {
                        releasePriorWrapper.countDown();
                    }

                    assertTrue(firstResult.get(5, TimeUnit.SECONDS));
                    assertTrue(secondResult.get(5, TimeUnit.SECONDS));
                    monitorHolder.join(5_000L);
                    assertFalse(secondWriteOverlappedFirst, "same-key disk puts must not interleave their store/pool transitions");
                    assertArrayEquals(second, cache.getOrNull("k"));
                } finally {
                    cache.close();
                }
            } finally {
                releasePriorWrapper.countDown();
                executor.shutdownNow();
            }
        }

        /** A stale disk miss must not detach and reinsert a concurrently installed replacement. */
        @Test
        void staleDiskReadDoesNotReinsertConcurrentReplacement() throws Exception {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();
            final AtomicBoolean blockFirstRead = new AtomicBoolean(true);
            final CountDownLatch firstReadEntered = new CountDownLatch(1);
            final CountDownLatch releaseFirstRead = new CountDownLatch(1);
            final ExecutorService executor = Executors.newFixedThreadPool(2);

            final OffHeapStore<String> store = new OffHeapStore<>() {
                @Override
                public boolean put(final String key, final byte[] value) {
                    backing.put(key, value);
                    return true;
                }

                @Override
                public byte[] get(final String key) {
                    if (blockFirstRead.compareAndSet(true, false)) {
                        firstReadEntered.countDown();
                        try {
                            assertTrue(releaseFirstRead.await(10, TimeUnit.SECONDS));
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return null;
                    }

                    return backing.get(key);
                }

                @Override
                public boolean remove(final String key) {
                    return backing.remove(key) != null;
                }
            };

            try {
                final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                        .capacityInMB(1)
                        .evictDelay(0)
                        .offHeapStore(store)
                        .storeSelector((k, v, size) -> 2)
                        .build();

                try {
                    assertTrue(cache.put("k", new byte[] { 1 }));
                    final Future<byte[]> staleRead = executor.submit(() -> cache.getOrNull("k"));
                    assertTrue(firstReadEntered.await(5, TimeUnit.SECONDS));

                    final byte[] replacement = { 2, 2 };
                    final AtomicReference<Thread> replacementThread = new AtomicReference<>();
                    final Future<Boolean> replacementPut = executor.submit(() -> {
                        replacementThread.set(Thread.currentThread());
                        return cache.put("k", replacement);
                    });
                    com.landawn.abacus.TestSupport.awaitCondition("replacement to block behind the stale read",
                            () -> replacementThread.get() != null && replacementThread.get().getState() == Thread.State.BLOCKED);
                    releaseFirstRead.countDown();

                    assertEquals(null, staleRead.get(5, TimeUnit.SECONDS));
                    assertTrue(replacementPut.get(5, TimeUnit.SECONDS));
                    assertEquals(2L, cache.stats().putCount(), "stale cleanup must not count a remove/reinsert of the replacement as another put");
                    assertArrayEquals(replacement, cache.getOrNull("k"));
                } finally {
                    releaseFirstRead.countDown();
                    cache.close();
                }
            } finally {
                releaseFirstRead.countDown();
                executor.shutdownNow();
            }
        }

        /**
         * Replacing a memory-backed entry with a disk-routed value: {@code putToDisk} removes and
         * destroys the prior in-memory wrapper before installing the new {@code StoreWrapper}.
         */
        @Test
        void putToDiskReplacesMemoryWrapper() {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();
            // Small values (< 1000 bytes) go to memory; larger values go to disk.
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                    .capacityInMB(8)
                    .evictDelay(0)
                    .defaultLiveTime(600_000)
                    .defaultMaxIdleTime(600_000)
                    .offHeapStore(newInMemoryStore(backing))
                    .storeSelector((k, v, size) -> size < 1000 ? 1 : 2)
                    .build();
            try {
                assertTrue(cache.put("k", new byte[100])); // memory-backed SlotWrapper
                assertEquals(0L, cache.stats().sizeOnDisk());

                final byte[] large = new byte[5000];
                for (int i = 0; i < large.length; i++) {
                    large[i] = (byte) (i % 251);
                }
                // Replace the same key with a disk-routed value: the prior memory entry is retired.
                assertTrue(cache.put("k", large));
                assertEquals(1L, cache.stats().sizeOnDisk());
                assertArrayEquals(large, cache.getOrNull("k"));
            } finally {
                cache.close();
            }
        }

        /**
         * Deterministic regression for the detached-free race: a {@code remove(key)} whose disk
         * cleanup is delayed (its entry monitor is pinned) must not delete the store bytes a
         * concurrent same-key disk put writes. The remove now frees inside the per-key compute, so
         * the racing put serializes behind it and the final state is the put's value — both in the
         * map and in the store.
         */
        @Test
        void removeRacingSameKeyDiskPutCannotDeleteNewBytes() throws Exception {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();
            final CountDownLatch entryLocked = new CountDownLatch(1);
            final CountDownLatch releaseEntry = new CountDownLatch(1);
            final ExecutorService executor = Executors.newFixedThreadPool(2);

            try {
                final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                        .capacityInMB(1)
                        .evictDelay(0)
                        .offHeapStore(newInMemoryStore(backing))
                        .storeSelector((k, v, size) -> 2)
                        .build();

                try {
                    assertTrue(cache.put("k", new byte[] { 1 }));

                    final AbstractOffHeapCache.Entry<byte[]> priorEntry = entriesOf(cache).get("k");
                    final Thread monitorHolder = new Thread(() -> {
                        synchronized (priorEntry) {
                            entryLocked.countDown();
                            try {
                                assertTrue(releaseEntry.await(10, TimeUnit.SECONDS));
                            } catch (final InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }, "offheap-test-remove-race-holder");
                    monitorHolder.start();
                    assertTrue(entryLocked.await(5, TimeUnit.SECONDS));

                    // The remove enters the per-key compute (owning the bin) and blocks on the pinned
                    // entry monitor; the same-key disk put then queues behind it on the bin.
                    final AtomicReference<Thread> removingThread = new AtomicReference<>();
                    final Future<?> removeResult = executor.submit(() -> {
                        removingThread.set(Thread.currentThread());
                        cache.remove("k");
                    });
                    com.landawn.abacus.TestSupport.awaitCondition("remove to block on the entry monitor",
                            () -> removingThread.get() != null && removingThread.get().getState() == Thread.State.BLOCKED);
                    final byte[] newValue = { 2, 2 };
                    final AtomicReference<Thread> puttingThread = new AtomicReference<>();
                    final Future<Boolean> putResult = executor.submit(() -> {
                        puttingThread.set(Thread.currentThread());
                        return cache.put("k", newValue);
                    });
                    com.landawn.abacus.TestSupport.awaitCondition("put to block behind remove",
                            () -> puttingThread.get() != null && puttingThread.get().getState() == Thread.State.BLOCKED);

                    releaseEntry.countDown();
                    removeResult.get(5, TimeUnit.SECONDS);
                    assertTrue(putResult.get(5, TimeUnit.SECONDS));
                    monitorHolder.join(5_000L);

                    assertArrayEquals(newValue, cache.getOrNull("k"), "the put's value must survive the racing remove's disk cleanup");
                    assertArrayEquals(newValue, backing.get("k"), "the put's store bytes must survive the racing remove's disk cleanup");
                } finally {
                    releaseEntry.countDown();
                    cache.close();
                }
            } finally {
                releaseEntry.countDown();
                executor.shutdownNow();
            }
        }

        /**
         * With a disk store, a put that finds only an empty segment dedicated to another slot class
         * must reuse that segment instead of spilling a value that fits the (empty) memory region.
         */
        @Test
        void emptySegmentOfAnotherClassIsReusedInsteadOfSpillingToDisk() {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                    .capacityInMB(1)
                    .evictDelay(0)
                    .offHeapStore(newInMemoryStore(backing))
                    .build();
            try {
                assertTrue(cache.put("first", new byte[100])); // the only segment: 128-byte class
                cache.remove("first");

                assertTrue(cache.put("other-class", new byte[1000]));

                assertEquals(0L, cache.stats().sizeOnDisk(), "a value that fits the empty memory region must not spill to disk");
                assertTrue(backing.isEmpty());
            } finally {
                cache.close();
            }
        }

        /**
         * A zero-length value routed to the disk tier: the store receives an empty array, the read
         * passes the size consistency check ({@code storeBytes.length == entry.size == 0}), and the
         * value reads back as a non-null empty array counted as a disk hit.
         */
        @Test
        void zeroLengthValueOnDiskTier() {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();

            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                    .capacityInMB(1)
                    .evictDelay(0)
                    .offHeapStore(newInMemoryStore(backing))
                    .storeSelector((k, v, size) -> 2)
                    .build();
            try {
                assertTrue(cache.put("empty", new byte[0]));
                assertEquals(0, backing.get("empty").length, "the store must receive the zero-length payload");
                assertEquals(1L, cache.stats().sizeOnDisk());

                final byte[] out = cache.getOrNull("empty");
                assertNotNull(out, "a zero-length disk-tier value must read back as a hit, not a miss");
                assertEquals(0, out.length);
                assertEquals(1L, cache.stats().hitCountFromDisk());
            } finally {
                cache.close();
            }
        }

        /** An {@link Error} from the store's {@code remove} propagates from remove() and leaves the entry readable and removable. */
        @Test
        void removeStoreErrorLeavesDiskEntryLiveAndRetryable() {
            final Map<String, byte[]> stored = new ConcurrentHashMap<>();
            final AtomicInteger failingRemoves = new AtomicInteger();
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                    .capacityInMB(1)
                    .offHeapStore(newRemoveFailingStore(stored, failingRemoves, new AssertionError("forced store remove failure")))
                    .storeSelector((key, value, size) -> 2)
                    .build();
            try {
                assertTrue(cache.put("k", new byte[] { 1, 2, 3 }));
                failingRemoves.set(1);

                assertThrows(AssertionError.class, () -> cache.remove("k"));
                assertArrayEquals(new byte[] { 1, 2, 3 }, cache.getOrNull("k"), "the entry must stay readable after the failed removal");
                assertEquals(1, cache.size());
                assertEquals(1L, cache.stats().sizeOnDisk());

                cache.remove("k");

                assertNull(cache.getOrNull("k"));
                assertEquals(0, cache.size());
                assertEquals(0L, cache.stats().sizeOnDisk());
                assertFalse(stored.containsKey("k"));
            } finally {
                cache.close();
            }
        }

        /**
         * Replacing a disk entry with a memory-routed value retires the prior entry, removing its store
         * bytes. An {@link Error} from that removal must fail the put without losing the prior entry.
         */
        @Test
        void replacementWhosePriorByteRemovalThrowsErrorKeepsPriorEntry() {
            final Map<String, byte[]> stored = new ConcurrentHashMap<>();
            final AtomicInteger failingRemoves = new AtomicInteger();
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                    .capacityInMB(1)
                    .offHeapStore(newRemoveFailingStore(stored, failingRemoves, new AssertionError("forced store remove failure")))
                    .storeSelector((key, value, size) -> value[0] == 1 ? 2 : 1) // value 1 -> disk only, otherwise memory only
                    .build();
            try {
                assertTrue(cache.put("k", new byte[] { 1 }));
                failingRemoves.set(1);

                assertThrows(AssertionError.class, () -> cache.put("k", new byte[] { 2 }));
                assertArrayEquals(new byte[] { 1 }, cache.getOrNull("k"), "the failed replacement must keep the prior entry");
                assertEquals(1L, cache.stats().sizeOnDisk());
                assertEquals(0L, cache.stats().occupiedMemory(), "the rejected replacement's slots must have been released");

                assertTrue(cache.put("k", new byte[] { 2 }));
                assertArrayEquals(new byte[] { 2 }, cache.getOrNull("k"));
                assertEquals(0L, cache.stats().sizeOnDisk());
                assertFalse(stored.containsKey("k"));
            } finally {
                cache.close();
            }
        }

        /**
         * A checked exception thrown undeclared by the store's {@code remove} is an ordinary removal
         * failure, like a {@link RuntimeException}: it is logged, the mapping is removed, and only the
         * store bytes leak. It used to escape remove() and strand a freed entry in the map.
         */
        @Test
        void removeLogsUndeclaredCheckedStoreExceptionAndDropsMapping() {
            final Map<String, byte[]> stored = new ConcurrentHashMap<>();
            final AtomicInteger failingRemoves = new AtomicInteger();
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                    .capacityInMB(1)
                    .offHeapStore(newRemoveFailingStore(stored, failingRemoves, new IOException("forced store remove failure")))
                    .storeSelector((key, value, size) -> 2)
                    .build();
            try {
                assertTrue(cache.put("k", new byte[] { 1 }));
                failingRemoves.set(1);

                assertDoesNotThrow(() -> cache.remove("k"));
                assertEquals(0, failingRemoves.get());
                assertNull(cache.getOrNull("k"));
                assertEquals(0, cache.size());
                assertEquals(0L, cache.stats().sizeOnDisk());
                assertTrue(stored.containsKey("k"), "the failed byte removal leaks the store bytes, as documented");
            } finally {
                cache.close();
            }
        }
    }

    @Nested
    class Promotion {

        @ParameterizedTest
        @ValueSource(strings = { "full", "oversized", "expired" })
        void rejectedPromotionStillReturnsTheValueReadFromDisk(final String reason) {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                    .capacityInMB(1)
                    .defaultLiveTime(60_000)
                    .offHeapStore(newInMemoryStore(backing))
                    .storeSelector((key, value, size) -> key.equals("disk") ? 2 : 1)
                    .testerForLoadingItemFromDiskToMemory((activity, size, elapsed) -> {
                        if ("expired".equals(reason)) {
                            try {
                                setActivityTime(activity, "createdTime", 0);
                            } catch (final Exception failure) {
                                throw new AssertionError(failure);
                            }
                        }
                        return true;
                    })
                    .build();
            try {
                if ("full".equals(reason)) {
                    assertTrue(cache.put("memory", new byte[AbstractOffHeapCache.SEGMENT_SIZE]));
                }
                final byte[] value = new byte["oversized".equals(reason) ? AbstractOffHeapCache.SEGMENT_SIZE + 1 : 16];
                value[0] = 42;
                assertTrue(cache.put("disk", value));
                assertArrayEquals(value, cache.getOrNull("disk"));
                assertEquals(1, cache.stats().sizeOnDisk());
                if ("expired".equals(reason)) {
                    assertFalse(cache.containsKey("disk"));
                    assertNull(cache.getOrNull("disk"));
                    assertFalse(backing.containsKey("disk"));
                } else {
                    assertArrayEquals(value, backing.get("disk"));
                }
            } finally {
                cache.close();
            }
        }

        @Test
        void mappedByteBufferDiskReadsAndPromotionReturnHeapBuffers(@TempDir final Path directory) throws Exception {
            assertMappedByteBufferRoundTrip(directory, true);
        }

        @Test
        void directByteBufferDiskReadsAndPromotionReturnHeapBuffers() {
            assertDirectByteBufferRoundTrip(true, false);
            assertDirectByteBufferRoundTrip(true, true);
        }

        /** A custom in-place decoder must not alter bytes that a successful disk read promotes. */
        @Test
        void mutatingDeserializerCannotCorruptPromotedBytes() {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();

            final OffHeapCache<String, String> cache = OffHeapCache.<String, String> builder()
                    .capacityInMB(1)
                    .evictDelay(0)
                    .offHeapStore(newInMemoryStore(backing))
                    .storeSelector((key, value, size) -> 2)
                    .testerForLoadingItemFromDiskToMemory((activity, size, elapsed) -> true)
                    .serializer((value, output) -> {
                        final byte[] encoded = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        output.write(encoded, 0, encoded.length);
                    })
                    .deserializer((bytes, type) -> {
                        final String decoded = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
                        Arrays.fill(bytes, (byte) 0); // a legitimate in-place decode/scrub strategy
                        return decoded;
                    })
                    .build();
            try {
                assertTrue(cache.put("key", "value"));
                assertEquals("value", cache.getOrNull("key")); // disk read + promotion
                assertEquals(0L, cache.stats().sizeOnDisk());
                assertEquals("value", cache.getOrNull("key"), "the promoted memory copy must retain the original encoded bytes");
            } finally {
                cache.close();
            }
        }

        // With a promotion tester that always returns true, a disk-only put followed by a get reads the
        // bytes from disk and copies them back into off-heap memory, retiring the on-disk copy.

        /** Promotion of a small disk-stored value back into a single memory slot. */
        @Test
        void diskToMemoryPromotionSingleSlot() {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();
            final byte[] data = new byte[256];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) (i + 1);
            }
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                    .capacityInMB(8)
                    .evictDelay(0)
                    .defaultLiveTime(600_000)
                    .defaultMaxIdleTime(600_000)
                    .offHeapStore(newInMemoryStore(backing))
                    .storeSelector((k, v, size) -> 2) // disk only on put
                    .testerForLoadingItemFromDiskToMemory((activityPrint, size, elapsed) -> true)
                    .build();
            try {
                assertTrue(cache.put("k", data));
                assertEquals(1L, cache.stats().sizeOnDisk());

                // First get reads from disk and promotes the value into a memory slot.
                assertArrayEquals(data, cache.getOrNull("k"));
                // After promotion the on-disk copy is retired.
                assertEquals(0L, cache.stats().sizeOnDisk());
                // Value is still readable from memory.
                assertArrayEquals(data, cache.getOrNull("k"));
            } finally {
                cache.close();
            }
        }

        /** Promotion of a large disk-stored value back into multiple memory slots. */
        @Test
        void diskToMemoryPromotionMultiSlot() {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();
            final byte[] data = new byte[20_000];
            for (int i = 0; i < data.length; i++) {
                data[i] = (byte) (i * 17 + 3);
            }
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                    .capacityInMB(16)
                    .evictDelay(0)
                    .defaultLiveTime(600_000)
                    .defaultMaxIdleTime(600_000)
                    .offHeapStore(newInMemoryStore(backing))
                    .storeSelector((k, v, size) -> 2) // disk only on put
                    .testerForLoadingItemFromDiskToMemory((activityPrint, size, elapsed) -> true)
                    .build();
            try {
                assertTrue(cache.put("big", data));
                assertEquals(1L, cache.stats().sizeOnDisk());

                // First get reads from disk and promotes the value into multiple memory slots.
                assertArrayEquals(data, cache.getOrNull("big"));
                assertEquals(0L, cache.stats().sizeOnDisk());
                assertArrayEquals(data, cache.getOrNull("big"));
            } finally {
                cache.close();
            }
        }

        /** Promotion must carry forward the original entry's remaining TTL, not reset or collapse it. */
        @Test
        void diskToMemoryPromotionPreservesRemainingLiveTime() throws Exception {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();
            final long configuredLiveTime = 5_000L;

            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                    .capacityInMB(1)
                    .evictDelay(0)
                    .offHeapStore(newInMemoryStore(backing))
                    .storeSelector((k, v, size) -> 2)
                    .testerForLoadingItemFromDiskToMemory((activity, size, elapsed) -> true)
                    .build();
            try {
                assertTrue(cache.put("k", new byte[] { 1, 2, 3 }, configuredLiveTime, 10_000L));
                final ActivityPrint original = entriesOf(cache).get("k").activityPrint;
                setActivityTime(original, "createdTime", System.currentTimeMillis() - 75L);
                assertArrayEquals(new byte[] { 1, 2, 3 }, cache.getOrNull("k"));
                assertEquals(0L, cache.stats().sizeOnDisk(), "the entry should have been promoted");

                final AbstractOffHeapCache.Entry<byte[]> promoted = entriesOf(cache).get("k");
                assertEquals(original.getExpirationTime(), promoted.activityPrint.getExpirationTime(), "promotion must preserve the original TTL deadline");
                assertEquals(original.getLastAccessTime(), promoted.activityPrint.getLastAccessTime(), "promotion must preserve the latest idle deadline");
                assertEquals(original.getAccessCount(), promoted.activityPrint.getAccessCount(), "promotion must preserve prior accesses");
            } finally {
                cache.close();
            }
        }

        @Test
        void promotionDoesNotReviveEntryWhoseTtlExpiresDuringCopy() throws Exception {
            assertPromotionDoesNotReviveEntry("createdTime");
        }

        @Test
        void promotionDoesNotReviveEntryWhoseIdleTimeExpiresDuringCopy() throws Exception {
            assertPromotionDoesNotReviveEntry("lastAccessTime");
        }

        @Test
        void promotionCopyFailureLeavesDiskEntryAndReleasesSlots() {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();
            final PromotionCopyOffHeapCache cache = new PromotionCopyOffHeapCache(backing);
            try {
                assertTrue(cache.put("key", new byte[] { 7 }));
                cache.duringCopy = () -> {
                    throw new IllegalStateException("forced promotion copy failure");
                };

                assertThrows(IllegalStateException.class, () -> cache.getOrNull("key"));
                assertEquals(1L, cache.stats().sizeOnDisk());
                assertTrue(cache.stats().occupiedSlots().isEmpty());
                assertArrayEquals(new byte[] { 7 }, backing.get("key"));

                cache.duringCopy = () -> {
                };
                assertArrayEquals(new byte[] { 7 }, cache.getOrNull("key"));
                assertEquals(0L, cache.stats().sizeOnDisk());
                assertEquals(1, totalOccupiedSlots(cache.stats()));
            } finally {
                cache.close();
            }
        }

        /** Reclamation may fail after releasing slots; its exception must not trigger another release. */
        @SuppressWarnings("unchecked")
        @Test
        void rejectedPromotionReclamationFailureDoesNotReleaseSlotsTwice() throws Exception {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();
            final PromotionCopyOffHeapCache cache = new PromotionCopyOffHeapCache(backing);
            try {
                assertTrue(cache.put("key", new byte[] { 7 }));
                final Field queuesField = AbstractOffHeapCache.class.getDeclaredField("segmentQueues");
                queuesField.setAccessible(true);
                final Deque<AbstractOffHeapCache.Segment>[] queues = (Deque<AbstractOffHeapCache.Segment>[]) queuesField.get(cache);
                cache.duringCopy = () -> {
                    cache.remove("key"); // Make this promotion stale after it reserved its memory slot.
                    queues[0] = new FailingReclamationQueue(queues[0]);
                };

                final OutOfMemoryError failure = assertThrows(OutOfMemoryError.class, () -> cache.getOrNull("key"));
                assertEquals("forced reclamation iterator failure", failure.getMessage());
                assertEquals(0, cache.size());
                assertEquals(0, totalOccupiedSlots(cache.stats()), "each unpublished slot must be released exactly once");
                cache.clear();
                assertTrue(cache.stats().occupiedSlots().isEmpty());
            } finally {
                cache.close();
            }
        }

        /** A stale promotion attempt must likewise leave a concurrent replacement continuously installed. */
        @Test
        void stalePromotionDoesNotReinsertConcurrentReplacement() throws Exception {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();
            final AtomicBoolean blockFirstPromotion = new AtomicBoolean(true);
            final CountDownLatch firstPromotionEntered = new CountDownLatch(1);
            final CountDownLatch releaseFirstPromotion = new CountDownLatch(1);
            final ExecutorService executor = Executors.newSingleThreadExecutor();

            try {
                final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                        .capacityInMB(1)
                        .evictDelay(0)
                        .offHeapStore(newInMemoryStore(backing))
                        .storeSelector((k, v, size) -> 2)
                        .testerForLoadingItemFromDiskToMemory((activity, size, elapsed) -> {
                            if (blockFirstPromotion.compareAndSet(true, false)) {
                                firstPromotionEntered.countDown();
                                try {
                                    assertTrue(releaseFirstPromotion.await(10, TimeUnit.SECONDS));
                                } catch (final InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                return true;
                            }

                            return false;
                        })
                        .build();

                try {
                    final byte[] oldValue = { 1 };
                    final byte[] replacement = { 2, 2 };
                    assertTrue(cache.put("k", oldValue));

                    final Future<byte[]> staleRead = executor.submit(() -> cache.getOrNull("k"));
                    assertTrue(firstPromotionEntered.await(5, TimeUnit.SECONDS));
                    assertTrue(cache.put("k", replacement));
                    releaseFirstPromotion.countDown();

                    assertArrayEquals(oldValue, staleRead.get(5, TimeUnit.SECONDS));
                    assertEquals(2L, cache.stats().putCount(), "stale promotion must not count a remove/reinsert of the replacement as another put");
                    assertArrayEquals(replacement, cache.getOrNull("k"));
                } finally {
                    cache.close();
                }
            } finally {
                releaseFirstPromotion.countDown();
                executor.shutdownNow();
            }
        }

        /**
         * The same failure during a disk-to-memory promotion: the promotion's {@code compute} re-hashes
         * the key after its bytes have been copied into freshly allocated slots. An undeclared checked
         * exception must propagate without stranding those slots, and the disk entry must stay in place.
         */
        @Test
        void promotionReleasesFreshSlotsWhenKeyHashCodeThrowsUndeclaredCheckedException() {
            final Map<ThrowingHashKey, byte[]> backing = new IdentityHashMap<>();
            final ThrowingHashKey key = new ThrowingHashKey();
            final OffHeapStore<ThrowingHashKey> store = new OffHeapStore<>() {
                @Override
                public byte[] get(final ThrowingHashKey key) {
                    final byte[] value = backing.get(key);

                    if (value != null) {
                        // Armed here, after the read: the next hashCode call comes from the promotion's
                        // entries.compute, the first operation to hash the key after this point.
                        key.throwOnNextHashCalls(1, new IOException("forced hashCode failure"));
                    }

                    return value;
                }

                @Override
                public boolean put(final ThrowingHashKey key, final byte[] value) {
                    backing.put(key, value);
                    return true;
                }

                @Override
                public boolean remove(final ThrowingHashKey key) {
                    return backing.remove(key) != null;
                }
            };
            final OffHeapCache<ThrowingHashKey, byte[]> cache = OffHeapCache.<ThrowingHashKey, byte[]> builder()
                    .capacityInMB(1)
                    .evictDelay(0)
                    .defaultLiveTime(600_000)
                    .defaultMaxIdleTime(600_000)
                    .offHeapStore(store)
                    .storeSelector((k, v, size) -> 2) // disk only on put
                    .testerForLoadingItemFromDiskToMemory((activityPrint, size, elapsed) -> true)
                    .build();
            try {
                assertTrue(cache.put(key, new byte[] { 1, 2, 3 }));
                assertEquals(1L, cache.stats().sizeOnDisk());

                assertThrows(IOException.class, () -> cache.getOrNull(key));
                assertEquals(0, key.remainingFailures(), "the promotion's compute must have hit the armed failure");
                assertTrue(cache.stats().occupiedSlots().isEmpty(), "an aborted promotion must release its freshly allocated slots");
                assertEquals(1L, cache.stats().sizeOnDisk(), "the disk entry must survive the aborted promotion");
            } finally {
                cache.close();
            }
        }
    }

    @Nested
    class ExpirationAndMaintenance {

        @Test
        void vacationPreservesDiskEntriesWhileRelievingMemoryPressure() {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                    .capacityInMB(1)
                    .evictDelay(0)
                    .offHeapStore(newInMemoryStore(backing))
                    .storeSelector((key, value, size) -> "disk".equals(key) ? 2 : 1)
                    .build();
            try {
                assertTrue(cache.put("disk", new byte[] { 1 }));
                assertTrue(cache.put("memory", new byte[AbstractOffHeapCache.SEGMENT_SIZE]));
                assertFalse(cache.put("crowded-out", new byte[8192]));
                awaitCondition("memory entry evicted", () -> cache.size() == 1);

                assertEquals(1, cache.stats().sizeOnDisk());
                assertEquals(1, cache.stats().evictionCount());
                assertEquals(0, cache.stats().occupiedMemory());
                assertArrayEquals(new byte[] { 1 }, cache.getOrNull("disk"));
                assertArrayEquals(new byte[] { 1 }, backing.get("disk"));
            } finally {
                cache.close();
            }
        }

        /**
         * A failed in-memory put of a value that COULD fit an empty cache (genuine memory pressure,
         * not an impossible size) schedules the asynchronous vacating task. Exercises the
         * {@code vacate()} scheduling path end-to-end: the task evicts entries and reclaims their
         * now-empty segments, keeping the cache usable. (A value larger than the entire capacity no
         * longer triggers a vacate - the in-memory attempt is skipped for it entirely.)
         */
        @Test
        void vacateScheduledWhenMemoryFullForNewKey() throws InterruptedException {
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder().capacityInMB(1).evictDelay(0).build();
            try {
                // Fill most of the single 1 MB segment, then ask for more than remains. Sizes are
                // exact multiples of the 8192-byte max block size (byte[] values are stored raw, at
                // exactly array length) so every chunk uses the segment's single 8192 slot class: 100
                // of its 128 slots are taken, and the second put needs 100 more. The second value
                // would fit an empty cache, so the failure is genuine memory pressure and schedules
                // the vacating task.
                assertTrue(cache.put("occupant", new byte[100 * 8192]));
                assertFalse(cache.put("crowded-out", new byte[100 * 8192]));

                // Allow the asynchronously-scheduled vacating task to run (covers its lambda body).
                Thread.sleep(200);

                // The vacate evicted the occupant and reclaimed its now-empty segment, so the cache
                // is usable again - even for a different slot-size class.
                final byte[] ok = new byte[128];
                for (int i = 0; i < ok.length; i++) {
                    ok[i] = (byte) i;
                }
                assertTrue(cache.put("ok", ok));
                assertArrayEquals(ok, cache.getOrNull("ok"));
            } finally {
                cache.close();
            }
        }

        /**
         * Idle-time expiry (as opposed to TTL expiry, which every other expiry test uses): an entry
         * whose {@code maxIdleTime} elapses without an access reads as absent, is removed lazily, and
         * is counted as an eviction.
         */
        @Test
        void idleTimeExpiryLazyOnAccess() throws Exception {
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder().capacityInMB(1).evictDelay(0).build();
            try {
                assertTrue(cache.put("k", new byte[] { 1, 2, 3 }, 0, 150));
                setActivityTime(entriesOf(cache).get("k").activityPrint, "lastAccessTime", System.currentTimeMillis() - 1_000);

                assertNull(cache.getOrNull("k"), "an entry idle past maxIdleTime must read as absent");
                assertEquals(0, cache.size(), "the lazily expired entry must be removed");
                assertEquals(1L, cache.stats().evictionCount(), "lazy expiry must be counted as an eviction");
            } finally {
                cache.close();
            }
        }

        /**
         * The periodic maintenance sweep (evictDelay &gt; 0) must remove an expired entry, count it as
         * an eviction, and reclaim its segment — without any intervening {@code get}.
         */
        @Test
        void maintenanceSweepRemovesExpiredEntries() throws Exception {
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder().capacityInMB(1).evictDelay(100).build();
            try {
                assertTrue(cache.put("k", new byte[] { 1, 2, 3 }, 150, 0));

                for (int i = 0; i < 100 && cache.size() != 0; i++) {
                    Thread.sleep(50L);
                }

                assertEquals(0, cache.size(), "the maintenance sweep must remove the expired entry without a get");
                assertEquals(1L, cache.stats().evictionCount());
                assertEquals(0, totalOccupiedSlots(cache.stats()), "the sweep must reclaim the expired entry's slots");
            } finally {
                cache.close();
            }
        }

        /** A sweep must recheck idle expiry after an in-flight read releases the entry monitor. */
        @Test
        void maintenancePreservesEntryRefreshedWhileWaitingForItsMonitor() throws Exception {
            final ExecutorService executor = Executors.newSingleThreadExecutor();
            final AtomicReference<Thread> maintenanceThread = new AtomicReference<>();
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder().capacityInMB(1).build();

            try {
                assertTrue(cache.put("key", new byte[] { 9 }, 0L, 60_000L));
                final AbstractOffHeapCache.Entry<byte[]> entry = entriesOf(cache).get("key");
                final Method maintenance = AbstractOffHeapCache.class.getDeclaredMethod("runMaintenance");
                maintenance.setAccessible(true);
                final Future<?> sweep;

                synchronized (entry) {
                    setActivityTime(entry.activityPrint, "lastAccessTime", 0L);
                    sweep = executor.submit(() -> {
                        maintenanceThread.set(Thread.currentThread());
                        maintenance.invoke(cache);
                        return null;
                    });

                    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    while ((maintenanceThread.get() == null || maintenanceThread.get().getState() != Thread.State.BLOCKED) && System.nanoTime() < deadline) {
                        Thread.sleep(1L);
                    }

                    assertNotNull(maintenanceThread.get());
                    assertEquals(Thread.State.BLOCKED, maintenanceThread.get().getState(), "maintenance must be waiting to retire the expired candidate");
                    // Complete an access already holding the monitor while maintenance waited.
                    entry.touch();
                }

                sweep.get(5, TimeUnit.SECONDS);
                assertArrayEquals(new byte[] { 9 }, cache.getOrNull("key"));
                assertEquals(1, cache.size());
                assertEquals(0L, cache.stats().evictionCount(), "a refreshed entry must not count as an eviction");
                assertEquals(1, totalOccupiedSlots(cache.stats()));
            } finally {
                executor.shutdownNow();
                cache.close();
            }
        }

        /**
         * A value whose serialized form exceeds the ENTIRE capacity skips the in-memory attempt but
         * still spills to a configured disk store — without scheduling a vacate (nothing could ever
         * make it fit in memory).
         */
        @Test
        void oversizedValueSpillsToDiskWithoutVacate() {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();

            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                    .capacityInMB(1)
                    .evictDelay(0)
                    .offHeapStore(newInMemoryStore(backing))
                    .build();
            try {
                assertTrue(cache.put("small", new byte[] { 1 }));

                final byte[] oversized = new byte[2 * 1024 * 1024];
                oversized[0] = 42;
                assertTrue(cache.put("huge", oversized), "an oversized value must spill to the configured store");

                assertEquals(1L, cache.stats().sizeOnDisk());
                assertArrayEquals(oversized, cache.getOrNull("huge"));
                assertArrayEquals(new byte[] { 1 }, cache.getOrNull("small"), "the doomed put must not vacate the in-memory entry");
                assertEquals(0L, cache.stats().evictionCount(), "a doomed oversized put must not schedule a vacate");
            } finally {
                cache.close();
            }
        }

        /**
         * A value no larger than the capacity in bytes can still be impossible to place: its
         * slot-rounded chunks need whole segments per size class. In a 2 MB cache with the default
         * 8192-byte max block, a value of {@code 2 MB - 100} bytes needs 255 full chunks (two whole
         * 8192-class segments) plus a final 8092-byte chunk in the 8128-byte class, i.e. a third
         * segment. Such a put is as doomed as an oversized one, so it must neither schedule a vacate
         * (which would evict unrelated entries without ever making room) nor be promoted from disk.
         */
        @Test
        void valueNeedingMoreSegmentsThanCapacityDoesNotVacate() throws Exception {
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder().capacityInMB(2).evictDelay(0).build();
            try {
                assertTrue(cache.put("small", new byte[] { 1 }));

                final byte[] doomed = new byte[2 * 1024 * 1024 - 100];
                assertFalse(cache.put("doomed", doomed), "the value can never be placed in memory and there is no disk store");

                // Give a (wrongly) scheduled asynchronous vacate pass ample time to run.
                Thread.sleep(500L);

                assertArrayEquals(new byte[] { 1 }, cache.getOrNull("small"), "a doomed put must not vacate unrelated in-memory entries");
                assertEquals(0L, cache.stats().evictionCount(), "a doomed put must not schedule a vacate");
            } finally {
                cache.close();
            }
        }

        /**
         * The vacate target must never exceed the candidate count. Computed in {@code float}
         * arithmetic, {@code (int) (16_777_219 * 1.0f)} is 16_777_220, so a vacate over that many
         * memory-resident entries with {@code vacatingFactor(1.0f)} indexed past its candidate list
         * (an {@code IndexOutOfBoundsException} that aborted the pass before segment reclamation).
         * Populating 2^24 entries is impractical in a unit test, so the target computation is checked
         * directly.
         */
        @Test
        void vacateTargetNeverExceedsCandidateCountForLargeCounts() throws Exception {
            final int candidateCount = 16_777_219;
            assertTrue((int) (candidateCount * 1.0f) > candidateCount, "precondition: float arithmetic rounds this count up");

            final Method evictTargetCountOf = AbstractOffHeapCache.class.getDeclaredMethod("evictTargetCountOf", int.class);
            evictTargetCountOf.setAccessible(true);

            final OffHeapCache<String, byte[]> all = OffHeapCache.<String, byte[]> builder().capacityInMB(1).evictDelay(0).vacatingFactor(1.0f).build();
            try {
                assertEquals(candidateCount, evictTargetCountOf.invoke(all, candidateCount));
                assertEquals(1, evictTargetCountOf.invoke(all, 1));
            } finally {
                all.close();
            }

            final OffHeapCache<String, byte[]> fraction = OffHeapCache.<String, byte[]> builder().capacityInMB(1).evictDelay(0).vacatingFactor(0.2f).build();
            try {
                assertEquals(1, evictTargetCountOf.invoke(fraction, 1), "at least one candidate is always evicted");
                assertEquals(20, evictTargetCountOf.invoke(fraction, 100));
            } finally {
                fraction.close();
            }
        }

        /** {@code vacatingFactor(1.0f)}: a vacate pass evicts every memory-resident entry. */
        @Test
        void vacatingFactorOneEvictsEverything() throws Exception {
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder().capacityInMB(1).evictDelay(0).vacatingFactor(1.0f).build();
            try {
                assertTrue(cache.put("a", new byte[60 * 8192]));
                assertTrue(cache.put("b", new byte[40 * 8192]));
                assertFalse(cache.put("crowded-out", new byte[100 * 8192]), "the segment is full; this put fails and schedules a vacate");

                for (int i = 0; i < 100 && cache.size() != 0; i++) {
                    Thread.sleep(50L);
                }

                assertEquals(0, cache.size(), "vacatingFactor 1.0 must evict every entry");
                assertEquals(2L, cache.stats().evictionCount());
                assertTrue(cache.put("after", new byte[100 * 8192]), "the reclaimed segment must be reusable");
            } finally {
                cache.close();
            }
        }

        /**
         * A maintenance pass whose sweep hits an {@link Error} (here a key's {@code hashCode} throwing
         * inside the removal's {@code compute}) must be logged and retried on the next cycle:
         * {@code scheduleWithFixedDelay} permanently suppresses every future run when a throwable
         * escapes the task. The expired entry must still be swept by a later pass.
         */
        @Test
        void maintenancePassSurvivesErrorAndKeepsSweeping() throws Exception {
            assertMaintenancePassSurvivesAndKeepsSweeping(new AssertionError("forced hashCode failure"));
        }

        /**
         * As above, for a checked exception thrown without being declared (as code written in Kotlin, or
         * using sneaky throws, can do). It is neither a {@link RuntimeException} nor an {@link Error}, so a
         * catch limited to those two would still let it escape and cancel every future maintenance run.
         */
        @Test
        void maintenancePassSurvivesUndeclaredCheckedExceptionAndKeepsSweeping() throws Exception {
            assertMaintenancePassSurvivesAndKeepsSweeping(new IOException("forced hashCode failure"));
        }

        /**
         * The vacate task's future is never read, so a failed pass is visible only through its log line.
         * An undeclared checked exception (here from a key's {@code hashCode} while the pass evicts it) must
         * be logged like an {@link Error} or {@link RuntimeException}, and the single-flight gate released.
         */
        @Test
        void vacatePassLogsUndeclaredCheckedExceptionAndReleasesGate() throws Exception {
            final Logger logger = mock(Logger.class);
            final HeapOffHeapCache cache = new HeapOffHeapCache(logger);
            try {
                final ThrowingHashKey occupant = new ThrowingHashKey();
                // Exact 8192 multiples keep every chunk in the segment's single slot class (see
                // vacateScheduledWhenMemoryFullForNewKey).
                assertTrue(cache.put(occupant, new byte[100 * 8192]));
                occupant.throwOnNextHashCalls(1, new IOException("forced hashCode failure"));
                assertFalse(cache.put("crowded-out", new byte[100 * 8192]), "the segment is full; this put fails and schedules a vacate");

                verify(logger, timeout(5_000)).warn(eq("Off-heap cache vacate pass failed; a later put under memory pressure will retry"),
                        isA(IOException.class));
                assertEquals(0, occupant.remainingFailures());

                final Field vacatingField = AbstractOffHeapCache.class.getDeclaredField("vacating");
                vacatingField.setAccessible(true);
                final AtomicBoolean vacating = (AtomicBoolean) vacatingField.get(cache);

                for (int i = 0; i < 100 && vacating.get(); i++) {
                    Thread.sleep(10L);
                }

                assertFalse(vacating.get(), "a failed vacate pass must release the single-flight gate");
                assertEquals(1, cache.size(), "the failed pass evicted nothing");
            } finally {
                cache.close();
            }
        }

        /**
         * An {@link Error} from the store's {@code remove} must leave an expired disk entry live and mapped,
         * so a later maintenance pass retries the removal. The entry used to be marked freed before the
         * store call: the Error then aborted the removal's {@code compute}, stranding a freed entry that
         * size() still counted, every later sweep skipped, and whose store bytes were never removed.
         */
        @Test
        void maintenanceRetriesExpiredDiskEntryAfterStoreRemoveError() throws Exception {
            final Map<String, byte[]> stored = new ConcurrentHashMap<>();
            final AtomicInteger failingRemoves = new AtomicInteger();
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                    .capacityInMB(1)
                    .evictDelay(50)
                    .offHeapStore(newRemoveFailingStore(stored, failingRemoves, new AssertionError("forced store remove failure")))
                    .storeSelector((key, value, size) -> 2)
                    .build();
            try {
                assertTrue(cache.put("k", new byte[] { 1 }, 200, 0));
                failingRemoves.set(1);

                for (int i = 0; i < 100 && cache.size() != 0; i++) {
                    Thread.sleep(50L);
                }

                assertEquals(0, failingRemoves.get(), "the first removal attempt must have failed");
                assertEquals(0, cache.size(), "a later pass must retry and complete the removal");
                assertFalse(stored.containsKey("k"), "the retried removal must delete the store bytes");

                final OffHeapCacheStats stats = cache.stats();
                assertEquals(0L, stats.sizeOnDisk());
                assertEquals(0L, stats.dataSizeOnDisk());
                assertEquals(1L, stats.evictionCount());
                assertEquals(1L, stats.evictionCountFromDisk());
            } finally {
                cache.close();
            }
        }
    }

    @Nested
    class Lifecycle {

        @Test
        void closeContinuesReleasingEntriesAndTheStoreWhenDiagnosticsFail() throws Exception {
            final Logger logger = mock(Logger.class);
            final OffHeapStore<Object> store = mock(OffHeapStore.class);
            when(store.put(any(), any(byte[].class))).thenReturn(true);
            doThrow(new IllegalStateException("store unavailable")).when(store).remove(any());
            final IllegalStateException logFailure = new IllegalStateException("logging backend failed");
            doThrow(logFailure).when(logger).warn(anyString(), any(Throwable.class));
            final HeapOffHeapCache cache = new HeapOffHeapCache(logger, store);
            try {
                assertTrue(cache.put("first", new byte[] { 1 }));
                assertTrue(cache.put("second", new byte[] { 2 }));
                assertSame(logFailure, assertThrows(IllegalStateException.class, cache::close));
                assertTrue(cache.isClosed());
                assertTrue(entriesOf(cache).isEmpty());
                verify(store, times(2)).remove(any());
                verify(store).close();
                assertEquals(0, logFailure.getSuppressed().length);
            } finally {
                assertDoesNotThrow(cache::close);
            }
        }

        @Test
        void shutdownHookClosesTheCacheAndLateMaintenanceDoesNothing() throws Exception {
            final Logger logger = mock(Logger.class);
            final HeapOffHeapCache cache = new HeapOffHeapCache(logger);
            final Field hookField = AbstractOffHeapCache.class.getDeclaredField("shutdownHook");
            hookField.setAccessible(true);
            final Thread hook = (Thread) hookField.get(cache);
            try {
                assertTrue(cache.put("key", new byte[] { 1 }));
                hook.run();
                assertTrue(cache.isClosed());
                verify(logger).info("Closing off-heap cache on JVM shutdown");
                final Method maintenance = AbstractOffHeapCache.class.getDeclaredMethod("runMaintenance");
                maintenance.setAccessible(true);
                assertDoesNotThrow(() -> maintenance.invoke(cache));
                assertTrue(entriesOf(cache).isEmpty());
            } finally {
                cache.close();
            }
        }

        @ParameterizedTest
        @ValueSource(booleans = { false, true })
        void closeToleratesAnAlreadyShuttingDownJvm(final boolean debug) throws Exception {
            final Logger logger = mock(Logger.class);
            when(logger.isDebugEnabled()).thenReturn(debug);
            final HeapOffHeapCache cache = new HeapOffHeapCache(logger);
            final Field hookField = AbstractOffHeapCache.class.getDeclaredField("shutdownHook");
            hookField.setAccessible(true);
            final Thread hook = (Thread) hookField.get(cache);
            final Runtime actualRuntime = Runtime.getRuntime();
            final Runtime shuttingDown = mock(Runtime.class);
            doThrow(new IllegalStateException("JVM shutting down")).when(shuttingDown).removeShutdownHook(hook);
            try (MockedStatic<Runtime> runtimes = Mockito.mockStatic(Runtime.class)) {
                runtimes.when(Runtime::getRuntime).thenReturn(shuttingDown);
                assertDoesNotThrow(cache::close);
                assertTrue(cache.isClosed());
                assertNull(hookField.get(cache));
                verify(logger, times(debug ? 1 : 0)).debug(anyString(), any(Throwable.class));
            } finally {
                actualRuntime.removeShutdownHook(hook);
                cache.close();
            }
        }

        @Test
        void closeContinuesCleanupAfterANativeReleaseRuntimeFailure() throws Exception {
            final IllegalStateException failure = new IllegalStateException("native release failed after freeing memory");
            final OffHeapStore<String> store = mock(OffHeapStore.class);
            final OffHeapCache<String, byte[]> cache = new OffHeapCache<>(1, AbstractOffHeapCache.DEFAULT_MAX_BLOCK_SIZE, 0, 60_000, 0,
                    AbstractOffHeapCache.DEFAULT_VACATING_FACTOR, null, null, store, false, null, null) {
                @Override
                protected void deallocate() {
                    super.deallocate();
                    throw failure;
                }
            };
            try {
                assertTrue(cache.put("key", new byte[] { 1 }));
                assertSame(failure, assertThrows(IllegalStateException.class, cache::close));
                assertTrue(cache.isClosed());
                assertTrue(entriesOf(cache).isEmpty());
                verify(store).close();
            } finally {
                assertDoesNotThrow(cache::close);
            }
        }

        /**
         * Regression coverage for the cancelled-eviction-task GC leak in
         * {@link AbstractOffHeapCache}.
         *
         * <p>Without {@code setRemoveOnCancelPolicy(true)} on the shared scheduled executor, a
         * cancelled {@code scheduleFuture} sits in the executor's task queue until its scheduled fire
         * time, holding a strong reference to the closed cache and preventing GC of its off-heap
         * allocation. The fix enables the policy on the executor so {@code cancel()} purges the task
         * from the queue immediately.
         *
         * <p>This smoke test creates and immediately closes many short-lived off-heap caches in a
         * tight loop. With the fix, cancelled scheduled tasks are purged on close, so memory stays
         * bounded. Without the fix, the executor's queue grows unbounded — each cache's eviction task
         * remains scheduled at its full {@code evictDelay} into the future even after close. The test
         * fails (OOM or excessive time) regress when the fix is reverted.
         */
        @Test
        void repeatedCreateAndCloseDoesNotLeakScheduledTasks() {
            // 200 caches with a 5-minute evictDelay each. Without the cancel-on-purge policy, all 200
            // scheduled tasks would remain in the executor's queue holding cache references after close.
            for (int i = 0; i < 200; i++) {
                final OffHeapCache<String, String> cache = OffHeapCache.<String, String> builder()
                        .capacityInMB(16)
                        .evictDelay(5L * 60_000L)
                        .defaultLiveTime(60_000)
                        .defaultMaxIdleTime(60_000)
                        .build();
                try {
                    assertTrue(cache.put("k", "v"));
                } finally {
                    cache.close();
                }
            }
        }

        /**
         * Regression coverage for the post-close {@code put()} ordering bug. The off-heap cache frees
         * its native allocation in {@code close()}; a later {@code put()} must fail before any slot
         * allocation or native-copy hook is reached. Previously the closed state was only consulted
         * after {@code copyToMemory(...)}, which made {@code OffHeapCache} write to freed native memory.
         */
        @Test
        void putAfterCloseFailsBeforeNativeCopy() {
            final GuardedOffHeapCache cache = new GuardedOffHeapCache();
            cache.close();

            assertThrows(IllegalStateException.class, () -> cache.put("k", new byte[] { 1 }));
            assertFalse(cache.copiedAfterDeallocate.get(), "put after close must not copy into deallocated memory");
        }

        /** A user callback cannot upgrade the operation's lifecycle read lock into close's write lock. */
        @Test
        void reentrantCloseFromSerializerFailsFastAndCacheRemainsOpen() {
            final AtomicReference<OffHeapCache<String, String>> cacheRef = new AtomicReference<>();
            final AtomicReference<IllegalStateException> closeFailure = new AtomicReference<>();
            final OffHeapCache<String, String> cache = OffHeapCache.<String, String> builder().capacityInMB(1).evictDelay(0).serializer((value, output) -> {
                try {
                    cacheRef.get().close();
                    throw new AssertionError("reentrant close must fail instead of attempting a read-to-write lock upgrade");
                } catch (final IllegalStateException e) {
                    closeFailure.set(e);
                }

                final byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                output.write(bytes, 0, bytes.length);
            }).deserializer((bytes, type) -> new String(bytes, java.nio.charset.StandardCharsets.UTF_8)).build();
            cacheRef.set(cache);

            try {
                assertTrue(cache.put("key", "value"));
                assertNotNull(closeFailure.get());
                assertTrue(closeFailure.get().getMessage().contains("Cannot close this off-heap cache reentrantly"));
                assertFalse(cache.isClosed(), "the rejected reentrant close must leave the cache open");
                assertEquals("value", cache.getOrNull("key"));
            } finally {
                cache.close();
            }
        }

        @Test
        void closeRemovesEveryDiskEntryWithoutRehashingKeys() throws Exception {
            final Map<ExceptionalHashKey, byte[]> backing = new IdentityHashMap<>();
            final AtomicBoolean removedBeforeStoreClosed = new AtomicBoolean();
            final OffHeapStore<ExceptionalHashKey> store = new OffHeapStore<>() {
                @Override
                public byte[] get(final ExceptionalHashKey key) {
                    return backing.get(key);
                }

                @Override
                public boolean put(final ExceptionalHashKey key, final byte[] value) {
                    backing.put(key, value);
                    return true;
                }

                @Override
                public boolean remove(final ExceptionalHashKey key) {
                    return backing.remove(key) != null;
                }

                @Override
                public void close() {
                    removedBeforeStoreClosed.set(backing.isEmpty());
                }
            };
            final OffHeapCache<ExceptionalHashKey, byte[]> cache = OffHeapCache.<ExceptionalHashKey, byte[]> builder()
                    .capacityInMB(1)
                    .offHeapStore(store)
                    .storeSelector((key, value, size) -> 2)
                    .build();
            try {
                final ExceptionalHashKey first = new ExceptionalHashKey();
                final ExceptionalHashKey second = new ExceptionalHashKey();
                assertTrue(cache.put(first, new byte[] { 1 }));
                assertTrue(cache.put(second, new byte[] { 2 }));
                first.throwOnHashCall(1);
                second.throwOnHashCall(1);

                assertDoesNotThrow(cache::close);

                assertTrue(removedBeforeStoreClosed.get(), "all tracked store bytes must be removed before closing the store");
                assertTrue(entriesOf(cache).isEmpty(), "closing must discard the entry index without invoking key hashCode");
            } finally {
                cache.close();
            }
        }

        @Test
        void closeContinuesEntryCleanupAfterAnErrorAndClearsTheIndex() throws Exception {
            final AssertionError sharedFailure = new AssertionError("forced store cleanup failure");
            final AtomicInteger removeAttempts = new AtomicInteger();
            final AtomicInteger deallocateCalls = new AtomicInteger();
            final AtomicInteger storeCloseCalls = new AtomicInteger();
            final OffHeapStore<String> store = new OffHeapStore<>() {
                @Override
                public byte[] get(final String key) {
                    return new byte[] { 1 };
                }

                @Override
                public boolean put(final String key, final byte[] value) {
                    return true;
                }

                @Override
                public boolean remove(final String key) {
                    removeAttempts.incrementAndGet();
                    throw sharedFailure;
                }

                @Override
                public void close() {
                    storeCloseCalls.incrementAndGet();
                    throw sharedFailure;
                }
            };
            final OffHeapCache<String, byte[]> cache = new OffHeapCache<>(1, AbstractOffHeapCache.DEFAULT_MAX_BLOCK_SIZE, 0, 60_000L, 60_000L,
                    AbstractOffHeapCache.DEFAULT_VACATING_FACTOR, null, null, store, false, null, (key, value, size) -> 2) {
                @Override
                protected void deallocate() {
                    deallocateCalls.incrementAndGet();
                    super.deallocate();
                }
            };
            try {
                assertTrue(cache.put("first", new byte[] { 1 }));
                assertTrue(cache.put("second", new byte[] { 2 }));

                assertSame(sharedFailure, assertThrows(AssertionError.class, cache::close));

                assertEquals(2, removeAttempts.get(), "a failed entry cleanup must not skip subsequent entries");
                assertEquals(0, sharedFailure.getSuppressed().length, "a reused failure must never suppress itself");
                assertEquals(1, deallocateCalls.get(), "native memory must still be released after entry cleanup errors");
                assertEquals(1, storeCloseCalls.get(), "the store must still be closed after entry cleanup errors");
                assertTrue(cache.isClosed());
                assertTrue(entriesOf(cache).isEmpty(), "failed cleanup must not retain cache keys and entry metadata");
            } finally {
                cache.close();
            }
            assertEquals(1, deallocateCalls.get(), "a second close must not free native memory twice");
            assertEquals(1, storeCloseCalls.get(), "a second close must remain a no-op");
        }

        @Test
        void closePreservesDistinctEntryAndResourceCleanupFailures() throws Exception {
            final AssertionError firstEntryFailure = new AssertionError("first disk removal failed");
            final AssertionError secondEntryFailure = new AssertionError("second disk removal failed");
            final IllegalStateException deallocationFailure = new IllegalStateException("native cleanup failed after releasing memory");
            final IllegalArgumentException storeCloseFailure = new IllegalArgumentException("store close failed");
            final AtomicInteger cleanupSteps = new AtomicInteger();
            final OffHeapStore<String> store = new OffHeapStore<>() {
                @Override
                public byte[] get(final String key) {
                    return new byte[] { 1 };
                }

                @Override
                public boolean put(final String key, final byte[] value) {
                    return true;
                }

                @Override
                public boolean remove(final String key) {
                    throw cleanupSteps.incrementAndGet() == 1 ? firstEntryFailure : secondEntryFailure;
                }

                @Override
                public void close() {
                    assertEquals(3, cleanupSteps.getAndIncrement(), "the store must close after both disk removals and native cleanup");
                    throw storeCloseFailure;
                }
            };
            final OffHeapCache<String, byte[]> cache = new OffHeapCache<>(1, AbstractOffHeapCache.DEFAULT_MAX_BLOCK_SIZE, 0, 60_000L, 60_000L,
                    AbstractOffHeapCache.DEFAULT_VACATING_FACTOR, null, null, store, false, null, (key, value, size) -> key.equals("memory") ? 1 : 2) {
                @Override
                protected void deallocate() {
                    super.deallocate();
                    assertEquals(2, cleanupSteps.getAndIncrement(), "both disk removals must be attempted before native cleanup");
                    throw deallocationFailure;
                }
            };
            try {
                assertTrue(cache.put("first", new byte[] { 1 }));
                assertTrue(cache.put("second", new byte[] { 2 }));
                assertTrue(cache.put("memory", new byte[] { 3 }));
                final AbstractOffHeapCache.Entry<byte[]> memoryEntry = entriesOf(cache).get("memory");

                assertSame(firstEntryFailure, assertThrows(AssertionError.class, cache::close));

                assertArrayEquals(new Throwable[] { secondEntryFailure, deallocationFailure, storeCloseFailure }, firstEntryFailure.getSuppressed());
                assertTrue(memoryEntry.freed, "disk cleanup errors must not skip memory entries");
                assertTrue(entriesOf(cache).isEmpty());
                assertEquals(4, cleanupSteps.get());
            } finally {
                cache.close();
            }
            assertEquals(4, cleanupSteps.get(), "a second close must not retry any resource cleanup");
        }

        @Test
        void closeLogsDiskRemovalRuntimeFailuresAndStillClosesBothBackends() throws Exception {
            for (final boolean useForeignMemory : new boolean[] { false, true }) {
                final AtomicInteger removeAttempts = new AtomicInteger();
                final AtomicInteger storeCloseCalls = new AtomicInteger();
                final OffHeapStore<String> store = new OffHeapStore<>() {
                    @Override
                    public byte[] get(final String key) {
                        return new byte[] { 1 };
                    }

                    @Override
                    public boolean put(final String key, final byte[] value) {
                        return true;
                    }

                    @Override
                    public boolean remove(final String key) {
                        removeAttempts.incrementAndGet();
                        throw new IllegalStateException("store is unavailable for removal");
                    }

                    @Override
                    public void close() {
                        assertEquals(2, removeAttempts.get(), "all disk entries must be attempted before the store closes");
                        storeCloseCalls.incrementAndGet();
                    }
                };
                final AbstractOffHeapCache<String, byte[]> cache = useForeignMemory
                        ? ForeignMemoryOffHeapCache.<String, byte[]> builder()
                                .capacityInMB(1)
                                .evictDelay(0)
                                .offHeapStore(store)
                                .storeSelector((key, value, size) -> 2)
                                .build()
                        : OffHeapCache.<String, byte[]> builder()
                                .capacityInMB(1)
                                .evictDelay(0)
                                .offHeapStore(store)
                                .storeSelector((key, value, size) -> 2)
                                .build();
                try {
                    assertTrue(cache.put("first", new byte[] { 1 }));
                    assertTrue(cache.put("second", new byte[] { 2 }));

                    assertDoesNotThrow(cache::close);

                    assertTrue(cache.isClosed());
                    assertTrue(entriesOf(cache).isEmpty());
                    assertEquals(1, storeCloseCalls.get());
                } finally {
                    cache.close();
                }
                assertEquals(2, removeAttempts.get(), "a second close must not retry discarded entries");
                assertEquals(1, storeCloseCalls.get());
            }
        }

        /**
         * Memory-tier reads must participate in the lifecycle exclusion: a reader holds the lifecycle
         * read lock across its native copy, so {@code close()} (which deallocates under the write
         * lock) can never free the region underneath it - even when the entry has been detached from
         * the map by a concurrent removal and close's own entry sweep therefore never sees it. This
         * test simulates the detach (a bare map remove), parks a reader inside {@code copyFromMemory},
         * and verifies {@code close()} cannot complete - and native memory cannot be poisoned - until
         * the read finishes.
         */
        @Test
        void memoryReadBlocksCloseUntilReadCompletes() throws Exception {
            final LatchedReadOffHeapCache cache = new LatchedReadOffHeapCache();
            final ExecutorService executor = Executors.newFixedThreadPool(2);

            try {
                assertTrue(cache.put("key", "value"));

                cache.latchReads.set(true);
                final Future<String> reader = executor.submit(() -> cache.getOrNull("key"));
                assertTrue(cache.readEntered.await(5, TimeUnit.SECONDS), "reader must reach copyFromMemory");

                // Simulate a concurrent detach: the entry leaves the map unfreed, so close()'s own
                // entry sweep below cannot see it and will not block on its monitor.
                entriesOf(cache).remove("key");

                final Future<?> closer = executor.submit(cache::close);
                assertThrows(TimeoutException.class, () -> closer.get(300, TimeUnit.MILLISECONDS),
                        "close() must wait for the in-flight memory read instead of deallocating under it");

                cache.releaseReads.countDown();
                assertEquals("value", reader.get(5, TimeUnit.SECONDS), "the read must complete against live memory");
                closer.get(5, TimeUnit.SECONDS);
                assertTrue(cache.isClosed());
            } finally {
                cache.releaseReads.countDown();
                executor.shutdownNow();
                cache.close();
            }
        }

        /**
         * Close must wait for an in-flight {@code remove(key)} of a disk entry (blocked on the
         * entry's monitor while a reader-style holder pins it) to finish its {@code OffHeapStore}
         * cleanup; otherwise close could close the store underneath the in-flight
         * {@code OffHeapStore.remove} call.
         */
        @Test
        void removeFinishesDiskCleanupBeforeStoreClose() throws Exception {
            final Map<String, byte[]> backing = new ConcurrentHashMap<>();
            final AtomicBoolean storeClosed = new AtomicBoolean();
            final AtomicBoolean removeCalledAfterClose = new AtomicBoolean();
            final CountDownLatch wrapperLocked = new CountDownLatch(1);
            final CountDownLatch releaseWrapper = new CountDownLatch(1);
            final CountDownLatch closeFinished = new CountDownLatch(1);
            final ExecutorService executor = Executors.newFixedThreadPool(2);

            final OffHeapStore<String> store = new OffHeapStore<>() {
                @Override
                public boolean put(final String key, final byte[] value) {
                    backing.put(key, value);
                    return true;
                }

                @Override
                public byte[] get(final String key) {
                    return backing.get(key);
                }

                @Override
                public boolean remove(final String key) {
                    removeCalledAfterClose.compareAndSet(false, storeClosed.get());
                    return backing.remove(key) != null;
                }

                @Override
                public void close() {
                    storeClosed.set(true);
                }
            };

            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                    .capacityInMB(1)
                    .evictDelay(0)
                    .offHeapStore(store)
                    .storeSelector((k, v, size) -> 2)
                    .build();

            try {
                assertTrue(cache.put("k", new byte[] { 1 }));
                final AbstractOffHeapCache.Entry<byte[]> wrapper = entriesOf(cache).get("k");
                final Thread monitorHolder = new Thread(() -> {
                    synchronized (wrapper) {
                        wrapperLocked.countDown();
                        try {
                            assertTrue(releaseWrapper.await(10, TimeUnit.SECONDS));
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }, "offheap-test-remove-wrapper-holder");
                monitorHolder.start();
                assertTrue(wrapperLocked.await(5, TimeUnit.SECONDS));

                final Future<?> removeResult = executor.submit(() -> cache.remove("k"));
                // The remove frees the entry inside the per-key compute (serialized with same-key disk
                // writes), so it blocks on the pinned entry monitor with the mapping still installed.
                Thread.sleep(100L);
                assertFalse(removeResult.isDone(), "remove should be blocked on the pinned entry monitor");

                final Future<?> closeResult = executor.submit(() -> {
                    cache.close();
                    closeFinished.countDown();
                });

                final boolean closeOvertookRemove;
                try {
                    closeOvertookRemove = closeFinished.await(500, TimeUnit.MILLISECONDS);
                } finally {
                    releaseWrapper.countDown();
                }

                removeResult.get(5, TimeUnit.SECONDS);
                closeResult.get(5, TimeUnit.SECONDS);
                monitorHolder.join(5_000L);
                assertFalse(closeOvertookRemove, "close must wait for a detached disk wrapper to finish store cleanup");
                assertFalse(removeCalledAfterClose.get(), "OffHeapStore.remove must not run after OffHeapStore.close");
            } finally {
                releaseWrapper.countDown();
                cache.close();
                executor.shutdownNow();
            }
        }

        /**
         * An asynchronous vacate can finish evicting entries and still be reclaiming segment metadata.
         * Close must wait for that entire pass; otherwise it can deallocate the cache while the task
         * is still traversing cache-owned structures. The allocator lock provides a deterministic
         * pause after the eviction while the lifecycle read lock remains held.
         */
        @Test
        void closeWaitsForInFlightVacationTask() throws Exception {
            final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder().capacityInMB(1).evictDelay(0).build();
            final ExecutorService executor = Executors.newSingleThreadExecutor();

            final Field allocatorLockField = AbstractOffHeapCache.class.getDeclaredField("allocatorLock");
            allocatorLockField.setAccessible(true);
            final Object allocatorLock = allocatorLockField.get(cache);

            final Field lifecycleLockField = AbstractOffHeapCache.class.getDeclaredField("lifecycleLock");
            lifecycleLockField.setAccessible(true);
            final ReentrantReadWriteLock lifecycleLock = (ReentrantReadWriteLock) lifecycleLockField.get(cache);

            final Method vacateMethod = AbstractOffHeapCache.class.getDeclaredMethod("vacate");
            vacateMethod.setAccessible(true);

            Future<?> closeResult = null;
            try {
                synchronized (allocatorLock) {
                    vacateMethod.invoke(cache);

                    for (int i = 0; i < 500 && lifecycleLock.getReadLockCount() == 0; i++) {
                        Thread.sleep(10L);
                    }
                    assertTrue(lifecycleLock.getReadLockCount() > 0, "vacation task should reach segment reclamation while holding the lifecycle lock");

                    closeResult = executor.submit(cache::close);
                    Thread.sleep(300L);
                    assertFalse(closeResult.isDone(), "close must wait for the in-flight vacation task");
                }

                closeResult.get(5, TimeUnit.SECONDS);
            } finally {
                cache.close();
                executor.shutdownNow();
            }
        }
    }

    private static int totalOccupiedSlots(final OffHeapCacheStats stats) {
        int total = 0;
        for (final Map<Integer, Integer> perSegment : stats.occupiedSlots().values()) {
            for (final int cardinality : perSegment.values()) {
                total += cardinality;
            }
        }
        return total;
    }

    // ByteBufferType.byteArrayOf() reads bytes [0, position); build a buffer whose written content
    // (position advanced to the end) is exactly the supplied data so it round-trips through the cache.
    private static ByteBuffer bufferOf(final byte[] data) {
        final ByteBuffer bb = ByteBuffer.allocate(data.length);
        bb.put(data);
        return bb;
    }

    private static OffHeapStore<String> newInMemoryStore(final Map<String, byte[]> backing) {
        return new OffHeapStore<>() {
            @Override
            public boolean put(final String key, final byte[] value) {
                backing.put(key, value);
                return true;
            }

            @Override
            public byte[] get(final String key) {
                return backing.get(key);
            }

            @Override
            public boolean remove(final String key) {
                return backing.remove(key) != null;
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static <K, V> ConcurrentHashMap<K, AbstractOffHeapCache.Entry<V>> entriesOf(final AbstractOffHeapCache<K, V> cache)
            throws ReflectiveOperationException {
        final Field entriesField = AbstractOffHeapCache.class.getDeclaredField("entries");
        entriesField.setAccessible(true);
        return (ConcurrentHashMap<K, AbstractOffHeapCache.Entry<V>>) entriesField.get(cache);
    }

    private static final class GuardedOffHeapCache extends AbstractOffHeapCache<String, byte[]> {
        private final AtomicBoolean deallocated = new AtomicBoolean();
        private final AtomicBoolean copiedAfterDeallocate = new AtomicBoolean();

        GuardedOffHeapCache() {
            super(1, DEFAULT_MAX_BLOCK_SIZE, 0, 60_000L, 60_000L, DEFAULT_VACATING_FACTOR, 0, null, null, null, false, null, null,
                    LoggerFactory.getLogger(GuardedOffHeapCache.class));
        }

        @Override
        protected long allocate(final long capacityInBytes) {
            return 0L;
        }

        @Override
        protected void deallocate() {
            deallocated.set(true);
        }

        @Override
        protected void copyToMemory(final long startPtr, final byte[] bytes, final int srcOffset, final int len) {
            if (deallocated.get()) {
                copiedAfterDeallocate.set(true);
                throw new AssertionError("copyToMemory called after deallocate");
            }
        }

        @Override
        protected void copyFromMemory(final long startPtr, final byte[] bytes, final int destOffset, final int len) {
            throw new UnsupportedOperationException();
        }
    }

    /** Heap-array fixture that can fail the first native-memory copy deterministically. */
    private static final class FailingCopyOffHeapCache extends AbstractOffHeapCache<String, byte[]> {
        private final byte[] memory = new byte[SEGMENT_SIZE];
        private final AtomicBoolean failCopies = new AtomicBoolean(true);

        FailingCopyOffHeapCache() {
            super(1, DEFAULT_MAX_BLOCK_SIZE, 0, 60_000L, 60_000L, DEFAULT_VACATING_FACTOR, 0, null, null, null, false, null, null,
                    LoggerFactory.getLogger(FailingCopyOffHeapCache.class));
        }

        @Override
        protected long allocate(final long capacityInBytes) {
            return 0L;
        }

        @Override
        protected void deallocate() {
            // Heap-array fixture: nothing to release.
        }

        @Override
        protected void copyToMemory(final long startPtr, final byte[] bytes, final int srcOffset, final int len) {
            if (failCopies.getAndSet(false)) {
                throw new IllegalStateException("forced copy failure");
            }

            System.arraycopy(bytes, srcOffset, memory, (int) startPtr, len);
        }

        @Override
        protected void copyFromMemory(final long startPtr, final byte[] bytes, final int destOffset, final int len) {
            System.arraycopy(memory, (int) startPtr, bytes, destOffset, len);
        }
    }

    private static final class ExceptionalHashKey {
        private final AtomicInteger hashCalls = new AtomicInteger();
        private volatile int exceptionalCall = Integer.MAX_VALUE;

        void throwOnHashCall(final int call) {
            hashCalls.set(0);
            exceptionalCall = call;
        }

        @Override
        public int hashCode() {
            if (hashCalls.incrementAndGet() == exceptionalCall) {
                exceptionalCall = Integer.MAX_VALUE;
                throw new IllegalStateException("forced hash failure");
            }

            return 17;
        }
    }

    /** Heap-array fixture with a hook for expiring an entry while promotion copies its bytes. */
    private static final class PromotionCopyOffHeapCache extends AbstractOffHeapCache<String, byte[]> {
        private final byte[] memory = new byte[SEGMENT_SIZE];
        private Runnable duringCopy = () -> {
        };

        PromotionCopyOffHeapCache(final Map<String, byte[]> backing) {
            super(1, DEFAULT_MAX_BLOCK_SIZE, 0, 60_000L, 60_000L, DEFAULT_VACATING_FACTOR, 0, null, null, newInMemoryStore(backing), false,
                    (activity, size, elapsed) -> true, (key, value, size) -> 2, LoggerFactory.getLogger(PromotionCopyOffHeapCache.class));
        }

        @Override
        protected long allocate(final long capacityInBytes) {
            return 0L;
        }

        @Override
        protected void deallocate() {
            // Heap-array fixture: nothing to release.
        }

        @Override
        protected void copyToMemory(final long startPtr, final byte[] bytes, final int srcOffset, final int len) {
            duringCopy.run();
            System.arraycopy(bytes, srcOffset, memory, (int) startPtr, len);
        }

        @Override
        protected void copyFromMemory(final long startPtr, final byte[] bytes, final int destOffset, final int len) {
            System.arraycopy(memory, (int) startPtr, bytes, destOffset, len);
        }
    }

    private static void setActivityTime(final ActivityPrint activity, final String fieldName, final long value) {
        try {
            final Field field = ActivityPrint.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.setLong(activity, value);
        } catch (final ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /** Simulates allocation failure while creating the iterator used by segment reclamation. */
    private static final class FailingReclamationQueue extends ArrayDeque<AbstractOffHeapCache.Segment> {
        private boolean failNextIterator = true;

        FailingReclamationQueue(final Deque<AbstractOffHeapCache.Segment> segments) {
            super(segments);
        }

        @Override
        public Iterator<AbstractOffHeapCache.Segment> iterator() {
            if (failNextIterator) {
                failNextIterator = false;
                throw new OutOfMemoryError("forced reclamation iterator failure");
            }

            return super.iterator();
        }
    }

    /**
     * Memory-only fixture whose "native" memory is a heap array. {@code deallocate()} poisons the
     * array so any read that races past {@code close()} deserializes garbage instead of the stored
     * value, and {@code copyFromMemory} can park on a latch to hold a read in flight.
     */
    private static final class LatchedReadOffHeapCache extends AbstractOffHeapCache<String, String> {
        final byte[] memory = new byte[SEGMENT_SIZE];
        final AtomicBoolean latchReads = new AtomicBoolean();
        final CountDownLatch readEntered = new CountDownLatch(1);
        final CountDownLatch releaseReads = new CountDownLatch(1);

        LatchedReadOffHeapCache() {
            super(1, DEFAULT_MAX_BLOCK_SIZE, 0, 60_000L, 60_000L, DEFAULT_VACATING_FACTOR, 0, (value, output) -> {
                final byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                output.write(bytes, 0, bytes.length);
            }, (bytes, type) -> new String(bytes, java.nio.charset.StandardCharsets.UTF_8), null, false, null, null,
                    LoggerFactory.getLogger(LatchedReadOffHeapCache.class));
        }

        @Override
        protected long allocate(final long capacityInBytes) {
            return 0L;
        }

        @Override
        protected void deallocate() {
            Arrays.fill(memory, (byte) 0x55);
        }

        @Override
        protected void copyToMemory(final long startPtr, final byte[] bytes, final int srcOffset, final int len) {
            System.arraycopy(bytes, srcOffset, memory, (int) startPtr, len);
        }

        @Override
        protected void copyFromMemory(final long startPtr, final byte[] bytes, final int destOffset, final int len) {
            if (latchReads.get()) {
                readEntered.countDown();

                try {
                    releaseReads.await(5, TimeUnit.SECONDS);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            System.arraycopy(memory, (int) startPtr, bytes, destOffset, len);
        }
    }

    private static void assertMappedByteBufferRoundTrip(final Path directory, final boolean diskOnly) throws Exception {
        for (final boolean useForeignMemory : new boolean[] { false, true }) {
            for (final int size : new int[] { 3, AbstractOffHeapCache.DEFAULT_MAX_BLOCK_SIZE + 1 }) {
                final byte[] expected = new byte[size];
                for (int i = 0; i < size; i++) {
                    expected[i] = (byte) (i * 31 + 7);
                }
                final Path file = directory.resolve("mapped-" + useForeignMemory + "-" + size);
                Files.write(file, expected);
                final Map<String, byte[]> backing = new ConcurrentHashMap<>();
                final AbstractOffHeapCache<String, ByteBuffer> cache = useForeignMemory
                        ? ForeignMemoryOffHeapCache.<String, ByteBuffer> builder()
                                .capacityInMB(2)
                                .offHeapStore(newInMemoryStore(backing))
                                .storeSelector((key, value, bytes) -> diskOnly ? 2 : 1)
                                .testerForLoadingItemFromDiskToMemory((activity, bytes, elapsed) -> true)
                                .build()
                        : OffHeapCache.<String, ByteBuffer> builder()
                                .capacityInMB(2)
                                .offHeapStore(newInMemoryStore(backing))
                                .storeSelector((key, value, bytes) -> diskOnly ? 2 : 1)
                                .testerForLoadingItemFromDiskToMemory((activity, bytes, elapsed) -> true)
                                .build();
                try {
                    // A real, read-only file mapping exercises the public subtype without depending
                    // on its hidden implementation class. Closing the arena also proves put copied it.
                    try (Arena arena = Arena.ofConfined();
                         FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
                        final MappedByteBuffer input = (MappedByteBuffer) channel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena).asByteBuffer();
                        input.position(1).mark().position(size);
                        assertTrue(cache.put("mapped", input));
                        assertEquals(size, input.position());
                        input.reset();
                        assertEquals(1, input.position());
                    }

                    assertEquals(diskOnly ? 1L : 0L, cache.stats().sizeOnDisk());
                    // Buffer values preserve content, not the mapped subtype or original mapping.
                    final ByteBuffer first = cache.getOrNull("mapped");
                    assertHeapBufferContent(expected, first);
                    assertEquals(0L, cache.stats().sizeOnDisk(), "a disk read must promote the copied bytes");
                    first.array()[0] = 99;
                    assertHeapBufferContent(expected, cache.getOrNull("mapped"));
                } finally {
                    cache.close();
                }
            }
        }
    }

    private static void assertDirectByteBufferRoundTrip(final boolean diskOnly, final boolean promote) {
        for (final boolean useForeignMemory : new boolean[] { false, true }) {
            for (final boolean readOnly : new boolean[] { false, true }) {
                for (final int size : new int[] { 3, AbstractOffHeapCache.DEFAULT_MAX_BLOCK_SIZE + 1 }) {
                    final byte[] expected = new byte[size];
                    for (int i = 0; i < size; i++) {
                        expected[i] = (byte) (i * 31 + 7);
                    }
                    final Map<String, byte[]> backing = new ConcurrentHashMap<>();
                    final AbstractOffHeapCache<String, ByteBuffer> cache = useForeignMemory
                            ? ForeignMemoryOffHeapCache.<String, ByteBuffer> builder()
                                    .capacityInMB(2)
                                    .offHeapStore(newInMemoryStore(backing))
                                    .storeSelector((key, value, bytes) -> diskOnly ? 2 : 1)
                                    .testerForLoadingItemFromDiskToMemory((activity, bytes, elapsed) -> promote)
                                    .build()
                            : OffHeapCache.<String, ByteBuffer> builder()
                                    .capacityInMB(2)
                                    .offHeapStore(newInMemoryStore(backing))
                                    .storeSelector((key, value, bytes) -> diskOnly ? 2 : 1)
                                    .testerForLoadingItemFromDiskToMemory((activity, bytes, elapsed) -> promote)
                                    .build();
                    try {
                        // Ordinary direct buffers share the mapped-buffer superclass on this JDK.
                        // They must still read back on the heap without a native allocation per get.
                        final ByteBuffer writable = ByteBuffer.allocateDirect(size).put(expected);
                        final ByteBuffer input = readOnly ? writable.asReadOnlyBuffer() : writable;
                        input.position(1).mark().position(size);
                        assertTrue(cache.put("direct", input));
                        assertEquals(size, input.position());
                        assertEquals(size, input.limit());
                        input.reset();
                        assertEquals(1, input.position());
                        writable.put(0, (byte) 99);

                        assertEquals(diskOnly ? 1L : 0L, cache.stats().sizeOnDisk());
                        final ByteBuffer first = cache.getOrNull("direct");
                        assertHeapBufferContent(expected, first);
                        assertEquals(diskOnly && !promote ? 1L : 0L, cache.stats().sizeOnDisk());
                        first.array()[0] = 99;
                        assertHeapBufferContent(expected, cache.getOrNull("direct"));
                    } finally {
                        cache.close();
                    }
                }
            }
        }
    }

    private static void assertHeapBufferContent(final byte[] expected, final ByteBuffer result) {
        assertNotNull(result);
        assertFalse(result.isDirect(), "buffer reads must not allocate direct memory");
        assertFalse(result.isReadOnly());
        assertTrue(result.hasArray());
        assertEquals(expected.length, result.position());
        assertEquals(expected.length, result.limit());
        assertArrayEquals(expected, result.array());
        assertArrayEquals(expected, ByteBufferType.byteArrayOf(result));
    }

    private static void assertPromotionDoesNotReviveEntry(final String expiryField) throws Exception {
        final Map<String, byte[]> backing = new ConcurrentHashMap<>();
        final PromotionCopyOffHeapCache cache = new PromotionCopyOffHeapCache(backing);
        try {
            assertTrue(cache.put("key", new byte[] { 1, 2, 3 }));
            final ActivityPrint original = entriesOf(cache).get("key").activityPrint;
            // Advance the entry's elapsed time at the native-copy boundary without wall-clock sleeps.
            cache.duringCopy = () -> setActivityTime(original, expiryField, 0L);

            assertArrayEquals(new byte[] { 1, 2, 3 }, cache.getOrNull("key"), "the read began while the entry was live");
            assertFalse(cache.containsKey("key"), "promotion must not give the expired entry a fresh clock");
            assertEquals(1L, cache.stats().sizeOnDisk(), "an expired promotion must retain the original disk mapping until cleanup");
            assertEquals(0L, cache.stats().occupiedMemory());
            assertTrue(cache.stats().occupiedSlots().isEmpty(), "abandoned promotion slots must be released and reclaimed");
            assertNull(cache.getOrNull("key"));
            assertEquals(0, cache.size());
            assertEquals(1L, cache.stats().evictionCountFromDisk());
            assertTrue(backing.isEmpty());
        } finally {
            cache.close();
        }
    }

    private static void assertMaintenancePassSurvivesAndKeepsSweeping(final Throwable failure) throws InterruptedException {
        final OffHeapCache<ThrowingHashKey, byte[]> cache = OffHeapCache.<ThrowingHashKey, byte[]> builder().capacityInMB(1).evictDelay(50).build();
        try {
            final ThrowingHashKey key = new ThrowingHashKey();
            // Long enough that no pass can sweep the entry before the failure is armed.
            assertTrue(cache.put(key, new byte[] { 1 }, 200, 0));

            // A pass hashes only an expired entry: the next hashCode call comes from the sweep's
            // removeIfCurrent -> entries.compute.
            key.throwOnNextHashCalls(1, failure);

            for (int i = 0; i < 100 && cache.size() != 0; i++) {
                Thread.sleep(50L);
            }

            assertEquals(0, key.remainingFailures(), "a maintenance pass must have hit the armed failure");
            assertEquals(0, cache.size(), "the maintenance task must survive the failure and sweep the expired entry on a later pass");
            assertEquals(1L, cache.stats().evictionCount());
        } finally {
            cache.close();
        }
    }

    /** Map-backed store whose {@code remove} throws {@code failure} (undeclared if checked) while {@code failingRemoves} is positive. */
    private static OffHeapStore<String> newRemoveFailingStore(final Map<String, byte[]> backing, final AtomicInteger failingRemoves, final Throwable failure) {
        return new OffHeapStore<>() {
            @Override
            public boolean put(final String key, final byte[] value) {
                backing.put(key, value);
                return true;
            }

            @Override
            public byte[] get(final String key) {
                return backing.get(key);
            }

            @Override
            public boolean remove(final String key) {
                if (failingRemoves.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
                    throw AbstractOffHeapCacheTest.<RuntimeException> sneakyThrow(failure);
                }

                return backing.remove(key) != null;
            }
        };
    }

    /** Throws {@code failure} unchanged; a checked one escapes without being declared. */
    @SuppressWarnings("unchecked")
    private static <T extends Throwable> T sneakyThrow(final Throwable failure) throws T {
        throw (T) failure;
    }

    /** Key whose {@code hashCode} can be armed to throw a given throwable (undeclared if checked) a set number of times. */
    private static final class ThrowingHashKey {
        private final AtomicInteger remainingFailures = new AtomicInteger();
        private volatile Throwable failure;

        void throwOnNextHashCalls(final int count, final Throwable failure) {
            this.failure = failure;
            remainingFailures.set(count);
        }

        int remainingFailures() {
            return remainingFailures.get();
        }

        @Override
        public int hashCode() {
            if (remainingFailures.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
                throw AbstractOffHeapCacheTest.<RuntimeException> sneakyThrow(failure);
            }

            return 31;
        }
    }

    /** Heap-array fixture over {@code Object} keys whose logger can be observed. */
    private static final class HeapOffHeapCache extends AbstractOffHeapCache<Object, byte[]> {
        private final byte[] memory = new byte[SEGMENT_SIZE];

        HeapOffHeapCache(final Logger logger) {
            this(logger, null);
        }

        HeapOffHeapCache(final Logger logger, final OffHeapStore<Object> store) {
            super(1, DEFAULT_MAX_BLOCK_SIZE, 0, 60_000L, 60_000L, DEFAULT_VACATING_FACTOR, 0, null, null, store, false, null,
                    store == null ? null : (key, value, size) -> 2, logger);
        }

        @Override
        protected long allocate(final long capacityInBytes) {
            return 0L;
        }

        @Override
        protected void deallocate() {
            // Heap-array fixture: nothing to release.
        }

        @Override
        protected void copyToMemory(final long startPtr, final byte[] bytes, final int srcOffset, final int len) {
            System.arraycopy(bytes, srcOffset, memory, (int) startPtr, len);
        }

        @Override
        protected void copyFromMemory(final long startPtr, final byte[] bytes, final int destOffset, final int len) {
            System.arraycopy(memory, (int) startPtr, bytes, destOffset, len);
        }
    }
}
