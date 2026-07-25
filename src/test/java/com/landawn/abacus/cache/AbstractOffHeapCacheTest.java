/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.type.ByteBufferType;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.N;

@Tag("2025")
public class AbstractOffHeapCacheTest {

    private static int totalOccupiedSlots(final OffHeapCacheStats stats) {
        int total = 0;
        for (final Map<Integer, Integer> perSegment : stats.occupiedSlots().values()) {
            for (final int cardinality : perSegment.values()) {
                total += cardinality;
            }
        }
        return total;
    }

    /**
     * The {@code stats()} occupied-slot reporting reads each segment's occupied-slot count under
     * the allocator lock. This verifies the reported per-segment occupied-slot total matches the
     * number of in-memory entries.
     */
    @Test
    public void testStatsOccupiedSlotsReflectInMemoryEntries() {
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
    public void testClearReleasesAllSegments() {
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
     * Exercises allocation across multiple slot-size classes, which depends on every size-class queue
     * in {@code _segmentQueues} being present. Those queues are now populated eagerly in the
     * constructor (instead of via unsafe lazy double-checked locking), so writes spanning several size
     * classes must all succeed and round-trip.
     */
    @Test
    public void testAllocationAcrossMultipleSizeClasses() {
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
    public void testRepeatedCreateAndCloseDoesNotLeakScheduledTasks() {
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
    public void testPutAfterCloseFailsBeforeNativeCopy() {
        final GuardedOffHeapCache cache = new GuardedOffHeapCache();
        cache.close();

        assertThrows(IllegalStateException.class, () -> cache.put("k", new byte[] { 1 }));
        assertFalse(cache.copiedAfterDeallocate.get(), "put after close must not copy into deallocated memory");
    }

    /** A user callback cannot upgrade the operation's lifecycle read lock into close's write lock. */
    @Test
    public void testReentrantCloseFromSerializerFailsFastAndCacheRemainsOpen() {
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

    /** Invalid custom routing results are programming errors, not undocumented aliases. */
    @Test
    public void testStoreSelectorRejectsUnknownAndNullResults() {
        final OffHeapCache<String, byte[]> unknownSelectorCache = OffHeapCache.<String, byte[]> builder().capacityInMB(1).storeSelector((key, value, size) -> 3).build();
        try {
            assertThrows(IllegalArgumentException.class, () -> unknownSelectorCache.put("k", new byte[] { 1 }));
            assertEquals(0, unknownSelectorCache.size());
        } finally {
            unknownSelectorCache.close();
        }

        final OffHeapCache<String, byte[]> nullSelectorCache = OffHeapCache.<String, byte[]> builder().capacityInMB(1).storeSelector((key, value, size) -> null).build();
        try {
            assertThrows(IllegalArgumentException.class, () -> nullSelectorCache.put("k", new byte[] { 1 }));
            assertEquals(0, nullSelectorCache.size());
        } finally {
            nullSelectorCache.close();
        }
    }

    /**
     * A failure raised before the per-key atomic replacement begins (here: an exceptional
     * {@code hashCode} thrown by the map lookup itself) must fail the put BEFORE the key-only
     * backing store is mutated; the prior memory entry must survive and the store must remain
     * untouched.
     */
    @Test
    public void testDiskOwnershipFailurePrecedesBackingStoreMutation() {
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
    public void testDiskTimingStatsReflectStoreIo() {
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
    public void testFreedDiskEntryReadsAsMissWithoutTouchingStore() throws Exception {
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

    /**
     * The chunk-count arithmetic must not overflow int for serialized sizes near
     * {@code Integer.MAX_VALUE} (legal in a sufficiently large cache): computing
     * {@code size + maxBlockSize - 1} in int arithmetic wraps negative, which made {@code put}
     * fail with {@code NegativeArraySizeException} instead of storing the value.
     */
    @Test
    public void testChunkCountDoesNotOverflowForHugeSizes() throws Exception {
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
     * Memory-tier reads must participate in the lifecycle exclusion: a reader holds the lifecycle
     * read lock across its native copy, so {@code close()} (which deallocates under the write
     * lock) can never free the region underneath it - even when the entry has been detached from
     * the map by a concurrent removal and close's own entry sweep therefore never sees it. This
     * test simulates the detach (a bare map remove), parks a reader inside {@code copyFromMemory},
     * and verifies {@code close()} cannot complete - and native memory cannot be poisoned - until
     * the read finishes.
     */
    @Test
    public void testMemoryReadBlocksCloseUntilReadCompletes() throws Exception {
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

    // --- Default DESERIALIZER raw-type branches -------------------------------------------------
    // The memory/disk wrappers special-case byte[] and ByteBuffer values before ever calling the
    // configured deserializer, so the raw-type branches of the package-private default DESERIALIZER
    // are only reachable by invoking it directly. These two tests exercise those branches.

    /** {@code DESERIALIZER} returns the supplied array unchanged for a primitive {@code byte[]} type. */
    @Test
    public void testDeserializer_PrimitiveByteArray() {
        final byte[] raw = { 1, 2, 3, 4, 5 };
        final Type<?> type = N.typeOf(byte[].class);

        final Object result = AbstractOffHeapCache.DESERIALIZER.apply(raw, type);

        assertSame(raw, result, "primitive byte[] must be returned as-is without copying");
    }

    /** {@code DESERIALIZER} wraps the supplied bytes in a {@link ByteBuffer} for a ByteBuffer type. */
    @Test
    public void testDeserializer_ByteBuffer() {
        final byte[] raw = "byte-buffer-payload".getBytes();
        final Type<?> type = N.typeOf(ByteBuffer.class);

        final Object result = AbstractOffHeapCache.DESERIALIZER.apply(raw, type);

        assertTrue(result instanceof ByteBuffer, "ByteBuffer type must deserialize to a ByteBuffer");
        assertArrayEquals(raw, ByteBufferType.byteArrayOf((ByteBuffer) result));
    }

    // --- ByteBuffer value round-trips through each storage form ---------------------------------
    // Exercise the ByteBuffer serialization branch in put() and the ByteBuffer read branch for
    // each entry form (single-slot, multi-slot, and disk-backed).

    /** Small ByteBuffer value: stored in a single slot and read back from memory. */
    @Test
    public void testByteBufferValue_SingleSlot_roundtrip() {
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
    public void testByteBufferPutPreservesCallerStateAndMark() {
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
    public void testByteBufferValue_MultiSlot_roundtrip() {
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

    /** Disk-routed ByteBuffer value: read back from the store and deserialized. */
    @Test
    public void testByteBufferValue_DiskStore_roundtrip() {
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
    public void testDiskReadReturnsDefensiveCopy() {
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
    public void testThrowingDeserializerDoesNotIncrementDiskHitCount() {
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

    /** A custom in-place decoder must not alter bytes that a successful disk read promotes. */
    @Test
    public void testMutatingDeserializerCannotCorruptPromotedBytes() {
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

    // --- Disk -> memory promotion (testerForLoadingItemFromDiskToMemory) ------------------------
    // With a promotion tester that always returns true, a disk-only put followed by a get reads the
    // bytes from disk and copies them back into off-heap memory, retiring the on-disk copy.

    /** Promotion of a small disk-stored value back into a single memory slot. */
    @Test
    public void testDiskToMemoryPromotion_SingleSlot() {
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
    public void testDiskToMemoryPromotion_MultiSlot() {
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
    public void testDiskToMemoryPromotionPreservesRemainingLiveTime() throws Exception {
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
            Thread.sleep(75L);
            assertArrayEquals(new byte[] { 1, 2, 3 }, cache.getOrNull("k"));
            assertEquals(0L, cache.stats().sizeOnDisk(), "the entry should have been promoted");

            final AbstractOffHeapCache.Entry<byte[]> promoted = entriesOf(cache).get("k");
            final long promotedLiveTime = promoted.activityPrint.getMaxLiveTime();
            assertTrue(promotedLiveTime > 0 && promotedLiveTime < configuredLiveTime,
                    "promotion should install the positive remaining TTL, not a fresh/full or elapsed lifetime: " + promotedLiveTime);
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
    public void testConcurrentSameKeyDiskPutsAreAtomicAcrossStoreAndPool() throws Exception {
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
                            releasePriorWrapper.await();
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
    public void testStaleDiskReadDoesNotReinsertConcurrentReplacement() throws Exception {
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
                        releaseFirstRead.await();
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
                final Future<Boolean> replacementPut = executor.submit(() -> cache.put("k", replacement));
                // The replacement blocks retiring the prior entry (the stale reader holds its monitor
                // during the store fetch), so the bytes cannot be overwritten underneath the reader.
                // Give the put a moment to reach that blocking point; the assertions below must hold
                // in every interleaving either way.
                Thread.sleep(100L);
                releaseFirstRead.countDown();

                assertEquals(null, staleRead.get(5, TimeUnit.SECONDS));
                assertTrue(replacementPut.get(5, TimeUnit.SECONDS));
                assertEquals(2L, cache.stats().putCount(), "stale cleanup must not count a remove/reinsert of the replacement as another put");
                assertArrayEquals(replacement, cache.getOrNull("k"));
            } finally {
                cache.close();
            }
        } finally {
            releaseFirstRead.countDown();
            executor.shutdownNow();
        }
    }

    /** A stale promotion attempt must likewise leave a concurrent replacement continuously installed. */
    @Test
    public void testStalePromotionDoesNotReinsertConcurrentReplacement() throws Exception {
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
                                releaseFirstPromotion.await();
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
     * Close must wait for an in-flight {@code remove(key)} of a disk entry (blocked on the
     * entry's monitor while a reader-style holder pins it) to finish its {@code OffHeapStore}
     * cleanup; otherwise close could close the store underneath the in-flight
     * {@code OffHeapStore.remove} call.
     */
    @Test
    public void testRemoveFinishesDiskCleanupBeforeStoreClose() throws Exception {
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
                        releaseWrapper.await();
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
     * A failed in-memory put of a value that COULD fit an empty cache (genuine memory pressure,
     * not an impossible size) schedules the asynchronous vacating task. Exercises the
     * {@code vacate()} scheduling path end-to-end: the task evicts entries and reclaims their
     * now-empty segments, keeping the cache usable. (A value larger than the entire capacity no
     * longer triggers a vacate - the in-memory attempt is skipped for it entirely.)
     */
    @Test
    public void testVacateScheduledWhenMemoryFullForNewKey() throws InterruptedException {
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
     * An asynchronous vacate can finish evicting entries and still be reclaiming segment metadata.
     * Close must wait for that entire pass; otherwise it can deallocate the cache while the task
     * is still traversing cache-owned structures. The allocator lock provides a deterministic
     * pause after the eviction while the lifecycle read lock remains held.
     */
    @Test
    public void testCloseWaitsForInFlightVacationTask() throws Exception {
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

    /**
     * Replacing a memory-backed entry with a disk-routed value: {@code putToDisk} removes and
     * destroys the prior in-memory wrapper before installing the new {@code StoreWrapper}.
     */
    @Test
    public void testPutToDisk_replacesMemoryWrapper() {
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
     * Idle-time expiry (as opposed to TTL expiry, which every other expiry test uses): an entry
     * whose {@code maxIdleTime} elapses without an access reads as absent, is removed lazily, and
     * is counted as an eviction.
     */
    @Test
    public void testIdleTimeExpiryLazyOnAccess() throws Exception {
        final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder().capacityInMB(1).evictDelay(0).build();
        try {
            assertTrue(cache.put("k", new byte[] { 1, 2, 3 }, 0, 150));
            Thread.sleep(400L);

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
    public void testMaintenanceSweepRemovesExpiredEntries() throws Exception {
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

    /**
     * A value whose serialized form exceeds the ENTIRE capacity skips the in-memory attempt but
     * still spills to a configured disk store — without scheduling a vacate (nothing could ever
     * make it fit in memory).
     */
    @Test
    public void testOversizedValueSpillsToDiskWithoutVacate() {
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
     * Deterministic regression for the detached-free race: a {@code remove(key)} whose disk
     * cleanup is delayed (its entry monitor is pinned) must not delete the store bytes a
     * concurrent same-key disk put writes. The remove now frees inside the per-key compute, so
     * the racing put serializes behind it and the final state is the put's value — both in the
     * map and in the store.
     */
    @Test
    public void testRemoveRacingSameKeyDiskPutCannotDeleteNewBytes() throws Exception {
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
                            releaseEntry.await();
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }, "offheap-test-remove-race-holder");
                monitorHolder.start();
                assertTrue(entryLocked.await(5, TimeUnit.SECONDS));

                // The remove enters the per-key compute (owning the bin) and blocks on the pinned
                // entry monitor; the same-key disk put then queues behind it on the bin.
                final Future<?> removeResult = executor.submit(() -> cache.remove("k"));
                Thread.sleep(100L);
                final byte[] newValue = { 2, 2 };
                final Future<Boolean> putResult = executor.submit(() -> cache.put("k", newValue));
                Thread.sleep(100L);

                releaseEntry.countDown();
                removeResult.get(5, TimeUnit.SECONDS);
                assertTrue(putResult.get(5, TimeUnit.SECONDS));
                monitorHolder.join(5_000L);

                assertArrayEquals(newValue, cache.getOrNull("k"), "the put's value must survive the racing remove's disk cleanup");
                assertArrayEquals(newValue, backing.get("k"), "the put's store bytes must survive the racing remove's disk cleanup");
            } finally {
                cache.close();
            }
        } finally {
            releaseEntry.countDown();
            executor.shutdownNow();
        }
    }

    /** {@code vacatingFactor(1.0f)}: a vacate pass evicts every memory-resident entry. */
    @Test
    public void testVacatingFactorOneEvictsEverything() throws Exception {
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
     * A failed multi-class allocation followed by a successful disk fallback must not leave its
     * now-empty segment dedicated to the attempted class; the very next differently sized memory
     * put must be able to reuse it without waiting for an asynchronous vacate pass.
     */
    @Test
    public void testPartialAllocationRollbackImmediatelyReclaimsEmptySegments() {
        final Map<String, byte[]> backing = new ConcurrentHashMap<>();

        final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                .capacityInMB(1)
                .maxBlockSizeInBytes(512 * 1024)
                .evictDelay(0)
                .offHeapStore(newInMemoryStore(backing))
                .storeSelector((key, value, size) -> "spill".equals(key) ? 0 : 1)
                .build();
        try {
            // One 512 KiB chunk dedicates the only segment; the final one-byte chunk needs a
            // different size class, so memory placement rolls back and the value spills to disk.
            assertTrue(cache.put("spill", new byte[512 * 1024 + 1]));
            assertEquals(1L, cache.stats().sizeOnDisk());

            assertTrue(cache.put("small", new byte[] { 7 }), "the rollback must make the sole segment immediately reusable");
            assertArrayEquals(new byte[] { 7 }, cache.getOrNull("small"));
        } finally {
            cache.close();
        }
    }

    /** Failed entry construction/copy must release and reclaim all unpublished slots. */
    @Test
    public void testInitialCopyFailureDoesNotStrandSegment() {
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
     * A zero-length {@code byte[]} value is a legal cache value that exercises the size-0 path of
     * the slot math ({@code slotSizeOfChunk} maps a 0-byte chunk to one {@code MIN_BLOCK_SIZE}
     * slot): the entry must occupy a slot, read back as a non-null empty array (a hit, never
     * mistaken for a miss), and release its slot on removal.
     */
    @Test
    public void testZeroLengthByteArrayRoundTripInMemory() {
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
    public void testZeroPositionByteBufferRoundTrip() {
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

    /**
     * A zero-length value routed to the disk tier: the store receives an empty array, the read
     * passes the size consistency check ({@code storeBytes.length == entry.size == 0}), and the
     * value reads back as a non-null empty array counted as a disk hit.
     */
    @Test
    public void testZeroLengthValueOnDiskTier() {
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

    // TODO: The shutdown-hook-registration failure path and the background-eviction error handler
    // (AbstractOffHeapCache constructor catch blocks) require the JVM to be mid-shutdown, which cannot
    // be simulated deterministically in an isolated unit test; left uncovered intentionally.
}
