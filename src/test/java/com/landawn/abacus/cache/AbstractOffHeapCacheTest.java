/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.pool.KeyedObjectPool;
import com.landawn.abacus.type.ByteBufferType;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.AsyncExecutor;
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
     * Regression coverage for the {@code stats()} occupied-slot reporting, which reads each segment's
     * slot {@link java.util.BitSet} cardinality. The read now goes through the synchronized
     * {@code Segment.cardinality()} accessor (the {@code BitSet} is mutated under its own monitor and
     * is not thread-safe). This verifies the reported per-segment occupied-slot total matches the
     * number of in-memory entries.
     */
    @Test
    public void testStatsOccupiedSlotsReflectInMemoryEntries() {
        try (OffHeapCache<String, String> cache = OffHeapCache.<String, String> builder()
                .capacityInMB(16)
                .evictDelay(0)
                .defaultLiveTime(600_000)
                .defaultMaxIdleTime(600_000)
                .build()) {
            for (int i = 0; i < 50; i++) {
                assertTrue(cache.put("k" + i, "v" + i));
            }

            final OffHeapCacheStats stats = cache.stats();
            assertEquals(50, stats.size(), "in-memory entry count");
            assertEquals(50, totalOccupiedSlots(stats), "sum of per-segment occupied slots should equal the entry count");
        }
    }

    /**
     * Regression coverage for {@code evict()} releasing now-empty segments. Emptiness is checked via
     * the synchronized {@code Segment.isEmpty()} accessor. After {@code clear()} (which evicts every
     * entry and then reclaims empty segments) no occupied slots should remain.
     */
    @Test
    public void testClearReleasesAllSegments() {
        try (OffHeapCache<String, String> cache = OffHeapCache.<String, String> builder()
                .capacityInMB(16)
                .evictDelay(0)
                .defaultLiveTime(600_000)
                .defaultMaxIdleTime(600_000)
                .build()) {
            for (int i = 0; i < 50; i++) {
                assertTrue(cache.put("k" + i, "v" + i));
            }
            assertTrue(totalOccupiedSlots(cache.stats()) > 0);

            cache.clear();

            final OffHeapCacheStats stats = cache.stats();
            assertEquals(0, stats.size(), "cache should be empty after clear()");
            assertEquals(0, totalOccupiedSlots(stats), "all segments should be released after clear()");
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
        try (OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                .capacityInMB(32)
                .evictDelay(0)
                .defaultLiveTime(600_000)
                .defaultMaxIdleTime(600_000)
                .build()) {
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
        }
    }

    /**
     * Regression coverage for {@code stats()} being crash-safe against a negative disk-I/O timing
     * observation. Disk read/write elapsed times are measured with wall-clock
     * {@code System.currentTimeMillis()}; a backward clock adjustment (NTP correction, manual change,
     * VM migration) during an in-flight I/O op can yield a negative duration. That negative would
     * flow through {@link java.util.LongSummaryStatistics} into {@code stats()}, whose
     * {@code OffHeapCacheStats.MinMaxAvg} canonical constructor rejects negatives with
     * {@code IllegalArgumentException} — turning a monitoring call into a failure.
     *
     * <p>After the fix {@code stats()} clamps each min/max/avg component to {@code >= 0}, so it
     * returns a valid, non-negative snapshot regardless of a stray negative observation. The
     * package-private timing accumulators are written under their own monitor in production;
     * single-threaded direct injection here simulates the recorded-negative state.
     */
    @Test
    public void testStatsClampsNegativeDiskTimingObservations() {
        try (OffHeapCache<String, String> cache = OffHeapCache.<String, String> builder().capacityInMB(16).evictDelay(0).build()) {
            cache.totalReadFromDiskTimeStats.accept(-5L);
            cache.totalWriteToDiskTimeStats.accept(-7L);

            // Before the fix this threw IllegalArgumentException from the MinMaxAvg constructor.
            final OffHeapCacheStats stats = cache.stats();

            assertEquals(0.0D, stats.readFromDiskTimeStats().min(), "negative read min must be clamped to 0");
            assertEquals(0.0D, stats.readFromDiskTimeStats().max(), "negative read max must be clamped to 0");
            assertEquals(0.0D, stats.readFromDiskTimeStats().avg(), "negative read avg must be clamped to 0");
            assertEquals(0.0D, stats.writeToDiskTimeStats().min(), "negative write min must be clamped to 0");
            assertEquals(0.0D, stats.writeToDiskTimeStats().max(), "negative write max must be clamped to 0");
            assertEquals(0.0D, stats.writeToDiskTimeStats().avg(), "negative write avg must be clamped to 0");
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
            try (OffHeapCache<String, String> cache = OffHeapCache.<String, String> builder()
                    .capacityInMB(16)
                    .evictDelay(5L * 60_000L)
                    .defaultLiveTime(60_000)
                    .defaultMaxIdleTime(60_000)
                    .build()) {
                assertTrue(cache.put("k", "v"));
            }
        }
    }

    /**
     * Regression coverage for the post-close {@code put()} ordering bug. The off-heap cache frees
     * its native allocation in {@code close()}; a later {@code put()} must fail before any slot
     * allocation or native-copy hook is reached. Previously the closed pool was only consulted after
     * {@code copyToMemory(...)}, which made {@code OffHeapCache} write to freed native memory.
     */
    @Test
    public void testPutAfterCloseFailsBeforeNativeCopy() {
        final GuardedOffHeapCache cache = new GuardedOffHeapCache();
        cache.close();

        assertThrows(IllegalStateException.class, () -> cache.put("k", new byte[] { 1 }));
        assertFalse(cache.copiedAfterDeallocate.get(), "put after close must not copy into deallocated memory");
    }

    /** Invalid custom routing results are programming errors, not undocumented aliases. */
    @Test
    public void testStoreSelectorRejectsUnknownAndNullResults() {
        try (OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder().capacityInMB(1).storeSelector((key, value, size) -> 3).build()) {
            assertThrows(IllegalArgumentException.class, () -> cache.put("k", new byte[] { 1 }));
            assertEquals(0, cache.size());
        }

        try (OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder().capacityInMB(1).storeSelector((key, value, size) -> null).build()) {
            assertThrows(IllegalArgumentException.class, () -> cache.put("k", new byte[] { 1 }));
            assertEquals(0, cache.size());
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
    private static <K, V> KeyedObjectPool<K, AbstractOffHeapCache.Wrapper<V>> poolOf(final AbstractOffHeapCache<K, V> cache)
            throws ReflectiveOperationException {
        final Field poolField = AbstractOffHeapCache.class.getDeclaredField("_pool");
        poolField.setAccessible(true);
        return (KeyedObjectPool<K, AbstractOffHeapCache.Wrapper<V>>) poolField.get(cache);
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

    // --- ByteBuffer value round-trips through each wrapper kind ---------------------------------
    // Exercise the ByteBuffer serialization branch in put() and the ByteBuffer read branch in each
    // wrapper (single-slot, multi-slot, and disk-backed StoreWrapper).

    /** Small ByteBuffer value: stored in a single slot and read back through SlotWrapper. */
    @Test
    public void testByteBufferValue_SingleSlot_roundtrip() {
        final byte[] data = "hello-single-slot-byte-buffer".getBytes();
        try (OffHeapCache<String, ByteBuffer> cache = OffHeapCache.<String, ByteBuffer> builder()
                .capacityInMB(8)
                .evictDelay(0)
                .defaultLiveTime(600_000)
                .defaultMaxIdleTime(600_000)
                .build()) {
            assertTrue(cache.put("k", bufferOf(data)));

            final ByteBuffer out = cache.getOrNull("k");
            assertNotNull(out);
            assertArrayEquals(data, ByteBufferType.byteArrayOf(out));
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

        try (OffHeapCache<String, ByteBuffer> cache = OffHeapCache.<String, ByteBuffer> builder().capacityInMB(1).evictDelay(0).build()) {
            assertTrue(cache.put("k", input));
            assertEquals(data.length, input.position(), "put must preserve the caller's position");
            assertEquals(data.length, input.limit(), "put must preserve the caller's limit");

            input.reset(); // Before the fix this threw InvalidMarkException.
            assertEquals(3, input.position(), "put must preserve the caller's mark");
            assertArrayEquals(data, ByteBufferType.byteArrayOf(cache.getOrNull("k")));
        }
    }

    /** Large ByteBuffer value (> maxBlockSize): split across slots and read back through MultiSlotsWrapper. */
    @Test
    public void testByteBufferValue_MultiSlot_roundtrip() {
        final byte[] data = new byte[20_000];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i * 31 + 7);
        }
        try (OffHeapCache<String, ByteBuffer> cache = OffHeapCache.<String, ByteBuffer> builder()
                .capacityInMB(16)
                .evictDelay(0)
                .defaultLiveTime(600_000)
                .defaultMaxIdleTime(600_000)
                .build()) {
            assertTrue(cache.put("big", bufferOf(data)));

            final ByteBuffer out = cache.getOrNull("big");
            assertNotNull(out);
            assertArrayEquals(data, ByteBufferType.byteArrayOf(out));
        }
    }

    /** Disk-routed ByteBuffer value: read back through StoreWrapper.deserialize. */
    @Test
    public void testByteBufferValue_DiskStore_roundtrip() {
        final Map<String, byte[]> backing = new ConcurrentHashMap<>();
        final byte[] data = "disk-resident-byte-buffer-payload".getBytes();
        try (OffHeapCache<String, ByteBuffer> cache = OffHeapCache.<String, ByteBuffer> builder()
                .capacityInMB(8)
                .evictDelay(0)
                .defaultLiveTime(600_000)
                .defaultMaxIdleTime(600_000)
                .offHeapStore(newInMemoryStore(backing))
                .storeSelector((k, v, size) -> 2) // disk only
                .build()) {
            assertTrue(cache.put("k", bufferOf(data)));
            assertEquals(1L, cache.stats().sizeOnDisk());

            final ByteBuffer out = cache.getOrNull("k");
            assertNotNull(out);
            assertArrayEquals(data, ByteBufferType.byteArrayOf(out));
        }
    }

    /** A store may retain and return its own array; disk reads must never expose that mutable array. */
    @Test
    public void testDiskReadReturnsDefensiveCopy() {
        final Map<String, byte[]> backing = new ConcurrentHashMap<>();
        final byte[] expected = { 1, 2, 3, 4 };

        try (OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                .capacityInMB(1)
                .evictDelay(0)
                .offHeapStore(newInMemoryStore(backing))
                .storeSelector((k, v, size) -> 2)
                .build()) {
            assertTrue(cache.put("k", expected));

            final byte[] firstRead = cache.getOrNull("k");
            firstRead[0] = 99;

            assertArrayEquals(expected, cache.getOrNull("k"), "mutating a returned value must not alter the disk-resident cache entry");
            assertArrayEquals(expected, backing.get("k"), "the store's retained array must remain private");
        }
    }

    /** A failed custom deserialization is not a successful disk hit. */
    @Test
    public void testThrowingDeserializerDoesNotIncrementDiskHitCount() {
        final Map<String, byte[]> backing = new ConcurrentHashMap<>();

        try (OffHeapCache<String, String> cache = OffHeapCache.<String, String> builder()
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
                .build()) {
            assertTrue(cache.put("k", "value"));
            assertThrows(IllegalStateException.class, () -> cache.getOrNull("k"));
            assertEquals(0L, cache.stats().hitCountFromDisk());
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
        try (OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                .capacityInMB(8)
                .evictDelay(0)
                .defaultLiveTime(600_000)
                .defaultMaxIdleTime(600_000)
                .offHeapStore(newInMemoryStore(backing))
                .storeSelector((k, v, size) -> 2) // disk only on put
                .testerForLoadingItemFromDiskToMemory((activityPrint, size, elapsed) -> true)
                .build()) {
            assertTrue(cache.put("k", data));
            assertEquals(1L, cache.stats().sizeOnDisk());

            // First get reads from disk and promotes the value into a memory slot.
            assertArrayEquals(data, cache.getOrNull("k"));
            // After promotion the on-disk copy is retired.
            assertEquals(0L, cache.stats().sizeOnDisk());
            // Value is still readable from memory.
            assertArrayEquals(data, cache.getOrNull("k"));
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
        try (OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                .capacityInMB(16)
                .evictDelay(0)
                .defaultLiveTime(600_000)
                .defaultMaxIdleTime(600_000)
                .offHeapStore(newInMemoryStore(backing))
                .storeSelector((k, v, size) -> 2) // disk only on put
                .testerForLoadingItemFromDiskToMemory((activityPrint, size, elapsed) -> true)
                .build()) {
            assertTrue(cache.put("big", data));
            assertEquals(1L, cache.stats().sizeOnDisk());

            // First get reads from disk and promotes the value into multiple memory slots.
            assertArrayEquals(data, cache.getOrNull("big"));
            assertEquals(0L, cache.stats().sizeOnDisk());
            assertArrayEquals(data, cache.getOrNull("big"));
        }
    }

    /** Promotion must carry forward the original entry's remaining TTL, not reset or collapse it. */
    @Test
    public void testDiskToMemoryPromotionPreservesRemainingLiveTime() throws Exception {
        final Map<String, byte[]> backing = new ConcurrentHashMap<>();
        final long configuredLiveTime = 5_000L;

        try (OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                .capacityInMB(1)
                .evictDelay(0)
                .offHeapStore(newInMemoryStore(backing))
                .storeSelector((k, v, size) -> 2)
                .testerForLoadingItemFromDiskToMemory((activity, size, elapsed) -> true)
                .build()) {
            assertTrue(cache.put("k", new byte[] { 1, 2, 3 }, configuredLiveTime, 10_000L));
            Thread.sleep(75L);
            assertArrayEquals(new byte[] { 1, 2, 3 }, cache.getOrNull("k"));
            assertEquals(0L, cache.stats().sizeOnDisk(), "the entry should have been promoted");

            final AbstractOffHeapCache.Wrapper<byte[]> promoted = poolOf(cache).get("k");
            final long promotedLiveTime = promoted.activityPrint().getMaxLiveTime();
            assertTrue(promotedLiveTime > 0 && promotedLiveTime < configuredLiveTime,
                    "promotion should install the positive remaining TTL, not a fresh/full or elapsed lifetime: " + promotedLiveTime);
        }
    }

    /**
     * A same-key disk replacement spans both the store and the pool. Without a mutation lock around
     * that whole transition, a second put can overwrite the bytes while the first put is paused
     * retiring its prior wrapper, leaving the final pool wrapper owned by different store bytes.
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

        try (OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                .capacityInMB(1)
                .evictDelay(0)
                .offHeapStore(store)
                .storeSelector((k, v, size) -> routeToDisk.get() ? 2 : 1)
                .build()) {
            assertTrue(cache.put("k", new byte[] { 0 }));
            final AbstractOffHeapCache.Wrapper<byte[]> priorWrapper = poolOf(cache).get("k");

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

        try (OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                .capacityInMB(1)
                .evictDelay(0)
                .offHeapStore(store)
                .storeSelector((k, v, size) -> 2)
                .build()) {
            assertTrue(cache.put("k", new byte[] { 1 }));
            final Future<byte[]> staleRead = executor.submit(() -> cache.getOrNull("k"));
            assertTrue(firstReadEntered.await(5, TimeUnit.SECONDS));

            final byte[] replacement = { 2, 2 };
            final Future<Boolean> replacementPut = executor.submit(() -> cache.put("k", replacement));
            // The replacement removes the old pool mapping before waiting for the first read's
            // per-store-key lock. Seeing size == 0 guarantees it owns the mutation lock already.
            for (int i = 0; i < 500 && cache.size() != 0; i++) {
                Thread.sleep(10L);
            }
            assertEquals(0, cache.size());
            releaseFirstRead.countDown();

            assertEquals(null, staleRead.get(5, TimeUnit.SECONDS));
            assertTrue(replacementPut.get(5, TimeUnit.SECONDS));
            assertEquals(2L, cache.stats().putCount(), "stale cleanup must not count a remove/reinsert of the replacement as another put");
            assertArrayEquals(replacement, cache.getOrNull("k"));
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

        try (OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
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
                .build()) {
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
            releaseFirstPromotion.countDown();
            executor.shutdownNow();
        }
    }

    /**
     * Removing a disk entry detaches its wrapper before invoking {@code OffHeapStore.remove}. Close
     * must wait for that detached wrapper's cleanup; otherwise it can close the store in the gap and
     * make the in-flight remove call into an already-closed resource.
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
            final AbstractOffHeapCache.Wrapper<byte[]> wrapper = poolOf(cache).get("k");
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
            for (int i = 0; i < 500 && cache.size() != 0; i++) {
                Thread.sleep(10L);
            }
            assertEquals(0, cache.size(), "remove should have detached the wrapper before blocking in store cleanup");

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
     * A failed in-memory put of a brand-new (non-replacing) oversized key schedules the asynchronous
     * vacating task. Exercises the {@code vacate()} scheduling path and keeps the cache usable.
     */
    @Test
    public void testVacateScheduledWhenMemoryFullForNewKey() throws InterruptedException {
        try (OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder().capacityInMB(1).evictDelay(0).build()) {
            // 2 MB value cannot fit the 1 MB cache; key is new so this is not a replacement and
            // therefore triggers vacate().
            assertFalse(cache.put("brand-new-oversized-key", new byte[2 * 1024 * 1024]));

            // Allow the asynchronously-scheduled vacating task to run (covers its lambda body).
            Thread.sleep(200);

            // Slots allocated during the failed put were released, so the cache is still usable.
            final byte[] ok = new byte[128];
            for (int i = 0; i < ok.length; i++) {
                ok[i] = (byte) i;
            }
            assertTrue(cache.put("ok", ok));
            assertArrayEquals(ok, cache.getOrNull("ok"));
        }
    }

    @Test
    public void testCloseShutsDownVacationExecutor() throws Exception {
        final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder().capacityInMB(1).evictDelay(0).build();

        try {
            // An oversized write activates the otherwise-lazy per-cache vacation executor.
            assertFalse(cache.put("oversized", new byte[2 * 1024 * 1024]));

            final Field field = AbstractOffHeapCache.class.getDeclaredField("_asyncExecutor");
            field.setAccessible(true);
            final AsyncExecutor executor = (AsyncExecutor) field.get(cache);

            cache.close();

            for (int i = 0; i < 100 && !executor.isTerminated(); i++) {
                Thread.sleep(10L);
            }

            assertTrue(executor.isTerminated(), "close() must terminate the vacation executor and release its shutdown hook");
        } finally {
            cache.close();
        }
    }

    /**
     * An asynchronous vacation can finish pool eviction and still be reclaiming segment metadata.
     * Close must wait for that entire maintenance pass; otherwise it can deallocate the cache while
     * the task is still traversing cache-owned structures. The segment-bit-set monitor provides a
     * deterministic pause after {@code evict()} while the lifecycle read lock remains held.
     */
    @Test
    public void testCloseWaitsForInFlightVacationTask() throws Exception {
        final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder().capacityInMB(1).evictDelay(0).build();
        final ExecutorService executor = Executors.newSingleThreadExecutor();

        final Field segmentBitSetField = AbstractOffHeapCache.class.getDeclaredField("_segmentBitSet");
        segmentBitSetField.setAccessible(true);
        final Object segmentBitSet = segmentBitSetField.get(cache);

        final Field lifecycleLockField = AbstractOffHeapCache.class.getDeclaredField("lifecycleLock");
        lifecycleLockField.setAccessible(true);
        final ReentrantReadWriteLock lifecycleLock = (ReentrantReadWriteLock) lifecycleLockField.get(cache);

        final Method vacateMethod = AbstractOffHeapCache.class.getDeclaredMethod("vacate");
        vacateMethod.setAccessible(true);

        Future<?> closeResult = null;
        try {
            synchronized (segmentBitSet) {
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
        try (OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder()
                .capacityInMB(8)
                .evictDelay(0)
                .defaultLiveTime(600_000)
                .defaultMaxIdleTime(600_000)
                .offHeapStore(newInMemoryStore(backing))
                .storeSelector((k, v, size) -> size < 1000 ? 1 : 2)
                .build()) {
            assertTrue(cache.put("k", new byte[100])); // memory-backed SlotWrapper
            assertEquals(0L, cache.stats().sizeOnDisk());

            final byte[] large = new byte[5000];
            for (int i = 0; i < large.length; i++) {
                large[i] = (byte) (i % 251);
            }
            // Replace the same key with a disk-routed value: prior memory wrapper is destroyed.
            assertTrue(cache.put("k", large));
            assertEquals(1L, cache.stats().sizeOnDisk());
            assertArrayEquals(large, cache.getOrNull("k"));
        }
    }

    // TODO: The shutdown-hook-registration failure path and the background-eviction error handler
    // (AbstractOffHeapCache constructor catch blocks) require the JVM to be mid-shutdown, which cannot
    // be simulated deterministically in an isolated unit test; left uncovered intentionally.
}
