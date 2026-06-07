/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
        try (OffHeapCache<String, String> cache = OffHeapCache.<String, String> builder()
                .capacityInMB(16)
                .evictDelay(0)
                .build()) {
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
