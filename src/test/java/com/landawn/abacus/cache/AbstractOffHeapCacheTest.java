/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
}
