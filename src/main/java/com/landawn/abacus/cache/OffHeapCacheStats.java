/*
 * Copyright (c) 2025, Haiyang Li.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.landawn.abacus.cache;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * An immutable snapshot of off-heap cache statistics at a specific point in time.
 * This record provides comprehensive metrics about off-heap cache performance, including
 * memory usage, disk operations, and segment allocation details. In addition to the basic
 * cache counters (size, put/get/hit/miss/eviction counts) it exposes off-heap-specific
 * metrics such as disk I/O timings and memory segment utilization.
 *
 * <p><b>&#9888;&#65039; Independently sampled fields:</b> The cache gathers these components from
 * several concurrent counters and data structures. Relationships described below are
 * conceptual and may be transiently inconsistent while operations are in flight.
 *
 * <p>Understanding the metrics:
 * <ul>
 * <li>Memory metrics: {@code allocatedMemory} is the total reserved off-heap memory;
 *     {@code occupiedMemory} is the slot space currently in use by memory-resident entries.</li>
 * <li>Data metrics: {@code dataSize} is the total serialized payload bytes tracked by the cache and
 *     covers BOTH the off-heap memory pool and the disk store. {@code dataSizeOnDisk} is the disk
 *     subset; the in-memory portion is {@code dataSize - dataSizeOnDisk}. The in-memory portion is
 *     typically smaller than {@code occupiedMemory} because the slot allocator rounds each entry up
 *     to the next multiple of the minimum block size (64 bytes).</li>
 * <li>Hit metrics: conceptually, {@code hitCount + missCount = getCount}. {@code hitCountFromDisk}
 *     normally represents the subset of {@code hitCount} served from disk.</li>
 * <li>Put metrics: {@code putCount} counts successful inserts into the pool (both memory- and
 *     disk-resident wrappers go through the same pool). Failed put attempts (e.g., wrapper allocation
 *     failed before reaching the pool) are not included. As an approximate identity,
 *     {@code putCount} &asymp; {@code size + evictionCount + removed/replaced entries}
 *     (note that {@code evictionCountFromDisk} is already included in {@code evictionCount};
 *     it is the disk-resident subset, not an additional count).</li>
 * </ul>
 *
 * <p><b>Usage Examples:</b>
 * <pre>{@code
 * OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]>builder()
 *     .capacityInMB(100)
 *     .evictDelay(60000)
 *     .build();
 * // ... use cache ...
 * OffHeapCacheStats stats = cache.stats();
 *
 * // Memory efficiency
 * double memUtilization = (double) stats.occupiedMemory() / stats.allocatedMemory();
 * System.out.println("Memory utilization: " + (memUtilization * 100) + "%");
 *
 * // Hit rate (hitRate() is zero-safe: it returns 0.0 when there have been no requests)
 * double hitRate = stats.hitRate();
 * System.out.println("Hit rate: " + (hitRate * 100) + "%");
 *
 * // Disk performance
 * System.out.println("Disk write avg time: " + stats.writeToDiskTimeStats().avg() + "ms");
 * System.out.println("Disk read avg time: " + stats.readFromDiskTimeStats().avg() + "ms");
 *
 * // Fragmentation analysis (compare in-memory occupancy against the in-memory data size).
 * // With a quiescent cache the ratio is normally >= 1.0 because slots are rounded up;
 * // under concurrent updates treat the independently sampled result as approximate.
 * long inMemoryDataSize = Math.max(0L, stats.dataSize() - stats.dataSizeOnDisk());
 * double occupancyRatio = inMemoryDataSize == 0 ? 1.0
 *         : (double) stats.occupiedMemory() / inMemoryDataSize;
 * System.out.println("Memory overhead: " + ((occupancyRatio - 1) * 100) + "%");
 * cache.close();
 * }</pre>
 *
 * @param capacity the configured upper bound on the number of in-memory entries the underlying keyed object
 *                 pool will hold before eviction kicks in. It is derived from the off-heap memory budget as
 *                 {@code allocatedMemory / MIN_BLOCK_SIZE} (currently {@code allocatedMemory / 64}), capped at
 *                 {@link Integer#MAX_VALUE} (as computed by the cache). Because each entry consumes a slot whose size is rounded up to
 *                 the per-entry block size, the cache will typically exhaust off-heap memory and start evicting
 *                 long before {@code size} approaches this value.
 * @param size the current number of entries tracked by the underlying keyed object pool. Both
 *             memory-resident wrappers and disk-spilled (store) wrappers live in the same pool, so this
 *             count includes both. To compute the memory-only entry count, use {@code size - sizeOnDisk}.
 * @param sizeOnDisk the current number of entries whose value bytes are stored on disk via the configured
 *                   {@link OffHeapStore}. These entries are still represented as wrappers in the pool (and
 *                   therefore counted in {@code size}); only their payloads live on disk.
 * @param putCount the total number of <em>successful</em> put operations performed since cache creation,
 *                 counting both memory-backed and disk-spilled stores. A put that fails before the wrapper
 *                 is installed in the pool (e.g., neither memory nor disk could accept the value) is NOT
 *                 counted here.
 * @param putCountToDisk the number of put operations that resulted in writing data to disk. This occurs
 *                       when off-heap memory is full and the value is stored to disk via the configured
 *                       {@link OffHeapStore}, or when the {@code storeSelector} explicitly routes the value to disk.
 * @param getCount the total number of get operations performed since cache creation. Conceptually,
 *                 {@code getCount = hitCount + missCount}; a concurrent snapshot may temporarily differ.
 * @param hitCount the number of successful get operations, including those that ultimately served the value
 *                 from disk. To compute the number of hits served purely from off-heap memory, subtract
 *                 {@code hitCountFromDisk} from {@code hitCount}. Note that this counter is maintained at the
 *                 pool-lookup level: a disk-backed entry whose bytes turn out to be missing from the
 *                 {@link OffHeapStore} (the degraded path where {@code get} returns {@code null}) is still
 *                 counted as a hit, so a flaky store can slightly inflate the hit rate.
 * @param hitCountFromDisk the number of successful get operations where the entry was read from disk via the
 *                       configured {@link OffHeapStore}. It normally forms a subset of {@code hitCount},
 *                       but independently sampled counters can be transiently inconsistent.
 * @param missCount the number of failed get operations where the entry was not found in either memory or
 *                  disk. This can occur when the key never existed, was explicitly removed, or has expired.
 * @param evictionCount the total number of entries removed by the eviction / vacate paths (i.e., entries
 *                      reclaimed because the cache reached capacity, or because the periodic eviction sweep
 *                      noticed they had expired). Explicit {@code remove()} / {@code clear()} calls and
 *                      {@code put()} replacements use a different code path and are NOT counted here.
 * @param evictionCountFromDisk the number of disk-stored entries removed by eviction or vacate (expired
 *                              or reclaimed to free capacity). Explicit {@code remove()} / {@code clear()}
 *                              and {@code put()} replacements of a disk-stored key are NOT counted here.
 * @param allocatedMemory the total allocated off-heap memory in bytes. This represents the maximum memory
 *                        that has been reserved for the cache, typically organized into fixed-size segments.
 * @param occupiedMemory the currently occupied off-heap memory in bytes, including both data and internal
 *                       overhead. This value includes the actual data size plus any metadata and alignment
 *                       padding required by the slot-based allocation system.
 * @param dataSize the total size of actual serialized data tracked by the cache in bytes, across both
 *                 the off-heap memory pool and the disk store, excluding any slot-allocation padding or
 *                 internal overhead. To isolate the in-memory portion, subtract {@code dataSizeOnDisk}
 *                 from {@code dataSize}.
 * @param dataSizeOnDisk the total size of serialized data currently stored on disk in bytes. This is a
 *                       subset of {@code dataSize} and counts only entries that have been persisted to
 *                       disk storage.
 * @param writeToDiskTimeStats statistics for disk-spilled put operations, tracking the minimum, maximum, and
 *                             average time in milliseconds. The measured window is the end-to-end put latency
 *                             for entries that ended up on disk (serialization, the failed in-memory slot
 *                             search, the store write, and the pool insert) - not just the raw store write -
 *                             so these values are not directly comparable to {@code readFromDiskTimeStats}.
 * @param readFromDiskTimeStats statistics for disk read operations, tracking the minimum, maximum, and
 *                              average time in milliseconds for reading entry bytes from the store. This
 *                              helps monitor disk read performance and cache hit efficiency.
 * @param segmentSize the size of each memory segment in bytes. The off-heap memory is organized into
 *                    fixed-size segments (typically 1MB = 1048576 bytes) to manage memory allocation
 *                    and reduce fragmentation.
 * @param occupiedSlots a detailed map showing memory slot occupation across segments. The outer map's key
 *                      is the slot size in bytes - any multiple of the 64-byte minimum block size up to
 *                      {@code maxBlockSize} (e.g., 64, 128, 192, 256, 320, ...), not only powers of two - and
 *                      the inner map contains segment index as key and the number of occupied slots in
 *                      that segment as value. This provides granular visibility into memory fragmentation
 *                      and utilization patterns.
 * @see AbstractOffHeapCache#stats()
 * @see OffHeapCache
 * @see ForeignMemoryOffHeapCache
 * @see MinMaxAvg
 */
public record OffHeapCacheStats(int capacity, int size, long sizeOnDisk, long putCount, long putCountToDisk, long getCount, long hitCount,
        long hitCountFromDisk, long missCount, long evictionCount, long evictionCountFromDisk, long allocatedMemory, long occupiedMemory, long dataSize,
        long dataSizeOnDisk, MinMaxAvg writeToDiskTimeStats, MinMaxAvg readFromDiskTimeStats, int segmentSize,
        Map<Integer, Map<Integer, Integer>> occupiedSlots) {

    /**
     * Canonical constructor that validates the time-statistics arguments and stores a deeply
     * unmodifiable defensive copy of {@code occupiedSlots}.
     *
     * <p>All numeric components must be non-negative per the field documentation. The
     * cross-component invariants between counters (e.g. {@code getCount == hitCount + missCount})
     * are <em>not</em> enforced because the underlying counters are sampled non-atomically and
     * may be transiently inconsistent under concurrent activity.
     *
     * <p>Unlike {@link CacheStats}, which uses {@code -1} as a "not tracked" sentinel for its
     * {@code maxMemory}/{@code dataSize} components, the off-heap cache always tracks its memory and
     * data sizes, so every numeric component here is strictly non-negative (there is no {@code -1}
     * sentinel).
     *
     * <p><b>Exception convention:</b> by deliberate design, {@code null} reference components are
     * rejected with {@link NullPointerException} (via {@link Objects#requireNonNull}), while an
     * out-of-range numeric component is rejected with {@link IllegalArgumentException}. This mirrors
     * the {@link java.util.Objects} convention for record/invariant null-checks and is intentionally
     * distinct from the argument-validation helpers used elsewhere in the cache API.
     *
     * @throws NullPointerException if {@code writeToDiskTimeStats}, {@code readFromDiskTimeStats},
     *         or {@code occupiedSlots} is {@code null}, or if {@code occupiedSlots} contains a
     *         {@code null} key or a {@code null} nested map
     * @throws IllegalArgumentException if any numeric component is negative
     */
    public OffHeapCacheStats {
        Objects.requireNonNull(writeToDiskTimeStats, "writeToDiskTimeStats cannot be null");
        Objects.requireNonNull(readFromDiskTimeStats, "readFromDiskTimeStats cannot be null");
        if (capacity < 0 || size < 0 || sizeOnDisk < 0 || putCount < 0 || putCountToDisk < 0 || getCount < 0 || hitCount < 0 || hitCountFromDisk < 0
                || missCount < 0 || evictionCount < 0 || evictionCountFromDisk < 0 || allocatedMemory < 0 || occupiedMemory < 0 || dataSize < 0
                || dataSizeOnDisk < 0 || segmentSize < 0) {
            throw new IllegalArgumentException("OffHeapCacheStats numeric components must all be non-negative");
        }
        occupiedSlots = immutableCopyOf(Objects.requireNonNull(occupiedSlots, "occupiedSlots cannot be null"));
    }

    /**
     * Returns the cache hit rate as a fraction in {@code [0.0, 1.0]}.
     * The rate is computed as {@code hitCount / (hitCount + missCount)} and includes hits served
     * from disk (see {@link #hitCountFromDisk()} for the disk-only subset).
     *
     * @return the ratio of hits to total get requests, or {@code 0.0} when no get requests have been
     *         recorded (i.e. {@code hitCount + missCount == 0})
     * @see #missRate()
     */
    public double hitRate() {
        // Convert before adding so two valid, non-negative long counters cannot overflow their
        // denominator and produce a negative or out-of-range rate.
        final double requestCount = (double) hitCount + missCount;
        return requestCount == 0.0D ? 0.0D : hitCount / requestCount;
    }

    /**
     * Returns the cache miss rate as a fraction in {@code [0.0, 1.0]}.
     * The rate is computed as {@code missCount / (hitCount + missCount)}.
     *
     * @return the ratio of misses to total get requests, or {@code 0.0} when no get requests have been
     *         recorded (i.e. {@code hitCount + missCount == 0})
     * @see #hitRate()
     */
    public double missRate() {
        final double requestCount = (double) hitCount + missCount;
        return requestCount == 0.0D ? 0.0D : missCount / requestCount;
    }

    /**
     * Returns a map of occupied memory slots organized by slot size.
     * The outer map's key is the slot size in bytes, and the inner map contains the segment
     * index as key and the number of occupied slots in that segment as value. This provides
     * detailed information about memory fragmentation and utilization.
     *
     * <p>The map returned by this accessor is deeply unmodifiable: both the outer map and
     * the nested maps are defensive copies captured during record construction.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Map<Integer, Map<Integer, Integer>> slots = stats.occupiedSlots();
     * // For this snapshot, slots == {1024={0=5, 1=3}, 2048={2=2}}:
     * //   5 slots of 1KB in segment 0, 3 slots of 1KB in segment 1, 2 slots of 2KB in segment 2.
     * slots.get(1024).get(0);                                          // returns 5 (Integer)
     * slots.get(1024).get(1);                                          // returns 3 (Integer)
     * slots.get(2048).get(2);                                          // returns 2 (Integer)
     *
     * // Sum the occupied 1KB slots across every segment.
     * int total1k = slots.get(1024).values().stream()
     *         .mapToInt(Integer::intValue).sum();                     // total1k == 8
     *
     * // The returned map is deeply unmodifiable (outer map and nested maps are defensive copies).
     * slots.put(4096, Map.of());                                      // throws UnsupportedOperationException
     * slots.get(1024).put(9, 9);                                      // throws UnsupportedOperationException
     *
     * // When no slots are occupied this returns an empty (and still unmodifiable) map, never null.
     * Map<Integer, Map<Integer, Integer>> empty = emptyStats.occupiedSlots();
     * empty.isEmpty();                                                // returns true
     * }</pre>
     *
     * @return an unmodifiable map of slot sizes (in bytes) to per-segment occupation details;
     *         never {@code null}
     */
    @Override
    public Map<Integer, Map<Integer, Integer>> occupiedSlots() {
        return occupiedSlots;
    }

    private static Map<Integer, Map<Integer, Integer>> immutableCopyOf(final Map<Integer, Map<Integer, Integer>> occupiedSlots) {
        if (occupiedSlots.isEmpty()) {
            return Map.of();
        }

        final Map<Integer, Map<Integer, Integer>> copy = new LinkedHashMap<>(occupiedSlots.size());

        for (final Map.Entry<Integer, Map<Integer, Integer>> entry : occupiedSlots.entrySet()) {
            final Integer sizeOfSlot = Objects.requireNonNull(entry.getKey(), "occupiedSlots contains a null key");
            final Map<Integer, Integer> segmentSlots = Objects.requireNonNull(entry.getValue(),
                    "occupiedSlots contains a null nested map for slot size: " + sizeOfSlot);

            copy.put(sizeOfSlot, Collections.unmodifiableMap(new LinkedHashMap<>(segmentSlots)));
        }

        return Collections.unmodifiableMap(copy);
    }

    /**
     * Statistics for the minimum, maximum, and average values of a metric.
     * Within {@link OffHeapCacheStats} this is used to track disk I/O timings (values are in
     * milliseconds). When no observations have been recorded yet, all three values are reported
     * as {@code 0.0}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * MinMaxAvg writeStats = stats.writeToDiskTimeStats();
     * System.out.println("Write time - Min: " + writeStats.min() + "ms, " +
     *                    "Max: " + writeStats.max() + "ms, " +
     *                    "Avg: " + writeStats.avg() + "ms");
     * }</pre>
     *
     * @param min the minimum observed value ({@code 0.0} if no observations have been recorded)
     * @param max the maximum observed value ({@code 0.0} if no observations have been recorded)
     * @param avg the average of all observed values ({@code 0.0} if no observations have been recorded)
     */
    public record MinMaxAvg(double min, double max, double avg) {
        /**
         * Canonical constructor that validates the values are non-negative and finite. The tracked
         * metric (disk I/O timing in milliseconds) can never be negative, NaN, or infinite, so any
         * such value indicates a programming error.
         *
         * @throws IllegalArgumentException if {@code min}, {@code max}, or {@code avg} is negative, NaN, or infinite
         */
        public MinMaxAvg {
            // N.checkArgNotNegative(double) only rejects values strictly less than 0; because every
            // comparison with NaN is false, NaN (and +Infinity) would slip through and silently
            // violate the documented "can never be negative" / finite-millisecond invariant. Reject
            // non-finite values explicitly so an invalid computed statistic fails fast.
            checkNonNegativeFinite(min, "min");
            checkNonNegativeFinite(max, "max");
            checkNonNegativeFinite(avg, "avg");
        }

        private static void checkNonNegativeFinite(final double value, final String name) {
            if (Double.isNaN(value) || Double.isInfinite(value) || value < 0) {
                throw new IllegalArgumentException("'" + name + "' must be a non-negative finite number but was: " + value);
            }
        }

        /**
         * Returns a string representation of the statistics in a JSON-like format.
         * The format is {@code {min: <value>, max: <value>, avg: <value>}}, where each value is
         * a {@code double} (typically representing milliseconds in the context of
         * {@link OffHeapCacheStats}).
         *
         * <p><b>Usage Examples:</b>
         * <pre>{@code
         * MinMaxAvg stats = new MinMaxAvg(5.2, 150.8, 45.3);
         * stats.toString();                                  // returns "{min: 5.2, max: 150.8, avg: 45.3}"
         *
         * // When no observations have been recorded every value is 0.0 (a double, so it prints as "0.0").
         * new MinMaxAvg(0, 0, 0).toString();                 // returns "{min: 0.0, max: 0.0, avg: 0.0}"
         * }</pre>
         *
         * @return a formatted string showing the min, max, and avg values in a JSON-like format
         */
        @Override
        public String toString() {
            return "{min: " + min + ", max: " + max + ", avg: " + avg + "}";
        }
    }
}
