package com.landawn.abacus.cache;

import java.util.Map;

/**
 * An immutable snapshot of off-heap cache statistics at a specific point in time.
 * This record provides comprehensive metrics about off-heap cache performance, including
 * memory usage, disk operations, and segment allocation details. It extends the basic
 * cache statistics with off-heap specific metrics like disk I/O performance and memory
 * segment utilization.
 *
 * <br><br>
 * Understanding the metrics:
 * <ul>
 * <li>Memory metrics: allocatedMemory is the total reserved, occupiedMemory is actually used</li>
 * <li>Data metrics: dataSize is the actual serialized data size, smaller than occupiedMemory due to slot allocation</li>
 * <li>Hit metrics: hitCount + hitCountByDisk + missCount = getCount</li>
 * <li>Put metrics: putCount = size + sizeOnDisk + evictions (approximately)</li>
 * </ul>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * OffHeapCache<String, byte[]> cache = OffHeapCache.builder()
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
 * // Hit rate
 * double hitRate = (double) stats.hitCount() / stats.getCount();
 * System.out.println("Hit rate: " + (hitRate * 100) + "%");
 *
 * // Disk performance
 * System.out.println("Disk write avg time: " + stats.writeToDiskTimeStats().avg() + "ms");
 * System.out.println("Disk read avg time: " + stats.readFromDiskTimeStats().avg() + "ms");
 *
 * // Fragmentation analysis
 * double overhead = (double) stats.occupiedMemory() / stats.dataSize();
 * System.out.println("Memory overhead: " + (overhead * 100) + "%");
 * }</pre>
 *
 * @param capacity the maximum number of entries the cache can hold. This is the configured capacity limit
 *                 that determines when evictions start occurring. Once the total number of entries
 *                 (size + sizeOnDisk) reaches this limit, older entries will be evicted to make room
 *                 for new ones
 * @param size the current number of entries stored in memory (off-heap). This count includes only
 *             entries that are currently in the off-heap memory cache, not those that have been
 *             persisted to disk
 * @param sizeOnDisk the current number of entries stored on disk. These are entries that have been
 *                   evicted from memory but are still accessible through disk I/O operations
 * @param putCount the total number of put operations performed since cache creation. This is a cumulative
 *                 counter that includes all successful and failed put attempts
 * @param putCountToDisk the number of put operations that resulted in writing data to disk. This occurs
 *                       when entries are evicted from memory due to capacity constraints or explicit
 *                       eviction policies
 * @param getCount the total number of get operations performed since cache creation. This equals the sum
 *                 of hitCount, hitCountByDisk, and missCount
 * @param hitCount the number of successful get operations where the entry was found in memory (off-heap).
 *                 These are the fastest cache accesses as they don't require disk I/O
 * @param hitCountByDisk the number of successful get operations where the entry was retrieved from disk.
 *                       These operations are slower due to disk I/O but still count as cache hits
 * @param missCount the number of failed get operations where the entry was not found in either memory or
 *                  disk. This can occur when the key never existed, was explicitly removed, or has expired
 * @param evictionCount the total number of entries that have been evicted from memory. Evictions occur
 *                      when the cache reaches capacity or when entries are explicitly removed or expired
 * @param evictionCountFromDisk the number of entries that have been permanently removed from disk storage.
 *                              This typically happens when entries expire or are explicitly deleted
 * @param allocatedMemory the total allocated off-heap memory in bytes. This represents the maximum memory
 *                        that has been reserved for the cache, typically organized into fixed-size segments
 * @param occupiedMemory the currently occupied off-heap memory in bytes, including both data and internal
 *                       overhead. This value includes the actual data size plus any metadata and alignment
 *                       padding required by the slot-based allocation system
 * @param dataSize the total size of actual serialized data stored in memory in bytes, excluding any internal
 *                 overhead or padding. This represents the raw size of the cached values without the
 *                 slot allocation overhead
 * @param dataSizeOnDisk the total size of data stored on disk in bytes. This includes all entries that
 *                       have been persisted to disk storage
 * @param writeToDiskTimeStats statistics for disk write operations, tracking the minimum, maximum, and
 *                             average time in milliseconds for writing entries to disk. This helps monitor
 *                             disk write performance and identify potential I/O bottlenecks
 * @param readFromDiskTimeStats statistics for disk read operations, tracking the minimum, maximum, and
 *                              average time in milliseconds for reading entries from disk. This helps
 *                              monitor disk read performance and cache hit efficiency
 * @param segmentSize the size of each memory segment in bytes. The off-heap memory is organized into
 *                    fixed-size segments (typically 1MB = 1048576 bytes) to manage memory allocation
 *                    and reduce fragmentation
 * @param occupiedSlots a detailed map showing memory slot occupation across segments. The outer map's key
 *                      is the slot size in bytes (e.g., 64, 128, 256, 512, 1024, 2048, 4096, 8192), and
 *                      the inner map contains segment index as key and the number of occupied slots in
 *                      that segment as value. This provides granular visibility into memory fragmentation
 *                      and utilization patterns
 * @see OffHeapCache#stats()
 * @see OffHeapCache25#stats()
 * @see MinMaxAvg
 */
public record OffHeapCacheStats(int capacity, int size, long sizeOnDisk, long putCount, long putCountToDisk, long getCount, long hitCount, long hitCountByDisk,
        long missCount, long evictionCount, long evictionCountFromDisk, long allocatedMemory, long occupiedMemory, long dataSize, long dataSizeOnDisk,
        MinMaxAvg writeToDiskTimeStats, MinMaxAvg readFromDiskTimeStats, int segmentSize, Map<Integer, Map<Integer, Integer>> occupiedSlots) {

    /**
     * Returns a map of occupied memory slots organized by slot size.
     * The outer map's key is the slot size in bytes, and the inner map contains
     * segment index as key and number of occupied slots in that segment as value.
     * This provides detailed information about memory fragmentation and utilization.
     *
     * <br><br>
     * Note: This is an accessor method for the record component. It simply returns
     * the occupiedSlots map that was passed during record construction.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<Integer, Map<Integer, Integer>> slots = stats.occupiedSlots();
     * // slots might contain: {1024 -> {0 -> 5, 1 -> 3}, 2048 -> {2 -> 2}}
     * // meaning: 5 slots of 1KB in segment 0, 3 slots of 1KB in segment 1, etc.
     * }</pre>
     *
     * @return map of slot sizes to segment occupation details
     */
    @Override
    public Map<Integer, Map<Integer, Integer>> occupiedSlots() {
        return occupiedSlots;
    }

    /**
     * Statistics for minimum, maximum, and average values of a metric.
     * Used to track performance characteristics of disk I/O operations.
     * All values are in milliseconds. If no operations have been recorded,
     * all values will be 0.0.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MinMaxAvg writeStats = stats.writeToDiskTimeStats();
     * System.out.println("Write time - Min: " + writeStats.min() + "ms, " +
     *                    "Max: " + writeStats.max() + "ms, " +
     *                    "Avg: " + writeStats.avg() + "ms");
     * }</pre>
     *
     * @param min the minimum observed value in milliseconds
     * @param max the maximum observed value in milliseconds
     * @param avg the average of all observed values in milliseconds
     */
    public record MinMaxAvg(double min, double max, double avg) {
        /**
         * Returns a string representation of the statistics in JSON-like format.
         *
         * @return formatted string showing min, max, and avg values
         */
        @Override
        public String toString() {
            return "{min: " + min + ", max: " + max + ", avg: " + avg + "}";
        }
    }

    /**
     * Represents the occupation details of a specific slot size in the cache.
     * This record provides information about how many slots of a particular size
     * are occupied across different memory segments. This is useful for analyzing
     * memory fragmentation and understanding how memory is being utilized.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<Integer, Integer> segmentOccupation = new LinkedHashMap<>();
     * segmentOccupation.put(0, 5);  // Segment 0 has 5 occupied slots
     * segmentOccupation.put(1, 3);  // Segment 1 has 3 occupied slots
     * OccupiedSlot slotInfo = new OccupiedSlot(1024, segmentOccupation);
     * System.out.println("1KB slots: " + slotInfo.occupiedSlots().values().stream()
     *     .mapToInt(Integer::intValue).sum() + " total");
     * }</pre>
     *
     * @param sizeOfSlot the size of each slot in bytes (e.g., 64, 128, 256, 512, 1024, 2048, 4096, 8192)
     * @param occupiedSlots map of segment index to number of occupied slots in that segment
     */
    public record OccupiedSlot(int sizeOfSlot, Map<Integer, Integer> occupiedSlots) {
    }
}