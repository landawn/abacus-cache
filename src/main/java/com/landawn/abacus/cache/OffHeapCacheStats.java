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
 * Example usage:
 * <pre>{@code
 * OffHeapCache<String, byte[]> cache = OffHeapCache.builder()
 *     .capacityInMB(100)
 *     .evictDelay(60000)
 *     .build();
 * // ... use cache ...
 * OffHeapCacheStats stats = cache.stats();
 * System.out.println("Disk write avg time: " + stats.writeToDiskTimeStats().avg() + "ms");
 * System.out.println("Memory utilization: " + 
 *     (double) stats.occupiedMemory() / stats.allocatedMemory());
 * }</pre>
 *
 * @param capacity the maximum number of entries the cache can hold
 * @param size the current number of entries in memory
 * @param sizeOnDisk the current number of entries stored on disk
 * @param putCount the total number of put operations
 * @param putCountToDisk the number of put operations that went to disk
 * @param getCount the total number of get operations
 * @param hitCount the number of successful get operations from memory
 * @param hitCountByDisk the number of successful get operations from disk
 * @param missCount the number of failed get operations
 * @param evictionCount the total number of evictions from memory
 * @param evictionCountFromDisk the number of evictions from disk
 * @param allocatedMemory the total allocated off-heap memory in bytes
 * @param occupiedMemory the currently occupied off-heap memory in bytes
 * @param dataSize the total size of data in memory in bytes
 * @param dataSizeOnDisk the total size of data on disk in bytes
 * @param writeToDiskTimeStats statistics for disk write operations (min/max/avg in milliseconds)
 * @param readFromDiskTimeStats statistics for disk read operations (min/max/avg in milliseconds)
 * @param segmentSize the size of each memory segment in bytes
 * @param occupiedSlots map of slot sizes to segment occupation details
 * @see OffHeapCache#stats()
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
     * Example:
     * <pre>{@code
     * Map<Integer, Map<Integer, Integer>> slots = stats.occupiedSlots();
     * // slots might contain: {1024 -> {0 -> 5, 1 -> 3}, 2048 -> {2 -> 2}}
     * // meaning: 5 slots of 1KB in segment 0, 3 slots of 1KB in segment 1, etc.
     * }</pre>
     *
     * @return map of slot sizes to segment occupation details
     */
    public Map<Integer, Map<Integer, Integer>> occupiedSlots() {
        return occupiedSlots;
    }

    /**
     * Statistics for minimum, maximum, and average values of a metric.
     * Used to track performance characteristics of disk I/O operations.
     * 
     * @param min the minimum observed value
     * @param max the maximum observed value
     * @param avg the average of all observed values
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
     * are occupied across different memory segments.
     * 
     * @param sizeOfSlot the size of each slot in bytes
     * @param occupiedSlots map of segment index to number of occupied slots
     */
    public record OccupiedSlot(int sizeOfSlot, Map<Integer, Integer> occupiedSlots) {
    }
}