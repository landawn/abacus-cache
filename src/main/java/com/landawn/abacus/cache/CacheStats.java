/*
 * Copyright 2025 Haiyang Li. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.landawn.abacus.cache;

/**
 * An immutable snapshot of cache statistics at a specific point in time.
 * This record provides comprehensive metrics about cache performance and usage,
 * including hit rates, miss counts, evictions, and memory consumption.
 *
 * <p>As a Java record, this class automatically generates the following members:
 * <ul>
 *   <li>Accessor methods: {@code capacity()}, {@code size()}, {@code putCount()}, {@code getCount()},
 *       {@code hitCount()}, {@code missCount()}, {@code evictionCount()}, {@code maxMemory()}, {@code dataSize()}.</li>
 *   <li>{@code equals(Object)}: compares all components for equality.</li>
 *   <li>{@code hashCode()}: generates a hash code based on all components.</li>
 *   <li>{@code toString()}: returns a string representation of all components.</li>
 * </ul>
 *
 * <p>Instances are immutable and thread-safe. Once created, the captured values cannot change;
 * to get updated statistics, call the {@code stats()} method on the cache again to obtain a
 * new snapshot.
 *
 * <p><b>Statistics categories:</b>
 * <ul>
 *   <li><b>Capacity metrics:</b> {@code capacity()} (maximum cache size) and {@code size()} (current number of entries).</li>
 *   <li><b>Operation counts:</b> {@code putCount()} (total puts) and {@code getCount()} (total gets).</li>
 *   <li><b>Performance metrics:</b> {@code hitCount()} (successful gets) and {@code missCount()} (failed gets).</li>
 *   <li><b>Eviction metrics:</b> {@code evictionCount()} (total entries removed by eviction).</li>
 *   <li><b>Memory metrics:</b> {@code maxMemory()} (maximum allowed memory) and {@code dataSize()} (current memory usage).</li>
 * </ul>
 *
 * <p><b>Thread Safety:</b> Instances are immutable and thread-safe; multiple threads may read
 * the statistics concurrently without external synchronization.
 *
 * <p>Inspired by <a href="https://github.com/ben-manes/caffeine">Caffeine</a>'s cache statistics concept.
 *
 * <p><b>Usage Examples:</b>
 * <pre>{@code
 * // Create a cache and perform operations
 * LocalCache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
 * cache.put("user1", new User("Alice", 25));
 * cache.put("user2", new User("Bob", 30));
 * cache.get("user1");   // hit
 * cache.get("user3");   // miss
 *
 * // Get statistics snapshot
 * CacheStats stats = cache.stats();
 *
 * // Calculate cache hit rate
 * long totalRequests = stats.hitCount() + stats.missCount();
 * double hitRate = totalRequests > 0 ? (double) stats.hitCount() / totalRequests : 0.0;
 * System.out.printf("Cache hit rate: %.2f%%\n", hitRate * 100);
 *
 * // Check cache utilization
 * double utilizationPercent = (double) stats.size() / stats.capacity() * 100;
 * System.out.printf("Cache utilization: %d/%d entries (%.1f%%)\n",
 *     stats.size(), stats.capacity(), utilizationPercent);
 *
 * // Monitor memory usage
 * if (stats.maxMemory() > 0) {
 *     double memoryUsagePercent = (double) stats.dataSize() / stats.maxMemory() * 100;
 *     System.out.printf("Memory usage: %d/%d bytes (%.1f%%)\n",
 *         stats.dataSize(), stats.maxMemory(), memoryUsagePercent);
 * }
 *
 * // Check if evictions are occurring (indicates cache pressure)
 * if (stats.evictionCount() > 0) {
 *     System.out.println("Warning: Cache is experiencing evictions: " + stats.evictionCount());
 *     System.out.println("Consider increasing cache capacity or memory limit");
 * }
 *
 * // Verify cache invariants
 * assert stats.getCount() == stats.hitCount() + stats.missCount() :
 *     "Get count should equal sum of hits and misses";
 * if (stats.maxMemory() > 0) {
 *     assert stats.dataSize() <= stats.maxMemory() :
 *         "Data size should not exceed max memory";
 * }
 * }</pre>
 *
 * @param capacity the maximum number of entries the cache can hold. This value is set during cache creation
 *                 and represents the upper limit of entries before eviction occurs. Must be non-negative.
 * @param size the current number of entries stored in the cache at the time of this snapshot. This count
 *             may transiently include expired entries that have not yet been evicted. Must be non-negative.
 * @param putCount the total cumulative number of put operations performed since cache creation,
 *                 including both insertions of new entries and updates of existing entries. Must be non-negative.
 * @param getCount the total cumulative number of get operations attempted since cache creation,
 *                 regardless of whether the operation resulted in a hit or miss. Must be non-negative.
 *                 This should equal {@code hitCount + missCount}.
 * @param hitCount the number of get operations that successfully found and returned a cached value.
 *                 Must be non-negative and not exceed {@code getCount}.
 * @param missCount the number of get operations that did not find a cached value (cache miss).
 *                  Must be non-negative and not exceed {@code getCount}.
 * @param evictionCount the total number of entries that have been evicted from the cache due to
 *                      capacity constraints or expiration policies (TTL or idle timeout).
 *                      Must be non-negative.
 * @param maxMemory the maximum memory in bytes allocated for the cache. A value of 0 indicates
 *                  unlimited memory (no memory-based eviction); a value of {@code -1} indicates that
 *                  the underlying cache does not track memory usage. Must be {@code -1} or non-negative.
 * @param dataSize the total size in bytes of all data currently stored in the cache at the time
 *                 of this snapshot. Should not exceed {@code maxMemory} if memory limits are
 *                 enforced; a value of {@code -1} indicates that the underlying cache does not
 *                 track memory usage. Must be {@code -1} or non-negative.
 * @see Cache
 * @see LocalCache#stats()
 */
public record CacheStats(int capacity, int size, long putCount, long getCount, long hitCount, long missCount, long evictionCount, long maxMemory,
        long dataSize) {

    /**
     * Compact constructor that enforces the documented non-negative invariant on every component
     * except {@code maxMemory} and {@code dataSize}, which additionally allow {@code -1} as a
     * sentinel meaning "not tracked by the underlying cache implementation". The cross-component
     * invariant {@code getCount == hitCount + missCount} is intentionally <em>not</em> enforced
     * because the underlying counters are sampled non-atomically and may be transiently
     * inconsistent under concurrent activity.
     *
     * @throws IllegalArgumentException if any non-memory component is negative, or if
     *         {@code maxMemory}/{@code dataSize} is negative but not {@code -1}.
     */
    public CacheStats {
        if (capacity < 0 || size < 0 || putCount < 0 || getCount < 0 || hitCount < 0 || missCount < 0 || evictionCount < 0 || maxMemory < -1 || dataSize < -1) {
            throw new IllegalArgumentException("CacheStats components must be non-negative (maxMemory/dataSize may also be -1 for 'not tracked'); got capacity="
                    + capacity + ", size=" + size + ", putCount=" + putCount + ", getCount=" + getCount + ", hitCount=" + hitCount + ", missCount=" + missCount
                    + ", evictionCount=" + evictionCount + ", maxMemory=" + maxMemory + ", dataSize=" + dataSize);
        }
    }
}