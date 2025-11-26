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
 * <p>As a Java record, this class automatically generates the following methods:
 * <ul>
 *   <li>Accessor methods: {@code capacity()}, {@code size()}, {@code putCount()}, {@code getCount()},
 *       {@code hitCount()}, {@code missCount()}, {@code evictionCount()}, {@code maxMemory()}, {@code dataSize()}</li>
 *   <li>{@code equals(Object)}: Compares all fields for equality</li>
 *   <li>{@code hashCode()}: Generates hash code based on all fields</li>
 *   <li>{@code toString()}: Returns a string representation of all fields</li>
 * </ul>
 *
 * <p>This class is immutable and thread-safe. Once created, its values cannot be changed.
 * To get updated statistics, call the {@code stats()} method on the cache again to
 * obtain a new snapshot.
 *
 * <p><b>Statistics Categories:</b>
 * <ul>
 *   <li><b>Capacity metrics:</b> {@code capacity()} - Maximum cache size, {@code size()} - Current number of entries</li>
 *   <li><b>Operation counts:</b> {@code putCount()} - Total puts, {@code getCount()} - Total gets</li>
 *   <li><b>Performance metrics:</b> {@code hitCount()} - Successful gets, {@code missCount()} - Failed gets</li>
 *   <li><b>Eviction metrics:</b> {@code evictionCount()} - Total entries removed</li>
 *   <li><b>Memory metrics:</b> {@code maxMemory()} - Maximum allowed memory, {@code dataSize()} - Current memory usage</li>
 * </ul>
 *
 * <p><b>Thread Safety:</b> This class is immutable and thread-safe. Multiple threads can
 * safely read the statistics without synchronization.
 *
 * <p>Part of this code is copied from <a href="https://github.com/ben-manes/caffeine">caffeine</a> under Apache License 2.0.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Create a cache and perform operations
 * LocalCache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
 * cache.put("user1", new User("Alice", 25));
 * cache.put("user2", new User("Bob", 30));
 * cache.get("user1"); // hit
 * cache.get("user3"); // miss
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
 * assert stats.size() <= stats.capacity() :
 *     "Current size should not exceed capacity";
 * if (stats.maxMemory() > 0) {
 *     assert stats.dataSize() <= stats.maxMemory() :
 *         "Data size should not exceed max memory";
 * }
 * }</pre>
 *
 * @param capacity the maximum number of entries the cache can hold. This value is set during cache creation
 *                 and represents the upper limit of entries before eviction occurs. Must be non-negative.
 * @param size the current number of entries actually stored in the cache at the time of this snapshot.
 *             Must be between 0 and {@code capacity}, inclusive.
 * @param putCount the total cumulative number of put operations performed since cache creation,
 *                 including both insertions of new entries and updates of existing entries. Must be non-negative.
 * @param getCount the total cumulative number of get operations attempted since cache creation,
 *                 regardless of whether the operation resulted in a hit or miss. Must be non-negative.
 *                 This should equal {@code hitCount + missCount}.
 * @param hitCount the number of get operations that successfully found and returned a cached value.
 *                 Must be non-negative and not exceed {@code getCount}.
 * @param missCount the number of get operations that did not find a cached value (cache miss).
 *                  Must be non-negative and not exceed {@code getCount}.
 * @param evictionCount the total number of entries that have been removed from the cache due to
 *                      capacity constraints, expiration policies, or explicit removal operations.
 *                      Must be non-negative.
 * @param maxMemory the maximum memory in bytes allocated for the cache. A value of 0 indicates
 *                  unlimited memory (no memory-based eviction). Must be non-negative.
 * @param dataSize the total size in bytes of all data currently stored in the cache at the time
 *                 of this snapshot. Must be non-negative and should not exceed {@code maxMemory} if memory limits are enforced.
 * @see Cache
 * @see LocalCache#stats()
 */
public record CacheStats(int capacity, int size, long putCount, long getCount, long hitCount, long missCount, long evictionCount, long maxMemory,
        long dataSize) {

}