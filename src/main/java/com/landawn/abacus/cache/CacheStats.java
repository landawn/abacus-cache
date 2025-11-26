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
 * <br><br>
 * All accessor methods ({@code capacity()}, {@code size()}, {@code putCount()}, etc.) are automatically generated
 * by the record and return the corresponding field values. This class is immutable and
 * thread-safe.
 *
 * <br><br>
 * The statistics captured in this record include:
 * <ul>
 *   <li><b>Capacity metrics:</b> Maximum and current cache size</li>
 *   <li><b>Operation counts:</b> Number of put and get operations</li>
 *   <li><b>Performance metrics:</b> Hit and miss counts for calculating cache efficiency</li>
 *   <li><b>Eviction metrics:</b> Number of entries removed due to capacity or expiration</li>
 *   <li><b>Memory metrics:</b> Maximum allowed memory and current memory usage</li>
 * </ul>
 *
 * <br>
 * Part of this code is copied from <a href="https://github.com/ben-manes/caffeine">caffeine</a> under Apache License 2.0.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * LocalCache<String, Object> cache = CacheFactory.createLocalCache(1000, 60000);
 * // ... perform cache operations ...
 * CacheStats stats = cache.stats();
 *
 * // Calculate cache hit rate
 * double hitRate = (double) stats.hitCount() / (stats.hitCount() + stats.missCount());
 * System.out.println("Cache hit rate: " + hitRate);
 *
 * // Check cache utilization
 * double utilizationPercent = (double) stats.size() / stats.capacity() * 100;
 * System.out.println("Cache utilization: " + utilizationPercent + "%");
 *
 * // Monitor memory usage
 * System.out.println("Memory used: " + stats.dataSize() + " / " + stats.maxMemory() + " bytes");
 *
 * // Check if evictions are occurring
 * if (stats.evictionCount() > 0) {
 *     System.out.println("Cache is experiencing evictions: " + stats.evictionCount());
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