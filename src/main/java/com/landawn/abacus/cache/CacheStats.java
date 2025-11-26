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
 * All accessor methods (capacity(), size(), putCount(), etc.) are automatically generated
 * by the record and return the corresponding field values. This class is immutable and
 * thread-safe.
 *
 * <br><br>
 * Part of this code is copied from <a href="https://github.com/ben-manes/caffeine">caffeine</a> under Apache License 2.0.
 *
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * LocalCache<String, Object> cache = CacheFactory.createLocalCache(1000, 60000);
 * // ... use cache ...
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
 * }</pre>
 *
 * @param capacity the maximum number of entries the cache can hold (must be non-negative)
 * @param size the current number of entries in the cache (must be between 0 and capacity)
 * @param putCount the total number of put operations performed since cache creation (must be non-negative)
 * @param getCount the total number of get operations performed since cache creation (must be non-negative)
 * @param hitCount the number of get operations that found a value (must be non-negative and not exceed getCount)
 * @param missCount the number of get operations that found no value (must be non-negative and not exceed getCount)
 * @param evictionCount the number of entries that have been evicted due to capacity constraints or expiration (must be non-negative)
 * @param maxMemory the maximum memory allocated for the cache in bytes (0 if unlimited, must be non-negative)
 * @param dataSize the total size of data currently stored in the cache in bytes (must be non-negative)
 * @see Cache
 * @see LocalCache#stats()
 */
public record CacheStats(int capacity, int size, long putCount, long getCount, long hitCount, long missCount, long evictionCount, long maxMemory,
        long dataSize) {

}