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
 * Part of this code is copied from <a href="https://github.com/ben-manes/caffeine">caffeine</a> under Apache License 2.0.
 * 
 * <br><br>
 * Example usage:
 * <pre>{@code
 * LocalCache<String, Object> cache = CacheFactory.createLocalCache(1000, 60000);
 * // ... use cache ...
 * CacheStats stats = cache.stats();
 * double hitRate = (double) stats.hitCount() / stats.getCount();
 * System.out.println("Cache hit rate: " + hitRate);
 * }</pre>
 *
 * @param capacity the maximum number of entries the cache can hold
 * @param size the current number of entries in the cache
 * @param putCount the total number of put operations performed
 * @param getCount the total number of get operations performed
 * @param hitCount the number of get operations that found a value
 * @param missCount the number of get operations that found no value
 * @param evictionCount the number of entries that have been evicted
 * @param maxMemory the maximum memory allocated for the cache in bytes
 * @param dataSize the total size of data stored in the cache in bytes
 * @see Cache
 * @see LocalCache#stats()
 */
public record CacheStats(int capacity, int size, long putCount, long getCount, long hitCount, long missCount, long evictionCount, long maxMemory,
        long dataSize) {

}