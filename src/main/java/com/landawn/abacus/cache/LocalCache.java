/*
 * Copyright (C) 2015 HaiYang Li
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.landawn.abacus.cache;

import java.util.Set;

import com.landawn.abacus.pool.KeyedObjectPool;
import com.landawn.abacus.pool.PoolFactory;
import com.landawn.abacus.pool.PoolStats;
import com.landawn.abacus.pool.PoolableWrapper;

/**
 * A thread-safe, in-memory cache implementation with automatic eviction and expiration support.
 * This cache stores objects in local memory and provides configurable time-to-live (TTL) and
 * idle timeout capabilities. It uses an underlying KeyedObjectPool for efficient memory management
 * and automatic eviction of expired entries.
 * 
 * <br><br>
 * Key features:
 * <ul>
 * <li>Thread-safe operations</li>
 * <li>Automatic eviction based on capacity and time</li>
 * <li>Per-entry TTL and idle timeout support</li>
 * <li>Comprehensive statistics via {@link #stats()}</li>
 * </ul>
 * 
 * <br>
 * Example usage:
 * <pre>{@code
 * // Create cache with 1000 max entries, 60 second eviction delay
 * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
 *
 * // Cache with custom TTL and idle time
 * User user = new User();
 * cache.put("user:123", user, 3600000, 1800000); // 1 hour TTL, 30 min idle
 *
 * // Retrieve from cache
 * User cached = cache.gett("user:123");
 *
 * // Get cache statistics
 * CacheStats stats = cache.stats();
 * System.out.println("Hit rate: " + (double) stats.hitCount() / (stats.hitCount() + stats.missCount()));
 * }</pre>
 *
 * @param <K> the type of keys used to identify cache entries
 * @param <V> the type of values stored in the cache
 * @see AbstractCache
 * @see KeyedObjectPool
 * @see CacheStats
 */
public class LocalCache<K, V> extends AbstractCache<K, V> {

    private final KeyedObjectPool<K, PoolableWrapper<V>> pool;

    /**
     * Creates a new LocalCache with specified capacity and eviction delay.
     * Uses default TTL of 3 hours and default idle time of 30 minutes.
     * The eviction delay determines how frequently the cache scans for expired entries.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000); // 1000 entries, 60s eviction
     * User user = new User();
     * cache.put("user:123", user);
     * }</pre>
     *
     * @param capacity the maximum number of entries the cache can hold (must be positive)
     * @param evictDelay the delay in milliseconds between eviction runs (must be non-negative)
     * @throws IllegalArgumentException if capacity is not positive or evictDelay is negative
     */
    public LocalCache(final int capacity, final long evictDelay) {
        this(capacity, evictDelay, DEFAULT_LIVE_TIME, DEFAULT_MAX_IDLE_TIME);
    }

    /**
     * Creates a new LocalCache with fully customized parameters.
     * This constructor allows complete control over cache behavior including
     * capacity, eviction timing, and default expiration settings. Entries exceeding
     * either the TTL or idle timeout will be automatically evicted.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, Data> cache = new LocalCache<>(5000, 30000, 7200000, 1800000);
     * // 5000 capacity, 30s eviction, 2h TTL, 30min idle
     * }</pre>
     *
     * @param capacity the maximum number of entries the cache can hold (must be positive)
     * @param evictDelay the delay in milliseconds between eviction runs (must be non-negative)
     * @param defaultLiveTime the default time-to-live in milliseconds for entries (0 for no expiration)
     * @param defaultMaxIdleTime the default maximum idle time in milliseconds for entries (0 for no idle timeout)
     * @throws IllegalArgumentException if capacity is not positive or evictDelay is negative
     */
    public LocalCache(final int capacity, final long evictDelay, final long defaultLiveTime, final long defaultMaxIdleTime) {
        super(defaultLiveTime, defaultMaxIdleTime);

        if (capacity <= 0) {
            throw new IllegalArgumentException("Capacity must be positive: " + capacity);
        }
        if (evictDelay < 0) {
            throw new IllegalArgumentException("Evict delay cannot be negative: " + evictDelay);
        }

        pool = PoolFactory.createKeyedObjectPool(capacity, evictDelay);
    }

    /**
     * Creates a new LocalCache with a custom KeyedObjectPool.
     * This constructor is useful for advanced use cases where you need
     * custom pool configuration or behavior, such as custom eviction strategies
     * or monitoring hooks.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * KeyedObjectPool<String, PoolableWrapper<User>> customPool = PoolFactory.createKeyedObjectPool(1000, 60000);
     * LocalCache<String, User> cache = new LocalCache<>(3600000, 1800000, customPool);
     * }</pre>
     *
     * @param defaultLiveTime the default time-to-live in milliseconds for entries (0 for no expiration)
     * @param defaultMaxIdleTime the default maximum idle time in milliseconds for entries (0 for no idle timeout)
     * @param pool the pre-configured KeyedObjectPool to use for storage (must not be null)
     * @throws IllegalArgumentException if pool is null
     */
    public LocalCache(final long defaultLiveTime, final long defaultMaxIdleTime, final KeyedObjectPool<K, PoolableWrapper<V>> pool) {
        super(defaultLiveTime, defaultMaxIdleTime);

        if (pool == null) {
            throw new IllegalArgumentException("Pool cannot be null");
        }

        this.pool = pool;
    }

    /**
     * Retrieves a value from the cache by its key.
     * This operation updates the last access time for idle timeout calculation.
     * If the entry has expired or been evicted, null will be returned.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * User user = cache.gett("user:123");
     * if (user != null) {
     *     System.out.println("Found: " + user.getName());
     * } else {
     *     System.out.println("Cache miss");
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be returned
     * @return the value associated with the specified key, or null if the key is not found, has expired, or has been evicted
     */
    @Override
    public V gett(final K key) {
        final PoolableWrapper<V> w = pool.get(key);

        return w == null ? null : w.value();
    }

    /**
     * Stores a key-value pair in the cache with custom expiration settings.
     * If the key already exists, its value and expiration settings will be replaced.
     * The entry will be evicted when either the TTL expires or the idle time is exceeded.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * User user = new User("John");
     * boolean success = cache.put("user:123", user, 3600000, 1800000); // 1h TTL, 30min idle
     * if (success) {
     *     System.out.println("Entry cached successfully");
     * }
     * cache.put("temp:data", data, 5000, 0); // 5s TTL, no idle timeout
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated (must not be null)
     * @param value the cache value to be associated with the specified key
     * @param liveTime the time-to-live in milliseconds (0 for no expiration based on time)
     * @param maxIdleTime the maximum idle time in milliseconds (0 for no idle timeout)
     * @return true if the entry was successfully stored; false if the cache is full and unable to evict entries, or if the underlying pool rejected the entry
     * @throws IllegalArgumentException if key is null
     */
    @Override
    public boolean put(final K key, final V value, final long liveTime, final long maxIdleTime) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        return pool.put(key, PoolableWrapper.of(value, liveTime, maxIdleTime));
    }

    /**
     * Removes the mapping for a key from the cache if it is present.
     * This operation succeeds whether the key exists or not. If the key is not found,
     * the operation completes silently without throwing an exception.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.put("user:123", user);
     * cache.remove("user:123"); // Removes the entry
     * cache.remove("user:123"); // No error, silent no-op
     * }</pre>
     *
     * @param key the cache key whose mapping is to be removed from the cache
     */
    @Override
    public void remove(final K key) {
        pool.remove(key);
    }

    /**
     * Checks if the cache contains a mapping for the specified key.
     * Note: This method checks for key existence but may or may not update the access time
     * for idle timeout calculation, depending on the underlying pool implementation.
     * Returns false if the entry has expired or been evicted.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.put("user:123", user);
     * if (cache.containsKey("user:123")) {
     *     System.out.println("User exists in cache");
     * } else {
     *     System.out.println("User not in cache");
     * }
     * }</pre>
     *
     * @param key the cache key whose presence in the cache is to be tested
     * @return true if the cache contains a mapping for the specified key and it has not expired; false otherwise
     */
    @Override
    public boolean containsKey(final K key) {
        return pool.containsKey(key);
    }

    /**
     * Returns a set of all keys currently in the cache.
     * The returned set is a snapshot and may include keys for expired entries
     * that have not been evicted yet by the background eviction process.
     * Modifications to the returned set do not affect the cache.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.put("user:1", user1);
     * cache.put("user:2", user2);
     * Set<String> keys = cache.keySet();
     * System.out.println("Cache contains " + keys.size() + " keys");
     * keys.forEach(key -> System.out.println("Cached key: " + key));
     * }</pre>
     *
     * @return a set containing all cache keys (including potentially expired entries)
     */
    @Override
    public Set<K> keySet() {
        return pool.keySet();
    }

    /**
     * Returns the current number of entries in the cache.
     * This count may include expired entries that have not been evicted yet
     * by the background eviction process. For the maximum capacity of the cache,
     * use {@link #stats()}.capacity().
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.put("user:1", user1);
     * cache.put("user:2", user2);
     * int count = cache.size();
     * System.out.println("Cache contains " + count + " entries");
     * }</pre>
     *
     * @return the number of entries currently in the cache (including potentially expired entries)
     */
    @Override
    public int size() {
        return pool.size();
    }

    /**
     * Removes all entries from the cache immediately.
     * This operation is atomic and thread-safe. After this operation completes,
     * {@link #size()} will return 0 (unless concurrent puts occur during the clear).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.put("user:1", user1);
     * cache.put("user:2", user2);
     * System.out.println("Before clear: " + cache.size());
     * cache.clear(); // Removes all cached entries
     * System.out.println("After clear: " + cache.size()); // Should print 0
     * }</pre>
     */
    @Override
    public void clear() {
        pool.clear();
    }

    /**
     * Returns comprehensive statistics about cache performance and usage.
     * The statistics include hit/miss rates, eviction counts, and memory usage.
     * This is a relatively expensive operation as it gathers current metrics from
     * the underlying pool. Use this method to monitor cache effectiveness and
     * tune configuration parameters for optimal performance.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * // ... perform cache operations
     * CacheStats stats = cache.stats();
     * long totalRequests = stats.hitCount() + stats.missCount();
     * if (totalRequests > 0) {
     *     double hitRate = (double) stats.hitCount() / totalRequests;
     *     System.out.println("Hit rate: " + String.format("%.2f%%", hitRate * 100));
     * }
     * System.out.println("Cache size: " + stats.size() + "/" + stats.capacity());
     * System.out.println("Evictions: " + stats.evictionCount());
     * }</pre>
     *
     * @return a snapshot of current cache statistics including capacity, size, hit/miss counts, and eviction metrics
     * @see CacheStats
     * @see PoolStats
     */
    public CacheStats stats() {
        final PoolStats poolStats = pool.stats();

        return new CacheStats(poolStats.capacity(), poolStats.size(), poolStats.putCount(), poolStats.getCount(), poolStats.hitCount(), poolStats.missCount(),
                poolStats.evictionCount(), poolStats.maxMemory(), poolStats.dataSize());
    }

    /**
     * Closes the cache and releases all resources.
     * Stops the eviction scheduler, clears all entries, and releases the underlying object pool.
     * After closing, the cache cannot be used - subsequent operations may throw IllegalStateException
     * or behave unexpectedly. This method is idempotent and thread-safe - multiple calls have no
     * additional effect beyond the first invocation.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * try {
     *     cache.put("key", value);
     *     User user = cache.gett("key");
     *     // ... use cache
     * } finally {
     *     cache.close(); // Always close to release resources
     * }
     * // Or with try-with-resources if cache implements AutoCloseable
     * }</pre>
     */
    @Override
    public synchronized void close() {
        pool.close();
    }

    /**
     * Checks if the cache has been closed.
     * Use this method to verify whether the cache is still operational before
     * performing operations, especially in long-running applications or when
     * sharing cache instances across components.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * if (!cache.isClosed()) {
     *     cache.put("key", value);
     * } else {
     *     System.out.println("Cache is closed, cannot perform operation");
     * }
     * }</pre>
     *
     * @return true if {@link #close()} has been called on this cache; false if the cache is still operational
     */
    @Override
    public boolean isClosed() {
        return pool.isClosed();
    }
}