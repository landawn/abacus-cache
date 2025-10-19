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
 * cache.put("user:123", user, 3600000, 1800000); // 1 hour TTL, 30 min idle
 * 
 * // Retrieve from cache
 * User cached = cache.gett("user:123");
 * 
 * // Get cache statistics
 * CacheStats stats = cache.stats();
 * System.out.println("Hit rate: " + (double)stats.hitCount() / stats.getCount());
 * }</pre>
 *
 * @param <K> the key type
 * @param <V> the value type
 * @see AbstractCache
 * @see KeyedObjectPool
 * @see CacheStats
 */
public class LocalCache<K, V> extends AbstractCache<K, V> {

    private final KeyedObjectPool<K, PoolableWrapper<V>> pool;

    /**
     * Creates a new LocalCache with specified capacity and eviction delay.
     * Uses default TTL of 3 hours and default idle time of 30 minutes.
     *
     * @param capacity the maximum number of entries the cache can hold
     * @param evictDelay the delay in milliseconds between eviction runs
     */
    public LocalCache(final int capacity, final long evictDelay) {
        this(capacity, evictDelay, DEFAULT_LIVE_TIME, DEFAULT_MAX_IDLE_TIME);
    }

    /**
     * Creates a new LocalCache with fully customized parameters.
     * This constructor allows complete control over cache behavior including
     * capacity, eviction timing, and default expiration settings.
     *
     * @param capacity the maximum number of entries the cache can hold
     * @param evictDelay the delay in milliseconds between eviction runs
     * @param defaultLiveTime the default time-to-live in milliseconds for entries
     * @param defaultMaxIdleTime the default maximum idle time in milliseconds for entries
     */
    public LocalCache(final int capacity, final long evictDelay, final long defaultLiveTime, final long defaultMaxIdleTime) {
        super(defaultLiveTime, defaultMaxIdleTime);

        pool = PoolFactory.createKeyedObjectPool(capacity, evictDelay);
    }

    /**
     * Creates a new LocalCache with a custom KeyedObjectPool.
     * This constructor is useful for advanced use cases where you need
     * custom pool configuration or behavior.
     *
     * @param defaultLiveTime the default time-to-live in milliseconds for entries
     * @param defaultMaxIdleTime the default maximum idle time in milliseconds for entries
     * @param pool the pre-configured KeyedObjectPool to use for storage
     */
    public LocalCache(final long defaultLiveTime, final long defaultMaxIdleTime, final KeyedObjectPool<K, PoolableWrapper<V>> pool) {
        super(defaultLiveTime, defaultMaxIdleTime);

        this.pool = pool;
    }

    /**
     * Retrieves a value from the cache by its key.
     * This operation updates the last access time for idle timeout calculation.
     *
     * @param key the key to look up
     * @return the cached value, or null if not found, expired, or evicted
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
     * @param key the key
     * @param value the value to cache
     * @param liveTime time-to-live in milliseconds (0 for no expiration)
     * @param maxIdleTime maximum idle time in milliseconds (0 for no idle timeout)
     * @return true if the entry was successfully stored
     */
    @Override
    public boolean put(final K key, final V value, final long liveTime, final long maxIdleTime) {
        return pool.put(key, PoolableWrapper.of(value, liveTime, maxIdleTime));
    }

    /**
     * Removes an entry from the cache.
     * This operation succeeds whether or not the key exists.
     *
     * @param key the key to remove
     */
    @Override
    public void remove(final K key) {
        pool.remove(key);
    }

    /**
     * Checks if the cache contains a specific key.
     * This method checks for key existence without updating access time.
     *
     * @param key the key to check
     * @return true if the key exists and hasn't expired
     */
    @Override
    public boolean containsKey(final K key) {
        return pool.containsKey(key);
    }

    /**
     * Returns a set of all keys currently in the cache.
     * The returned set is a snapshot and may include keys for expired entries
     * that haven't been evicted yet. Modifications to the returned set do not
     * affect the cache.
     *
     * @return an unmodifiable set of cache keys
     */
    @Override
    public Set<K> keySet() {
        return pool.keySet();
    }

    /**
     * Returns the current number of entries in the cache.
     * This count may include expired entries that haven't been evicted yet.
     *
     * @return the number of entries in the cache
     */
    @Override
    public int size() {
        return pool.size();
    }

    /**
     * Removes all entries from the cache.
     * This operation is atomic and thread-safe.
     */
    @Override
    public void clear() {
        pool.clear();
    }

    /**
     * Returns comprehensive statistics about cache performance and usage.
     * The statistics include hit/miss rates, eviction counts, and memory usage.
     * This is a relatively expensive operation as it gathers current metrics.
     *
     * @return a snapshot of current cache statistics
     * @see CacheStats
     */
    public CacheStats stats() {
        final PoolStats poolStats = pool.stats();

        return new CacheStats(poolStats.capacity(), poolStats.size(), poolStats.putCount(), poolStats.getCount(), poolStats.hitCount(),
                poolStats.missCount(), poolStats.evictionCount(), poolStats.maxMemory(), poolStats.dataSize());
    }

    /**
     * Closes the cache and releases all resources.
     * Stops the eviction scheduler, clears all entries, and releases the underlying object pool.
     * After closing, the cache cannot be used - subsequent operations will throw IllegalStateException.
     * This method is idempotent and thread-safe - multiple calls have no additional effect.
     */
    @Override
    public synchronized void close() {
        pool.close();
    }

    /**
     * Checks if the cache has been closed.
     *
     * @return true if {@link #close()} has been called
     */
    @Override
    public boolean isClosed() {
        return pool.isClosed();
    }
}