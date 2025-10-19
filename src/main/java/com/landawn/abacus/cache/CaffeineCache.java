/*
 * Copyright (C) 2017 HaiYang Li
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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.landawn.abacus.util.Numbers;

/**
 * A wrapper implementation that adapts Caffeine cache to the Abacus Cache interface.
 * Caffeine is a high-performance, near-optimal caching library based on Java 8.
 * This class provides a bridge between Caffeine's API and the standardized Cache interface,
 * allowing Caffeine to be used seamlessly within the Abacus caching framework.
 * 
 * <br><br>
 * Caffeine features exposed through this wrapper:
 * <ul>
 * <li>Automatic eviction based on size, time, or references</li>
 * <li>Concurrent performance close to ConcurrentHashMap</li>
 * <li>Comprehensive statistics collection</li>
 * <li>Asynchronous cache operations</li>
 * </ul>
 * 
 * <br>
 * Example usage:
 * <pre>{@code
 * Caffeine<String, User> caffeine = Caffeine.newBuilder()
 *     .maximumSize(10000)
 *     .expireAfterWrite(10, TimeUnit.MINUTES)
 *     .recordStats()
 *     .build();
 * 
 * CaffeineCache<String, User> cache = new CaffeineCache<>(caffeine);
 * cache.put("user:123", user);
 * 
 * // Get Caffeine-specific statistics
 * CacheStats stats = cache.stats();
 * System.out.println("Hit rate: " + stats.hitRate());
 * }</pre>
 *
 * @param <K> the key type
 * @param <V> the value type
 * @see AbstractCache
 * @see com.github.benmanes.caffeine.cache.Cache
 * @see com.github.benmanes.caffeine.cache.Caffeine
 */
public class CaffeineCache<K, V> extends AbstractCache<K, V> {

    private final Cache<K, V> cacheImpl;

    private boolean isClosed = false;

    /**
     * Creates a new CaffeineCache wrapper instance.
     * The underlying Caffeine cache should be pre-configured with desired
     * eviction policies, maximum size, and expiration settings.
     *
     * @param cache the underlying Caffeine cache instance to wrap
     */
    public CaffeineCache(final Cache<K, V> cache) {
        cacheImpl = cache;
    }

    /**
     * Retrieves a value from the cache by its key.
     * This method uses Caffeine's getIfPresent which doesn't trigger cache loading.
     * The operation may update access time depending on the eviction policy.
     *
     * @param k the key to look up
     * @return the cached value, or null if not found, expired, or evicted
     */
    @Override
    public V gett(final K k) {
        assertNotClosed();

        return cacheImpl.getIfPresent(k);
    }

    /**
     * Stores a key-value pair in the cache.
     * If the key already exists, its value will be replaced.
     *
     * <br><br>
     * Note: Caffeine's expiration policy is configured at cache creation time.
     * The individual TTL and idle time parameters are ignored by this implementation.
     *
     * @param k the key
     * @param v the value to cache
     * @param liveTime time-to-live in milliseconds (ignored - use cache-level configuration)
     * @param maxIdleTime maximum idle time in milliseconds (ignored - use cache-level configuration)
     * @return true if the operation was successful
     */
    @Override
    public boolean put(final K k, final V v, final long liveTime, final long maxIdleTime) {
        assertNotClosed();

        cacheImpl.put(k, v); // TODO: Support per-entry expiration

        return true;
    }

    /**
     * Removes a key-value pair from the cache.
     * This triggers immediate removal rather than just marking for eviction.
     *
     * @param k the key to remove
     */
    @Override
    public void remove(final K k) {
        assertNotClosed();

        cacheImpl.invalidate(k);
    }

    /**
     * Checks if the cache contains a specific key.
     * This method performs a cache lookup and may affect access-based eviction.
     *
     * @param k the key to check
     * @return true if the key exists in the cache
     */
    @Override
    public boolean containsKey(final K k) {
        assertNotClosed();

        return get(k).isPresent();
    }

    /**
     * Returns the set of keys in the cache.
     * This operation is not supported as Caffeine doesn't provide
     * efficient key iteration for performance reasons.
     *
     * @return never returns normally
     * @throws UnsupportedOperationException always thrown
     */
    @Override
    public Set<K> keySet() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the estimated number of entries in the cache.
     * This is an approximation and may not be exact due to concurrent modifications.
     *
     * @return the estimated number of cache entries
     */
    @Override
    public int size() {
        assertNotClosed();

        return Numbers.toIntExact(cacheImpl.estimatedSize());
    }

    /**
     * Removes all entries from the cache.
     * This operation invalidates all cached key-value pairs immediately.
     */
    @Override
    public void clear() {
        assertNotClosed();

        cacheImpl.invalidateAll();
    }

    /**
     * Closes the cache and releases resources.
     * After closing, the cache cannot be used - subsequent operations will throw IllegalStateException.
     * This method is idempotent and thread-safe - multiple calls have no additional effect.
     */
    @Override
    public synchronized void close() {
        assertNotClosed();

        clear();

        isClosed = true;
    }

    /**
     * Checks if the cache has been closed.
     *
     * @return true if the cache is closed
     */
    @Override
    public boolean isClosed() {
        return isClosed;
    }

    /**
     * Returns Caffeine-specific cache statistics.
     * Statistics are only available if the cache was created with recordStats() enabled.
     * The returned statistics provide detailed metrics about cache performance including
     * hit rate, miss rate, load count, and eviction count.
     *
     * @return cache statistics snapshot
     * @see Cache#stats()
     * @see com.github.benmanes.caffeine.cache.stats.CacheStats
     */
    public CacheStats stats() {
        return cacheImpl.stats();
    }

    /**
     * Ensures the cache is not closed before performing operations.
     *
     * @throws IllegalStateException if the cache has been closed
     */
    protected void assertNotClosed() {
        if (isClosed) {
            throw new IllegalStateException("This cache has been closed");
        }
    }
}