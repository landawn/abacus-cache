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
 * <p>
 * <b>Important Note:</b> Caffeine configures expiration policies at the cache level during
 * cache creation, not per-entry. Therefore, the {@code liveTime} and {@code maxIdleTime}
 * parameters in the {@link #put(Object, Object, long, long)} method are ignored.
 * Configure expiration settings when building the Caffeine cache instance instead.
 * </p>
 *
 * <p>
 * Caffeine features exposed through this wrapper:
 * <ul>
 * <li>Automatic eviction based on size, time, or references</li>
 * <li>Concurrent performance close to ConcurrentHashMap</li>
 * <li>Comprehensive statistics collection via {@link #stats()}</li>
 * <li>Window TinyLFU eviction policy for near-optimal hit rate</li>
 * </ul>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * import com.github.benmanes.caffeine.cache.Cache;
 * import com.github.benmanes.caffeine.cache.Caffeine;
 * import java.util.concurrent.TimeUnit;
 *
 * Cache<String, User> caffeine = Caffeine.newBuilder()
 *     .maximumSize(10000)
 *     .expireAfterWrite(10, TimeUnit.MINUTES)
 *     .recordStats()
 *     .build();
 *
 * CaffeineCache<String, User> cache = new CaffeineCache<>(caffeine);
 * cache.put("user:123", user, 0, 0);
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

    private volatile boolean isClosed = false;

    /**
     * Creates a new CaffeineCache wrapper instance.
     * The underlying Caffeine cache should be pre-configured with desired
     * eviction policies, maximum size, and expiration settings.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * import com.github.benmanes.caffeine.cache.Cache;
     * import com.github.benmanes.caffeine.cache.Caffeine;
     * import java.util.concurrent.TimeUnit;
     *
     * Cache<String, User> caffeine = Caffeine.newBuilder()
     *     .maximumSize(1000)
     *     .expireAfterWrite(10, TimeUnit.MINUTES)
     *     .build();
     * CaffeineCache<String, User> cache = new CaffeineCache<>(caffeine);
     * }</pre>
     *
     * @param cache the underlying Caffeine cache instance to wrap
     * @throws IllegalArgumentException if cache is null
     */
    public CaffeineCache(final Cache<K, V> cache) {
        if (cache == null) {
            throw new IllegalArgumentException("Cache cannot be null");
        }
        cacheImpl = cache;
    }

    /**
     * Retrieves a value from the cache by its key.
     * This method uses Caffeine's getIfPresent which doesn't trigger cache loading.
     * The operation may update access time depending on the eviction policy.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
     * cache.put("user:123", user, 0, 0);
     * User retrieved = cache.gett("user:123");
     * if (retrieved != null) {
     *     System.out.println("Found user: " + retrieved.getName());
     * }
     * }</pre>
     *
     * @param k the cache key whose associated value is to be returned
     * @return the value associated with the specified key, or {@code null} if not found, expired, or evicted
     * @throws IllegalStateException if the cache has been closed
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
     * <p>
     * Note: Caffeine's expiration policy is configured at cache creation time.
     * The liveTime and maxIdleTime parameters are ignored by this implementation.
     * All entries use the cache-wide expiration settings.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
     * cache.put("user:123", user, 0, 0); // liveTime and maxIdleTime are ignored
     * // Expiration is controlled by the underlying Caffeine cache configuration
     * }</pre>
     *
     * @param k the cache key with which the specified value is to be associated
     * @param v the cache value to be associated with the specified key
     * @param liveTime the time-to-live in milliseconds (ignored - use cache-level configuration)
     * @param maxIdleTime the maximum idle time in milliseconds (ignored - use cache-level configuration)
     * @return {@code true} always (operation always succeeds unless an exception is thrown)
     * @throws IllegalArgumentException if k is null
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public boolean put(final K k, final V v, final long liveTime, final long maxIdleTime) {
        assertNotClosed();

        if (k == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        cacheImpl.put(k, v); // TODO: Support per-entry expiration

        return true;
    }

    /**
     * Removes the mapping for a key from the cache if it is present.
     * This triggers immediate removal rather than just marking for eviction.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
     * cache.put("user:123", user, 0, 0);
     * cache.remove("user:123");
     * User retrieved = cache.gett("user:123"); // returns null
     * }</pre>
     *
     * @param k the cache key whose mapping is to be removed from the cache
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public void remove(final K k) {
        assertNotClosed();

        cacheImpl.invalidate(k);
    }

    /**
     * Checks if the cache contains a mapping for the specified key.
     * This method performs a cache lookup and may affect access-based eviction.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
     * cache.put("user:123", user, 0, 0);
     * if (cache.containsKey("user:123")) {
     *     System.out.println("User exists in cache");
     * }
     * }</pre>
     *
     * @param k the cache key whose presence in the cache is to be tested
     * @return {@code true} if the cache contains a mapping for the specified key
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public boolean containsKey(final K k) {
        assertNotClosed();

        return gett(k) != null;
    }

    /**
     * Returns the set of keys in the cache.
     * This operation is not supported by the CaffeineCache wrapper as Caffeine doesn't provide
     * efficient key iteration for performance reasons.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
     * try {
     *     Set<String> keys = cache.keySet(); // throws UnsupportedOperationException
     * } catch (UnsupportedOperationException e) {
     *     System.out.println("Key iteration is not supported");
     * }
     * }</pre>
     *
     * @return never returns normally
     * @throws UnsupportedOperationException always thrown
     * @deprecated Unsupported operation
     */
    @Deprecated
    @Override
    public Set<K> keySet() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the estimated number of entries in the cache.
     * This is an approximation and may not be exact due to concurrent modifications.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
     * cache.put("user:123", user1, 0, 0);
     * cache.put("user:456", user2, 0, 0);
     * int count = cache.size(); // approximately 2
     * System.out.println("Cache contains " + count + " entries");
     * }</pre>
     *
     * @return the estimated number of cache entries
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public int size() {
        assertNotClosed();

        return Numbers.toIntExact(cacheImpl.estimatedSize());
    }

    /**
     * Removes all entries from the cache.
     * This operation invalidates all cached key-value pairs immediately.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
     * cache.put("user:123", user1, 0, 0);
     * cache.put("user:456", user2, 0, 0);
     * cache.clear();
     * int size = cache.size(); // returns 0
     * }</pre>
     *
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public void clear() {
        assertNotClosed();

        cacheImpl.invalidateAll();
    }

    /**
     * Closes the cache and releases resources.
     * After closing, the cache cannot be used - subsequent operations will throw IllegalStateException.
     * This method is thread-safe but NOT idempotent - calling it multiple times will throw IllegalStateException.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
     * cache.put("user:123", user, 0, 0);
     * cache.close();
     * // cache.gett("user:123"); // throws IllegalStateException
     * }</pre>
     *
     * @throws IllegalStateException if the cache has already been closed
     */
    @Override
    public synchronized void close() {
        assertNotClosed();

        clear();

        isClosed = true;
    }

    /**
     * Checks if the cache has been closed.
     * This method can be used to verify cache state before performing operations,
     * though most operations will throw IllegalStateException if the cache is closed.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
     * if (!cache.isClosed()) {
     *     cache.put("user:123", user, 0, 0);
     * }
     * cache.close();
     * boolean closed = cache.isClosed(); // returns true
     * }</pre>
     *
     * @return {@code true} if the cache is closed, {@code false} otherwise
     */
    @Override
    public boolean isClosed() {
        return isClosed;
    }

    /**
     * Returns Caffeine-specific cache statistics.
     * Statistics are only available if the cache was created with {@code recordStats()} enabled
     * in the Caffeine builder. The returned statistics provide detailed metrics about cache
     * performance including hit rate, miss rate, load count, eviction count, and average load time.
     *
     * <p><b>Note:</b> This is a Caffeine-specific method not present in the base Cache interface.
     * If stats recording was not enabled, this method returns a stats object with all zero values.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create cache with stats enabled
     * Cache<String, User> caffeine = Caffeine.newBuilder()
     *     .maximumSize(1000)
     *     .recordStats()  // Enable stats recording
     *     .build();
     * CaffeineCache<String, User> cache = new CaffeineCache<>(caffeine);
     *
     * // Perform operations
     * cache.put("user:123", user, 0, 0);
     * User retrieved = cache.gett("user:123");
     *
     * // Get statistics
     * CacheStats stats = cache.stats();
     * System.out.println("Hit rate: " + stats.hitRate());
     * System.out.println("Miss rate: " + stats.missRate());
     * System.out.println("Eviction count: " + stats.evictionCount());
     * System.out.println("Load success count: " + stats.loadSuccessCount());
     * }</pre>
     *
     * @return a snapshot of the cache statistics at the time of invocation
     * @see Cache#stats()
     * @see com.github.benmanes.caffeine.cache.stats.CacheStats
     */
    public CacheStats stats() {
        return cacheImpl.stats();
    }

    /**
     * Ensures the cache is not closed before performing operations.
     * This is a utility method called by all cache operations to verify that
     * the cache is still in an operational state. It provides a consistent
     * way to enforce the "closed" state across all cache methods.
     *
     * @throws IllegalStateException if the cache has been closed
     */
    protected void assertNotClosed() {
        if (isClosed) {
            throw new IllegalStateException("This cache has been closed");
        }
    }
}