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
     * The operation may update access time depending on the eviction policy configured
     * when the Caffeine cache was created.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently
     * from multiple threads.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
     * cache.put("user:123", user, 0, 0);
     * User retrieved = cache.gett("user:123");
     * if (retrieved != null) {
     *     System.out.println("Found user: " + retrieved.getName());
     * } else {
     *     System.out.println("Cache miss - entry not found or evicted");
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be returned
     * @return the value associated with the specified key, or {@code null} if not found, expired, or evicted
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public V gett(final K key) {
        assertNotClosed();

        return cacheImpl.getIfPresent(key);
    }

    /**
     * Stores a key-value pair in the cache.
     * If the key already exists, its value will be replaced atomically.
     *
     * <p><b>Important:</b> Caffeine's expiration policy is configured at cache creation time
     * using the Caffeine builder, not per-entry. Therefore, the {@code liveTime} and
     * {@code maxIdleTime} parameters are ignored by this implementation. All entries use
     * the cache-wide expiration settings configured when building the Caffeine instance.</p>
     *
     * <p><b>Thread-Safety:</b> This method is thread-safe and can be called concurrently
     * from multiple threads. The underlying Caffeine cache handles synchronization.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Configure expiration at cache creation time
     * Cache<String, User> caffeine = Caffeine.newBuilder()
     *     .expireAfterWrite(10, TimeUnit.MINUTES)
     *     .build();
     * CaffeineCache<String, User> cache = new CaffeineCache<>(caffeine);
     *
     * // liveTime and maxIdleTime parameters are ignored
     * cache.put("user:123", user, 0, 0);
     * cache.put("user:456", anotherUser, 3600000, 1800000);   // Time params still ignored
     *
     * // All entries expire based on the cache-level configuration (10 minutes)
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated (must not be null)
     * @param value the cache value to be associated with the specified key (can be null if the underlying Caffeine cache allows it)
     * @param liveTime the time-to-live in milliseconds (ignored - use cache-level configuration via Caffeine builder)
     * @param maxIdleTime the maximum idle time in milliseconds (ignored - use cache-level configuration via Caffeine builder)
     * @return {@code true} always (operation always succeeds unless an exception is thrown)
     * @throws IllegalArgumentException if key is null
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public boolean put(final K key, final V value, final long liveTime, final long maxIdleTime) {
        assertNotClosed();

        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        cacheImpl.put(key, value);   // TODO: Support per-entry expiration

        return true;
    }

    /**
     * Removes the mapping for a key from the cache if it is present.
     * This triggers immediate removal from the cache rather than just marking
     * the entry for eviction. If the key is not found, the operation completes
     * silently without throwing an exception.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently
     * from multiple threads.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
     * cache.put("user:123", user, 0, 0);
     * cache.remove("user:123");
     * User retrieved = cache.gett("user:123");   // returns null
     *
     * // Removing non-existent key is safe
     * cache.remove("user:999");   // No error, silent no-op
     * }</pre>
     *
     * @param key the cache key whose mapping is to be removed from the cache
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public void remove(final K key) {
        assertNotClosed();

        cacheImpl.invalidate(key);
    }

    /**
     * Checks if the cache contains a mapping for the specified key.
     * This method performs a cache lookup via {@link #gett(Object)} and may affect
     * access-based eviction policies if configured in the underlying Caffeine cache.
     * Returns false if the entry has expired or been evicted.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently
     * from multiple threads.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
     * cache.put("user:123", user, 0, 0);
     * if (cache.containsKey("user:123")) {
     *     System.out.println("User exists in cache");
     * } else {
     *     System.out.println("User not in cache or has expired");
     * }
     * }</pre>
     *
     * @param key the cache key whose presence in the cache is to be tested
     * @return {@code true} if the cache contains a mapping for the specified key and it has not expired; {@code false} otherwise
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public boolean containsKey(final K key) {
        assertNotClosed();

        return gett(key) != null;
    }

    /**
     * Returns the set of keys in the cache.
     * This operation is not supported by the CaffeineCache wrapper as Caffeine doesn't provide
     * efficient key iteration for performance reasons. Caffeine prioritizes performance and
     * thread-safety over providing a key set view, which would require additional memory and
     * synchronization overhead.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
     * try {
     *     Set<String> keys = cache.keySet();   // throws UnsupportedOperationException
     * } catch (UnsupportedOperationException e) {
     *     System.out.println("Key iteration is not supported in CaffeineCache");
     *     // Use a different cache implementation if key iteration is required
     * }
     * }</pre>
     *
     * @return never returns normally
     * @throws UnsupportedOperationException always thrown as this operation is not supported
     * @deprecated This operation is not supported by CaffeineCache. Consider using {@link LocalCache} if you need key iteration.
     */
    @Deprecated
    @Override
    public Set<K> keySet() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("keySet() is not supported by CaffeineCache");
    }

    /**
     * Returns the estimated number of entries in the cache.
     * This is an approximation and may not be exact due to concurrent modifications
     * and asynchronous cleanup operations. Caffeine uses probabilistic counting to
     * provide fast size estimates without blocking concurrent operations.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently
     * from multiple threads without blocking.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
     * cache.put("user:123", user1, 0, 0);
     * cache.put("user:456", user2, 0, 0);
     * int count = cache.size();   // approximately 2
     * System.out.println("Cache contains approximately " + count + " entries");
     * }</pre>
     *
     * @return the estimated number of cache entries
     * @throws IllegalStateException if the cache has been closed
     * @throws ArithmeticException if the estimated size exceeds Integer.MAX_VALUE
     */
    @Override
    public int size() {
        assertNotClosed();

        return Numbers.toIntExact(cacheImpl.estimatedSize());
    }

    /**
     * Removes all entries from the cache immediately.
     * This operation invalidates all cached key-value pairs. The actual removal
     * may be performed asynchronously by Caffeine's cleanup process, but all entries
     * will be logically invalidated when this method returns.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe. However, concurrent put operations
     * may add new entries while the clear is in progress.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
     * cache.put("user:123", user1, 0, 0);
     * cache.put("user:456", user2, 0, 0);
     * System.out.println("Before clear: " + cache.size());
     * cache.clear();                                        // Removes all cached entries
     * System.out.println("After clear: " + cache.size());   // Approximately 0
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
     * This method clears all entries and marks the cache as closed. Unlike some other cache implementations,
     * the underlying Caffeine cache instance is not explicitly closed (as Caffeine caches don't implement
     * Closeable), but all entries are invalidated.
     *
     * <p><b>Thread Safety:</b> This method is synchronized and thread-safe, but NOT idempotent.
     * Calling it multiple times will throw IllegalStateException on subsequent calls.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Try-with-resources pattern (recommended)
     * try (CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance)) {
     *     cache.put("user:123", user, 0, 0);
     *     User retrieved = cache.gett("user:123");
     *     // Cache is automatically closed
     * }
     *
     * // Manual close
     * CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
     * try {
     *     cache.put("user:123", user, 0, 0);
     * } finally {
     *     cache.close();   // Always close to release resources
     * }
     *
     * // After closing
     * try {
     *     cache.gett("user:123");   // Throws IllegalStateException
     * } catch (IllegalStateException e) {
     *     System.out.println("Cache is closed");
     * }
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
     * Returns true if {@link #close()} has been called on this cache.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently.
     * The field is declared volatile to ensure visibility across threads.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
     * if (!cache.isClosed()) {
     *     cache.put("user:123", user, 0, 0);
     * } else {
     *     System.out.println("Cache is closed, cannot perform operation");
     * }
     *
     * cache.close();
     * boolean closed = cache.isClosed();   // returns true
     * }</pre>
     *
     * @return {@code true} if {@link #close()} has been called; {@code false} if the cache is still operational
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
     * If stats recording was not enabled during cache creation, this method returns a stats object
     * with all zero values. Statistics recording has a small performance overhead, so it should
     * only be enabled when monitoring is required.</p>
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and returns a consistent snapshot
     * of statistics at the time of invocation.</p>
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
     * User retrieved = cache.gett("user:123");   // Hit
     * User missing = cache.gett("user:999");     // Miss
     *
     * // Get statistics
     * CacheStats stats = cache.stats();
     * System.out.println("Hit rate: " + stats.hitRate());
     * System.out.println("Miss rate: " + stats.missRate());
     * System.out.println("Hit count: " + stats.hitCount());
     * System.out.println("Miss count: " + stats.missCount());
     * System.out.println("Eviction count: " + stats.evictionCount());
     * System.out.println("Load success count: " + stats.loadSuccessCount());
     * System.out.println("Average load penalty: " + stats.averageLoadPenalty() + " ns");
     * }</pre>
     *
     * @return a snapshot of the cache statistics at the time of invocation (all zeros if recordStats() was not enabled)
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
     * way to enforce the "closed" state across all cache methods. This method
     * checks the volatile {@code isClosed} field to ensure visibility across threads.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe due to the volatile
     * {@code isClosed} field.</p>
     *
     * @throws IllegalStateException if the cache has been closed via {@link #close()}
     */
    protected void assertNotClosed() {
        if (isClosed) {
            throw new IllegalStateException("This cache has been closed");
        }
    }
}
