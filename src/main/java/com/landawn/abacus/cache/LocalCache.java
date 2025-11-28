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
 * cache.put("user:123", user, 3600000, 1800000);  // 1 hour TTL, 30 min idle
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
     * Uses default TTL of 3 hours (10,800,000 milliseconds) and default idle time of 30 minutes
     * (1,800,000 milliseconds) as defined by {@link Cache#DEFAULT_LIVE_TIME} and
     * {@link Cache#DEFAULT_MAX_IDLE_TIME}. The eviction delay determines how frequently
     * the cache scans for expired entries to reclaim memory.
     *
     * <p>This constructor provides a simple way to create a cache with standard expiration
     * settings, ideal for most use cases where entries should remain cached for a few hours.</p>
     *
     * <p><b>Thread Safety:</b> This cache is fully thread-safe for concurrent access.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create cache with 1000 max entries, check every 60 seconds for expired entries
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * User user = new User("John", 30);
     * cache.put("user:123", user);  // Uses 3h TTL, 30min idle by default
     *
     * // Retrieve the cached user
     * User cached = cache.gett("user:123");
     * if (cached != null) {
     *     System.out.println("Found user: " + cached);
     * }
     * }</pre>
     *
     * @param capacity the maximum number of entries the cache can hold (must be positive)
     * @param evictDelay the delay in milliseconds between eviction runs (must be non-negative, 0 for no automatic eviction)
     * @throws IllegalArgumentException if capacity is not positive or evictDelay is negative
     */
    public LocalCache(final int capacity, final long evictDelay) {
        this(capacity, evictDelay, DEFAULT_LIVE_TIME, DEFAULT_MAX_IDLE_TIME);
    }

    /**
     * Creates a new LocalCache with fully customized parameters.
     * This constructor allows complete control over cache behavior including
     * capacity, eviction timing, and default expiration settings. Entries exceeding
     * either the TTL or idle timeout will be automatically evicted during the periodic
     * eviction process.
     *
     * <p>The defaultLiveTime and defaultMaxIdleTime parameters set the default expiration
     * behavior for entries added using {@link #put(Object, Object)}. Individual entries
     * can override these defaults by using {@link #put(Object, Object, long, long)}.</p>
     *
     * <p><b>Thread Safety:</b> This cache is fully thread-safe for concurrent access.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create cache with custom settings: 5000 capacity, 30s eviction check, 2h TTL, 30min idle
     * LocalCache<String, Data> cache = new LocalCache<>(5000, 30000, 7200000, 1800000);
     *
     * // Entry added without explicit times uses defaults (2h TTL, 30min idle)
     * cache.put("data:1", data1);
     *
     * // Override defaults for specific entry (10 second TTL, no idle timeout)
     * cache.put("data:2", data2, 10000, 0);
     * }</pre>
     *
     * @param capacity the maximum number of entries the cache can hold (must be positive)
     * @param evictDelay the delay in milliseconds between eviction runs (must be non-negative, 0 for no automatic eviction)
     * @param defaultLiveTime the default time-to-live in milliseconds for entries (0 or negative for no TTL expiration)
     * @param defaultMaxIdleTime the default maximum idle time in milliseconds for entries (0 or negative for no idle timeout)
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
     * custom pool configuration or behavior, such as custom eviction strategies,
     * monitoring hooks, or specialized memory management. The provided pool
     * must be properly configured and ready for use.
     *
     * <p>This constructor gives you full control over the underlying storage
     * mechanism while still benefiting from the cache abstraction and default
     * expiration time management.</p>
     *
     * <p><b>Thread Safety:</b> This cache is fully thread-safe if the provided pool is thread-safe.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create a custom pool with specific configuration
     * KeyedObjectPool<String, PoolableWrapper<User>> customPool =
     *     PoolFactory.createKeyedObjectPool(1000, 60000);
     *
     * // Create cache with 1 hour default TTL and 30 minute idle time
     * LocalCache<String, User> cache = new LocalCache<>(3600000, 1800000, customPool);
     *
     * // Use the cache normally
     * cache.put("user:123", user);
     * User retrieved = cache.gett("user:123");
     * }</pre>
     *
     * @param defaultLiveTime the default time-to-live in milliseconds for entries (0 or negative for no TTL expiration)
     * @param defaultMaxIdleTime the default maximum idle time in milliseconds for entries (0 or negative for no idle timeout)
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
     * This operation updates the last access time for idle timeout calculation,
     * resetting the idle timer for the entry. If the entry has expired (TTL exceeded
     * or idle timeout reached) or been evicted, null will be returned and the entry
     * will be removed from the cache during the next eviction cycle.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.put("user:123", new User("John"));
     *
     * // Retrieve from cache
     * User user = cache.gett("user:123");
     * if (user != null) {
     *     System.out.println("Found: " + user.getName());
     * } else {
     *     System.out.println("Cache miss - need to load from database");
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
     * The entry will be evicted when either the TTL expires (time since creation)
     * or the idle time is exceeded (time since last access via {@link #gett(Object)}
     * or {@link #containsKey(Object)}).
     *
     * <p>A liveTime of 0 or negative means the entry never expires based on age.
     * A maxIdleTime of 0 or negative means the entry never expires due to inactivity.
     * If both are 0 or negative, the entry will only be evicted when the cache
     * reaches capacity and needs to make room for new entries.</p>
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * User user = new User("John");
     *
     * // Store with 1 hour TTL and 30 minute idle timeout
     * boolean success = cache.put("user:123", user, 3600000, 1800000);
     * if (success) {
     *     System.out.println("Entry cached successfully");
     * } else {
     *     System.out.println("Cache is full and cannot accept new entries");
     * }
     *
     * // Temporary data with 5 second TTL, no idle timeout
     * User tempUser = new User("Temp");
     * cache.put("temp:data", tempUser, 5000, 0);
     *
     * // Permanent entry (no TTL or idle timeout, evicted only when cache is full)
     * cache.put("config:app", configData, 0, 0);
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated (must not be null)
     * @param value the cache value to be associated with the specified key (can be null)
     * @param liveTime the time-to-live in milliseconds from entry creation (0 or negative for no TTL expiration)
     * @param maxIdleTime the maximum idle time in milliseconds since last access (0 or negative for no idle timeout)
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
     * This operation is idempotent - it succeeds whether the key exists or not.
     * If the key is not found, the operation completes silently without throwing
     * an exception. After this operation, {@link #containsKey(Object)} will return
     * false for the specified key.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.put("user:123", user);
     *
     * // Remove the entry
     * cache.remove("user:123");
     * User removed = cache.gett("user:123");  // Returns null
     *
     * // Removing again is safe and has no effect
     * cache.remove("user:123");  // No error, silent no-op
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
     * This method verifies that the key exists and has not expired. Returns false
     * if the entry has expired or been evicted. Note that this method may update
     * the access time for idle timeout calculation, depending on the underlying
     * pool implementation, which could extend the entry's lifetime.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.put("user:123", user);
     *
     * if (cache.containsKey("user:123")) {
     *     System.out.println("User exists in cache");
     *     User cachedUser = cache.gett("user:123");
     * } else {
     *     System.out.println("User not in cache - loading from database");
     *     User user = loadFromDatabase("user:123");
     *     cache.put("user:123", user);
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
     * The returned set is a snapshot taken at the time of the call and may include
     * keys for expired entries that have not been evicted yet by the background
     * eviction process. Modifications to the returned set do not affect the cache
     * contents - it is a read-only view.
     *
     * <p>Use this method to enumerate all cached keys, inspect cache contents,
     * or perform bulk operations. Be aware that accessing values for returned keys
     * may still return null if entries have expired between obtaining the key set
     * and accessing the values.</p>
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.put("user:1", user1);
     * cache.put("user:2", user2);
     * cache.put("user:3", user3);
     *
     * // Get all keys and iterate
     * Set<String> keys = cache.keySet();
     * System.out.println("Cache contains " + keys.size() + " keys");
     * for (String key : keys) {
     *     User user = cache.gett(key);
     *     if (user != null) { // Entry may have expired
     *         System.out.println("Key: " + key + ", User: " + user.getName());
     *     }
     * }
     *
     * // Filter keys by pattern
     * keys.stream()
     *     .filter(key -> key.startsWith("user:"))
     *     .forEach(key -> System.out.println("User key: " + key));
     * }</pre>
     *
     * @return a set containing all cache keys (including potentially expired entries that haven't been evicted yet)
     */
    @Override
    public Set<K> keySet() {
        return pool.keySet();
    }

    /**
     * Returns the current number of entries in the cache.
     * This count may include expired entries that have not been evicted yet
     * by the background eviction process, so the actual number of accessible
     * (non-expired) entries may be lower. For the maximum capacity of the cache,
     * use {@link #stats()}.capacity().
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.put("user:1", user1);
     * cache.put("user:2", user2);
     * cache.put("user:3", user3);
     *
     * int count = cache.size();
     * System.out.println("Cache contains " + count + " entries");
     *
     * // Check cache utilization
     * CacheStats stats = cache.stats();
     * double utilization = (double) cache.size() / stats.capacity() * 100;
     * System.out.println("Cache utilization: " + String.format("%.2f%%", utilization));
     * }</pre>
     *
     * @return the number of entries currently in the cache (including potentially expired entries that haven't been evicted yet)
     */
    @Override
    public int size() {
        return pool.size();
    }

    /**
     * Removes all entries from the cache immediately.
     * This operation is atomic and thread-safe. After this operation completes,
     * {@link #size()} will return 0 (unless concurrent put operations occur during
     * or immediately after the clear). This is useful for cache invalidation scenarios
     * where all cached data needs to be refreshed.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.put("user:1", user1);
     * cache.put("user:2", user2);
     * cache.put("user:3", user3);
     *
     * System.out.println("Before clear: " + cache.size());  // Prints 3
     * cache.clear();  // Removes all cached entries
     * System.out.println("After clear: " + cache.size());  // Prints 0
     *
     * // Common use case: invalidate cache after data update
     * void updateAllUsers() {
     *     updateDatabase();
     *     cache.clear();  // Invalidate all cached users
     * }
     * }</pre>
     */
    @Override
    public void clear() {
        pool.clear();
    }

    /**
     * Returns comprehensive statistics about cache performance and usage.
     * The statistics include capacity, current size, operation counts (get/put),
     * hit/miss counts, eviction counts, and memory usage metrics. This is a
     * relatively inexpensive operation that provides a snapshot of cache metrics
     * at the time of the call.
     *
     * <p>Use this method to monitor cache effectiveness and tune configuration
     * parameters for optimal performance. Key metrics like hit rate can help
     * determine if the cache capacity and TTL settings are appropriate for
     * your workload.</p>
     *
     * <p><b>Statistics provided:</b></p>
     * <ul>
     * <li><b>capacity</b> - Maximum number of entries the cache can hold</li>
     * <li><b>size</b> - Current number of entries in the cache (may include expired entries)</li>
     * <li><b>putCount</b> - Total number of put operations since cache creation</li>
     * <li><b>getCount</b> - Total number of get operations (hits + misses)</li>
     * <li><b>hitCount</b> - Number of successful cache hits (entry found and not expired)</li>
     * <li><b>missCount</b> - Number of cache misses (entry not found or expired)</li>
     * <li><b>evictionCount</b> - Number of entries evicted (due to capacity, TTL, or idle timeout)</li>
     * <li><b>maxMemory</b> - Maximum memory available (implementation-specific)</li>
     * <li><b>dataSize</b> - Current data size (implementation-specific)</li>
     * </ul>
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * // ... perform cache operations ...
     *
     * CacheStats stats = cache.stats();
     *
     * // Calculate and display hit rate
     * long totalRequests = stats.hitCount() + stats.missCount();
     * if (totalRequests > 0) {
     *     double hitRate = (double) stats.hitCount() / totalRequests;
     *     System.out.println("Hit rate: " + String.format("%.2f%%", hitRate * 100));
     * }
     *
     * // Display cache utilization
     * System.out.println("Cache size: " + stats.size() + "/" + stats.capacity());
     * double utilization = (double) stats.size() / stats.capacity() * 100;
     * System.out.println("Utilization: " + String.format("%.2f%%", utilization));
     *
     * // Display operation counts
     * System.out.println("Total get operations: " + stats.getCount());
     * System.out.println("Total put operations: " + stats.putCount());
     * System.out.println("Evictions: " + stats.evictionCount());
     *
     * // Monitor cache effectiveness
     * if (hitRate < 0.5) {
     *     System.out.println("Warning: Low hit rate - consider increasing cache size or TTL");
     * }
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
     * Closes the cache and releases all associated resources.
     * Stops the eviction scheduler, clears all entries, and releases the underlying
     * object pool. After closing, the cache cannot be used - subsequent operations
     * may throw IllegalStateException or have undefined behavior.
     *
     * <p>This method is idempotent and thread-safe - multiple calls have no additional
     * effect beyond the first invocation. The method is synchronized to ensure proper
     * shutdown coordination. It is strongly recommended to always close the cache when
     * it's no longer needed to prevent resource leaks, especially the background
     * eviction thread.</p>
     *
     * <p><b>Thread Safety:</b> This method is synchronized and safe for concurrent calls.
     * All concurrent operations will be allowed to complete before the cache is fully closed.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Try-with-resources (recommended approach)
     * try (LocalCache<String, User> cache = new LocalCache<>(1000, 60000)) {
     *     cache.put("user:123", user);
     *     User cached = cache.gett("user:123");
     *     // Cache is automatically closed when exiting try block
     * } // close() called automatically here
     *
     * // Try-finally (manual resource management)
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * try {
     *     cache.put("user:123", user);
     *     User cached = cache.gett("user:123");
     *     // ... use cache ...
     * } finally {
     *     cache.close();  // Always close to release resources
     * }
     *
     * // Verify cache is closed before operations
     * if (!cache.isClosed()) {
     *     cache.put("key", value);
     * }
     * cache.close();  // Safe to call multiple times
     * cache.close();  // No effect on subsequent calls
     * }</pre>
     */
    @Override
    public synchronized void close() {
        pool.close();
    }

    /**
     * Checks if the cache has been closed.
     * Returns true if {@link #close()} has been called on this cache, false otherwise.
     * Use this method to verify whether the cache is still operational before
     * performing operations, especially in long-running applications or when
     * sharing cache instances across components. Once closed, the cache cannot
     * be reopened - a new instance must be created.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     *
     * // Check before performing operations
     * if (!cache.isClosed()) {
     *     cache.put("key", value);
     * } else {
     *     System.out.println("Cache is closed, cannot perform operation");
     * }
     *
     * // Pattern for conditional cache usage
     * public void cacheUserIfOpen(String userId, User user) {
     *     if (cache != null && !cache.isClosed()) {
     *         cache.put(userId, user);
     *     } else {
     *         System.out.println("Cache unavailable, skipping caching");
     *     }
     * }
     *
     * // Verify cache state after shutdown
     * cache.close();
     * System.out.println("Cache closed: " + cache.isClosed());  // Prints true
     * }</pre>
     *
     * @return true if {@link #close()} has been called on this cache; false if the cache is still operational
     */
    @Override
    public boolean isClosed() {
        return pool.isClosed();
    }
}