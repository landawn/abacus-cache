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
import com.landawn.abacus.pool.Poolable;
import com.landawn.abacus.pool.PoolableAdapter;
import com.landawn.abacus.util.N;

/**
 * A thread-safe, in-memory cache implementation with automatic eviction and expiration support.
 * This cache stores objects in local memory and provides configurable time-to-live (TTL) and
 * idle timeout capabilities. It uses an underlying KeyedObjectPool for efficient memory management
 * and automatic eviction of expired entries.
 *
 * <p>Key features:
 * <ul>
 * <li>Thread-safe operations</li>
 * <li>Automatic eviction based on capacity and time</li>
 * <li>Per-entry TTL and idle timeout support</li>
 * <li>Comprehensive statistics via {@link #stats()}</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * // Create cache with 1000 max entries, 60 second eviction delay
 * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
 *
 * // Cache with custom TTL and idle time
 * User user = new User();
 * cache.put("user:123", user, 3600000, 1800000);   // 1 hour TTL, 30 min idle
 *
 * // Retrieve from cache
 * User cached = cache.getOrNull("user:123");
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

    private final KeyedObjectPool<K, PoolableAdapter<V>> pool;

    /**
     * Creates a new LocalCache with specified capacity and eviction delay.
     * Uses default TTL of 3 hours (10,800,000 milliseconds) and default idle time of 30 minutes
     * (1,800,000 milliseconds) as defined by {@link Cache#DEFAULT_LIVE_TIME} and
     * {@link Cache#DEFAULT_MAX_IDLE_TIME}. The eviction delay determines how frequently
     * the cache scans for expired entries to reclaim memory.
     *
     * <p>This constructor provides a simple way to create a cache with standard expiration
     * settings, ideal for most use cases where entries should remain cached for a few hours.
     *
     * <p><b>Thread Safety:</b> This cache is fully thread-safe for concurrent access.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Create cache with 1000 max entries, check every 60 seconds for expired entries
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.stats().capacity();          // returns 1000
     * User user = new User("John", 30);
     * cache.put("user:123", user);       // returns true; uses 3h TTL, 30min idle by default
     *
     * // Retrieve the cached user
     * User cached = cache.getOrNull("user:123");   // returns the stored user (not null)
     * if (cached != null) {
     *     System.out.println("Found user: " + cached);   // prints "Found user: ..."
     * }
     *
     * // Edge cases (validated by the constructor):
     * new LocalCache<>(0, 60000);        // throws IllegalArgumentException (capacity must be positive)
     * new LocalCache<>(1000, -1);        // throws IllegalArgumentException (evictDelay must be non-negative)
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
     * can override these defaults by using {@link #put(Object, Object, long, long)}.
     *
     * <p><b>Thread Safety:</b> This cache is fully thread-safe for concurrent access.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Create cache with custom settings: 5000 capacity, 30s eviction check, 2h TTL, 30min idle
     * LocalCache<String, Data> cache = new LocalCache<>(5000, 30000, 7200000, 1800000);
     * cache.stats().capacity();          // returns 5000
     *
     * // Entry added without explicit times uses defaults (2h TTL, 30min idle)
     * cache.put("data:1", data1);        // returns true
     *
     * // Override defaults for specific entry (10 second TTL, no idle timeout)
     * cache.put("data:2", data2, 10000, 0);   // returns true
     *
     * // Edge cases (validated by the constructor):
     * new LocalCache<>(0, 30000, 7200000, 1800000);    // throws IllegalArgumentException (capacity must be positive)
     * new LocalCache<>(5000, -1, 7200000, 1800000);    // throws IllegalArgumentException (evictDelay must be non-negative)
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

        N.checkArgPositive(capacity, "capacity");
        N.checkArgNotNegative(evictDelay, "evictDelay");

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
     * expiration time management.
     *
     * <p><b>Thread Safety:</b> This cache is fully thread-safe if the provided pool is thread-safe.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Create a custom pool with specific configuration
     * KeyedObjectPool<String, PoolableAdapter<User>> customPool =
     *     PoolFactory.createKeyedObjectPool(1000, 60000);   // capacity 1000, 60s eviction delay
     *
     * // Create cache with 1 hour default TTL and 30 minute idle time
     * LocalCache<String, User> cache = new LocalCache<>(3600000, 1800000, customPool);
     *
     * // Use the cache normally
     * cache.put("user:123", user);                    // returns true
     * User retrieved = cache.getOrNull("user:123");   // returns the stored user
     *
     * // Edge: a null pool is rejected.
     * new LocalCache<>(3600000L, 1800000L, null);   // throws IllegalArgumentException (pool must not be null)
     * }</pre>
     *
     * @param defaultLiveTime the default time-to-live in milliseconds for entries (0 or negative for no TTL expiration)
     * @param defaultMaxIdleTime the default maximum idle time in milliseconds for entries (0 or negative for no idle timeout)
     * @param pool the pre-configured KeyedObjectPool to use for storage (must not be null)
     * @throws IllegalArgumentException if pool is null
     */
    public LocalCache(final long defaultLiveTime, final long defaultMaxIdleTime, final KeyedObjectPool<K, PoolableAdapter<V>> pool) {
        super(defaultLiveTime, defaultMaxIdleTime);

        this.pool = N.checkArgNotNull(pool, "pool");
    }

    /**
     * Retrieves a value from the cache by its key.
     * This operation updates the last access time for idle timeout calculation,
     * resetting the idle timer for the entry. If the entry has expired (TTL exceeded
     * or idle timeout reached) or been evicted, {@code null} will be returned and the entry
     * will be removed from the cache during the next eviction cycle.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.put("user:123", new User("John"));       // seed an entry; returns true
     *
     * // Retrieve from cache
     * User user = cache.getOrNull("user:123");       // returns the stored User (and resets its idle timer)
     * if (user != null) {
     *     System.out.println("Found: " + user.getName());   // prints "Found: John"
     * } else {
     *     System.out.println("Cache miss - need to load from database");   // not reached here (entry present)
     * }
     *
     * cache.getOrNull("absent");                     // returns null (key not present)
     * cache.getOrNull(null);                         // throws IllegalArgumentException (key must not be null)
     * }</pre>
     *
     * @param key the cache key whose associated value is to be returned (must not be null)
     * @return the value associated with the specified key, or {@code null} if the key is not found, has expired, or has been evicted
     * @throws IllegalArgumentException if key is null
     */
    @Override
    public V getOrNull(final K key) {
        N.checkArgNotNull(key, "key");

        final PoolableAdapter<V> w = pool.get(key);

        return w == null ? null : w.value();
    }

    /**
     * Stores a key-value pair in the cache with custom expiration settings.
     * If the key already exists, its value and expiration settings will be replaced.
     * The entry will be evicted when either the TTL expires (time since creation)
     * or the idle time is exceeded (time since last access via {@link #getOrNull(Object)}).
     *
     * <p>A liveTime of 0 or negative means the entry never expires based on age.
     * A maxIdleTime of 0 or negative means the entry never expires due to inactivity.
     * If both are 0 or negative, the entry will only be evicted when the cache
     * reaches capacity and needs to make room for new entries.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * User user = new User("John");
     *
     * // Store with 1 hour TTL and 30 minute idle timeout
     * boolean success = cache.put("user:123", user, 3600000, 1800000);   // returns true
     * if (success) {
     *     System.out.println("Entry cached successfully");   // prints this branch
     * } else {
     *     System.out.println("Cache is full and cannot accept new entries");   // not reached here (store succeeded)
     * }
     *
     * // Temporary data with 5 second TTL, no idle timeout
     * User tempUser = new User("Temp");
     * cache.put("temp:data", tempUser, 5000, 0);     // returns true (maxIdleTime <= 0 means "no idle timeout")
     *
     * // Permanent entry (no TTL or idle timeout, evicted only when cache is full)
     * cache.put("config:app", configData, 0, 0);     // returns true (both <= 0 means "no expiration")
     *
     * // A null value is accepted; getOrNull then returns null, indistinguishable from an absent key.
     * cache.put("nullable", (User) null, 1000, 1000);   // returns true
     * cache.getOrNull("nullable");                   // returns null
     *
     * cache.put(null, user, 1000, 1000);             // throws IllegalArgumentException (key must not be null)
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated (must not be null)
     * @param value the cache value to be associated with the specified key (can be null)
     * @param liveTime the time-to-live in milliseconds from entry creation (0 or negative for no TTL expiration)
     * @param maxIdleTime the maximum idle time in milliseconds since last access (0 or negative for no idle timeout)
     * @return {@code true} if the entry was successfully stored; {@code false} if the cache is full and unable to evict entries, or if the underlying pool rejected the entry
     * @throws IllegalArgumentException if key is null
     */
    @Override
    public boolean put(final K key, final V value, final long liveTime, final long maxIdleTime) {
        N.checkArgNotNull(key, "key");

        // A liveTime/maxIdleTime of 0 or negative means "no expiration" per the Cache contract.
        // The underlying ActivityPrint requires strictly positive values, so translate
        // non-positive values to Long.MAX_VALUE (effectively infinite), matching the semantics
        // used by Poolable.wrap(value).
        final long effectiveLiveTime = liveTime <= 0 ? Long.MAX_VALUE : liveTime;
        final long effectiveMaxIdleTime = maxIdleTime <= 0 ? Long.MAX_VALUE : maxIdleTime;

        return pool.put(key, Poolable.wrap(value, effectiveLiveTime, effectiveMaxIdleTime));
    }

    /**
     * Removes the mapping for a key from the cache if it is present.
     * This operation is idempotent - it succeeds whether the key exists or not.
     * If the key is not found, the operation completes silently without throwing
     * an exception. After this operation, {@link #containsKey(Object)} will return
     * {@code false} for the specified key.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.put("user:123", user);                  // seed an entry; returns true
     *
     * // Remove the entry
     * cache.remove("user:123");                     // entry gone afterwards
     * User removed = cache.getOrNull("user:123");   // returns null
     * cache.containsKey("user:123");                // returns false
     *
     * // Removing again is safe and has no effect
     * cache.remove("user:123");                     // no error, silent no-op
     *
     * cache.remove(null);                           // throws IllegalArgumentException (key must not be null)
     * }</pre>
     *
     * @param key the cache key whose mapping is to be removed from the cache (must not be null)
     * @throws IllegalArgumentException if key is null
     */
    @Override
    public void remove(final K key) {
        N.checkArgNotNull(key, "key");

        pool.remove(key);
    }

    /**
     * Checks if the cache contains a mapping for the specified key.
     * This method verifies that the key exists and has not expired. Returns {@code false}
     * if the entry has expired or been evicted. Unlike {@link #getOrNull(Object)}, this method
     * peeks at the entry without updating its last-access time, so it does not reset the idle
     * timer or otherwise extend the entry's lifetime.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.put("user:123", user);                  // seed an entry; returns true
     *
     * cache.containsKey("user:123");                // returns true
     * cache.containsKey("absent");                  // returns false
     *
     * if (cache.containsKey("user:123")) {
     *     System.out.println("User exists in cache");      // prints this branch
     *     User cachedUser = cache.getOrNull("user:123");   // returns the stored user
     * } else {
     *     System.out.println("User not in cache - loading from database");   // not reached here (key present)
     *     User user = loadFromDatabase("user:123");
     *     cache.put("user:123", user);   // would re-cache on a miss
     * }
     *
     * cache.containsKey(null);                      // throws IllegalArgumentException (key must not be null)
     * }</pre>
     *
     * @param key the cache key whose presence in the cache is to be tested (must not be null)
     * @return {@code true} if the cache contains a mapping for the specified key and it has not expired; {@code false} otherwise
     * @throws IllegalArgumentException if key is null
     */
    @Override
    public boolean containsKey(final K key) {
        N.checkArgNotNull(key, "key");

        return pool.peek(key) != null;
    }

    /**
     * Returns a set of all keys currently in the cache.
     * The returned set is a snapshot taken at the time of the call and may include
     * keys for expired entries that have not been evicted yet by the background
     * eviction process. The returned set is not a live view of the cache: subsequent
     * cache changes are not reflected in it, and changes to the returned set (if
     * mutation is supported at all) do not propagate back to the cache.
     *
     * <p>Use this method to enumerate all cached keys, inspect cache contents,
     * or perform bulk operations. Be aware that accessing values for returned keys
     * may still return {@code null} if entries have expired between obtaining the key set
     * and accessing the values.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.put("user:1", user1);   // returns true
     * cache.put("user:2", user2);   // returns true
     * cache.put("user:3", user3);   // returns true
     *
     * // Get all keys and iterate
     * Set<String> keys = cache.keySet();
     * keys.size();                                                     // returns 3
     * System.out.println("Cache contains " + keys.size() + " keys");   // prints "Cache contains 3 keys"
     * for (String key : keys) {
     *     User user = cache.getOrNull(key);
     *     if (user != null) {                                                  // Entry may have expired
     *         System.out.println("Key: " + key + ", User: " + user.getName()); // prints one line per live entry
     *     }
     * }
     *
     * // The set is a snapshot: later cache changes are not reflected in it.
     * cache.put("user:4", user4);                   // returns true
     * keys.size();                                  // still returns 3 (snapshot taken earlier)
     *
     * // Filter keys by pattern
     * keys.stream()
     *     .filter(key -> key.startsWith("user:"))   // all 3 keys match
     *     .forEach(key -> System.out.println("User key: " + key));   // prints each "user:" key
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
     * use {@code stats().capacity()}.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.put("user:1", user1);   // returns true
     * cache.put("user:2", user2);   // returns true
     * cache.put("user:3", user3);   // returns true
     *
     * int count = cache.size();                                     // returns 3
     * System.out.println("Cache contains " + count + " entries");   // prints "Cache contains 3 entries"
     *
     * // Check cache utilization
     * CacheStats stats = cache.stats();
     * stats.capacity();                                                                   // returns 1000
     * double utilization = (double) cache.size() / stats.capacity() * 100;                // 0.3
     * System.out.println("Cache utilization: " + String.format("%.2f%%", utilization));   // prints "Cache utilization: 0.30%"
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
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.put("user:1", user1);   // returns true
     * cache.put("user:2", user2);   // returns true
     * cache.put("user:3", user3);   // returns true
     *
     * cache.size();                                          // returns 3
     * System.out.println("Before clear: " + cache.size());   // prints "Before clear: 3"
     * cache.clear();                                         // removes all cached entries
     * cache.size();                                          // returns 0
     * System.out.println("After clear: " + cache.size());    // prints "After clear: 0"
     *
     * // Common use case: invalidate cache after data update
     * void updateAllUsers() {
     *     updateDatabase();   // persist changes first (application-supplied helper)
     *     cache.clear();      // invalidate all cached users
     * }
     * }</pre>
     *
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
     * your workload.
     *
     * <p><b>Statistics provided:</b>
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
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.put("a", user);          // putCount becomes 1
     * cache.getOrNull("a");          // hit; getCount/hitCount each become 1
     * cache.getOrNull("missing");    // miss; getCount becomes 2, missCount becomes 1
     *
     * CacheStats stats = cache.stats();
     * stats.capacity();              // returns 1000
     *
     * // Calculate and display hit rate
     * long totalRequests = stats.hitCount() + stats.missCount();   // 2
     * double hitRate = 0.0;
     * if (totalRequests > 0) {
     *     hitRate = (double) stats.hitCount() / totalRequests;                         // 0.5
     *     System.out.println("Hit rate: " + String.format("%.2f%%", hitRate * 100));   // prints "Hit rate: 50.00%"
     * }
     *
     * // Display cache utilization
     * System.out.println("Cache size: " + stats.size() + "/" + stats.capacity());   // prints "Cache size: 1/1000"
     * double utilization = (double) stats.size() / stats.capacity() * 100;   // 0.1
     * System.out.println("Utilization: " + String.format("%.2f%%", utilization));   // prints "Utilization: 0.10%"
     *
     * // Display operation counts
     * System.out.println("Total get operations: " + stats.getCount());   // prints "Total get operations: 2"
     * System.out.println("Total put operations: " + stats.putCount());   // prints "Total put operations: 1"
     * System.out.println("Evictions: " + stats.evictionCount());          // prints "Evictions: 0"
     *
     * // Monitor cache effectiveness
     * if (hitRate < 0.5) {   // false here (hitRate == 0.5)
     *     System.out.println("Warning: Low hit rate - consider increasing cache size or TTL");   // not reached here
     * }
     *
     * // Note: calling stats() after close() throws IllegalStateException.
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
     * object pool. After closing, the cache cannot be used - subsequent
     * {@code get}/{@code put}/{@code remove}/{@code containsKey} operations throw
     * {@link IllegalStateException} (raised by the underlying pool).
     *
     * <p>This method is idempotent and thread-safe - multiple calls have no additional
     * effect beyond the first invocation. The method is synchronized to ensure proper
     * shutdown coordination. It is strongly recommended to always close the cache when
     * it's no longer needed to prevent resource leaks, especially the background
     * eviction thread.
     *
     * <p><b>Thread Safety:</b> This method is synchronized on the cache instance, so concurrent
     * invocations of {@code close()} are serialized. Other cache methods are not synchronized on
     * the same monitor; thread safety for in-flight {@code get}/{@code put}/{@code remove}/etc.
     * operations during close is the responsibility of the underlying pool.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Try-with-resources (recommended approach)
     * try (LocalCache<String, User> cache = new LocalCache<>(1000, 60000)) {
     *     cache.put("user:123", user);                  // returns true
     *     User cached = cache.getOrNull("user:123");    // returns the stored user
     *     // Cache is automatically closed when exiting try block
     * } // close() called automatically here
     *
     * // Try-finally (manual resource management)
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * try {
     *     cache.put("user:123", user);                  // returns true
     *     User cached = cache.getOrNull("user:123");    // returns the stored user
     *     // ... use cache ...
     * } finally {
     *     cache.close();   // always close to release resources
     * }
     *
     * cache.isClosed();    // returns true after close()
     *
     * // After close, query methods such as size()/keySet()/stats() throw IllegalStateException.
     * cache.size();        // throws IllegalStateException (pool is closed)
     *
     * cache.close();   // safe to call multiple times; idempotent no-op after the first
     * cache.close();   // no effect on subsequent calls
     * }</pre>
     *
     */
    @Override
    public synchronized void close() {
        pool.close();
    }

    /**
     * Checks if the cache has been closed.
     * Returns {@code true} if {@link #close()} has been called on this cache, {@code false} otherwise.
     * Use this method to verify whether the cache is still operational before
     * performing operations, especially in long-running applications or when
     * sharing cache instances across components. Once closed, the cache cannot
     * be reopened - a new instance must be created.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.isClosed();              // returns false (freshly created)
     *
     * // Check before performing operations
     * if (!cache.isClosed()) {
     *     cache.put("key", value);   // executed: returns true
     * } else {
     *     System.out.println("Cache is closed, cannot perform operation");   // not reached here (cache open)
     * }
     *
     * // Pattern for conditional cache usage
     * public void cacheUserIfOpen(String userId, User user) {
     *     if (cache != null && !cache.isClosed()) {
     *         cache.put(userId, user);   // store only while the cache is open
     *     } else {
     *         System.out.println("Cache unavailable, skipping caching");   // taken once the cache is closed
     *     }
     * }
     *
     * // Verify cache state after shutdown
     * cache.close();   // closes the cache (idempotent)
     * System.out.println("Cache closed: " + cache.isClosed());   // prints "Cache closed: true"
     * }</pre>
     *
     * @return {@code true} if {@link #close()} has been called on this cache; {@code false} if the cache is still operational
     */
    @Override
    public boolean isClosed() {
        return pool.isClosed();
    }
}
