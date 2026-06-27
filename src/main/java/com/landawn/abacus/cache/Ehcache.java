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

import java.util.Map;
import java.util.Set;

import org.ehcache.Cache;
import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoadingException;
import org.ehcache.spi.loaderwriter.CacheWritingException;

import com.landawn.abacus.util.N;

/**
 * A wrapper implementation that adapts Ehcache 3.x to the Abacus Cache interface.
 * Ehcache is a widely-used, standards-based caching library for Java with support for
 * multiple tiers of storage (on-heap, off-heap, disk) and enterprise features.
 * This class provides a bridge between Ehcache's API and the standardized Cache interface,
 * allowing Ehcache to be used seamlessly within the Abacus caching framework.
 *
 * <p>
 * <b>Important Note:</b> Ehcache configures expiration policies at the cache level during
 * cache creation, not per-entry. Therefore, the {@code liveTime} and {@code maxIdleTime}
 * parameters in the {@link #put(Object, Object, long, long)} method are ignored.
 * Configure expiration settings when building the Ehcache instance instead.
 *
 * <p>
 * Ehcache features available through the underlying instance (bulk operations and {@code putIfAbsent}
 * are surfaced directly by this wrapper; the remaining capabilities are configured on the Ehcache
 * instance you pass in):
 * <ul>
 * <li>Multi-tier storage (on-heap, off-heap, disk)</li>
 * <li>Cache-through and cache-aside patterns with loaders/writers</li>
 * <li>Bulk operations for improved performance</li>
 * <li>Atomic operations like putIfAbsent</li>
 * <li>JSR-107 (JCache) compliance</li>
 * </ul>
 *
 * <p><b>No {@code stats()} method (by design):</b> unlike {@link LocalCache} and {@link CaffeineCache},
 * this wrapper does not expose a {@link CacheStats} snapshot. Ehcache 3.x statistics are produced by a
 * {@code StatisticsService} registered on the {@code CacheManager}, not by the {@code org.ehcache.Cache}
 * instance this wrapper holds, and the wrapper also cannot report entry count (see {@link #size()}).
 * A fabricated all-zero snapshot would be misleading, so {@code stats()} is intentionally omitted; obtain
 * statistics through the Ehcache {@code StatisticsService} on your {@code CacheManager} if required.
 *
 * <p><b>Usage Examples:</b>
 * <pre>{@code
 * // Create cache using Ehcache 3.x API
 * CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
 *     .withCache("personCache",
 *         CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, Person.class,
 *             ResourcePoolsBuilder.heap(100)))
 *     .build(true);
 * Cache<String, Person> ehcache = cacheManager.getCache("personCache", String.class, Person.class);
 *
 * // Wrap with Abacus Cache interface
 * Ehcache<String, Person> cache = new Ehcache<>(ehcache);
 * Person person = new Person();
 * cache.put("key1", person, 0, 0);   // TTL params ignored, use cache-level config
 * Person retrieved = cache.getOrNull("key1");
 * }</pre>
 *
 * @param <K> the type of keys used to identify cache entries
 * @param <V> the type of values stored in the cache
 * @see AbstractCache
 * @see org.ehcache.Cache
 */
public class Ehcache<K, V> extends AbstractCache<K, V> {

    /** The underlying Ehcache 3.x instance that this wrapper delegates supported operations to. */
    private final Cache<K, V> cacheImpl;

    /** Flag indicating whether this cache wrapper has been closed via {@link #close()}. */
    private volatile boolean isClosed = false;

    /**
     * Creates a new Ehcache wrapper instance.
     * The underlying Ehcache instance should be pre-configured with desired
     * storage tiers, eviction policies, expiration settings, and loaders/writers.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
     *     .withCache("userCache",
     *         CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, User.class,
     *             ResourcePoolsBuilder.heap(1000)))
     *     .build(true);   // build the cache manager
     * Cache<String, User> ehcache = cacheManager.getCache("userCache", String.class, User.class);
     *
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);   // wraps the Ehcache instance
     * boolean open = cache.isClosed();                        // returns false (freshly created)
     *
     * new Ehcache<>((Cache<String, User>) null);              // throws IllegalArgumentException (null cache)
     * }</pre>
     *
     * @param cache the underlying Ehcache instance to wrap
     * @throws IllegalArgumentException if cache is null
     */
    public Ehcache(final Cache<K, V> cache) {
        N.checkArgNotNull(cache, "cache");

        cacheImpl = cache;
    }

    /**
     * Retrieves a value from the cache by its key.
     * This method may trigger a cache loader if configured in the underlying Ehcache.
     * The operation may update access time depending on the eviction policy.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe. Ehcache guarantees thread-safe
     * concurrent access to cache entries.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     * cache.put("userId123", user, 0, 0);          // returns true; seeds an entry
     * User found = cache.getOrNull("userId123");   // returns the stored user
     * User missing = cache.getOrNull("absent");    // returns null (no such key)
     *
     * cache.getOrNull(null);                        // throws IllegalArgumentException (null key)
     * cache.close();                                // cache is now closed
     * cache.getOrNull("userId123");                 // throws IllegalStateException (cache closed)
     * }</pre>
     *
     * @param key the cache key whose associated value is to be returned (must not be {@code null})
     * @return the value associated with the specified key, or {@code null} if not found, expired, or evicted
     * @throws IllegalArgumentException if key is null
     * @throws IllegalStateException if the cache has been closed
     * @throws CacheLoadingException if the cache loader fails
     */
    @Override
    public V getOrNull(final K key) {
        assertNotClosed();

        N.checkArgNotNull(key, "key");

        return cacheImpl.get(key);
    }

    /**
     * Stores a key-value pair in the cache.
     * If the key already exists, its value will be replaced.
     *
     * <p>
     * <b>Important Note:</b> Ehcache's expiration policy is configured at cache creation time.
     * The liveTime and maxIdleTime parameters are ignored by this implementation.
     * All entries use the cache-wide expiration settings.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe. Ehcache guarantees thread-safe
     * concurrent updates to cache entries.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     * User user = new User("John", "john@example.com");
     * // Note: liveTime and maxIdleTime are ignored by Ehcache (expiration is cache-level config)
     * boolean stored = cache.put("userId123", user, 0, 0);   // returns true
     * cache.put("userId123", new User("Jane"), 0, 0);        // returns true; replaces the existing value
     *
     * cache.put(null, user, 0, 0);                            // throws IllegalArgumentException (null key)
     * cache.put("userId123", null, 0, 0);                     // throws IllegalArgumentException (null value)
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated (must not be {@code null})
     * @param value the cache value to be associated with the specified key (must not be {@code null}; this wrapper rejects {@code null} values with {@code IllegalArgumentException})
     * @param liveTime the time-to-live in milliseconds (ignored; configure expiration via the Ehcache builder)
     * @param maxIdleTime the maximum idle time in milliseconds (ignored; configure expiration via the Ehcache builder)
     * @return {@code true} (this implementation always succeeds unless an exception is thrown)
     * @throws IllegalArgumentException if {@code key} or {@code value} is {@code null}
     * @throws IllegalStateException if the cache has been closed
     * @throws CacheWritingException if the cache writer fails
     */
    @Override
    public boolean put(final K key, final V value, final long liveTime, final long maxIdleTime) {
        assertNotClosed();

        N.checkArgNotNull(key, "key");
        N.checkArgNotNull(value, "value");

        cacheImpl.put(key, value);

        return true;
    }

    /**
     * Removes the mapping for a key from the cache if it is present.
     * This operation is idempotent - removing a non-existent key has no effect.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe. Ehcache guarantees thread-safe
     * concurrent removal of cache entries.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     * cache.put("userId123", user, 0, 0);   // returns true; seeds an entry
     * cache.remove("userId123");            // entry removed; getOrNull("userId123") now returns null
     * cache.remove("absent");               // idempotent: removing a missing key is a no-op (no exception)
     *
     * cache.remove(null);             // throws IllegalArgumentException (null key)
     * cache.close();                  // cache is now closed
     * cache.remove("userId123");      // throws IllegalStateException (cache closed)
     * }</pre>
     *
     * @param key the cache key whose mapping is to be removed from the cache (must not be {@code null})
     * @throws IllegalArgumentException if key is null
     * @throws IllegalStateException if the cache has been closed
     * @throws CacheWritingException if the cache writer fails
     */
    @Override
    public void remove(final K key) {
        assertNotClosed();

        N.checkArgNotNull(key, "key");

        cacheImpl.remove(key);
    }

    /**
     * Checks if the cache contains a mapping for the specified key.
     * This method tests for the presence of a key without retrieving its value.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe. However, the result may become
     * stale immediately in concurrent scenarios due to other threads modifying the cache.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     * cache.put("userId123", user, 0, 0);                 // returns true; seeds an entry
     * boolean present = cache.containsKey("userId123");   // returns true
     * boolean absent = cache.containsKey("missing");      // returns false
     *
     * cache.containsKey(null);                            // throws IllegalArgumentException (null key)
     * cache.close();                                      // cache is now closed
     * cache.containsKey("userId123");                     // throws IllegalStateException (cache closed)
     * }</pre>
     *
     * @param key the cache key whose presence in the cache is to be tested (must not be {@code null})
     * @return {@code true} if the cache currently has a live (non-expired) mapping for the specified
     *         key; {@code false} otherwise. Ehcache evaluates expiration during this check (an expired
     *         entry reports {@code false} and is eagerly expired), but the check does not count as an
     *         access for time-to-idle purposes, so it does not extend the entry's lifetime.
     * @throws IllegalArgumentException if key is null
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public boolean containsKey(final K key) {
        assertNotClosed();

        N.checkArgNotNull(key, "key");

        return cacheImpl.containsKey(key);
    }

    /**
     * Atomically puts a value if the key is not already present.
     * This operation is atomic and thread-safe, ensuring that concurrent operations
     * maintain consistency. If the key already exists, the existing value is returned
     * unchanged and the cache is not modified. This is useful for implementing
     * cache-based locking or ensuring single initialization of cached values.
     *
     * <p><b>Note:</b> This is an Ehcache-specific method not present in the base Cache interface,
     * leveraging Ehcache's native atomic operations for optimal performance.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     *
     * // First insert: key absent, value is stored and null is returned
     * User previous = cache.putIfAbsent("user:123", newUser);   // returns null; newUser is stored
     *
     * // Second insert for the same key: existing value returned, cache left unchanged
     * User existing = cache.putIfAbsent("user:123", otherUser); // returns newUser; cache unchanged
     * cache.getOrNull("user:123");                              // returns newUser (not otherUser)
     *
     * cache.putIfAbsent(null, newUser);                         // throws IllegalArgumentException (null key)
     * cache.putIfAbsent("user:123", null);                      // throws IllegalArgumentException (null value)
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated (must not be {@code null})
     * @param value the cache value to be associated with the specified key (must not be {@code null}; this wrapper rejects {@code null} values with {@code IllegalArgumentException})
     * @return the previous value associated with the specified key, or {@code null} if there was no mapping
     * @throws IllegalArgumentException if {@code key} or {@code value} is {@code null}
     * @throws IllegalStateException if the cache has been closed
     * @throws CacheLoadingException if the cache loader fails
     * @throws CacheWritingException if the cache writer fails
     */
    public V putIfAbsent(final K key, final V value) throws CacheLoadingException, CacheWritingException {
        assertNotClosed();

        N.checkArgNotNull(key, "key");
        N.checkArgNotNull(value, "value");

        return cacheImpl.putIfAbsent(key, value);
    }

    /**
     * Retrieves multiple values from the cache in a single bulk operation.
     * This is significantly more efficient than multiple individual get operations, particularly
     * when fetching many entries or when using remote storage tiers.
     *
     * <p>The shape of the returned map depends on whether a cache loader is configured on the
     * underlying Ehcache instance:
     * <ul>
     *   <li>With NO cache loader (the common case), Ehcache 3 returns a map that contains an
     *       entry for every requested key, with a {@code null} value for any key not currently
     *       in the cache.</li>
     *   <li>With a cache loader, missing keys may be loaded; the returned map contains the
     *       values produced (which may still be {@code null} for keys the loader could not
     *       resolve).</li>
     * </ul>
     * In other words, do not rely on {@code result.containsKey(k)} == false meaning "absent" —
     * check {@code result.get(k) == null} instead.
     *
     * <p><b>Note:</b> This is an Ehcache-specific method not present in the base Cache interface,
     * providing optimized batch retrieval capabilities.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe. Ehcache guarantees thread-safe
     * concurrent bulk operations.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     * cache.put("user:1", alice, 0, 0);   // returns true; seeds an entry
     * cache.put("user:2", bob, 0, 0);     // returns true; seeds an entry
     *
     * Set<String> keys = new HashSet<>(Arrays.asList("user:1", "user:2", "user:3"));
     * Map<String, User> results = cache.getAll(keys);   // size 3: an entry for every requested key
     * results.get("user:1");                            // returns alice
     * results.get("user:3");                            // returns null (requested but absent; no loader)
     * results.containsKey("user:3");                    // returns true (key present, mapped to a null value)
     *
     * cache.getAll(null);                               // throws IllegalArgumentException (null keys)
     * }</pre>
     *
     * @param keys the set of cache keys to retrieve
     * @return a map keyed by the requested keys, with each value being the cached value or
     *         {@code null} if the key is not present in the cache (subject to the cache-loader
     *         semantics described above)
     * @throws IllegalArgumentException if keys is null or contains a null element
     * @throws IllegalStateException if the cache has been closed
     * @throws BulkCacheLoadingException if the bulk cache loader fails
     */
    public Map<K, V> getAll(final Set<? extends K> keys) throws BulkCacheLoadingException {
        assertNotClosed();

        N.checkArgNotNull(keys, "keys");
        N.checkElementNotNull(keys, "keys");

        return cacheImpl.getAll(keys);
    }

    /**
     * Stores multiple key-value pairs in the cache in a single bulk operation.
     * This is significantly more efficient than multiple individual put operations, particularly
     * when storing many entries or when using remote storage tiers. All key-value pairs are stored
     * in a single batch operation for optimal performance. If a cache writer is configured, all
     * writes are performed in a single batch. Existing entries are overwritten.
     *
     * <p><b>Note:</b> This is an Ehcache-specific method not present in the base Cache interface,
     * providing optimized batch storage capabilities.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe. Ehcache guarantees thread-safe
     * concurrent bulk operations.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     * Map<String, User> users = new HashMap<>();
     * users.put("user:1", new User("John"));   // add to the batch map
     * users.put("user:2", new User("Jane"));   // add to the batch map
     * cache.putAll(users);                     // both entries stored (any existing keys are overwritten)
     * cache.getOrNull("user:1");               // returns the John user
     *
     * cache.putAll(null);             // throws IllegalArgumentException (null entries)
     * cache.close();                  // cache is now closed
     * cache.putAll(users);            // throws IllegalStateException (cache closed)
     * }</pre>
     *
     * @param entries the map of key-value pairs to store; must not be {@code null} and must not contain
     *                a {@code null} key or {@code null} value
     * @throws IllegalArgumentException if {@code entries} is null or contains a null key or null value
     * @throws IllegalStateException if the cache has been closed
     * @throws BulkCacheWritingException if the bulk cache writer fails
     */
    public void putAll(final Map<? extends K, ? extends V> entries) throws BulkCacheWritingException {
        assertNotClosed();

        N.checkArgNotNull(entries, "entries");
        N.checkElementNotNull(entries.keySet(), "entries' keys");
        N.checkElementNotNull(entries.values(), "entries' values");

        cacheImpl.putAll(entries);
    }

    /**
     * Removes multiple keys from the cache in a single bulk operation.
     * This is significantly more efficient than multiple individual remove operations, particularly
     * when removing many entries or when using remote storage tiers. All keys are removed in a single
     * batch operation for optimal performance. The operation is idempotent - non-existent keys are
     * silently ignored. If a cache writer is configured, all deletions are performed in a single batch.
     *
     * <p><b>Note:</b> This is an Ehcache-specific method not present in the base Cache interface,
     * providing optimized batch removal capabilities.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe. Ehcache guarantees thread-safe
     * concurrent bulk operations.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     * cache.put("user:1", alice, 0, 0);   // returns true; seeds an entry
     * cache.put("user:2", bob, 0, 0);     // returns true; seeds an entry
     *
     * Set<String> keysToRemove = new HashSet<>(Arrays.asList("user:1", "user:2"));
     * cache.removeAll(keysToRemove);                 // both entries removed
     * cache.getOrNull("user:1");                     // returns null
     * cache.removeAll(Set.of("absent"));             // idempotent: non-existent keys are ignored (no exception)
     *
     * cache.removeAll(null);                         // throws IllegalArgumentException (null keys)
     * }</pre>
     *
     * @param keys the set of cache keys to remove
     * @throws IllegalArgumentException if keys is null or contains a null element
     * @throws IllegalStateException if the cache has been closed
     * @throws BulkCacheWritingException if the bulk cache writer fails
     */
    public void removeAll(final Set<? extends K> keys) throws BulkCacheWritingException {
        assertNotClosed();

        N.checkArgNotNull(keys, "keys");
        N.checkElementNotNull(keys, "keys");

        cacheImpl.removeAll(keys);
    }

    /**
     * Returns the set of keys in the cache.
     * This operation is not supported by the Ehcache wrapper because Ehcache 3.x does not provide
     * an efficient API for retrieving all keys. Key iteration would require materializing all keys
     * from potentially multiple storage tiers (on-heap, off-heap, disk), which could be extremely
     * expensive in terms of memory and performance. For distributed or disk-backed caches, this
     * operation would require scanning entire storage tiers.
     *
     * <p><b>Why Not Supported:</b>
     * <ul>
     * <li>Ehcache 3.x only offers entry iteration ({@code Iterable<Cache.Entry>}), which would
     *     require a full O(n) scan of all storage tiers to materialize a key set</li>
     * <li>Multi-tier storage makes key enumeration prohibitively expensive</li>
     * <li>Memory overhead of materializing all keys could exceed available heap</li>
     * <li>For disk/distributed tiers, full scans would severely impact performance</li>
     * <li>Inconsistent with Ehcache's design philosophy of avoiding full cache scans</li>
     * </ul>
     *
     * <p><b>Alternatives:</b> Track keys externally if enumeration is required, or use a different
     * cache implementation that supports key iteration.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     * cache.keySet();   // throws UnsupportedOperationException (Ehcache 3.x has no key iteration)
     *
     * // Alternative: maintain the key set yourself if you must enumerate keys
     * Set<String> trackedKeys = new HashSet<>();
     * cache.put("user:123", user, 0, 0);   // returns true
     * trackedKeys.add("user:123");         // record the key in the external set
     * }</pre>
     *
     * @return this method never returns normally; it always throws
     * @throws UnsupportedOperationException always thrown, because materializing a key set would require a full scan of all Ehcache storage tiers
     */
    @Override
    public Set<K> keySet() throws UnsupportedOperationException {
        throw new UnsupportedOperationException(
                "keySet() is not supported by Ehcache - materializing a key set would require a full scan of all storage tiers");
    }

    /**
     * Returns the number of entries in the cache.
     * This operation is not supported by the Ehcache wrapper because Ehcache 3.x does not provide
     * a size() method or equivalent API to efficiently count cache entries. Computing the size would
     * require iterating through all keys across all storage tiers (on-heap, off-heap, disk), which
     * is not supported by Ehcache's API and would be prohibitively expensive for large caches.
     *
     * <p><b>Why Not Supported:</b>
     * <ul>
     * <li>Ehcache 3.x API does not provide a size(), count(), or estimatedSize() method</li>
     * <li>Multi-tier storage (on-heap, off-heap, disk) makes accurate counting complex</li>
     * <li>The only alternative is the entry iterator ({@code Iterable<Cache.Entry>}), and a full
     *     iteration of all storage tiers would be prohibitively expensive</li>
     * <li>For disk-backed or distributed caches, counting could require extensive I/O operations</li>
     * <li>Result would be stale immediately in highly concurrent scenarios</li>
     * </ul>
     *
     * <p><b>Alternatives:</b> Track cache entry count externally using atomic counters if needed,
     * or use a different cache implementation that supports size reporting.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     * cache.size();   // throws UnsupportedOperationException (Ehcache 3.x has no size API)
     *
     * // Alternative: maintain the entry count yourself with an atomic counter
     * AtomicInteger counter = new AtomicInteger(0);
     * cache.put("user:123", user, 0, 0);   // returns true
     * counter.incrementAndGet();           // tracked size is now 1
     * }</pre>
     *
     * @return this method never returns normally; it always throws
     * @throws UnsupportedOperationException always thrown, because the underlying Ehcache 3.x API does not provide size reporting
     */
    @Override
    public int size() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("size() is not supported by Ehcache - the underlying Ehcache 3.x API does not provide size reporting");
    }

    /**
     * Removes all entries from the cache.
     * This operation invalidates all cached key-value pairs immediately across all storage tiers
     * (on-heap, off-heap, disk). This is a potentially expensive operation for large caches.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe. Ehcache guarantees thread-safe
     * execution of the clear operation.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     * cache.put("userId123", user, 0, 0);  // returns true; seeds an entry
     * cache.clear();                       // removes all entries from the cache
     * cache.getOrNull("userId123");        // returns null
     *
     * cache.close();                 // cache is now closed
     * cache.clear();                 // throws IllegalStateException (cache closed)
     * }</pre>
     *
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public void clear() {
        assertNotClosed();

        cacheImpl.clear();
    }

    /**
     * Closes this wrapper. The underlying Ehcache instance is NOT cleared or disposed
     * because its lifecycle is owned by the {@code CacheManager} that created it —
     * multiple wrappers may share the same Ehcache instance, and the manager is free to
     * keep using it directly after this wrapper is closed. After closing, this wrapper
     * rejects further operations with {@link IllegalStateException}.
     *
     * <p><b>Thread Safety:</b> This method is synchronized, thread-safe, and idempotent.
     * Calling it multiple times has no additional effect beyond the first invocation and will not throw exceptions.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     * try {
     *     cache.put("userId123", user, 0, 0);   // returns true
     *     // ... use the cache ...
     * } finally {
     *     cache.close();   // marks the wrapper closed; the underlying Ehcache is NOT disposed
     * }
     *
     * cache.close();                 // idempotent: a second close is a no-op (no exception)
     * cache.getOrNull("userId123");  // throws IllegalStateException (cache closed)
     * }</pre>
     *
     */
    @Override
    public synchronized void close() {
        if (isClosed) {
            return;
        }

        isClosed = true;
    }

    /**
     * Checks if the cache has been closed.
     * Returns {@code true} if {@link #close()} has been called on this cache, {@code false} otherwise.
     * This method can be used to verify cache state before performing operations,
     * though most operations will throw {@link IllegalStateException} if the cache is closed.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently.
     * The field is declared volatile to ensure visibility across threads.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     * boolean open = cache.isClosed();     // returns false (freshly created)
     * if (!cache.isClosed()) {
     *     User user = cache.getOrNull("userId123");   // safe to use while the cache is open
     * }
     *
     * cache.close();                       // cache is now closed
     * boolean closed = cache.isClosed();   // returns true
     * }</pre>
     *
     * @return {@code true} if {@link #close()} has been called on this cache; {@code false} if the cache is still operational
     */
    @Override
    public boolean isClosed() {
        return isClosed;
    }

    /**
     * Ensures the cache is not closed before performing operations.
     * This is a utility method called by cache operations to verify that
     * the cache is still in an operational state. It provides a consistent
     * way to enforce the "closed" state across cache methods.
     *
     * <p>Note: {@link #keySet()} and {@link #size()} do not call this method because
     * they always throw {@link UnsupportedOperationException} regardless of whether
     * the cache is open.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe due to the volatile
     * {@code isClosed} field.
     *
     * @throws IllegalStateException if the cache has been closed
     */
    protected void assertNotClosed() {
        if (isClosed) {
            throw new IllegalStateException("This cache has been closed");
        }
    }
}
