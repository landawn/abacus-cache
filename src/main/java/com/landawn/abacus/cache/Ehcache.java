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
 * </p>
 *
 * <p>
 * Ehcache features exposed through this wrapper:
 * <ul>
 * <li>Multi-tier storage (on-heap, off-heap, disk)</li>
 * <li>Cache-through and cache-aside patterns with loaders/writers</li>
 * <li>Bulk operations for improved performance</li>
 * <li>Atomic operations like putIfAbsent</li>
 * <li>JSR-107 (JCache) compliance</li>
 * </ul>
 *
 * <p><b>Usage Examples:</b></p>
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
 * @param <K> the key type
 * @param <V> the value type
 * @see AbstractCache
 * @see org.ehcache.Cache
 */
public class Ehcache<K, V> extends AbstractCache<K, V> {

    private final Cache<K, V> cacheImpl;

    private volatile boolean isClosed = false;

    /**
     * Creates a new Ehcache wrapper instance.
     * The underlying Ehcache instance should be pre-configured with desired
     * storage tiers, eviction policies, expiration settings, and loaders/writers.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
     *     .withCache("userCache",
     *         CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, User.class,
     *             ResourcePoolsBuilder.heap(1000)))
     *     .build(true);
     * Cache<String, User> ehcache = cacheManager.getCache("userCache", String.class, User.class);
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     * }</pre>
     *
     * @param cache the underlying Ehcache instance to wrap
     * @throws IllegalArgumentException if cache is null
     */
    public Ehcache(final Cache<K, V> cache) {
        if (cache == null) {
            throw new IllegalArgumentException("Cache cannot be null");
        }
        cacheImpl = cache;
    }

    /**
     * Retrieves a value from the cache by its key.
     * This method may trigger a cache loader if configured in the underlying Ehcache.
     * The operation may update access time depending on the eviction policy.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe. Ehcache guarantees thread-safe
     * concurrent access to cache entries.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     * User user = cache.getOrNull("userId123");
     * if (user != null) {
     *     // Process the retrieved user
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be returned
     * @return the value associated with the specified key, or null if not found, expired, or evicted
     * @throws IllegalStateException if the cache has been closed
     * @throws CacheLoadingException if the cache loader fails
     */
    @Override
    public V getOrNull(final K key) {
        assertNotClosed();

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
     * </p>
     *
     * <p><b>Thread Safety:</b> This method is thread-safe. Ehcache guarantees thread-safe
     * concurrent updates to cache entries.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     * User user = new User("John", "john@example.com");
     * // Note: liveTime and maxIdleTime are ignored by Ehcache
     * cache.put("userId123", user, 0, 0);
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated
     * @param value the cache value to be associated with the specified key
     * @param liveTime the time-to-live in milliseconds (ignored - use cache-level configuration)
     * @param maxIdleTime the maximum idle time in milliseconds (ignored - use cache-level configuration)
     * @return {@code true} always (operation always succeeds unless an exception is thrown)
     * @throws IllegalArgumentException if key is null
     * @throws IllegalStateException if the cache has been closed
     * @throws CacheWritingException if the cache writer fails
     */
    @Override
    public boolean put(final K key, final V value, final long liveTime, final long maxIdleTime) {
        assertNotClosed();

        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        cacheImpl.put(key, value);

        return true;
    }

    /**
     * Removes the mapping for a key from the cache if it is present.
     * This operation is idempotent - removing a non-existent key has no effect.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe. Ehcache guarantees thread-safe
     * concurrent removal of cache entries.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     * cache.remove("userId123");
     * // The entry is now removed from the cache
     * }</pre>
     *
     * @param key the cache key whose mapping is to be removed from the cache
     * @throws IllegalStateException if the cache has been closed
     * @throws CacheWritingException if the cache writer fails
     */
    @Override
    public void remove(final K key) {
        assertNotClosed();

        cacheImpl.remove(key);
    }

    /**
     * Checks if the cache contains a mapping for the specified key.
     * This method tests for the presence of a key without retrieving its value.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe. However, the result may become
     * stale immediately in concurrent scenarios due to other threads modifying the cache.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     * if (cache.containsKey("userId123")) {
     *     User user = cache.getOrNull("userId123");
     *     // Process the user
     * }
     * }</pre>
     *
     * @param key the cache key whose presence in the cache is to be tested
     * @return {@code true} if the cache contains a mapping for the specified key, {@code false} otherwise
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public boolean containsKey(final K key) {
        assertNotClosed();

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
     * leveraging Ehcache's native atomic operations for optimal performance.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Simple conditional caching
     * User newUser = createUser();
     * User existingUser = cache.putIfAbsent("user:123", newUser);
     * if (existingUser == null) {
     *     System.out.println("User was added to cache");
     * } else {
     *     System.out.println("User already exists, using existing: " + existingUser);
     * }
     *
     * // Cache-based initialization (ensures single initialization)
     * ExpensiveObject obj = cache.putIfAbsent("config:main", loadExpensiveConfiguration());
     * if (obj == null) {
     *     obj = cache.getOrNull("config:main");   // Retrieve the newly stored value
     * }
     *
     * // Thread-safe counter initialization
     * AtomicInteger counter = cache.putIfAbsent("counter", new AtomicInteger(0));
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated
     * @param value the cache value to be associated with the specified key
     * @return the previous value associated with the specified key, or {@code null} if there was no mapping
     * @throws IllegalArgumentException if key is null
     * @throws IllegalStateException if the cache has been closed
     * @throws CacheLoadingException if the cache loader fails
     * @throws CacheWritingException if the cache writer fails
     */
    public V putIfAbsent(final K key, final V value) throws CacheLoadingException, CacheWritingException {
        assertNotClosed();

        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        return cacheImpl.putIfAbsent(key, value);
    }

    /**
     * Retrieves multiple values from the cache in a single bulk operation.
     * This is significantly more efficient than multiple individual get operations, particularly
     * when fetching many entries or when using remote storage tiers. The returned map only includes
     * keys that were found in the cache; missing or expired keys are not included in the result.
     * If a cache loader is configured, missing keys may be loaded automatically.
     *
     * <p><b>Note:</b> This is an Ehcache-specific method not present in the base Cache interface,
     * providing optimized batch retrieval capabilities.</p>
     *
     * <p><b>Thread Safety:</b> This method is thread-safe. Ehcache guarantees thread-safe
     * concurrent bulk operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Batch retrieval
     * Set<String> keys = new HashSet<>(Arrays.asList("user:1", "user:2", "user:3"));
     * Map<String, User> results = cache.getAll(keys);
     * for (Map.Entry<String, User> entry : results.entrySet()) {
     *     System.out.println("Found: " + entry.getKey() + " -> " + entry.getValue());
     * }
     *
     * // Handle missing keys
     * Set<String> requestedKeys = Set.of("key1", "key2", "key3");
     * Map<String, Value> cached = cache.getAll(requestedKeys);
     * for (String key : requestedKeys) {
     *     if (!cached.containsKey(key)) {
     *         System.out.println("Missing key: " + key);
     *     }
     * }
     * }</pre>
     *
     * @param keys the set of cache keys to retrieve
     * @return a map of key-value pairs found in the cache (does not include missing keys)
     * @throws IllegalArgumentException if keys is null
     * @throws IllegalStateException if the cache has been closed
     * @throws BulkCacheLoadingException if the bulk cache loader fails
     */
    public Map<K, V> getAll(final Set<? extends K> keys) throws BulkCacheLoadingException {
        assertNotClosed();

        if (keys == null) {
            throw new IllegalArgumentException("Keys cannot be null");
        }

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
     * providing optimized batch storage capabilities.</p>
     *
     * <p><b>Thread Safety:</b> This method is thread-safe. Ehcache guarantees thread-safe
     * concurrent bulk operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Batch insertion
     * Map<String, User> users = new HashMap<>();
     * users.put("user:1", new User("John"));
     * users.put("user:2", new User("Jane"));
     * users.put("user:3", new User("Bob"));
     * cache.putAll(users);
     *
     * // Bulk update from database
     * List<Product> products = loadProductsFromDatabase();
     * Map<String, Product> productMap = products.stream()
     *     .collect(Collectors.toMap(p -> "product:" + p.getId(), p -> p));
     * cache.putAll(productMap);
     * }</pre>
     *
     * @param entries the map of key-value pairs to store
     * @throws IllegalArgumentException if entries is null
     * @throws IllegalStateException if the cache has been closed
     * @throws BulkCacheWritingException if the bulk cache writer fails
     */
    public void putAll(final Map<? extends K, ? extends V> entries) throws BulkCacheWritingException {
        assertNotClosed();

        if (entries == null) {
            throw new IllegalArgumentException("Entries cannot be null");
        }

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
     * providing optimized batch removal capabilities.</p>
     *
     * <p><b>Thread Safety:</b> This method is thread-safe. Ehcache guarantees thread-safe
     * concurrent bulk operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Batch removal
     * Set<String> keysToRemove = new HashSet<>(Arrays.asList("user:1", "user:2", "user:3"));
     * cache.removeAll(keysToRemove);
     *
     * // Remove all keys matching a pattern
     * Set<String> allKeys = findKeysMatchingPattern("temp:*");
     * cache.removeAll(allKeys);
     *
     * // Cleanup expired session keys
     * Set<String> expiredSessions = getExpiredSessionKeys();
     * cache.removeAll(expiredSessions);
     * }</pre>
     *
     * @param keys the set of cache keys to remove
     * @throws IllegalArgumentException if keys is null
     * @throws IllegalStateException if the cache has been closed
     * @throws BulkCacheWritingException if the bulk cache writer fails
     */
    public void removeAll(final Set<? extends K> keys) throws BulkCacheWritingException {
        assertNotClosed();

        if (keys == null) {
            throw new IllegalArgumentException("Keys cannot be null");
        }

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
     * <p><b>Why Not Supported:</b></p>
     * <ul>
     * <li>Ehcache 3.x API does not expose a keySet() or iterator() method</li>
     * <li>Multi-tier storage makes key enumeration prohibitively expensive</li>
     * <li>Memory overhead of materializing all keys could exceed available heap</li>
     * <li>For disk/distributed tiers, full scans would severely impact performance</li>
     * <li>Inconsistent with Ehcache's design philosophy of avoiding full cache scans</li>
     * </ul>
     *
     * <p><b>Alternatives:</b> Track keys externally if enumeration is required, or use a different
     * cache implementation that supports key iteration.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     * try {
     *     Set<String> keys = cache.keySet();   // throws UnsupportedOperationException
     * } catch (UnsupportedOperationException e) {
     *     System.out.println("Key iteration is not supported by Ehcache");
     *     // Alternative: maintain keys externally
     *     Set<String> trackedKeys = new HashSet<>();
     *     String key = "user:123";
     *     cache.put(key, user, 0, 0);
     *     trackedKeys.add(key);
     * }
     * }</pre>
     *
     * @return never returns normally
     * @throws UnsupportedOperationException always thrown
     * @deprecated Unsupported operation - Ehcache does not provide efficient key iteration
     */
    @Deprecated
    @Override
    public Set<K> keySet() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("keySet() is not supported by Ehcache - key iteration is not provided by the underlying Ehcache 3.x API");
    }

    /**
     * Returns the number of entries in the cache.
     * This operation is not supported by the Ehcache wrapper because Ehcache 3.x does not provide
     * a size() method or equivalent API to efficiently count cache entries. Computing the size would
     * require iterating through all keys across all storage tiers (on-heap, off-heap, disk), which
     * is not supported by Ehcache's API and would be prohibitively expensive for large caches.
     *
     * <p><b>Why Not Supported:</b></p>
     * <ul>
     * <li>Ehcache 3.x API does not provide a size(), count(), or estimatedSize() method</li>
     * <li>Multi-tier storage (on-heap, off-heap, disk) makes accurate counting complex</li>
     * <li>Would require full iteration of all storage tiers, which is not exposed by the API</li>
     * <li>For disk-backed or distributed caches, counting could require extensive I/O operations</li>
     * <li>Result would be stale immediately in highly concurrent scenarios</li>
     * </ul>
     *
     * <p><b>Alternatives:</b> Track cache entry count externally using atomic counters if needed,
     * or use a different cache implementation that supports size reporting.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     * try {
     *     int size = cache.size();   // throws UnsupportedOperationException
     * } catch (UnsupportedOperationException e) {
     *     System.out.println("Size operation is not supported by Ehcache");
     *     // Alternative: track size externally
     *     AtomicInteger counter = new AtomicInteger(0);
     *     cache.put("user:123", user, 0, 0);
     *     counter.incrementAndGet();
     *     System.out.println("Tracked size: " + counter.get());
     * }
     * }</pre>
     *
     * @return never returns normally
     * @throws UnsupportedOperationException always thrown
     * @deprecated Unsupported operation - Ehcache does not provide a size reporting API
     */
    @Deprecated
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
     * execution of the clear operation.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     * cache.clear();
     * // All entries are now removed from the cache
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
     * Closes the cache and releases resources.
     * After closing, the cache cannot be used - subsequent operations will throw IllegalStateException.
     * This method first clears all entries from the cache before marking it as closed.
     *
     * <p><b>Thread Safety:</b> This method is synchronized, thread-safe, and idempotent.
     * Calling it multiple times has no additional effect beyond the first invocation and will not throw exceptions.</p>
     *
     * <p><b>Note:</b> This method only marks the wrapper as closed; it does not close or dispose
     * the underlying Ehcache instance. The underlying cache manager is responsible for managing
     * the lifecycle of Ehcache instances.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     * try {
     *     cache.put("userId123", user, 0, 0);
     *     // Use the cache
     * } finally {
     *     cache.close();
     * }
     *
     * // Safe to call multiple times
     * cache.close();
     * cache.close();   // No exception thrown
     * }</pre>
     */
    @Override
    public synchronized void close() {
        if (isClosed) {
            return;
        }

        isClosed = true;

        clear();
    }

    /**
     * Checks if the cache has been closed.
     * This method can be used to verify cache state before performing operations,
     * though most operations will throw IllegalStateException if the cache is closed.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently.
     * The field is declared volatile to ensure visibility across threads.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Ehcache<String, User> cache = new Ehcache<>(ehcache);
     * if (!cache.isClosed()) {
     *     User user = cache.getOrNull("userId123");
     *     // Process the user
     * }
     * }</pre>
     *
     * @return {@code true} if the cache is closed, {@code false} otherwise
     */
    @Override
    public boolean isClosed() {
        return isClosed;
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