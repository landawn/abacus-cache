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
 * This class provides a bridge between Ehcache's API and the standardized Cache interface,
 * allowing Ehcache to be used seamlessly within the Abacus caching framework.
 * 
 * <br><br>
 * Example usage:
 * <pre>{@code
 * Cache<String, Person> ehcache = CacheBuilder.newBuilder()
 *     .build();
 * Ehcache<String, Person> cache = new Ehcache<>(ehcache);
 * Person person = new Person();
 * cache.put("key1", person, 3600000, 1800000); // 1 hour TTL, 30 min idle
 * Person retrieved = cache.gett("key1");
 * }</pre>
 *
 * @param <K> the type of keys used to identify cache entries
 * @param <V> the type of values stored in the cache
 * @see AbstractCache
 * @see org.ehcache.Cache
 */
public class Ehcache<K, V> extends AbstractCache<K, V> {

    private final Cache<K, V> cacheImpl;

    private volatile boolean isClosed = false;

    /**
     * Creates a new Ehcache wrapper instance.
     * This constructor wraps an existing Ehcache 3.x Cache to provide compatibility
     * with the Abacus Cache interface, enabling seamless integration with the caching framework.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> ehcache = CacheBuilder.newBuilder().build();
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
     * @param k the cache key whose associated value is to be returned
     * @return the value associated with the specified key, or null if not found, expired, or evicted
     * @throws IllegalStateException if the cache has been closed
     * @throws CacheLoadingException if the cache loader fails
     */
    @Override
    public V gett(final K k) {
        assertNotClosed();

        return cacheImpl.get(k);
    }

    /**
     * Stores a key-value pair in the cache with specified expiration parameters.
     * Note: The current implementation does not honor the individual TTL and idle time
     * parameters as Ehcache expiration is configured at cache level.
     *
     * @param k the cache key with which the specified value is to be associated
     * @param v the cache value to be associated with the specified key
     * @param liveTime the time-to-live in milliseconds (currently ignored)
     * @param maxIdleTime the maximum idle time in milliseconds (currently ignored)
     * @return true if the operation was successful
     * @throws IllegalArgumentException if key is null
     * @throws IllegalStateException if the cache has been closed
     * @throws CacheWritingException if the cache writer fails
     */
    @Override
    public boolean put(final K k, final V v, final long liveTime, final long maxIdleTime) {
        assertNotClosed();

        if (k == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        cacheImpl.put(k, v); // TODO

        return true;
    }

    /**
     * Removes the mapping for a key from the cache if it is present.
     *
     * @param k the cache key whose mapping is to be removed from the cache
     * @throws IllegalStateException if the cache has been closed
     * @throws CacheWritingException if the cache writer fails
     */
    @Override
    public void remove(final K k) {
        assertNotClosed();

        cacheImpl.remove(k);
    }

    /**
     * Checks if the cache contains a mapping for the specified key.
     *
     * @param k the cache key whose presence in the cache is to be tested
     * @return true if the cache contains a mapping for the specified key
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public boolean containsKey(final K k) {
        assertNotClosed();

        return cacheImpl.containsKey(k);
    }

    /**
     * Atomically puts a value if the key is not already present.
     * This operation is atomic and thread-safe, ensuring that concurrent operations
     * maintain consistency. If the key already exists, the existing value is returned
     * unchanged.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * V newValue = createNewValue();
     * V existingValue = cache.putIfAbsent("key1", newValue);
     * if (existingValue == null) {
     *     // Value was successfully added
     * }
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated
     * @param value the cache value to be associated with the specified key
     * @return the previous value associated with the specified key, or null if there was no mapping
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
     * Retrieves multiple values from the cache in a single operation.
     * This is more efficient than multiple individual get operations, particularly
     * when fetching many entries. The returned map only includes keys that were found
     * in the cache; missing keys are not included.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Set<String> keys = N.asSet("key1", "key2", "key3");
     * Map<String, User> results = cache.getAll(keys);
     * }</pre>
     *
     * @param keys the set of cache keys to retrieve
     * @return a map of key-value pairs found in the cache
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
     * Stores multiple key-value pairs in the cache in a single operation.
     * This is more efficient than multiple individual put operations, particularly
     * when storing many entries. All key-value pairs are stored in a single batch
     * operation for optimal performance.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user1 = new User();
     * User user2 = new User();
     * Map<String, User> users = N.asMap("user1", user1, "user2", user2);
     * cache.putAll(users);
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
     * Removes multiple keys from the cache in a single operation.
     * This is more efficient than multiple individual remove operations, particularly
     * when removing many entries. All keys are removed in a single batch operation
     * for optimal performance.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Set<String> keysToRemove = N.asSet("key1", "key2", "key3");
     * cache.removeAll(keysToRemove);
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
     * This operation is not supported by the Ehcache wrapper.
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
     * Returns the number of entries in the cache.
     * This operation is not supported by the Ehcache wrapper.
     *
     * @return never returns normally
     * @throws UnsupportedOperationException always thrown
     * @deprecated Unsupported operation
     */
    @Deprecated
    @Override
    public int size() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Removes all entries from the cache.
     * This operation affects only the local cache tier.
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
     * This method is thread-safe but NOT idempotent - calling it multiple times will throw IllegalStateException.
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
     *
     * @return true if the cache is closed
     */
    @Override
    public boolean isClosed() {
        return isClosed;
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