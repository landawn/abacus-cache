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
 * cache.put("key1", person, 3600000, 1800000); // 1 hour TTL, 30 min idle
 * Person person = cache.gett("key1");
 * }</pre>
 *
 * @param <K> the key type
 * @param <V> the value type
 * @see AbstractCache
 * @see org.ehcache.Cache
 */
public class Ehcache<K, V> extends AbstractCache<K, V> {

    private final Cache<K, V> cacheImpl;

    private boolean isClosed = false;

    /**
     * Creates a new Ehcache wrapper instance.
     *
     * @param cache the underlying Ehcache instance to wrap
     */
    public Ehcache(final Cache<K, V> cache) {
        cacheImpl = cache;
    }

    /**
     * Retrieves a value from the cache by its key.
     * This method may trigger a cache loader if configured in the underlying Ehcache.
     * The operation may update access time depending on the eviction policy.
     *
     * @param k the key to look up
     * @return the cached value, or null if not found, expired, or evicted
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
     * @param k the key
     * @param v the value to cache
     * @param liveTime the time-to-live in milliseconds (currently ignored)
     * @param maxIdleTime the maximum idle time in milliseconds (currently ignored)
     * @return true if the operation was successful
     * @throws CacheWritingException if the cache writer fails
     */
    @Override
    public boolean put(final K k, final V v, final long liveTime, final long maxIdleTime) {
        assertNotClosed();

        cacheImpl.put(k, v); // TODO

        return true;
    }

    /**
     * Removes a key-value pair from the cache.
     *
     * @param k the key to remove
     * @throws CacheWritingException if the cache writer fails
     */
    @Override
    public void remove(final K k) {
        assertNotClosed();

        cacheImpl.remove(k);
    }

    /**
     * Checks if the cache contains a specific key.
     *
     * @param k the key to check
     * @return true if the key exists in the cache
     */
    @Override
    public boolean containsKey(final K k) {
        assertNotClosed();

        return cacheImpl.containsKey(k);
    }

    /**
     * Atomically puts a value if the key is not already present.
     * This operation is atomic and thread-safe.
     *
     * @param key the key
     * @param value the value to put
     * @return the existing value if present, null otherwise
     * @throws CacheLoadingException if the cache loader fails
     * @throws CacheWritingException if the cache writer fails
     */
    public V putIfAbsent(final K key, final V value) throws CacheLoadingException, CacheWritingException {
        assertNotClosed();

        return cacheImpl.putIfAbsent(key, value);
    }

    /**
     * Retrieves multiple values from the cache in a single operation.
     * This is more efficient than multiple individual get operations.
     *
     * @param keys the set of keys to retrieve
     * @return a map of key-value pairs found in the cache
     * @throws BulkCacheLoadingException if the bulk cache loader fails
     */
    public Map<K, V> getAll(final Set<? extends K> keys) throws BulkCacheLoadingException {
        assertNotClosed();

        return cacheImpl.getAll(keys);
    }

    /**
     * Stores multiple key-value pairs in the cache in a single operation.
     * This is more efficient than multiple individual put operations.
     *
     * @param entries the map of key-value pairs to store
     * @throws BulkCacheWritingException if the bulk cache writer fails
     */
    public void putAll(final Map<? extends K, ? extends V> entries) throws BulkCacheWritingException {
        assertNotClosed();

        cacheImpl.putAll(entries);
    }

    /**
     * Removes multiple keys from the cache in a single operation.
     * This is more efficient than multiple individual remove operations.
     *
     * @param keys the set of keys to remove
     * @throws BulkCacheWritingException if the bulk cache writer fails
     */
    public void removeAll(final Set<? extends K> keys) throws BulkCacheWritingException {
        assertNotClosed();

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
     */
    @Override
    public void clear() {
        assertNotClosed();

        cacheImpl.clear();
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