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

import java.io.Closeable;
import java.util.Set;

import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.Properties;
import com.landawn.abacus.util.u.Optional;

/**
 * The core interface for all cache implementations in the Abacus framework.
 * This interface defines the contract for caching systems, providing both synchronous
 * and asynchronous operations, configurable expiration policies, and property management.
 * 
 * <br><br>
 * Key features:
 * <ul>
 * <li>Synchronous and asynchronous operations</li>
 * <li>Time-to-live (TTL) and idle timeout support</li>
 * <li>Optional-based API for null-safe operations</li>
 * <li>Property bag for custom configuration</li>
 * <li>Resource management via Closeable</li>
 * </ul>
 * 
 * <br>
 * Example usage:
 * <pre>{@code
 * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
 * 
 * // Synchronous operations
 * cache.put("user:123", user);
 * Optional<User> cached = cache.get("user:123");
 * 
 * // Asynchronous operations
 * ContinuableFuture<Boolean> future = cache.asyncPut("user:456", anotherUser);
 * future.thenAccept(success -> System.out.println("Cached: " + success));
 * 
 * // Custom expiration
 * cache.put("temp:data", data, 5000, 2000); // 5s TTL, 2s idle timeout
 * }</pre>
 *
 * @param <K> the key type
 * @param <V> the value type
 * @see LocalCache
 * @see DistributedCache
 * @see CacheFactory
 */
public interface Cache<K, V> extends Closeable {

    /**
     * Default time-to-live for cache entries: 3 hours (in milliseconds).
     */
    long DEFAULT_LIVE_TIME = 3 * 60 * 60 * 1000L;

    /**
     * Default maximum idle time for cache entries: 30 minutes (in milliseconds).
     */
    long DEFAULT_MAX_IDLE_TIME = 30 * 60 * 1000L;

    /**
     * Retrieves a value from the cache wrapped in an Optional.
     * This method provides a null-safe way to handle cache misses.
     *
     * @param k the key to look up
     * @return an Optional containing the value if present, or empty if not found
     */
    Optional<V> get(final K k);

    /**
     * Retrieves a value from the cache directly.
     * This method returns null for cache misses rather than using Optional.
     *
     * @param k the key to look up
     * @return the cached value, or null if not found
     */
    V gett(final K k);

    /**
     * Stores a key-value pair in the cache using default expiration settings.
     * The default TTL and idle time are implementation-specific.
     *
     * @param k the key
     * @param v the value to cache
     * @return true if the operation was successful
     */
    boolean put(final K k, final V v);

    /**
     * Stores a key-value pair in the cache with custom expiration settings.
     * If the key already exists, its value and expiration settings will be replaced.
     * The entry will be evicted when either the TTL expires or the idle time is exceeded.
     *
     * <br><br>
     * Note: Some cache implementations (particularly distributed caches) may not support
     * idle timeout and will only respect the liveTime parameter.
     *
     * @param k the key
     * @param v the value to cache
     * @param liveTime time-to-live in milliseconds (0 for no expiration)
     * @param maxIdleTime maximum idle time in milliseconds (0 for no idle timeout).
     *                    Note: Not supported by all implementations.
     * @return true if the operation was successful
     */
    boolean put(final K k, final V v, long liveTime, long maxIdleTime);

    /**
     * Removes an entry from the cache.
     * This operation succeeds whether the key exists.
     *
     * @param k the key to remove
     */
    void remove(final K k);

    /**
     * Checks if the cache contains a specific key.
     * This method should not affect access time for LRU-based caches.
     *
     * @param k the key to check
     * @return true if the key exists in the cache
     */
    boolean containsKey(final K k);

    /**
     * Asynchronously retrieves a value from the cache.
     * The operation is executed on a background thread from the shared async executor pool.
     *
     * @param k the key to look up
     * @return a future that will contain the Optional result
     */
    ContinuableFuture<Optional<V>> asyncGet(final K k);

    /**
     * Asynchronously retrieves a value from the cache directly.
     * The operation is executed on a background thread from the shared async executor pool.
     *
     * @param k the key to look up
     * @return a future that will contain the value or null
     */
    ContinuableFuture<V> asyncGett(final K k);

    /**
     * Asynchronously stores a key-value pair using default expiration.
     * The operation is executed on a background thread from the shared async executor pool.
     *
     * @param k the key
     * @param v the value to cache
     * @return a future that will contain true if successful
     */
    ContinuableFuture<Boolean> asyncPut(final K k, final V v);

    /**
     * Asynchronously stores a key-value pair with custom expiration.
     * The operation is executed on a background thread from the shared async executor pool.
     *
     * @param k the key
     * @param v the value to cache
     * @param liveTime time-to-live in milliseconds (0 for no expiration)
     * @param maxIdleTime maximum idle time in milliseconds (0 for no idle timeout)
     * @return a future that will contain true if successful
     */
    ContinuableFuture<Boolean> asyncPut(final K k, final V v, long liveTime, long maxIdleTime);

    /**
     * Asynchronously removes an entry from the cache.
     * The operation is executed on a background thread from the shared async executor pool.
     *
     * @param k the key to remove
     * @return a future that completes when the operation finishes
     */
    ContinuableFuture<Void> asyncRemove(final K k);

    /**
     * Asynchronously checks if the cache contains a key.
     * The operation is executed on a background thread from the shared async executor pool.
     *
     * @param k the key to check
     * @return a future that will contain true if the key exists
     */
    ContinuableFuture<Boolean> asyncContainsKey(final K k);

    /**
     * Returns a set of all keys in the cache.
     * The returned set may be a snapshot or a live view depending on implementation.
     * Some implementations may throw UnsupportedOperationException.
     *
     * @return a set of cache keys
     * @throws UnsupportedOperationException if not supported by the implementation
     */
    Set<K> keySet();

    /**
     * Returns the number of entries in the cache.
     * Some implementations may return an estimate or throw UnsupportedOperationException.
     *
     * @return the number of cache entries
     * @throws UnsupportedOperationException if not supported by the implementation
     */
    int size();

    /**
     * Removes all entries from the cache.
     * This operation may be expensive for distributed caches.
     */
    void clear();

    /**
     * Closes the cache and releases all resources.
     * After closing, the cache cannot be used - subsequent operations will throw IllegalStateException.
     * This method is idempotent and thread-safe - multiple calls have no additional effect.
     */
    @Override
    void close();

    /**
     * Checks if the cache has been closed.
     *
     * @return true if {@link #close()} has been called
     */
    boolean isClosed();

    /**
     * Returns the properties bag for this cache.
     * Properties can be used to store custom configuration or metadata.
     *
     * @return the properties container
     */
    Properties<String, Object> getProperties();

    /**
     * Retrieves a property value by name.
     * Returns null if the property doesn't exist.
     *
     * @param <T> the type of the property value to be returned
     * @param propName the property name
     * @return the property value, or null if not found
     */
    <T> T getProperty(String propName);

    /**
     * Sets a property value.
     * Properties can be used for custom configuration or metadata.
     *
     * @param <T> the type of the previous property value to be returned
     * @param propName the property name
     * @param propValue the property value
     * @return the previous value associated with the property, or null
     */
    <T> T setProperty(String propName, Object propValue);

    /**
     * Removes a property.
     *
     * @param <T> the type of the property value to be returned
     * @param propName the property name to remove
     * @return the removed value, or null if the property didn't exist
     */
    <T> T removeProperty(String propName);
}