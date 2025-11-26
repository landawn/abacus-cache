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
     * This method provides a null-safe way to handle cache misses and follows functional programming patterns.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * Optional<User> user = cache.get("user:123");
     * user.ifPresent(u -> System.out.println("Found: " + u.getName()));
     * }</pre>
     *
     * @param k the cache key to look up
     * @return an Optional containing the cached value if present, or an empty Optional if the key is not found or has expired
     */
    Optional<V> get(final K k);

    /**
     * Retrieves a value from the cache directly without wrapping in Optional.
     * This method returns null for cache misses rather than using Optional, providing a more
     * traditional API for scenarios where Optional overhead is not desired.
     *
     * <p><b>Note:</b> The method name uses double 't' (gett) to distinguish it from {@link #get(Object)},
     * which returns {@code Optional<V>}. This naming convention allows both APIs to coexist
     * while clearly indicating their different return types.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * User user = cache.gett("user:123");
     * if (user != null) {
     *     System.out.println(user.getName());
     * }
     * }</pre>
     *
     * @param k the cache key to look up
     * @return the cached value if present, or null if the key is not found or has expired
     */
    V gett(final K k);

    /**
     * Stores a key-value pair in the cache using default expiration settings.
     * The default TTL and idle time are implementation-specific. If the key already exists,
     * its value will be updated and its expiration time will be reset.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * boolean success = cache.put("user:123", user);
     * if (success) {
     *     System.out.println("User cached successfully");
     * }
     * }</pre>
     *
     * @param k the cache key to store the value under
     * @param v the value to cache (may be null depending on implementation)
     * @return true if the operation was successful, false otherwise
     */
    boolean put(final K k, final V v);

    /**
     * Stores a key-value pair in the cache with custom expiration settings.
     * If the key already exists, its value and expiration settings will be replaced.
     * The entry will be evicted when either the TTL expires (measured from the time of insertion)
     * or the idle time is exceeded (measured from the last access).
     *
     * <br><br>
     * Note: Some cache implementations (particularly distributed caches) may not support
     * idle timeout and will only respect the liveTime parameter.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Session with 1 hour TTL, 30 minute idle timeout
     * cache.put("session:abc", session, 3600000, 1800000);
     *
     * // Temporary data with 5 second TTL, no idle timeout
     * cache.put("temp:data", data, 5000, 0);
     *
     * // No expiration (use with caution)
     * cache.put("permanent:config", config, 0, 0);
     * }</pre>
     *
     * @param k the cache key to store the value under
     * @param v the value to cache (may be null depending on implementation)
     * @param liveTime the time-to-live in milliseconds from insertion (0 or negative for no TTL expiration)
     * @param maxIdleTime the maximum idle time in milliseconds since last access (0 or negative for no idle timeout).
     *                    Note: Not supported by all implementations - check implementation documentation.
     * @return true if the operation was successful, false otherwise
     */
    boolean put(final K k, final V v, long liveTime, long maxIdleTime);

    /**
     * Removes an entry from the cache.
     * This operation is idempotent - it succeeds whether the key exists or not.
     * If the key exists, the entry is removed; if not, the operation has no effect.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * cache.remove("user:123"); // Removes if exists, no error if not
     *
     * // Safe to call multiple times
     * cache.remove("user:123");
     * cache.remove("user:123"); // No exception thrown
     * }</pre>
     *
     * @param k the cache key to remove
     */
    void remove(final K k);

    /**
     * Checks if the cache contains a specific key.
     * Note: For most implementations, this method checks for the presence of the key
     * but does not affect the access time or LRU ordering. However, expired entries
     * may or may not be considered present depending on the implementation.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * if (cache.containsKey("user:123")) {
     *     System.out.println("User is cached");
     * } else {
     *     System.out.println("User not found in cache");
     * }
     * }</pre>
     *
     * @param k the cache key to check for
     * @return true if the key exists in the cache (and is not expired), false otherwise
     */
    boolean containsKey(final K k);

    /**
     * Asynchronously retrieves a value from the cache wrapped in an Optional.
     * The operation is executed on a background thread from the shared async executor pool.
     * This is the asynchronous version of {@link #get(Object)}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * cache.asyncGet("user:123")
     *      .thenAccept(opt -> opt.ifPresent(u -> System.out.println("Found: " + u.getName())));
     * }</pre>
     *
     * @param k the cache key to look up
     * @return a ContinuableFuture that will complete with an Optional containing the cached value if present,
     *         or an empty Optional if the key is not found or has expired
     */
    ContinuableFuture<Optional<V>> asyncGet(final K k);

    /**
     * Asynchronously retrieves a value from the cache directly without wrapping in Optional.
     * The operation is executed on a background thread from the shared async executor pool.
     * This is the asynchronous version of {@link #gett(Object)}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * cache.asyncGett("user:123")
     *      .thenAccept(user -> {
     *          if (user != null) {
     *              process(user);
     *          }
     *      });
     * }</pre>
     *
     * @param k the cache key to look up
     * @return a ContinuableFuture that will complete with the cached value if present,
     *         or null if the key is not found or has expired
     */
    ContinuableFuture<V> asyncGett(final K k);

    /**
     * Asynchronously stores a key-value pair using default expiration settings.
     * The operation is executed on a background thread from the shared async executor pool.
     * This is the asynchronous version of {@link #put(Object, Object)}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * cache.asyncPut("user:123", user)
     *      .thenAccept(success -> {
     *          if (success) {
     *              log("User cached successfully");
     *          }
     *      });
     * }</pre>
     *
     * @param k the cache key to store the value under
     * @param v the value to cache (may be null depending on implementation)
     * @return a ContinuableFuture that will complete with true if the operation was successful, false otherwise
     */
    ContinuableFuture<Boolean> asyncPut(final K k, final V v);

    /**
     * Asynchronously stores a key-value pair with custom expiration settings.
     * The operation is executed on a background thread from the shared async executor pool.
     * This is the asynchronous version of {@link #put(Object, Object, long, long)}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * cache.asyncPut("session:abc", session, 3600000, 1800000)
     *      .thenAccept(success -> {
     *          if (success) {
     *              log("Session cached with 1h TTL, 30min idle");
     *          }
     *      });
     * }</pre>
     *
     * @param k the cache key to store the value under
     * @param v the value to cache (may be null depending on implementation)
     * @param liveTime the time-to-live in milliseconds from insertion (0 or negative for no TTL expiration)
     * @param maxIdleTime the maximum idle time in milliseconds since last access (0 or negative for no idle timeout)
     * @return a ContinuableFuture that will complete with true if the operation was successful, false otherwise
     */
    ContinuableFuture<Boolean> asyncPut(final K k, final V v, long liveTime, long maxIdleTime);

    /**
     * Asynchronously removes an entry from the cache.
     * The operation is executed on a background thread from the shared async executor pool.
     * This is the asynchronous version of {@link #remove(Object)}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * cache.asyncRemove("user:123")
     *      .thenRun(() -> log("User removed from cache"));
     * }</pre>
     *
     * @param k the cache key to remove
     * @return a ContinuableFuture that completes when the operation finishes
     */
    ContinuableFuture<Void> asyncRemove(final K k);

    /**
     * Asynchronously checks if the cache contains a specific key.
     * The operation is executed on a background thread from the shared async executor pool.
     * This is the asynchronous version of {@link #containsKey(Object)}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * cache.asyncContainsKey("user:123")
     *      .thenAccept(exists -> log("User exists in cache: " + exists));
     * }</pre>
     *
     * @param k the cache key to check for
     * @return a ContinuableFuture that will complete with true if the key exists in the cache (and is not expired),
     *         false otherwise
     */
    ContinuableFuture<Boolean> asyncContainsKey(final K k);

    /**
     * Returns a set of all keys currently in the cache.
     * The returned set may be a snapshot (immutable) or a live view (reflecting cache changes)
     * depending on the implementation. Some implementations may throw UnsupportedOperationException.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * Set<String> keys = cache.keySet();
     * keys.forEach(key -> System.out.println("Cached key: " + key));
     *
     * // Check if any keys exist
     * if (!cache.keySet().isEmpty()) {
     *     System.out.println("Cache has entries");
     * }
     * }</pre>
     *
     * @return a set of all cache keys (may or may not include expired entries depending on implementation)
     * @throws UnsupportedOperationException if the operation is not supported by this cache implementation
     */
    Set<K> keySet();

    /**
     * Returns the number of entries currently in the cache.
     * The count may or may not include expired entries depending on the implementation.
     * Some implementations may return an estimate rather than an exact count,
     * and some may throw UnsupportedOperationException.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * int count = cache.size();
     * System.out.println("Cache contains " + count + " entries");
     *
     * // Check if cache is empty
     * if (cache.size() == 0) {
     *     System.out.println("Cache is empty");
     * }
     * }</pre>
     *
     * @return the number of cache entries (may be an estimate depending on implementation)
     * @throws UnsupportedOperationException if the operation is not supported by this cache implementation
     */
    int size();

    /**
     * Removes all entries from the cache.
     * After this operation, the cache will be empty ({@link #size()} returns 0).
     * This operation may be expensive for distributed caches with large numbers of entries.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * cache.clear(); // Removes all cached entries
     *
     * // Clear and verify
     * cache.clear();
     * System.out.println("Cache size after clear: " + cache.size()); // Should be 0
     * }</pre>
     */
    void clear();

    /**
     * Closes the cache and releases all associated resources.
     * After closing, the cache cannot be used - subsequent operations may throw exceptions
     * or have undefined behavior depending on the implementation.
     * This method is idempotent and thread-safe - multiple calls have no additional effect.
     *
     * <p>It is recommended to always close the cache when it's no longer needed to prevent
     * resource leaks. Use try-with-resources or try-finally blocks to ensure proper cleanup.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Try-with-resources (recommended)
     * try (Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000)) {
     *     cache.put("key", value);
     *     // Cache is automatically closed
     * }
     *
     * // Try-finally
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * try {
     *     cache.put("key", value);
     * } finally {
     *     cache.close(); // Always close to release resources
     * }
     * }</pre>
     */
    @Override
    void close();

    /**
     * Checks if the cache has been closed.
     * Once a cache is closed, it cannot be reopened and should not be used.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * if (!cache.isClosed()) {
     *     cache.put("key", value);
     * }
     *
     * // Verify state after closing
     * cache.close();
     * System.out.println("Is closed: " + cache.isClosed()); // true
     * }</pre>
     *
     * @return true if {@link #close()} has been called, false otherwise
     */
    boolean isClosed();

    /**
     * Returns the properties bag for this cache instance.
     * Properties can be used to store custom configuration, metadata, or application-specific data
     * associated with this cache. The returned Properties object is mutable and changes are reflected
     * in the cache.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * Properties<String, Object> props = cache.getProperties();
     * props.put("description", "User cache for active sessions");
     * props.put("region", "us-west-2");
     *
     * // Later retrieve the properties
     * String description = cache.getProperties().get("description");
     * }</pre>
     *
     * @return the properties container for this cache, never null
     */
    Properties<String, Object> getProperties();

    /**
     * Retrieves a property value by name.
     * This is a convenience method equivalent to calling {@code getProperties().get(propName)}.
     * Returns null if the property doesn't exist.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * cache.setProperty("description", "User cache");
     * cache.setProperty("maxRetries", 3);
     *
     * String description = cache.getProperty("description");
     * Integer retries = cache.getProperty("maxRetries");
     *
     * // Returns null for non-existent properties
     * String unknown = cache.getProperty("nonExistent"); // null
     * }</pre>
     *
     * @param <T> the type of the property value to be returned (caller should ensure correct type)
     * @param propName the property name to look up
     * @return the property value cast to type T, or null if not found
     */
    <T> T getProperty(String propName);

    /**
     * Sets a property value.
     * This is a convenience method equivalent to calling {@code getProperties().put(propName, propValue)}.
     * Properties can be used for custom configuration, metadata, or application-specific data.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Set various property types
     * cache.setProperty("description", "User cache for session data");
     * cache.setProperty("maxRetries", 3);
     * cache.setProperty("enableMetrics", true);
     *
     * // Update an existing property and get the old value
     * String oldDescription = cache.setProperty("description", "Updated description");
     * System.out.println("Old: " + oldDescription); // "User cache for session data"
     * }</pre>
     *
     * @param <T> the type of the previous property value to be returned (caller should ensure correct type)
     * @param propName the property name to set
     * @param propValue the property value to set (can be any object type)
     * @return the previous value associated with the property, or null if there was no previous value
     */
    <T> T setProperty(String propName, Object propValue);

    /**
     * Removes a property from the cache.
     * This is a convenience method equivalent to calling {@code getProperties().remove(propName)}.
     * This operation is idempotent - it succeeds whether the property exists or not.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * cache.setProperty("description", "User cache");
     *
     * // Remove and get the old value
     * String oldValue = cache.removeProperty("description");
     * System.out.println("Removed: " + oldValue); // "User cache"
     *
     * // Removing non-existent property returns null
     * String notFound = cache.removeProperty("nonExistent"); // null
     * }</pre>
     *
     * @param <T> the type of the property value to be returned (caller should ensure correct type)
     * @param propName the property name to remove
     * @return the removed value, or null if the property didn't exist
     */
    <T> T removeProperty(String propName);
}