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

import java.util.concurrent.TimeUnit;

import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Properties;
import com.landawn.abacus.util.u.Optional;

/**
 * Abstract base class for all cache implementations providing common functionality.
 * This class implements the asynchronous operations, property management, and default
 * behaviors defined in the Cache interface. It serves as the foundation for all
 * concrete cache implementations in the framework.
 * 
 * <br><br>
 * Key features provided:
 * <ul>
 * <li>Asynchronous operation implementations using a shared thread pool</li>
 * <li>Default TTL and idle time management</li>
 * <li>Property bag for custom configuration</li>
 * <li>Optional-based wrapper methods</li>
 * </ul>
 * 
 * <br>
 * Subclasses must implement:
 * <ul>
 * <li>{@link #gett(Object)} - Direct value retrieval</li>
 * <li>{@link #put(Object, Object, long, long)} - Storage with expiration</li>
 * <li>{@link #remove(Object)} - Entry removal</li>
 * <li>{@link #containsKey(Object)} - Key existence check</li>
 * <li>{@link #keySet()} - Key enumeration (optional)</li>
 * <li>{@link #size()} - Entry count (optional)</li>
 * <li>{@link #clear()} - Bulk removal</li>
 * <li>{@link #close()} - Resource cleanup</li>
 * <li>{@link #isClosed()} - State check</li>
 * </ul>
 * 
 * <br>
 * Example of extending this class:
 * <pre>{@code
 * public class MyCache<K, V> extends AbstractCache<K, V> {
 *     private final Map<K, V> storage = new ConcurrentHashMap<>();
 *     
 *     @Override
 *     public V gett(K key) {
 *         return storage.get(key);
 *     }
 *     
 *     @Override
 *     public boolean put(K key, V value, long liveTime, long maxIdleTime) {
 *         storage.put(key, value);
 *         // Handle expiration logic
 *         return true;
 *     }
 *     // ... implement other abstract methods
 * }
 * }</pre>
 *
 * @param <K> the key type
 * @param <V> the value type
 * @see Cache
 * @see LocalCache
 * @see DistributedCache
 */
public abstract class AbstractCache<K, V> implements Cache<K, V> {

    /**
     * Shared async executor for all cache implementations.
     * Configured with a thread pool sized based on CPU cores to efficiently
     * handle asynchronous cache operations without overwhelming the system.
     * Core pool size is max(64, CPU_CORES * 8), max pool size is max(128, CPU_CORES * 16),
     * and threads are kept alive for 180 seconds.
     */
    protected static final AsyncExecutor asyncExecutor = new AsyncExecutor(//
            N.max(64, IOUtil.CPU_CORES * 8), // coreThreadPoolSize
            N.max(128, IOUtil.CPU_CORES * 16), // maxThreadPoolSize
            180L, TimeUnit.SECONDS);

    /**
     * Property bag for storing custom configuration and metadata.
     * Can be used by cache implementations and users to store arbitrary properties.
     */
    protected final Properties<String, Object> properties = new Properties<>();

    /**
     * Default time-to-live for cache entries in milliseconds.
     * Used when put() is called without explicit TTL.
     */
    protected final long defaultLiveTime;

    /**
     * Default maximum idle time for cache entries in milliseconds.
     * Used when put() is called without explicit idle time.
     */
    protected final long defaultMaxIdleTime;

    /**
     * Creates an AbstractCache with default expiration times.
     * Uses DEFAULT_LIVE_TIME (3 hours) and DEFAULT_MAX_IDLE_TIME (30 minutes) as the
     * default TTL and idle time for entries added without explicit expiration settings.
     */
    protected AbstractCache() {
        this(DEFAULT_LIVE_TIME, DEFAULT_MAX_IDLE_TIME);
    }

    /**
     * Creates an AbstractCache with custom default expiration times.
     * These defaults are used when entries are added without explicit expiration.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create cache with 1 hour TTL and 15 minutes idle time
     * AbstractCache<String, User> cache = new MyCache<>(3600000L, 900000L);
     * }</pre>
     *
     * @param defaultLiveTime default TTL in milliseconds for new entries (use 0 for no expiration)
     * @param defaultMaxIdleTime default max idle time in milliseconds for new entries (use 0 for no idle timeout)
     */
    protected AbstractCache(final long defaultLiveTime, final long defaultMaxIdleTime) {
        this.defaultLiveTime = defaultLiveTime;
        this.defaultMaxIdleTime = defaultMaxIdleTime;
    }

    /**
     * Retrieves a value from the cache wrapped in an Optional.
     * This method provides a null-safe way to handle cache misses and follows functional programming patterns.
     * The operation is thread-safe and does not block other cache operations.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>Returns {@code Optional.empty()} if the key does not exist</li>
     * <li>Returns {@code Optional.empty()} if the entry has expired (TTL or idle timeout exceeded)</li>
     * <li>May update the last access time for idle timeout tracking (implementation-specific)</li>
     * <li>Does not throw exceptions for missing keys - returns empty Optional instead</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Basic retrieval with functional approach
     * Optional<User> user = cache.get("user:123");
     * user.ifPresent(u -> System.out.println("Found: " + u.getName()));
     *
     * // With default value
     * User result = cache.get("user:123").orElse(defaultUser);
     *
     * // Chaining operations
     * String userName = cache.get("user:123")
     *     .map(User::getName)
     *     .orElse("Unknown");
     * }</pre>
     *
     * @param k the cache key to look up (must not be null for most implementations)
     * @return an Optional containing the cached value if present and not expired, or an empty Optional otherwise
     * @throws IllegalStateException if the cache has been closed
     * @see #gett(Object)
     * @see #asyncGet(Object)
     */
    @Override
    public Optional<V> get(final K k) {
        return Optional.ofNullable(gett(k));
    }

    /**
     * Stores a key-value pair in the cache using default expiration settings.
     * The default TTL ({@link #DEFAULT_LIVE_TIME}) and idle time ({@link #DEFAULT_MAX_IDLE_TIME})
     * are used unless overridden by the implementation. If the key already exists, its value will
     * be updated and its expiration time will be reset. The operation is thread-safe and atomic.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>Overwrites existing entries with the same key</li>
     * <li>Resets TTL and idle timeout for existing entries</li>
     * <li>May trigger eviction of old entries if cache capacity is reached</li>
     * <li>Returns false if the operation fails (e.g., cache full and eviction not possible)</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Basic put operation
     * boolean success = cache.put("user:123", user);
     * if (success) {
     *     System.out.println("User cached successfully");
     * }
     *
     * // Update existing entry
     * cache.put("user:123", updatedUser);   // Replaces previous value
     *
     * // Cache-aside pattern
     * User user = cache.gett("user:123");
     * if (user == null) {
     *     user = loadFromDatabase("user:123");
     *     cache.put("user:123", user);
     * }
     * }</pre>
     *
     * @param k the cache key to store the value under (must not be null for most implementations)
     * @param v the value to cache (may be null depending on implementation, check implementation docs)
     * @return true if the operation was successful, false otherwise (e.g., cache full, closed, or write failure)
     * @throws IllegalStateException if the cache has been closed
     * @see #put(Object, Object, long, long)
     * @see #asyncPut(Object, Object)
     */
    @Override
    public boolean put(final K k, final V v) {
        return put(k, v, defaultLiveTime, defaultMaxIdleTime);
    }

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
     * @see #get(Object)
     * @see #asyncGett(Object)
     */
    @Override
    public ContinuableFuture<Optional<V>> asyncGet(final K k) {
        return asyncExecutor.execute(() -> get(k));
    }

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
     * @see #gett(Object)
     * @see #asyncGet(Object)
     */
    @Override
    public ContinuableFuture<V> asyncGett(final K k) {
        return asyncExecutor.execute(() -> gett(k));
    }

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
     * @see #put(Object, Object)
     * @see #asyncPut(Object, Object, long, long)
     */
    @Override
    public ContinuableFuture<Boolean> asyncPut(final K k, final V v) {
        return asyncExecutor.execute(() -> put(k, v));
    }

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
     * @see #put(Object, Object, long, long)
     * @see #asyncPut(Object, Object)
     */
    @Override
    public ContinuableFuture<Boolean> asyncPut(final K k, final V v, final long liveTime, final long maxIdleTime) {
        return asyncExecutor.execute(() -> put(k, v, liveTime, maxIdleTime));
    }

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
     * @see #remove(Object)
     * @see #asyncContainsKey(Object)
     */
    @Override
    public ContinuableFuture<Void> asyncRemove(final K k) {
        return asyncExecutor.execute(() -> {
            remove(k);

            return null;
        });
    }

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
     * @see #containsKey(Object)
     * @see #asyncGet(Object)
     */
    @Override
    public ContinuableFuture<Boolean> asyncContainsKey(final K k) {
        return asyncExecutor.execute(() -> containsKey(k));
    }

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
     * @see #getProperty(String)
     * @see #setProperty(String, Object)
     */
    @Override
    public Properties<String, Object> getProperties() {
        return properties;
    }

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
     * String unknown = cache.getProperty("nonExistent");   // null
     * }</pre>
     *
     * @param <T> the type of the property value to be returned (caller should ensure correct type)
     * @param propName the property name to look up
     * @return the property value cast to type T, or null if not found
     * @see #setProperty(String, Object)
     * @see #getProperties()
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T getProperty(final String propName) {
        return (T) properties.get(propName);
    }

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
     * System.out.println("Old: " + oldDescription);   // "User cache for session data"
     * }</pre>
     *
     * @param <T> the type of the previous property value to be returned (caller should ensure correct type)
     * @param propName the property name to set
     * @param propValue the property value to set (can be any object type)
     * @return the previous value associated with the property, or null if there was no previous value
     * @see #getProperty(String)
     * @see #removeProperty(String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T setProperty(final String propName, final Object propValue) {
        return (T) properties.put(propName, propValue);
    }

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
     * System.out.println("Removed: " + oldValue);   // "User cache"
     *
     * // Removing non-existent property returns null
     * String notFound = cache.removeProperty("nonExistent");   // null
     * }</pre>
     *
     * @param <T> the type of the property value to be returned (caller should ensure correct type)
     * @param propName the property name to remove
     * @return the removed value, or null if the property didn't exist
     * @see #setProperty(String, Object)
     * @see #getProperty(String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T removeProperty(final String propName) {
        return (T) properties.remove(propName);
    }
}