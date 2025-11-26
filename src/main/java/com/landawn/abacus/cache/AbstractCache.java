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
     * This method provides a null-safe alternative to {@link #gett(Object)} by
     * wrapping the result in an Optional. If the key is not found or has expired,
     * an empty Optional is returned. This method follows functional programming patterns
     * and allows for clean chaining with other Optional operations.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(1000, 60000);
     * Optional<User> userOpt = cache.get("user:123");
     * userOpt.ifPresent(u -> System.out.println("Found: " + u.getName()));
     *
     * // Or use with default value
     * User user = cache.get("user:123").orElse(new User("guest"));
     *
     * // Chain with other operations
     * String userName = cache.get("user:123")
     *                        .map(User::getName)
     *                        .orElse("Unknown");
     * }</pre>
     *
     * @param k the cache key to look up (must not be null)
     * @return an Optional containing the cached value if present and not expired, or empty otherwise
     * @throws NullPointerException if the key is null (behavior may vary by implementation)
     */
    @Override
    public Optional<V> get(final K k) {
        return Optional.ofNullable(gett(k));
    }

    /**
     * Stores a key-value pair using default expiration settings.
     * The default TTL and idle time specified in the constructor are used.
     * This method delegates to {@link #put(Object, Object, long, long)} with
     * defaultLiveTime and defaultMaxIdleTime. If the key already exists, its
     * value and expiration settings will be replaced.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(3600000, 1800000);
     * User user = new User("John", "john@example.com");
     * boolean success = cache.put("user:123", user);
     * System.out.println("Cached: " + success);
     *
     * // Replace existing entry
     * User updatedUser = new User("John", "john.doe@example.com");
     * cache.put("user:123", updatedUser); // Overwrites previous entry
     * }</pre>
     *
     * @param key the cache key (must not be null)
     * @param value the value to cache (may be null depending on implementation)
     * @return true if the operation was successful, false otherwise
     * @throws NullPointerException if the key is null (behavior may vary by implementation)
     */
    @Override
    public boolean put(final K key, final V value) {
        return put(key, value, defaultLiveTime, defaultMaxIdleTime);
    }

    /**
     * Asynchronously retrieves a value from the cache wrapped in an Optional.
     * The operation is executed on the shared async executor thread pool, allowing
     * the calling thread to continue without blocking. This is the asynchronous
     * version of {@link #get(Object)}. The returned ContinuableFuture allows for
     * composing and chaining asynchronous operations.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(3600000, 1800000);
     *
     * // Non-blocking retrieval
     * cache.asyncGet("user:123")
     *      .thenAccept(opt -> opt.ifPresent(u -> System.out.println("Found: " + u.getName())));
     *
     * // Chain with other async operations
     * cache.asyncGet("user:123")
     *      .thenApply(opt -> opt.orElse(new User("guest")))
     *      .thenAccept(user -> processUser(user));
     *
     * // Combine multiple async operations
     * ContinuableFuture<Optional<User>> userFuture = cache.asyncGet("user:123");
     * ContinuableFuture<Optional<Order>> orderFuture = cache.asyncGet("order:456");
     * userFuture.combine(orderFuture, (u, o) -> processUserAndOrder(u, o));
     * }</pre>
     *
     * @param k the cache key to look up (must not be null)
     * @return a ContinuableFuture that will complete with an Optional containing the cached value if found, or empty otherwise
     * @throws NullPointerException if the key is null (behavior may vary by implementation)
     */
    @Override
    public ContinuableFuture<Optional<V>> asyncGet(final K k) {
        return asyncExecutor.execute(() -> get(k));
    }

    /**
     * Asynchronously retrieves a value from the cache directly without wrapping in Optional.
     * The operation is executed on the shared async executor thread pool, allowing
     * the calling thread to continue without blocking. This is the asynchronous version
     * of {@link #gett(Object)}. Unlike {@link #asyncGet(Object)}, this method returns the
     * value directly (potentially null) rather than wrapped in an Optional, providing a
     * more traditional async API for scenarios where Optional overhead is not desired.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(3600000, 1800000);
     *
     * // Direct value retrieval
     * cache.asyncGett("user:123")
     *      .thenAccept(user -> {
     *          if (user != null) {
     *              processUser(user);
     *          }
     *      });
     *
     * // Chain with fallback
     * cache.asyncGett("user:123")
     *      .thenCompose(user -> user != null
     *          ? ContinuableFuture.completed(user)
     *          : loadUserFromDatabase("user:123"));
     * }</pre>
     *
     * @param k the cache key to look up (must not be null)
     * @return a ContinuableFuture that will complete with the cached value if found, or null if not found or expired
     * @throws NullPointerException if the key is null (behavior may vary by implementation)
     */
    @Override
    public ContinuableFuture<V> asyncGett(final K k) {
        return asyncExecutor.execute(() -> gett(k));
    }

    /**
     * Asynchronously stores a key-value pair using default expiration settings.
     * The operation is executed on the shared async executor thread pool, allowing
     * the calling thread to continue without blocking. The default TTL and idle time
     * specified in the constructor are used. This is the asynchronous version of
     * {@link #put(Object, Object)}, ideal for high-throughput scenarios where blocking
     * on cache operations would reduce performance.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(3600000, 1800000);
     * User user = new User("John", "john@example.com");
     *
     * cache.asyncPut("user:123", user)
     *      .thenAccept(success -> {
     *          if (success) {
     *              System.out.println("User cached successfully");
     *          }
     *      });
     *
     * // Chain multiple cache operations
     * cache.asyncPut("user:123", user)
     *      .thenCompose(success -> cache.asyncPut("user:456", anotherUser))
     *      .thenAccept(success -> System.out.println("All users cached"));
     * }</pre>
     *
     * @param k the cache key (must not be null)
     * @param v the value to cache (may be null depending on implementation)
     * @return a ContinuableFuture that will complete with true if the operation was successful, false otherwise
     * @throws NullPointerException if the key is null (behavior may vary by implementation)
     */
    @Override
    public ContinuableFuture<Boolean> asyncPut(final K k, final V v) {
        return asyncExecutor.execute(() -> put(k, v));
    }

    /**
     * Asynchronously stores a key-value pair with custom expiration settings.
     * The operation is executed on the shared async executor thread pool, allowing
     * the calling thread to continue without blocking. This is the asynchronous version
     * of {@link #put(Object, Object, long, long)}. This method allows fine-grained
     * control over entry expiration, overriding the default TTL and idle time.
     * The entry will be evicted when either the TTL expires or the idle time is exceeded.
     *
     * <p><b>Note:</b> Some cache implementations (particularly distributed caches) may not support
     * idle timeout and will only respect the liveTime parameter.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, Session> cache = new LocalCache<>(3600000, 1800000);
     * Session session = new Session("abc123");
     *
     * // Cache with 1 hour TTL and 30 minutes idle time
     * cache.asyncPut("session:abc", session, 3600000, 1800000)
     *      .thenAccept(success -> {
     *          if (success) {
     *              System.out.println("Session cached");
     *          }
     *      });
     *
     * // Temporary data with 5 second TTL
     * cache.asyncPut("temp:data", data, 5000, 0)
     *      .thenRun(() -> System.out.println("Temporary data cached"));
     * }</pre>
     *
     * @param k the cache key (must not be null)
     * @param v the value to cache (may be null depending on implementation)
     * @param liveTime the time-to-live in milliseconds from creation (use 0 or negative for no TTL expiration)
     * @param maxIdleTime the maximum idle time in milliseconds since last access (use 0 or negative for no idle timeout).
     *                    Note: Not supported by all implementations - check implementation documentation.
     * @return a ContinuableFuture that will complete with true if the operation was successful, false otherwise
     * @throws NullPointerException if the key is null (behavior may vary by implementation)
     */
    @Override
    public ContinuableFuture<Boolean> asyncPut(final K k, final V v, final long liveTime, final long maxIdleTime) {
        return asyncExecutor.execute(() -> put(k, v, liveTime, maxIdleTime));
    }

    /**
     * Asynchronously removes an entry from the cache.
     * The operation is executed on the shared async executor thread pool, allowing
     * the calling thread to continue without blocking. This is the asynchronous version
     * of {@link #remove(Object)}. This operation is idempotent - if the key doesn't exist,
     * the operation completes normally without error.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(3600000, 1800000);
     *
     * cache.asyncRemove("user:123")
     *      .thenRun(() -> System.out.println("User removed from cache"));
     *
     * // Chain with other operations
     * cache.asyncRemove("user:123")
     *      .thenRun(() -> updateDatabase("user:123"));
     *
     * // Multiple removals in sequence
     * cache.asyncRemove("user:123")
     *      .thenCompose(v -> cache.asyncRemove("user:456"))
     *      .thenRun(() -> System.out.println("Both users removed"));
     * }</pre>
     *
     * @param k the cache key to remove (must not be null)
     * @return a ContinuableFuture that completes when the removal operation finishes
     * @throws NullPointerException if the key is null (behavior may vary by implementation)
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
     * The operation is executed on the shared async executor thread pool, allowing
     * the calling thread to continue without blocking. This is the asynchronous version
     * of {@link #containsKey(Object)}. This method checks for key existence without
     * retrieving the actual value, making it more efficient than retrieving and checking
     * for null when you only need to know if a key exists.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(3600000, 1800000);
     *
     * cache.asyncContainsKey("user:123")
     *      .thenAccept(exists -> {
     *          if (exists) {
     *              System.out.println("User is in cache");
     *          } else {
     *              loadUserFromDatabase("user:123");
     *          }
     *      });
     *
     * // Use as condition for further processing
     * cache.asyncContainsKey("config:settings")
     *      .thenCompose(exists -> exists
     *          ? cache.asyncGett("config:settings")
     *          : loadConfigFromFile())
     *      .thenAccept(config -> applyConfiguration(config));
     * }</pre>
     *
     * @param k the cache key to check (must not be null)
     * @return a ContinuableFuture that will complete with true if the key exists and has not expired, false otherwise
     * @throws NullPointerException if the key is null (behavior may vary by implementation)
     */
    @Override
    public ContinuableFuture<Boolean> asyncContainsKey(final K k) {
        return asyncExecutor.execute(() -> containsKey(k));
    }

    /**
     * Returns the properties bag for this cache instance.
     * Properties can be used to store custom configuration, metadata, or application-specific
     * data associated with this cache. The returned Properties object is mutable and changes
     * are reflected in the cache instance. This provides a flexible way to attach arbitrary
     * metadata to cache instances without modifying the cache structure.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(3600000, 1800000);
     * Properties<String, Object> props = cache.getProperties();
     * props.put("description", "User cache for web application");
     * props.put("maxEntries", 10000);
     * props.put("region", "us-west-2");
     *
     * // Later retrieve properties
     * String description = cache.getProperties().get("description");
     * }</pre>
     *
     * @return the properties container for this cache, never null
     */
    @Override
    public Properties<String, Object> getProperties() {
        return properties;
    }

    /**
     * Retrieves a property value by name.
     * This is a convenience method equivalent to calling {@code getProperties().get(propName)}.
     * Returns null if the property doesn't exist. The returned value is cast to the expected
     * type, so care must be taken to ensure the correct type is used to avoid ClassCastException.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(3600000, 1800000);
     * cache.setProperty("description", "User cache");
     * cache.setProperty("maxRetries", 3);
     *
     * String description = cache.getProperty("description");
     * Integer maxRetries = cache.getProperty("maxRetries");
     * System.out.println(description + ", retries: " + maxRetries);
     *
     * // Returns null for non-existent properties
     * String unknown = cache.getProperty("nonExistent"); // null
     * }</pre>
     *
     * @param <T> the type of the property value to be returned (caller should ensure correct type)
     * @param propName the property name to look up (must not be null)
     * @return the property value cast to type T, or null if not found
     * @throws ClassCastException if the property value cannot be cast to type T
     */
    @Override
    public <T> T getProperty(final String propName) {
        return (T) properties.get(propName);
    }

    /**
     * Sets a property value.
     * This is a convenience method equivalent to calling {@code getProperties().put(propName, propValue)}.
     * Properties can be used for custom configuration, metadata, or application-specific data.
     * If a property with the same name already exists, its value will be replaced and the old
     * value is returned.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(3600000, 1800000);
     * cache.setProperty("description", "User cache for session data");
     * cache.setProperty("maxRetries", 3);
     * cache.setProperty("enableMetrics", true);
     * cache.setProperty("createdAt", System.currentTimeMillis());
     *
     * // Update existing property and get the old value
     * String oldDesc = cache.setProperty("description", "Updated user cache");
     * System.out.println("Previous description: " + oldDesc);
     * }</pre>
     *
     * @param <T> the type of the previous property value to be returned (caller should ensure correct type)
     * @param propName the property name to set (must not be null)
     * @param propValue the property value to set (can be any object type, null allowed)
     * @return the previous value associated with the property, or null if there was no previous value
     * @throws ClassCastException if the previous property value cannot be cast to type T
     */
    @Override
    public <T> T setProperty(final String propName, final Object propValue) {
        return (T) properties.put(propName, propValue);
    }

    /**
     * Removes a property from the cache.
     * This is a convenience method equivalent to calling {@code getProperties().remove(propName)}.
     * This operation is idempotent - it succeeds whether the property exists or not.
     * If the property doesn't exist, this method returns null without error.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(3600000, 1800000);
     * cache.setProperty("tempFlag", true);
     *
     * // Later, remove the property
     * Boolean oldValue = cache.removeProperty("tempFlag");
     * if (oldValue != null) {
     *     System.out.println("Removed property with value: " + oldValue);
     * }
     *
     * // Removing non-existent property returns null
     * String notFound = cache.removeProperty("nonExistent"); // null
     * }</pre>
     *
     * @param <T> the type of the property value to be returned (caller should ensure correct type)
     * @param propName the property name to remove (must not be null)
     * @return the removed value cast to type T, or null if the property didn't exist
     * @throws ClassCastException if the property value cannot be cast to type T
     */
    @Override
    public <T> T removeProperty(final String propName) {
        return (T) properties.remove(propName);
    }
}