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
     * @param defaultLiveTime default TTL in milliseconds for new entries
     * @param defaultMaxIdleTime default max idle time in milliseconds for new entries
     */
    protected AbstractCache(final long defaultLiveTime, final long defaultMaxIdleTime) {
        this.defaultLiveTime = defaultLiveTime;
        this.defaultMaxIdleTime = defaultMaxIdleTime;
    }

    /**
     * Retrieves a value from the cache wrapped in an Optional.
     * This method provides a null-safe alternative to gett().
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(1000, 60000);
     * Optional<User> user = cache.get("user:123");
     * user.ifPresent(u -> System.out.println("Found: " + u.getName()));
     * }</pre>
     *
     * @param k the cache key
     * @return an Optional containing the cached value if present, or empty if not found
     */
    @Override
    public Optional<V> get(final K k) {
        return Optional.ofNullable(gett(k));
    }

    /**
     * Stores a key-value pair using default expiration settings.
     * The default TTL and idle time specified in the constructor are used.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(1000, 60000);
     * boolean success = cache.put("user:123", user);
     * System.out.println("Cached: " + success);
     * }</pre>
     *
     * @param key the cache key
     * @param value the value to cache
     * @return true if the operation was successful, false otherwise
     */
    @Override
    public boolean put(final K key, final V value) {
        return put(key, value, defaultLiveTime, defaultMaxIdleTime);
    }

    /**
     * Asynchronously retrieves a value from the cache.
     * The operation is executed on the shared thread pool.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.asyncGet("user:123")
     *      .thenAccept(opt -> opt.ifPresent(u -> System.out.println("Found: " + u)));
     * }</pre>
     *
     * @param k the cache key
     * @return a ContinuableFuture that will contain the Optional result
     */
    @Override
    public ContinuableFuture<Optional<V>> asyncGet(final K k) {
        return asyncExecutor.execute(() -> get(k));
    }

    /**
     * Asynchronously retrieves a value from the cache directly.
     * The operation is executed on the shared thread pool.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.asyncGett("user:123")
     *      .thenAccept(user -> { if (user != null) process(user); });
     * }</pre>
     *
     * @param k the cache key
     * @return a ContinuableFuture that will contain the cached value, or null if not found
     */
    @Override
    public ContinuableFuture<V> asyncGett(final K k) {
        return asyncExecutor.execute(() -> gett(k));
    }

    /**
     * Asynchronously stores a key-value pair using default expiration.
     * The operation is executed on the shared thread pool.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.asyncPut("user:123", user)
     *      .thenAccept(success -> System.out.println("Cached: " + success));
     * }</pre>
     *
     * @param k the cache key
     * @param v the value to cache
     * @return a ContinuableFuture that will contain true if the operation was successful, false otherwise
     */
    @Override
    public ContinuableFuture<Boolean> asyncPut(final K k, final V v) {
        return asyncExecutor.execute(() -> put(k, v));
    }

    /**
     * Asynchronously stores a key-value pair with custom expiration.
     * The operation is executed on the shared thread pool.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.asyncPut("session:abc", session, 3600000, 1800000)
     *      .thenAccept(success -> System.out.println("Session cached"));
     * }</pre>
     *
     * @param k the cache key
     * @param v the value to cache
     * @param liveTime the time-to-live in milliseconds (0 for no expiration)
     * @param maxIdleTime the maximum idle time in milliseconds (0 for no idle timeout)
     * @return a ContinuableFuture that will contain true if the operation was successful, false otherwise
     */
    @Override
    public ContinuableFuture<Boolean> asyncPut(final K k, final V v, final long liveTime, final long maxIdleTime) {
        return asyncExecutor.execute(() -> put(k, v, liveTime, maxIdleTime));
    }

    /**
     * Asynchronously removes an entry from the cache.
     * The operation is executed on the shared thread pool.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.asyncRemove("user:123")
     *      .thenRun(() -> System.out.println("User removed from cache"));
     * }</pre>
     *
     * @param k the cache key
     * @return a ContinuableFuture that completes when the operation finishes
     */
    @Override
    public ContinuableFuture<Void> asyncRemove(final K k) {
        return asyncExecutor.execute(() -> {
            remove(k);

            return null;
        });
    }

    /**
     * Asynchronously checks if the cache contains a key.
     * The operation is executed on the shared thread pool.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.asyncContainsKey("user:123")
     *      .thenAccept(exists -> System.out.println("Exists: " + exists));
     * }</pre>
     *
     * @param k the cache key
     * @return a ContinuableFuture that will contain true if the key exists in the cache, false otherwise
     */
    @Override
    public ContinuableFuture<Boolean> asyncContainsKey(final K k) {
        return asyncExecutor.execute(() -> containsKey(k));
    }

    /**
     * Returns the properties container for this cache.
     * Properties can be used to store custom configuration or metadata
     * that needs to be associated with the cache instance.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(1000, 60000);
     * Properties<String, Object> props = cache.getProperties();
     * props.put("description", "User cache");
     * }</pre>
     *
     * @return the properties container
     */
    @Override
    public Properties<String, Object> getProperties() {
        return properties;
    }

    /**
     * Retrieves a property value by name.
     * Returns null if the property doesn't exist.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(1000, 60000);
     * String description = cache.getProperty("description");
     * Integer maxRetries = cache.getProperty("maxRetries");
     * }</pre>
     *
     * @param <T> the type of the property value to be returned
     * @param propName the property name
     * @return the property value, or null if not found
     */
    @Override
    public <T> T getProperty(final String propName) {
        return (T) properties.get(propName);
    }

    /**
     * Sets a property value.
     * Properties can be used for custom configuration or to store
     * metadata about the cache or its usage.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(1000, 60000);
     * cache.setProperty("description", "User cache for session data");
     * cache.setProperty("maxRetries", 3);
     * }</pre>
     *
     * @param <T> the type of the previous property value to be returned
     * @param propName the property name
     * @param propValue the property value
     * @return the previous value, or null if none existed
     */
    @Override
    public <T> T setProperty(final String propName, final Object propValue) {
        return (T) properties.put(propName, propValue);
    }

    /**
     * Removes a property from the cache.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = new LocalCache<>(1000, 60000);
     * String oldValue = cache.removeProperty("description");
     * System.out.println("Removed property: " + oldValue);
     * }</pre>
     *
     * @param <T> the type of the property value to be returned
     * @param propName the property name to remove
     * @return the removed value, or null if the property didn't exist
     */
    @Override
    public <T> T removeProperty(final String propName) {
        return (T) properties.remove(propName);
    }
}