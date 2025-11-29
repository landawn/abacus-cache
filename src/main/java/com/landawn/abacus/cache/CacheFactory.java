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

import static com.landawn.abacus.cache.DistributedCacheClient.DEFAULT_TIMEOUT;

import com.landawn.abacus.pool.KeyedObjectPool;
import com.landawn.abacus.pool.PoolableWrapper;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Numbers;
import com.landawn.abacus.util.Strings;
import com.landawn.abacus.util.TypeAttrParser;

/**
 * Factory class for creating various types of cache implementations.
 * This factory provides convenient methods to create local and distributed caches
 * with different configurations. It supports both programmatic creation and
 * string-based configuration for dynamic cache instantiation.
 *
 * <p>Supported cache types:
 * <ul>
 * <li>LocalCache - In-memory cache with eviction support</li>
 * <li>DistributedCache - Wrapper for distributed cache clients</li>
 * <li>Memcached - Via SpyMemcached client</li>
 * <li>Redis - Via JRedis client</li>
 * <li>Custom implementations via class name</li>
 * </ul>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Create local cache
 * LocalCache<String, User> localCache = CacheFactory.createLocalCache(
 *     1000,     // capacity
 *     60000,    // evict delay (1 minute)
 *     3600000,  // default TTL (1 hour)
 *     1800000   // default idle time (30 minutes)
 * );
 *
 * // Create distributed cache with Memcached
 * Cache<String, User> memcached = CacheFactory.createCache(
 *     "Memcached(localhost:11211,localhost:11212)"
 * );
 *
 * // Create distributed cache with Redis and key prefix
 * Cache<String, User> redis = CacheFactory.createCache(
 *     "Redis(localhost:6379,myapp:cache:,5000)"
 * );
 * }</pre>
 *
 * @see LocalCache
 * @see DistributedCache
 * @see SpyMemcached
 * @see JRedis
 */
public final class CacheFactory {

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private CacheFactory() {
    }

    /**
     * Creates a new LocalCache with specified capacity and eviction delay.
     * Uses default time-to-live of 3 hours and default idle time of 30 minutes
     * as defined in Cache.DEFAULT_LIVE_TIME and Cache.DEFAULT_MAX_IDLE_TIME.
     *
     * <p>The eviction delay determines how frequently the cache checks for and removes
     * expired entries. A value of 0 disables periodic eviction, but entries will still
     * be evicted on access if expired.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create cache with 1000 entries capacity, checking for expired entries every minute
     * LocalCache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * cache.put("user:123", user);
     * User retrieved = cache.gett("user:123");
     * }</pre>
     *
     * @param <K> the type of keys maintained by the cache
     * @param <V> the type of cached values
     * @param capacity the maximum number of entries the cache can hold (must be positive)
     * @param evictDelay the delay in milliseconds between eviction runs (0 to disable periodic eviction, must be non-negative)
     * @return a new LocalCache instance with the specified configuration
     * @throws IllegalArgumentException if capacity is not positive or evictDelay is negative
     * @see #createLocalCache(int, long, long, long)
     * @see #createLocalCache(long, long, KeyedObjectPool)
     */
    public static <K, V> LocalCache<K, V> createLocalCache(final int capacity, final long evictDelay) {
        return new LocalCache<>(capacity, evictDelay);
    }

    /**
     * Creates a new LocalCache with fully customized parameters.
     * This method provides complete control over cache capacity, eviction timing, and expiration behavior.
     *
     * <p>The defaultLiveTime (TTL) determines how long an entry remains in the cache from the time
     * it was added, regardless of access. The defaultMaxIdleTime determines how long an entry can
     * remain in the cache without being accessed before it expires.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create session cache: 500 capacity, check every 30s, expire after 30min or 15min idle
     * LocalCache<String, Session> cache = CacheFactory.createLocalCache(
     *     500,       // capacity: 500 entries max
     *     30000,     // evictDelay: check every 30 seconds
     *     1800000,   // defaultLiveTime: 30 minutes TTL
     *     900000     // defaultMaxIdleTime: 15 minutes idle timeout
     * );
     * cache.put("session:abc123", session);  // Uses default TTL and idle time
     * }</pre>
     *
     * @param <K> the type of keys maintained by the cache
     * @param <V> the type of cached values
     * @param capacity the maximum number of entries the cache can hold (must be positive)
     * @param evictDelay the delay in milliseconds between eviction runs (0 to disable periodic eviction, must be non-negative)
     * @param defaultLiveTime the default time-to-live in milliseconds for entries added without explicit TTL (0 for no expiration)
     * @param defaultMaxIdleTime the default maximum idle time in milliseconds for entries added without explicit idle time (0 for no idle timeout)
     * @return a new LocalCache instance with the specified configuration
     * @throws IllegalArgumentException if capacity is not positive or evictDelay is negative
     * @see #createLocalCache(int, long)
     * @see #createLocalCache(long, long, KeyedObjectPool)
     */
    public static <K, V> LocalCache<K, V> createLocalCache(final int capacity, final long evictDelay, final long defaultLiveTime,
            final long defaultMaxIdleTime) {
        return new LocalCache<>(capacity, evictDelay, defaultLiveTime, defaultMaxIdleTime);
    }

    /**
     * Creates a new LocalCache with a custom KeyedObjectPool.
     * This method is for advanced use cases requiring custom pool implementations for
     * fine-grained control over cache entry management, pooling strategies, or integration
     * with existing pool infrastructure.
     *
     * <p>The provided pool must be configured to handle PoolableWrapper objects and will
     * be used directly by the cache for all entry storage and retrieval operations.
     * The pool's capacity and eviction settings will override any defaults.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create custom pool with specific configuration
     * KeyedObjectPool<String, PoolableWrapper<User>> customPool =
     *     PoolFactory.createKeyedObjectPool(1000, 60000);
     *
     * // Create cache using the custom pool
     * LocalCache<String, User> cache = CacheFactory.createLocalCache(
     *     3600000,   // defaultLiveTime: 1 hour
     *     1800000,   // defaultMaxIdleTime: 30 minutes
     *     customPool // custom pool implementation
     * );
     * }</pre>
     *
     * @param <K> the type of keys maintained by the cache
     * @param <V> the type of cached values
     * @param defaultLiveTime the default time-to-live in milliseconds for entries added without explicit TTL (0 for no expiration)
     * @param defaultMaxIdleTime the default maximum idle time in milliseconds for entries added without explicit idle time (0 for no idle timeout)
     * @param pool the pre-configured KeyedObjectPool to use for managing cache entries (must not be null)
     * @return a new LocalCache instance configured with the specified pool
     * @throws IllegalArgumentException if pool is null
     * @see #createLocalCache(int, long)
     * @see #createLocalCache(int, long, long, long)
     */
    public static <K, V> LocalCache<K, V> createLocalCache(final long defaultLiveTime, final long defaultMaxIdleTime,
            final KeyedObjectPool<K, PoolableWrapper<V>> pool) {
        return new LocalCache<>(defaultLiveTime, defaultMaxIdleTime, pool);
    }

    /**
     * Creates a DistributedCache wrapper for a distributed cache client.
     * The wrapper provides a Cache interface implementation around the distributed cache client,
     * adding error handling and retry logic for resilience against transient failures.
     *
     * <p>This is the simplest way to create a distributed cache, using default settings:
     * <ul>
     * <li>No key prefix (keys used as-is)</li>
     * <li>Default retry configuration: max 100 consecutive failures, 1000ms retry delay</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create Memcached client and wrap it
     * SpyMemcached<User> memcachedClient = new SpyMemcached<>("localhost:11211", 5000);
     * DistributedCache<String, User> cache = CacheFactory.createDistributedCache(memcachedClient);
     *
     * // Use the cache
     * cache.put("user:123", user);
     * User retrieved = cache.get("user:123");
     * }</pre>
     *
     * @param <K> the type of keys maintained by the cache
     * @param <V> the type of cached values
     * @param dcc the distributed cache client to wrap (must not be null)
     * @return a new DistributedCache instance wrapping the provided client
     * @throws IllegalArgumentException if dcc is null
     * @see #createDistributedCache(DistributedCacheClient, String)
     * @see #createDistributedCache(DistributedCacheClient, String, int, long)
     * @see #createCache(String)
     */
    public static <K, V> DistributedCache<K, V> createDistributedCache(final DistributedCacheClient<V> dcc) {
        return new DistributedCache<>(dcc);
    }

    /**
     * Creates a DistributedCache with a key prefix for namespace isolation.
     * All cache keys will be automatically prefixed with the specified string,
     * allowing multiple applications or modules to share the same cache server
     * without key collisions. Uses default retry configuration: max 100 consecutive failures, 1000ms retry delay.
     *
     * <p>Key prefixing is useful for:
     * <ul>
     * <li>Multi-tenant applications sharing a cache server</li>
     * <li>Different environments (dev, staging, prod) using the same cache infrastructure</li>
     * <li>Logical separation of different cache regions within an application</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create Redis client
     * JRedis<Session> redisClient = new JRedis<>("localhost:6379", 3000);
     *
     * // Create cache with namespace prefix
     * DistributedCache<String, Session> cache =
     *     CacheFactory.createDistributedCache(redisClient, "myapp:sessions:");
     *
     * // Keys are automatically prefixed
     * cache.put("user123", session);      // Stored as "myapp:sessions:user123"
     * Session s = cache.get("user123");   // Retrieves "myapp:sessions:user123"
     * }</pre>
     *
     * @param <K> the type of keys maintained by the cache
     * @param <V> the type of cached values
     * @param dcc the distributed cache client to wrap (must not be null)
     * @param keyPrefix the key prefix to prepend to all keys (can be empty string or null for no prefix)
     * @return a new DistributedCache instance with key prefixing enabled
     * @throws IllegalArgumentException if dcc is null
     * @see #createDistributedCache(DistributedCacheClient)
     * @see #createDistributedCache(DistributedCacheClient, String, int, long)
     * @see #createCache(String)
     */
    public static <K, V> DistributedCache<K, V> createDistributedCache(final DistributedCacheClient<V> dcc, final String keyPrefix) {
        return new DistributedCache<>(dcc, keyPrefix);
    }

    /**
     * Creates a DistributedCache with custom retry configuration.
     * This method allows fine-tuning of error handling and retry behavior for distributed
     * cache operations, which is useful for handling transient network failures or service disruptions.
     *
     * <p>The retry mechanism works as follows:
     * <ul>
     * <li>After each failed operation, the cache waits for {@code retryDelay} milliseconds before retrying</li>
     * <li>If consecutive failures reach {@code maxFailedNumForRetry}, the cache stops retrying</li>
     * <li>Successful operations reset the failure counter</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create Redis client
     * JRedis<User> redisClient = new JRedis<>("localhost:6379", 3000);
     *
     * // Create cache with custom retry configuration
     * DistributedCache<String, User> cache = CacheFactory.createDistributedCache(
     *     redisClient,
     *     "app:",    // Key prefix for namespace isolation
     *     3,         // Retry up to 3 times on consecutive failures
     *     1000       // Wait 1 second between retry attempts
     * );
     *
     * // Cache will retry automatically on transient failures
     * cache.put("user:123", user);
     * }</pre>
     *
     * @param <K> the type of keys maintained by the cache
     * @param <V> the type of cached values
     * @param dcc the distributed cache client to wrap (must not be null)
     * @param keyPrefix the key prefix to prepend to all keys (can be empty string or null for no prefix)
     * @param maxFailedNumForRetry the maximum number of consecutive failures before giving up on retries (must be positive)
     * @param retryDelay the delay in milliseconds between retry attempts after failure threshold is reached (must be non-negative)
     * @return a new DistributedCache instance with custom retry configuration
     * @throws IllegalArgumentException if dcc is null
     * @see #createDistributedCache(DistributedCacheClient)
     * @see #createDistributedCache(DistributedCacheClient, String)
     * @see #createCache(String)
     */
    public static <K, V> DistributedCache<K, V> createDistributedCache(final DistributedCacheClient<V> dcc, final String keyPrefix,
            final int maxFailedNumForRetry, final long retryDelay) {
        return new DistributedCache<>(dcc, keyPrefix, maxFailedNumForRetry, retryDelay);
    }

    /**
     * Creates a cache instance from a string specification.
     * This method supports dynamic cache creation based on configuration strings,
     * making it ideal for configuration-driven cache setup. The method parses the
     * provider string and instantiates the appropriate cache implementation.
     *
     * <p>This method is particularly useful for:
     * <ul>
     * <li>Loading cache configuration from properties files or environment variables</li>
     * <li>Runtime cache selection based on deployment environment</li>
     * <li>Configuring cache settings without code changes</li>
     * </ul>
     *
     * <p><b>Supported formats:</b>
     * <ul>
     * <li>{@code Memcached(serverUrl)} - Creates SpyMemcached client with default timeout (1000ms)</li>
     * <li>{@code Memcached(serverUrl,keyPrefix)} - With key prefix for namespace isolation and default timeout</li>
     * <li>{@code Memcached(serverUrl,keyPrefix,timeout)} - With key prefix and custom timeout in milliseconds</li>
     * <li>{@code Redis(serverUrl)} - Creates JRedis client with default timeout (1000ms)</li>
     * <li>{@code Redis(serverUrl,keyPrefix)} - With key prefix for namespace isolation and default timeout</li>
     * <li>{@code Redis(serverUrl,keyPrefix,timeout)} - With key prefix and custom timeout in milliseconds</li>
     * <li>{@code com.example.CustomCache(params...)} - Custom implementation with fully qualified class name</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Memcached with single server and default timeout
     * Cache<String, User> cache1 = CacheFactory.createCache("Memcached(localhost:11211)");
     *
     * // Redis with key prefix and custom 5-second timeout
     * Cache<String, Session> cache2 = CacheFactory.createCache(
     *     "Redis(localhost:6379,app:cache:,5000)"
     * );
     *
     * // Multiple Memcached servers (space-separated in serverUrl)
     * Cache<String, Object> cache3 = CacheFactory.createCache(
     *     "Memcached(host1:11211 host2:11211,myprefix:,3000)"
     * );
     *
     * // Load from configuration
     * String cacheConfig = System.getProperty("cache.provider");
     * Cache<String, Data> cache4 = CacheFactory.createCache(cacheConfig);
     *
     * // Custom cache implementation
     * Cache<String, Object> cache5 = CacheFactory.createCache(
     *     "com.mycompany.CustomCache(param1,param2)"
     * );
     * }</pre>
     *
     * @param <K> the type of keys maintained by the cache
     * @param <V> the type of cached values
     * @param provider the cache provider specification string in format "ClassName(param1,param2,...)" (must not be null or empty)
     * @return a new Cache instance configured according to the specification
     * @throws IllegalArgumentException if provider is null or empty, missing required parameters, has unsupported number of parameters
     *         (more than 3 for Memcached/Redis), or contains invalid timeout value (non-numeric)
     * @throws RuntimeException if custom class cannot be instantiated (class not found, no suitable constructor, instantiation fails, etc.)
     * @see #createDistributedCache(DistributedCacheClient)
     * @see #createDistributedCache(DistributedCacheClient, String)
     * @see #createLocalCache(int, long)
     */
    @SuppressWarnings("unchecked")
    public static <K, V> Cache<K, V> createCache(final String provider) {
        final TypeAttrParser attrResult = TypeAttrParser.parse(provider);

        final String[] parameters = attrResult.getParameters();

        if (N.isEmpty(parameters)) {
            throw new IllegalArgumentException("Invalid provider specification: missing parameters");
        }

        final String url = parameters[0];
        final String className = attrResult.getClassName();
        Class<?> cls = null;

        if (DistributedCacheClient.MEMCACHED.equalsIgnoreCase(className)) {
            if (parameters.length == 1) {
                return new DistributedCache<>(new SpyMemcached<>(url, DEFAULT_TIMEOUT));
            } else if (parameters.length == 2) {
                return new DistributedCache<>(new SpyMemcached<>(url, DEFAULT_TIMEOUT), parameters[1]);
            } else if (parameters.length == 3) {
                try {
                    return new DistributedCache<>(new SpyMemcached<>(url, Numbers.toLong(parameters[2])), parameters[1]);
                } catch (final NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid timeout parameter: " + parameters[2], e);
                }
            } else {
                throw new IllegalArgumentException("Unsupported parameters: " + Strings.join(parameters));
            }
        } else if (DistributedCacheClient.REDIS.equalsIgnoreCase(className)) {
            if (parameters.length == 1) {
                return new DistributedCache<>(new JRedis<>(url, DEFAULT_TIMEOUT));
            } else if (parameters.length == 2) {
                return new DistributedCache<>(new JRedis<>(url, DEFAULT_TIMEOUT), parameters[1]);
            } else if (parameters.length == 3) {
                try {
                    return new DistributedCache<>(new JRedis<>(url, Numbers.toLong(parameters[2])), parameters[1]);
                } catch (final NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid timeout parameter: " + parameters[2], e);
                }
            } else {
                throw new IllegalArgumentException("Unsupported parameters: " + Strings.join(parameters));
            }
        } else {
            cls = ClassUtil.forClass(className);

            return TypeAttrParser.newInstance(cls, provider);
        }
    }
}
