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
import com.landawn.abacus.util.*;

/**
 * Factory class for creating various types of cache implementations.
 * This factory provides convenient methods to create local and distributed caches
 * with different configurations. It supports both programmatic creation and
 * string-based configuration for dynamic cache instantiation.
 * 
 * <br><br>
 * Supported cache types:
 * <ul>
 * <li>LocalCache - In-memory cache with eviction support</li>
 * <li>DistributedCache - Wrapper for distributed cache clients</li>
 * <li>Memcached - Via SpyMemcached client</li>
 * <li>Redis - Via JRedis client</li>
 * <li>Custom implementations via class name</li>
 * </ul>
 * 
 * <br>
 * Example usage:
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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * cache.put("user:123", user);
     * }</pre>
     *
     * @param <K> the type of keys maintained by the cache
     * @param <V> the type of cached values
     * @param capacity the maximum number of entries the cache can hold
     * @param evictDelay the delay in milliseconds between eviction runs (0 to disable periodic eviction)
     * @return the new LocalCache instance
     */
    public static <K, V> LocalCache<K, V> createLocalCache(final int capacity, final long evictDelay) {
        return new LocalCache<>(capacity, evictDelay);
    }

    /**
     * Creates a new LocalCache with fully customized parameters.
     * This method provides complete control over cache capacity, eviction timing, and expiration behavior.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * LocalCache<String, Session> cache = CacheFactory.createLocalCache(
     *     500, 30000, 1800000, 900000); // 500 capacity, 30s evict, 30min TTL, 15min idle
     * }</pre>
     *
     * @param <K> the type of keys maintained by the cache
     * @param <V> the type of cached values
     * @param capacity the maximum number of entries the cache can hold
     * @param evictDelay the delay in milliseconds between eviction runs (0 to disable periodic eviction)
     * @param defaultLiveTime the default time-to-live in milliseconds for entries added without explicit TTL
     * @param defaultMaxIdleTime the default maximum idle time in milliseconds for entries added without explicit idle time
     * @return the new LocalCache instance
     */
    public static <K, V> LocalCache<K, V> createLocalCache(final int capacity, final long evictDelay, final long defaultLiveTime,
            final long defaultMaxIdleTime) {
        return new LocalCache<>(capacity, evictDelay, defaultLiveTime, defaultMaxIdleTime);
    }

    /**
     * Creates a new LocalCache with a custom KeyedObjectPool.
     * This method is for advanced use cases requiring custom pool implementations.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * KeyedObjectPool<String, PoolableWrapper<User>> customPool = PoolFactory.createKeyedObjectPool(...);
     * LocalCache<String, User> cache = CacheFactory.createLocalCache(3600000, 1800000, customPool);
     * }</pre>
     *
     * @param <K> the type of keys maintained by the cache
     * @param <V> the type of cached values
     * @param defaultLiveTime the default time-to-live in milliseconds
     * @param defaultMaxIdleTime the default maximum idle time in milliseconds
     * @param pool the pre-configured KeyedObjectPool to use
     * @return the new LocalCache instance
     */
    public static <K, V> LocalCache<K, V> createLocalCache(final long defaultLiveTime, final long defaultMaxIdleTime,
            final KeyedObjectPool<K, PoolableWrapper<V>> pool) {
        return new LocalCache<>(defaultLiveTime, defaultMaxIdleTime, pool);
    }

    /**
     * Creates a DistributedCache wrapper for a distributed cache client.
     * The wrapper provides a Cache interface implementation around the distributed cache client,
     * adding error handling and retry logic for resilience.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SpyMemcached<User> memcachedClient = new SpyMemcached<>("localhost:11211", 5000);
     * DistributedCache<String, User> cache = CacheFactory.createDistributedCache(memcachedClient);
     * }</pre>
     *
     * @param <K> the type of keys maintained by the cache
     * @param <V> the type of cached values
     * @param dcc the distributed cache client to wrap (must not be null)
     * @return the new DistributedCache instance
     * @throws IllegalArgumentException if dcc is null
     */
    public static <K, V> DistributedCache<K, V> createDistributedCache(final DistributedCacheClient<V> dcc) {
        return new DistributedCache<>(dcc);
    }

    /**
     * Creates a DistributedCache with a key prefix for namespace isolation.
     * All cache keys will be automatically prefixed with the specified string,
     * allowing multiple applications or modules to share the same cache server
     * without key collisions.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * JRedis<Session> redisClient = new JRedis<>("localhost:6379", 3000);
     * DistributedCache<String, Session> cache = CacheFactory.createDistributedCache(redisClient, "myapp:sessions:");
     * cache.put("user123", session); // Actually stored as "myapp:sessions:user123"
     * }</pre>
     *
     * @param <K> the type of keys maintained by the cache
     * @param <V> the type of cached values
     * @param dcc the distributed cache client to wrap (must not be null)
     * @param keyPrefix the key prefix to prepend to all keys (can be empty string for no prefix, must not be null)
     * @return the new DistributedCache instance
     * @throws IllegalArgumentException if dcc or keyPrefix is null
     */
    public static <K, V> DistributedCache<K, V> createDistributedCache(final DistributedCacheClient<V> dcc, final String keyPrefix) {
        return new DistributedCache<>(dcc, keyPrefix);
    }

    /**
     * Creates a DistributedCache with custom retry configuration.
     * This method allows fine-tuning of error handling and retry behavior for distributed
     * cache operations, which is useful for handling transient network failures or service disruptions.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * JRedis<User> redisClient = new JRedis<>("localhost:6379", 3000);
     * DistributedCache<String, User> cache = CacheFactory.createDistributedCache(
     *     redisClient, "app:", 3, 1000); // 3 max failures, 1s retry delay
     * }</pre>
     *
     * @param <K> the type of keys maintained by the cache
     * @param <V> the type of cached values
     * @param dcc the distributed cache client to wrap (must not be null)
     * @param keyPrefix the key prefix to prepend to all keys (can be empty string for no prefix, must not be null)
     * @param maxFailedNumForRetry the maximum number of consecutive failures before giving up on retries (must be non-negative)
     * @param retryDelay the delay in milliseconds between retry attempts (must be non-negative)
     * @return the new DistributedCache instance
     * @throws IllegalArgumentException if dcc or keyPrefix is null, or if maxFailedNumForRetry or retryDelay is negative
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
     * <br><br>
     * Supported formats:
     * <ul>
     * <li>Memcached(serverUrl) - Creates SpyMemcached client with default timeout</li>
     * <li>Memcached(serverUrl,keyPrefix) - With key prefix</li>
     * <li>Memcached(serverUrl,keyPrefix,timeout) - With key prefix and custom timeout in milliseconds</li>
     * <li>Redis(serverUrl) - Creates JRedis client with default timeout</li>
     * <li>Redis(serverUrl,keyPrefix) - With key prefix</li>
     * <li>Redis(serverUrl,keyPrefix,timeout) - With key prefix and custom timeout in milliseconds</li>
     * <li>com.example.CustomCache(params...) - Custom implementation with fully qualified class name</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Memcached with single server
     * Cache<String, User> cache1 = CacheFactory.createCache("Memcached(localhost:11211)");
     *
     * // Redis with key prefix and timeout
     * Cache<String, Session> cache2 = CacheFactory.createCache("Redis(localhost:6379,app:cache:,5000)");
     *
     * // Multiple Memcached servers (in serverUrl format)
     * Cache<String, Object> cache3 = CacheFactory.createCache("Memcached(host1:11211 host2:11211)");
     * }</pre>
     *
     * @param <K> the type of keys maintained by the cache
     * @param <V> the type of cached values
     * @param provider the cache provider specification string
     * @return the new Cache instance
     * @throws IllegalArgumentException if the provider specification is invalid or has incorrect parameters
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