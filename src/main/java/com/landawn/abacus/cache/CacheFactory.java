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
     * Uses default TTL of 3 hours and default idle time of 30 minutes.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param capacity the maximum number of entries the cache can hold
     * @param evictDelay the delay in milliseconds between eviction runs
     * @return a new LocalCache instance
     */
    public static <K, V> LocalCache<K, V> createLocalCache(final int capacity, final long evictDelay) {
        return new LocalCache<>(capacity, evictDelay);
    }

    /**
     * Creates a new LocalCache with fully customized parameters.
     * This method provides complete control over cache behavior.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param capacity the maximum number of entries the cache can hold
     * @param evictDelay the delay in milliseconds between eviction runs
     * @param defaultLiveTime default time-to-live in milliseconds (default: 3 hours)
     * @param defaultMaxIdleTime default maximum idle time in milliseconds (default: 30 minutes)
     * @return a new LocalCache instance
     */
    public static <K, V> LocalCache<K, V> createLocalCache(final int capacity, final long evictDelay, final long defaultLiveTime,
            final long defaultMaxIdleTime) {
        return new LocalCache<>(capacity, evictDelay, defaultLiveTime, defaultMaxIdleTime);
    }

    /**
     * Creates a new LocalCache with a custom KeyedObjectPool.
     * This method is for advanced use cases requiring custom pool implementations.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param defaultLiveTime default time-to-live in milliseconds
     * @param defaultMaxIdleTime default maximum idle time in milliseconds
     * @param pool the pre-configured KeyedObjectPool to use
     * @return a new LocalCache instance
     */
    public static <K, V> LocalCache<K, V> createLocalCache(final long defaultLiveTime, final long defaultMaxIdleTime,
            final KeyedObjectPool<K, PoolableWrapper<V>> pool) {
        return new LocalCache<>(defaultLiveTime, defaultMaxIdleTime, pool);
    }

    /**
     * Creates a DistributedCache wrapper for a distributed cache client.
     * The wrapper adds retry logic and error handling around the client.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param dcc the distributed cache client to wrap
     * @return a new DistributedCache instance
     */
    public static <K, V> DistributedCache<K, V> createDistributedCache(final DistributedCacheClient<V> dcc) {
        return new DistributedCache<>(dcc);
    }

    /**
     * Creates a DistributedCache with a key prefix.
     * All keys will be prefixed with the specified string for namespace isolation.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param dcc the distributed cache client to wrap
     * @param keyPrefix the prefix to prepend to all keys
     * @return a new DistributedCache instance
     */
    public static <K, V> DistributedCache<K, V> createDistributedCache(final DistributedCacheClient<V> dcc, final String keyPrefix) {
        return new DistributedCache<>(dcc, keyPrefix);
    }

    /**
     * Creates a DistributedCache with custom retry configuration.
     * This allows fine-tuning of error handling behavior.
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param dcc the distributed cache client to wrap
     * @param keyPrefix the prefix to prepend to all keys
     * @param maxFailedNumForRetry maximum failures before stopping retries
     * @param retryDelay delay in milliseconds between retry attempts
     * @return a new DistributedCache instance
     */
    public static <K, V> DistributedCache<K, V> createDistributedCache(final DistributedCacheClient<V> dcc, final String keyPrefix,
            final int maxFailedNumForRetry, final long retryDelay) {
        return new DistributedCache<>(dcc, keyPrefix, maxFailedNumForRetry, retryDelay);
    }

    /**
     * Creates a cache instance from a string specification.
     * This method supports dynamic cache creation based on configuration strings.
     * 
     * <br><br>
     * Supported formats:
     * <ul>
     * <li>Memcached(serverUrl) - Creates SpyMemcached client</li>
     * <li>Memcached(serverUrl,keyPrefix) - With key prefix</li>
     * <li>Memcached(serverUrl,keyPrefix,timeout) - With custom timeout</li>
     * <li>Redis(serverUrl) - Creates JRedis client</li>
     * <li>Redis(serverUrl,keyPrefix) - With key prefix</li>
     * <li>Redis(serverUrl,keyPrefix,timeout) - With custom timeout</li>
     * <li>com.example.CustomCache(params...) - Custom implementation</li>
     * </ul>
     * 
     * <br>
     * Examples:
     * <pre>{@code
     * Cache<String, Object> cache1 = CacheFactory.createCache(
     *     "Memcached(localhost:11211)"
     * );
     * 
     * Cache<String, Object> cache2 = CacheFactory.createCache(
     *     "Redis(localhost:6379,myapp:,5000)"
     * );
     * }</pre>
     *
     * @param <K> the key type
     * @param <V> the value type
     * @param provider the cache specification string
     * @return a new cache instance
     * @throws IllegalArgumentException if the specification is invalid
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