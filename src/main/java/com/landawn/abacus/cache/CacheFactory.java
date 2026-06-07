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
import com.landawn.abacus.pool.PoolableAdapter;
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
 * <p><b>Usage Examples:</b>
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
     * Creates a new LocalCache with the specified capacity and eviction delay.
     * Uses default TTL ({@link Cache#DEFAULT_LIVE_TIME}, 3 hours) and default idle time
     * ({@link Cache#DEFAULT_MAX_IDLE_TIME}, 30 minutes).
     *
     * <p>The eviction delay controls how frequently the cache scans for and removes
     * expired entries. A value of 0 disables the periodic eviction scan; entries can
     * still be lazily evicted when accessed.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Create cache with 1000 entries capacity, checking for expired entries every minute
     * LocalCache<String, User> cache = CacheFactory.createLocalCache(1000, 60000); // returns a non-null LocalCache
     * cache.put("user:123", user);                                                 // returns true (entry stored)
     * User retrieved = cache.getOrNull("user:123");                                // returns the stored user (null if absent/expired)
     *
     * // Edge cases (validated by the underlying constructor):
     * CacheFactory.createLocalCache(0, 60000);      // throws IllegalArgumentException (capacity must be positive)
     * CacheFactory.createLocalCache(1000, -1);      // throws IllegalArgumentException (evictDelay must be non-negative)
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
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Create session cache: 500 capacity, check every 30s, expire after 30min or 15min idle
     * LocalCache<String, Session> cache = CacheFactory.createLocalCache(
     *     500,                                // capacity: 500 entries max
     *     30000,                              // evictDelay: check every 30 seconds
     *     1800000,                            // defaultLiveTime: 30 minutes TTL
     *     900000                              // defaultMaxIdleTime: 15 minutes idle timeout
     * );                                      // returns a non-null LocalCache
     * cache.put("session:abc123", session);   // returns true; uses default TTL and idle time
     *
     * // Edge case: capacity must be positive (TTL/idle of 0 are accepted as "no expiration")
     * CacheFactory.createLocalCache(0, 30000, 1800000, 900000);   // throws IllegalArgumentException (capacity must be positive)
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
     * <p>The provided pool must be configured to handle PoolableAdapter objects and will
     * be used directly by the cache for all entry storage and retrieval operations.
     * The pool's capacity and eviction settings will override any defaults.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Create custom pool with specific configuration
     * KeyedObjectPool<String, PoolableAdapter<User>> customPool =
     *     PoolFactory.createKeyedObjectPool(1000, 60000);   // capacity 1000, 60s eviction delay
     *
     * // Create cache using the custom pool
     * LocalCache<String, User> cache = CacheFactory.createLocalCache(
     *     3600000,                 // defaultLiveTime: 1 hour
     *     1800000,                 // defaultMaxIdleTime: 30 minutes
     *     customPool               // custom pool implementation
     * );                           // returns a non-null LocalCache backed by customPool
     * cache.put("user:123", user); // returns true (entry stored in customPool)
     *
     * // Edge case: a null pool is rejected
     * CacheFactory.createLocalCache(3600000L, 1800000L, (KeyedObjectPool<String, PoolableAdapter<User>>) null);
     *     // throws IllegalArgumentException (pool must not be null)
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
            final KeyedObjectPool<K, PoolableAdapter<V>> pool) {
        return new LocalCache<>(defaultLiveTime, defaultMaxIdleTime, pool);
    }

    /**
     * Creates a DistributedCache wrapper for a distributed cache client.
     * The wrapper provides a Cache interface implementation around the distributed cache client,
     * adding key prefixing (Base64 encoding) and a circuit breaker pattern on read operations
     * for resilience against cascading failures.
     *
     * <p>This is the simplest way to create a distributed cache, using default settings:
     * <ul>
     * <li>No key prefix (keys are Base64-encoded only)</li>
     * <li>Default circuit breaker configuration (max 100 consecutive failures, 1000ms retry delay)</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Create Memcached client and wrap it
     * SpyMemcached<User> memcachedClient = new SpyMemcached<>("localhost:11211", 5000);
     * DistributedCache<String, User> cache = CacheFactory.createDistributedCache(memcachedClient);   // returns a non-null DistributedCache
     *
     * // Use the cache
     * cache.put("user:123", user, 3600000, 0);      // returns true (sent to the wrapped client)
     * User retrieved = cache.getOrNull("user:123"); // returns the value, or null if absent/circuit-open
     *
     * // Edge case: a null client is rejected
     * CacheFactory.createDistributedCache((DistributedCacheClient<User>) null);   // throws IllegalArgumentException (dcc must not be null)
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
     * All cache keys will be automatically prefixed and Base64-encoded,
     * allowing multiple applications or modules to share the same cache server
     * without key collisions. Uses default circuit breaker configuration
     * (max 100 consecutive failures, 1000ms retry delay).
     *
     * <p>Key prefixing is useful for:
     * <ul>
     * <li>Multi-tenant applications sharing a cache server</li>
     * <li>Different environments (dev, staging, prod) using the same cache infrastructure</li>
     * <li>Logical separation of different cache regions within an application</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Create Redis client
     * JRedis<Session> redisClient = new JRedis<>("localhost:6379", 3000);
     *
     * // Create cache with namespace prefix
     * DistributedCache<String, Session> cache =
     *     CacheFactory.createDistributedCache(redisClient, "myapp:sessions:");   // returns a non-null DistributedCache
     *
     * // Keys are automatically prefixed and Base64-encoded
     * cache.put("user123", session, 3600000, 0);    // returns true
     * // Actual cache key: "myapp:sessions:" + Base64("user123")
     * Session s = cache.getOrNull("user123");        // returns the value, or null if absent/circuit-open
     *
     * // A null or empty prefix is accepted (no prefix applied)
     * CacheFactory.createDistributedCache(redisClient, (String) null);   // returns a DistributedCache with no key prefix
     *
     * // Edge case: a null client is rejected
     * CacheFactory.createDistributedCache((DistributedCacheClient<Session>) null, "myapp:");   // throws IllegalArgumentException (dcc must not be null)
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
     * Creates a DistributedCache with custom circuit breaker configuration.
     * This method allows fine-tuning of the circuit breaker pattern for distributed
     * cache read operations, which protects against cascading failures when the
     * distributed cache becomes unavailable.
     *
     * <p>The circuit breaker pattern works as follows:
     * <ul>
     * <li>When consecutive failures reach {@code maxFailedNumForRetry}, the circuit opens
     *     and read operations return {@code null} immediately without attempting cache access</li>
     * <li>After {@code retryDelay} milliseconds since the last failure, the circuit enters
     *     a half-open state and allows one read attempt to test if the cache has recovered</li>
     * <li>Successful operations reset the failure counter and close the circuit</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Create Redis client
     * JRedis<User> redisClient = new JRedis<>("localhost:6379", 3000);
     *
     * // Create cache with custom circuit breaker configuration
     * DistributedCache<String, User> cache = CacheFactory.createDistributedCache(
     *     redisClient,
     *     "app:",    // Key prefix for namespace isolation
     *     50,        // Open circuit after 50 consecutive failures
     *     2000       // Wait 2 seconds before attempting retry after circuit opens
     * );             // returns a non-null DistributedCache
     *
     * // Circuit breaker protects against cascading failures on reads
     * User user = cache.getOrNull("user:123");      // returns the value, or null if absent/circuit-open
     * cache.put("user:123", user, 3600000, 0);      // returns true
     *
     * // Edge cases (validated by the underlying constructor):
     * CacheFactory.createDistributedCache(redisClient, "app:", -1, 2000);   // throws IllegalArgumentException (maxFailedNumForRetry must be non-negative)
     * CacheFactory.createDistributedCache(redisClient, "app:", 50, -1);     // throws IllegalArgumentException (retryDelay must be non-negative)
     * }</pre>
     *
     * @param <K> the type of keys maintained by the cache
     * @param <V> the type of cached values
     * @param dcc the distributed cache client to wrap (must not be null)
     * @param keyPrefix the key prefix to prepend to all keys (can be empty string or null for no prefix)
     * @param maxFailedNumForRetry the maximum number of consecutive failures before the circuit breaker opens (must be non-negative)
     * @param retryDelay the delay in milliseconds before attempting a retry after the circuit breaker opens (must be non-negative)
     * @return a new DistributedCache instance with custom circuit breaker configuration
     * @throws IllegalArgumentException if dcc is null, maxFailedNumForRetry is negative, or retryDelay is negative
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
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Memcached with single server and default timeout; the result is a DistributedCache
     * Cache<String, User> cache1 = CacheFactory.createCache("Memcached(localhost:11211)");   // returns a non-null DistributedCache
     *
     * // Redis with key prefix and custom 5-second timeout
     * Cache<String, Session> cache2 = CacheFactory.createCache(
     *     "Redis(localhost:6379,app:cache:,5000)"
     * );                                            // returns a non-null DistributedCache
     *
     * // Multiple Memcached servers (space-separated in serverUrl)
     * Cache<String, Object> cache3 = CacheFactory.createCache(
     *     "Memcached(host1:11211 host2:11211,myprefix:,3000)"
     * );                                            // returns a non-null DistributedCache
     *
     * // Provider name matching is case-insensitive
     * Cache<String, Object> cache4 = CacheFactory.createCache("memcached(localhost:11211)");   // returns a non-null DistributedCache
     *
     * // Custom cache implementation (fully qualified class name implementing Cache)
     * Cache<String, Object> cache5 = CacheFactory.createCache(
     *     "com.mycompany.CustomCache(param1,param2)"
     * );                                            // returns an instance of the named Cache class
     *
     * // Edge cases (all throw IllegalArgumentException):
     * CacheFactory.createCache(null);                              // "Provider specification cannot be null or empty"
     * CacheFactory.createCache("");                                // "Provider specification cannot be null or empty"
     * CacheFactory.createCache("Memcached()");                     // "server URL cannot be empty"
     * CacheFactory.createCache("Memcached(localhost,p:,0)");       // non-positive timeout rejected
     * CacheFactory.createCache("Memcached(localhost,p:,abc)");     // "Invalid timeout parameter: abc"
     * CacheFactory.createCache("Memcached(a,b,1000,extra)");       // "Unsupported parameters" (more than 3)
     * CacheFactory.createCache("Memcached(localhost,app:");        // unbalanced parenthesis -> "Failed to parse provider specification"
     * CacheFactory.createCache("com.example.NoSuchCache(host)");   // "Cannot find class: com.example.NoSuchCache"
     * CacheFactory.createCache("java.lang.String(host)");          // "Custom cache class must implement Cache"
     * }</pre>
     *
     * @param <K> the type of keys maintained by the cache
     * @param <V> the type of cached values
     * @param provider the cache provider specification string in format "ClassName(param1,param2,...)" (must not be null or empty)
     * @return a new Cache instance configured according to the specification
     * @throws IllegalArgumentException if the provider string is null or empty, cannot be parsed, has no
     *         parameters, has an empty server URL, has an empty class name, specifies an unsupported number
     *         of parameters (more than 3 for Memcached/Redis), specifies a non-numeric or non-positive timeout,
     *         names a class that cannot be found, or names a custom class with no matching constructor
     * @throws RuntimeException if a custom class is found but cannot be instantiated (constructor invocation
     *         fails, security restrictions, etc.)
     * @see #createDistributedCache(DistributedCacheClient)
     * @see #createDistributedCache(DistributedCacheClient, String)
     * @see #createLocalCache(int, long)
     */
    @SuppressWarnings("unchecked")
    public static <K, V> Cache<K, V> createCache(final String provider) {
        if (Strings.isEmpty(provider)) {
            throw new IllegalArgumentException("Provider specification cannot be null or empty");
        }

        final TypeAttrParser attrResult;

        try {
            attrResult = TypeAttrParser.parse(provider);
        } catch (final IllegalArgumentException e) {
            throw e;
        } catch (final RuntimeException e) {
            // Malformed DSL (e.g. an unbalanced parenthesis) can make the parser throw a low-level
            // exception such as StringIndexOutOfBoundsException. Surface it as the documented
            // IllegalArgumentException instead of leaking the parser's internal failure.
            throw new IllegalArgumentException("Failed to parse provider specification: " + provider, e);
        }

        if (attrResult == null) {
            throw new IllegalArgumentException("Failed to parse provider specification: " + provider);
        }

        final String[] parameters = attrResult.getParameters();

        if (N.isEmpty(parameters)) {
            throw new IllegalArgumentException("Invalid provider specification: missing parameters");
        }

        final String url = parameters[0];

        if (Strings.isEmpty(url)) {
            throw new IllegalArgumentException("Invalid provider specification: server URL cannot be empty");
        }

        final String className = attrResult.getClassName();

        if (Strings.isEmpty(className)) {
            throw new IllegalArgumentException("Invalid provider specification: class name cannot be empty");
        }

        Class<?> cls = null;

        if (DistributedCacheClient.MEMCACHED.equalsIgnoreCase(className)) {
            if (parameters.length == 1) {
                return new DistributedCache<>(new SpyMemcached<>(url, DEFAULT_TIMEOUT));
            } else if (parameters.length == 2) {
                return new DistributedCache<>(new SpyMemcached<>(url, DEFAULT_TIMEOUT), parameters[1]);
            } else if (parameters.length == 3) {
                try {
                    final long timeout = Numbers.toLong(parameters[2]);
                    N.checkArgPositive(timeout, "timeout");
                    return new DistributedCache<>(new SpyMemcached<>(url, timeout), parameters[1]);
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
                    final long timeout = Numbers.toLong(parameters[2]);
                    N.checkArgPositive(timeout, "timeout");
                    return new DistributedCache<>(new JRedis<>(url, timeout), parameters[1]);
                } catch (final NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid timeout parameter: " + parameters[2], e);
                }
            } else {
                throw new IllegalArgumentException("Unsupported parameters: " + Strings.join(parameters));
            }
        } else {
            try {
                cls = ClassUtil.forName(className);
            } catch (final IllegalArgumentException e) {
                // ClassUtil.forName throws IllegalArgumentException (not null) when the class cannot
                // be found; rethrow with this method's documented "Cannot find class" message so the
                // null-check below is not relied upon as dead code.
                throw new IllegalArgumentException("Cannot find class: " + className, e);
            }

            if (cls == null) {
                throw new IllegalArgumentException("Cannot find class: " + className);
            }

            if (!Cache.class.isAssignableFrom(cls)) {
                throw new IllegalArgumentException("Custom cache class must implement Cache: " + className);
            }

            return TypeAttrParser.newInstance(cls, provider);
        }
    }
}
