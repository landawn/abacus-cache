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

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.landawn.abacus.exception.UncheckedIOException;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.parser.ParserFactory;
import com.landawn.abacus.util.AddrUtil;
import com.landawn.abacus.util.ExceptionUtil;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

/**
 * A Memcached distributed cache client implementation using the SpyMemcached library.
 * This class provides high-performance, asynchronous access to Memcached servers with
 * support for both synchronous and asynchronous operations. It uses Kryo serialization
 * when available for efficient object storage.
 * 
 * <br><br>
 * Key features:
 * <ul>
 * <li>Asynchronous and synchronous operations</li>
 * <li>Bulk get operations for efficiency</li>
 * <li>Atomic increment/decrement operations</li>
 * <li>Configurable timeouts and serialization</li>
 * <li>Future-based async API</li>
 * </ul>
 * 
 * <br>
 * Example usage:
 * <pre>{@code
 * SpyMemcached<User> cache = new SpyMemcached<>("localhost:11211");
 * 
 * // Synchronous operations
 * cache.set("user:123", user, 3600000); // Cache for 1 hour
 * User cached = cache.get("user:123");
 * 
 * // Asynchronous operations
 * Future<Boolean> future = cache.asyncSet("user:456", anotherUser, 3600000);
 * boolean success = future.get(); // Wait for completion
 * 
 * // Bulk operations
 * Map<String, User> users = cache.getBulk("user:123", "user:456", "user:789");
 * }</pre>
 *
 * @param <T> the type of values stored and retrieved from the cache
 * @see AbstractDistributedCacheClient
 * @see MemcachedClient
 */
public class SpyMemcached<T> extends AbstractDistributedCacheClient<T> {

    static final Logger logger = LoggerFactory.getLogger(SpyMemcached.class);

    private final MemcachedClient mc;
    private volatile boolean isShutdown = false;

    /**
     * Creates a new SpyMemcached instance with the default timeout.
     * The server URL should contain comma-separated host:port pairs for multiple servers.
     * The default timeout value is defined in the parent class {@link AbstractDistributedCacheClient#DEFAULT_TIMEOUT}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SpyMemcached<String> cache = new SpyMemcached<>("localhost:11211");
     * cache.set("key1", "value1", 3600000);
     * }</pre>
     *
     * @param serverUrl the Memcached server URL(s) in format "host1:port1,host2:port2,...". Must not be {@code null} or empty.
     * @throws IllegalArgumentException if serverUrl is invalid or cannot be parsed
     * @throws RuntimeException if connection to Memcached servers fails
     */
    public SpyMemcached(final String serverUrl) {
        this(serverUrl, DEFAULT_TIMEOUT);
    }

    /**
     * Creates a new SpyMemcached instance with a specified timeout.
     * The timeout applies to all cache operations (get, set, delete, etc.). If Kryo is available
     * on the classpath (checked via {@link ParserFactory#isKryoAvailable()}), it will be used for
     * object serialization via {@link KryoTranscoder}; otherwise, the default SpyMemcached
     * serialization mechanism is used.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create cache with 5-second operation timeout
     * SpyMemcached<User> cache = new SpyMemcached<>("localhost:11211", 5000);
     * cache.set("user:123", user, 3600000);
     * }</pre>
     *
     * @param serverUrl the Memcached server URL(s) in format "host1:port1,host2:port2,...". Must not be {@code null} or empty.
     * @param timeout the operation timeout in milliseconds (must be positive). This timeout applies to all cache operations.
     * @throws IllegalArgumentException if timeout is not positive or serverUrl is invalid
     * @throws RuntimeException if connection to Memcached servers fails
     */
    public SpyMemcached(final String serverUrl, final long timeout) {
        super(serverUrl);

        if (timeout <= 0) {
            throw new IllegalArgumentException("Timeout must be positive: " + timeout);
        }

        MemcachedClient tempMc = null;
        try {
            final Transcoder<Object> transcoder = ParserFactory.isKryoAvailable() ? new KryoTranscoder<>() : null;

            final ConnectionFactory connFactory = new DefaultConnectionFactory() {
                @Override
                public long getOperationTimeout() {
                    return timeout;
                }

                @Override
                public Transcoder<Object> getDefaultTranscoder() {
                    if (transcoder != null) {
                        return transcoder;
                    } else {
                        return super.getDefaultTranscoder();
                    }
                }
            };

            tempMc = createSpyMemcachedClient(serverUrl, connFactory);
            this.mc = tempMc;
        } catch (final Exception e) {
            throw ExceptionUtil.toRuntimeException(e);
        }
    }

    /**
     * Retrieves an object from the cache by its key.
     * This is a synchronous operation that blocks until complete or timeout is reached.
     * The operation timeout is configured during client construction.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = cache.get("user:123");
     * if (user != null) {
     *     System.out.println("Found user: " + user.getName());
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be retrieved. Must not be {@code null}.
     * @return the cached object of type T, or {@code null} if not found or expired
     * @throws RuntimeException if the operation times out or encounters an error
     */
    @SuppressWarnings("unchecked")
    @Override
    public T get(final String key) {
        return (T) mc.get(key);
    }

    /**
     * Asynchronously retrieves an object from the cache by its key.
     * The operation is executed asynchronously by the underlying SpyMemcached client
     * and returns immediately. The returned Future can be used to check completion status
     * and retrieve the result when available. The operation timeout is configured during
     * client construction.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Future<User> future = cache.asyncGet("user:123");
     * User user = future.get(); // Blocks until complete
     * }</pre>
     *
     * @param key the cache key whose associated value is to be retrieved. Must not be {@code null}.
     * @return a Future that will contain the cached object of type T, or {@code null} if not found or expired
     * @throws RuntimeException if the key is invalid
     */
    @SuppressWarnings("unchecked")
    public Future<T> asyncGet(final String key) {
        return (Future<T>) mc.asyncGet(key);
    }

    /**
     * Retrieves multiple objects from the cache in a single operation.
     * This is more efficient than multiple individual get operations as it uses a single
     * network round-trip to fetch all values. Keys not found in the cache or that have expired
     * will not be present in the returned map. This is a synchronous operation that blocks until
     * complete or timeout.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, User> users = cache.getBulk("user:1", "user:2", "user:3");
     * for (Map.Entry<String, User> entry : users.entrySet()) {
     *     System.out.println("Found: " + entry.getKey());
     * }
     * }</pre>
     *
     * @param keys the cache keys whose associated values are to be retrieved. Must not be {@code null}.
     * @return a map containing the found key-value pairs. Never {@code null}, but may be empty if no keys are found.
     * @throws RuntimeException if the operation times out or encounters an error
     */
    @SuppressWarnings("unchecked")
    @Override
    public final Map<String, T> getBulk(final String... keys) {
        return (Map<String, T>) mc.getBulk(keys);
    }

    /**
     * Asynchronously retrieves multiple objects from the cache.
     * This method returns immediately without blocking. The returned Future contains a map of
     * found key-value pairs. Keys not found in the cache or that have expired will not be present
     * in the returned map. This operation is more efficient than multiple individual async get
     * operations as it uses a single network round-trip.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Future<Map<String, User>> future = cache.asyncGetBulk("user:1", "user:2");
     * Map<String, User> users = future.get(); // Blocks until complete
     * }</pre>
     *
     * @param keys the cache keys whose associated values are to be retrieved. Must not be {@code null}.
     * @return a Future that will contain the map of found key-value pairs. The map is never {@code null}, but may be empty.
     * @throws RuntimeException if the keys parameter is invalid
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public final Future<Map<String, T>> asyncGetBulk(final String... keys) {
        return (Future) mc.asyncGetBulk(keys);
    }

    /**
     * Retrieves multiple objects from the cache using a collection of keys.
     * This is more efficient than multiple individual get operations as it uses a single
     * network round-trip to fetch all values. Keys not found in the cache or that have expired
     * will not be present in the returned map. This is a synchronous operation that blocks until
     * complete or timeout.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> keys = Arrays.asList("user:1", "user:2", "user:3");
     * Map<String, User> users = cache.getBulk(keys);
     * System.out.println("Found " + users.size() + " users");
     * }</pre>
     *
     * @param keys the collection of cache keys whose associated values are to be retrieved. Must not be {@code null}.
     * @return a map containing the found key-value pairs. Never {@code null}, but may be empty if no keys are found.
     * @throws RuntimeException if the operation times out or encounters an error
     */
    @SuppressWarnings("unchecked")
    @Override
    public Map<String, T> getBulk(final Collection<String> keys) {
        return (Map<String, T>) mc.getBulk(keys);
    }

    /**
     * Asynchronously retrieves multiple objects using a collection of keys.
     * This method returns immediately without blocking. The returned Future contains a map of
     * found key-value pairs. Keys not found in the cache or that have expired will not be present
     * in the returned map. This operation is more efficient than multiple individual async get
     * operations as it uses a single network round-trip.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Set<String> keys = Set.of("user:1", "user:2");
     * Future<Map<String, User>> future = cache.asyncGetBulk(keys);
     * Map<String, User> users = future.get(); // Blocks until complete
     * }</pre>
     *
     * @param keys the collection of cache keys whose associated values are to be retrieved. Must not be {@code null}.
     * @return a Future that will contain the map of found key-value pairs. The map is never {@code null}, but may be empty.
     * @throws RuntimeException if the keys parameter is invalid
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Future<Map<String, T>> asyncGetBulk(final Collection<String> keys) {
        return (Future) mc.asyncGetBulk(keys);
    }

    /**
     * Stores an object in the cache with a specified time-to-live.
     * This operation replaces any existing value for the key. The method blocks until
     * the operation completes or times out. The liveTime is converted from milliseconds to
     * seconds for Memcached (rounded up if not exact).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = new User("John", "john@example.com");
     * boolean success = cache.set("user:123", user, 3600000); // 1 hour TTL
     * if (success) {
     *     System.out.println("User cached successfully");
     * }
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated. Must not be {@code null}.
     * @param obj the object value to cache. May be {@code null}.
     * @param liveTime the time-to-live in milliseconds (converted to seconds, rounded up if not exact). Must be non-negative.
     * @return {@code true} if the operation was successful, {@code false} otherwise
     * @throws RuntimeException if the operation times out or encounters an error
     */
    @Override
    public boolean set(final String key, final T obj, final long liveTime) {
        return resultOf(mc.set(key, toSeconds(liveTime), obj));
    }

    /**
     * Asynchronously stores an object in the cache with a specified time-to-live.
     * The returned Future can be used to check if the operation succeeded. This method
     * returns immediately without blocking. The liveTime is converted from milliseconds to
     * seconds for Memcached (rounded up if not exact).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Future<Boolean> future = cache.asyncSet("user:123", user, 3600000);
     * boolean success = future.get(); // Blocks until complete
     * if (success) {
     *     System.out.println("Set operation succeeded");
     * }
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated. Must not be {@code null}.
     * @param obj the object value to cache. May be {@code null}.
     * @param liveTime the time-to-live in milliseconds (converted to seconds, rounded up if not exact). Must be non-negative.
     * @return a Future that will indicate success ({@code true}) or failure ({@code false})
     * @throws RuntimeException if the key is invalid
     */
    public Future<Boolean> asyncSet(final String key, final T obj, final long liveTime) {
        return mc.set(key, toSeconds(liveTime), obj);
    }

    /**
     * Adds an object to the cache only if the key doesn't already exist.
     * This operation is atomic and thread-safe across all clients. The method blocks until
     * the operation completes or times out. If the key already exists, this operation will
     * fail and return {@code false}. The liveTime is converted from milliseconds to seconds
     * for Memcached (rounded up if not exact).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * if (cache.add("user:123", user, 3600000)) {
     *     System.out.println("User added successfully");
     * } else {
     *     System.out.println("User already exists in cache");
     * }
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated. Must not be {@code null}.
     * @param obj the object value to cache. May be {@code null}.
     * @param liveTime the time-to-live in milliseconds (converted to seconds, rounded up if not exact). Must be non-negative.
     * @return {@code true} if the object was added, {@code false} if the key already exists
     * @throws RuntimeException if the operation times out or encounters an error
     */
    public boolean add(final String key, final T obj, final long liveTime) {
        return resultOf(mc.add(key, toSeconds(liveTime), obj));
    }

    /**
     * Asynchronously adds an object to the cache only if the key doesn't already exist.
     * This operation is atomic and thread-safe across all clients. The method returns immediately
     * without blocking. If the key already exists, the Future will contain {@code false}. The liveTime
     * is converted from milliseconds to seconds for Memcached (rounded up if not exact).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Future<Boolean> future = cache.asyncAdd("user:123", user, 3600000);
     * if (future.get()) {
     *     System.out.println("Added");
     * } else {
     *     System.out.println("Key already exists");
     * }
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated. Must not be {@code null}.
     * @param obj the object value to cache. May be {@code null}.
     * @param liveTime the time-to-live in milliseconds (converted to seconds, rounded up if not exact). Must be non-negative.
     * @return a Future that will indicate {@code true} if the add succeeded, {@code false} if the key already exists
     * @throws RuntimeException if the key is invalid
     */
    public Future<Boolean> asyncAdd(final String key, final T obj, final long liveTime) {
        return mc.add(key, toSeconds(liveTime), obj);
    }

    /**
     * Replaces an object in the cache only if the key already exists.
     * This operation is atomic and thread-safe across all clients. The method blocks until
     * the operation completes or times out. If the key doesn't exist, this operation will
     * fail and return {@code false}. The liveTime is converted from milliseconds to seconds
     * for Memcached (rounded up if not exact).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * if (cache.replace("user:123", updatedUser, 3600000)) {
     *     System.out.println("User updated");
     * } else {
     *     System.out.println("User not found in cache");
     * }
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated. Must not be {@code null}.
     * @param obj the object value to cache. May be {@code null}.
     * @param liveTime the time-to-live in milliseconds (converted to seconds, rounded up if not exact). Must be non-negative.
     * @return {@code true} if the object was replaced, {@code false} if the key doesn't exist
     * @throws RuntimeException if the operation times out or encounters an error
     */
    public boolean replace(final String key, final T obj, final long liveTime) {
        return resultOf(mc.replace(key, toSeconds(liveTime), obj));
    }

    /**
     * Asynchronously replaces an object in the cache only if the key already exists.
     * This operation is atomic and thread-safe across all clients. The method returns immediately
     * without blocking. If the key doesn't exist, the Future will contain {@code false}. The liveTime
     * is converted from milliseconds to seconds for Memcached (rounded up if not exact).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Future<Boolean> future = cache.asyncReplace("user:123", updatedUser, 3600000);
     * boolean replaced = future.get();
     * if (replaced) {
     *     System.out.println("Replaced successfully");
     * }
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated. Must not be {@code null}.
     * @param obj the object value to cache. May be {@code null}.
     * @param liveTime the time-to-live in milliseconds (converted to seconds, rounded up if not exact). Must be non-negative.
     * @return a Future that will indicate {@code true} if the replacement succeeded, {@code false} if the key doesn't exist
     * @throws RuntimeException if the key is invalid
     */
    public Future<Boolean> asyncReplace(final String key, final T obj, final long liveTime) {
        return mc.replace(key, toSeconds(liveTime), obj);
    }

    /**
     * Removes an object from the cache.
     * The method blocks until the operation completes or times out. The return value indicates
     * whether the operation was acknowledged by the server, not necessarily whether the key existed
     * before deletion. This operation succeeds whether or not the key exists in the cache.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * boolean success = cache.delete("user:123");
     * if (success) {
     *     System.out.println("Delete operation sent successfully");
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be removed. Must not be {@code null}.
     * @return {@code true} if the delete operation was successfully acknowledged by the server
     * @throws RuntimeException if the operation times out or encounters an error
     */
    @Override
    public boolean delete(final String key) {
        return resultOf(mc.delete(key));
    }

    /**
     * Asynchronously removes an object from the cache.
     * The method returns immediately without blocking. The returned Future indicates if the
     * operation was acknowledged by the server, not necessarily whether the key existed before
     * deletion. This operation succeeds whether or not the key exists in the cache.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Future<Boolean> future = cache.asyncDelete("user:123");
     * boolean deleted = future.get();
     * if (deleted) {
     *     System.out.println("Delete operation acknowledged");
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be removed. Must not be {@code null}.
     * @return a Future that will indicate success ({@code true}) or failure ({@code false})
     * @throws RuntimeException if the key is invalid
     */
    public Future<Boolean> asyncDelete(final String key) {
        return mc.delete(key);
    }

    /**
     * Atomically increments a numeric value by 1.
     * If the key doesn't exist, the behavior depends on the Memcached server implementation
     * (typically returns -1 or NOT_FOUND). This operation is atomic and thread-safe across all
     * clients. Only works with string representations of 64-bit unsigned integers stored in Memcached.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long pageViews = cache.incr("page:views");
     * if (pageViews != -1) {
     *     System.out.println("Page views: " + pageViews);
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be incremented. Must not be {@code null}.
     * @return the value after increment, or -1 if the key doesn't exist
     * @throws RuntimeException if the operation times out or encounters an error
     */
    @Override
    public long incr(final String key) {
        return mc.incr(key, 1);
    }

    /**
     * Atomically increments a numeric value by a specified amount.
     * If the key doesn't exist, the behavior depends on the Memcached server implementation
     * (typically returns -1 or NOT_FOUND). This operation is atomic and thread-safe across all
     * clients. Only works with string representations of 64-bit unsigned integers stored in Memcached.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long score = cache.incr("player:score", 10);
     * if (score != -1) {
     *     System.out.println("New score: " + score);
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be incremented. Must not be {@code null}.
     * @param delta the amount by which to increment the value (must be non-negative)
     * @return the value after increment, or -1 if the key doesn't exist
     * @throws RuntimeException if the operation times out or encounters an error
     */
    @Override
    public long incr(final String key, final int delta) {
        return mc.incr(key, delta);
    }

    /**
     * Atomically increments a numeric value with a default value if key doesn't exist.
     * If the key doesn't exist, it is created with the defaultValue, then incremented by delta.
     * The value will not expire (passing -1 as expiration to SpyMemcached means no expiration).
     * This operation is atomic and thread-safe across all clients. Works with 64-bit unsigned integers.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Initialize counter to 0 if not exists, then increment by 1
     * long count = cache.incr("counter:views", 1, 0);
     * System.out.println("View count: " + count);
     * }</pre>
     *
     * @param key the cache key whose associated value is to be incremented. Must not be {@code null}.
     * @param delta the amount by which to increment the value (must be non-negative)
     * @param defaultValue the initial value to set if the key doesn't exist, which will then be incremented by delta
     * @return the value after increment (defaultValue + delta if key didn't exist, otherwise existing value + delta)
     * @throws RuntimeException if the operation times out or encounters an error
     */
    public long incr(final String key, final int delta, final long defaultValue) {
        return mc.incr(key, delta, defaultValue, -1);
    }

    /**
     * Atomically increments a numeric value with default value and expiration.
     * If the key doesn't exist, it's created with the defaultValue, then incremented by delta,
     * and the expiration time is set to the specified liveTime. This operation is atomic and
     * thread-safe across all clients. Works with 64-bit unsigned integers. The liveTime is
     * converted from milliseconds to seconds for Memcached (rounded up if not exact).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Hourly counter that expires after 1 hour
     * long count = cache.incr("counter:hourly", 1, 0, 3600000);
     * System.out.println("Hourly count: " + count);
     * }</pre>
     *
     * @param key the cache key whose associated value is to be incremented. Must not be {@code null}.
     * @param delta the amount by which to increment the value (must be non-negative)
     * @param defaultValue the initial value to set if the key doesn't exist, which will then be incremented by delta
     * @param liveTime the time-to-live in milliseconds for the key (converted to seconds, rounded up if not exact). Must be non-negative.
     * @return the value after increment (defaultValue + delta if key didn't exist, otherwise existing value + delta)
     * @throws RuntimeException if the operation times out or encounters an error
     */
    public long incr(final String key, final int delta, final long defaultValue, final long liveTime) {
        return mc.incr(key, delta, defaultValue, toSeconds(liveTime));
    }

    /**
     * Atomically decrements a numeric value by 1.
     * If the key doesn't exist, the behavior depends on the Memcached server implementation
     * (typically returns -1 or NOT_FOUND). Values cannot go below 0 (Memcached prevents underflow).
     * This operation is atomic and thread-safe across all clients. Only works with string
     * representations of 64-bit unsigned integers stored in Memcached.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long remainingTokens = cache.decr("api:tokens");
     * if (remainingTokens != -1 && remainingTokens <= 0) {
     *     System.out.println("Rate limit exceeded");
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be decremented. Must not be {@code null}.
     * @return the value after decrement (cannot be negative), or -1 if the key doesn't exist
     * @throws RuntimeException if the operation times out or encounters an error
     */
    @Override
    public long decr(final String key) {
        return mc.decr(key, 1);
    }

    /**
     * Atomically decrements a numeric value by a specified amount.
     * If the key doesn't exist, the behavior depends on the Memcached server implementation
     * (typically returns -1 or NOT_FOUND). Values cannot go below 0 (Memcached prevents underflow).
     * This operation is atomic and thread-safe across all clients. Only works with string
     * representations of 64-bit unsigned integers stored in Memcached.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long inventory = cache.decr("product:stock", 5);
     * if (inventory != -1) {
     *     System.out.println("Remaining inventory: " + inventory);
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be decremented. Must not be {@code null}.
     * @param delta the amount by which to decrement the value (must be non-negative)
     * @return the value after decrement (cannot be negative), or -1 if the key doesn't exist
     * @throws RuntimeException if the operation times out or encounters an error
     */
    @Override
    public long decr(final String key, final int delta) {
        return mc.decr(key, delta);
    }

    /**
     * Atomically decrements a numeric value with a default value if key doesn't exist.
     * If the key doesn't exist, it is created with the defaultValue, then decremented by delta.
     * The value will not expire (passing -1 as expiration to SpyMemcached means no expiration).
     * Values cannot go below 0 (Memcached prevents underflow). This operation is atomic and
     * thread-safe across all clients. Works with 64-bit unsigned integers.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Initialize inventory to 100 if not exists, then decrement by 1
     * long remaining = cache.decr("inventory:item123", 1, 100);
     * System.out.println("Items remaining: " + remaining);
     * }</pre>
     *
     * @param key the cache key whose associated value is to be decremented. Must not be {@code null}.
     * @param delta the amount by which to decrement the value (must be non-negative)
     * @param defaultValue the initial value to set if the key doesn't exist, which will then be decremented by delta
     * @return the value after decrement (defaultValue - delta if key didn't exist, otherwise existing value - delta). Cannot be negative.
     * @throws RuntimeException if the operation times out or encounters an error
     */
    public long decr(final String key, final int delta, final long defaultValue) {
        return mc.decr(key, delta, defaultValue, -1);
    }

    /**
     * Atomically decrements a numeric value with default value and expiration.
     * If the key doesn't exist, it's created with the defaultValue, then decremented by delta,
     * and the expiration time is set to the specified liveTime. Values cannot go below 0
     * (Memcached prevents underflow). This operation is atomic and thread-safe across all clients.
     * Works with 64-bit unsigned integers. The liveTime is converted from milliseconds to seconds
     * for Memcached (rounded up if not exact).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Daily quota that expires after 24 hours
     * long remaining = cache.decr("quota:user:123", 1, 1000, 86400000);
     * System.out.println("Remaining quota: " + remaining);
     * }</pre>
     *
     * @param key the cache key whose associated value is to be decremented. Must not be {@code null}.
     * @param delta the amount by which to decrement the value (must be non-negative)
     * @param defaultValue the initial value to set if the key doesn't exist, which will then be decremented by delta
     * @param liveTime the time-to-live in milliseconds for the key (converted to seconds, rounded up if not exact). Must be non-negative.
     * @return the value after decrement (defaultValue - delta if key didn't exist, otherwise existing value - delta). Cannot be negative.
     * @throws RuntimeException if the operation times out or encounters an error
     */
    public long decr(final String key, final int delta, final long defaultValue, final long liveTime) {
        return mc.decr(key, delta, defaultValue, toSeconds(liveTime));
    }

    /**
     * Flushes all data from all connected Memcached servers immediately.
     * This operation removes all keys from all servers. The method blocks until the
     * flush completes or times out. Use with extreme caution in production environments
     * as this is a destructive operation that cannot be undone.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Warning: This removes ALL data from ALL connected servers!
     * cache.flushAll();
     * System.out.println("All cache data cleared");
     * }</pre>
     *
     * @throws RuntimeException if the operation times out or encounters an error
     */
    @Override
    public void flushAll() {
        resultOf(mc.flush());
    }

    /**
     * Asynchronously flushes all data from all connected Memcached servers.
     * The method returns immediately without blocking. The returned Future can be used to
     * check when the operation completes. This is a destructive operation that removes all
     * keys from all servers and cannot be undone. Use with extreme caution in production.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Future<Boolean> future = cache.asyncFlushAll();
     * boolean flushed = future.get();
     * if (flushed) {
     *     System.out.println("All data flushed");
     * }
     * }</pre>
     *
     * @return a Future that will indicate when the flush completes successfully ({@code true}) or fails ({@code false})
     */
    public Future<Boolean> asyncFlushAll() {
        return mc.flush();
    }

    /**
     * Flushes all data from all connected Memcached servers after a specified delay.
     * The method blocks until the scheduling completes or times out. The flush will occur
     * on all servers after the delay expires. This is a destructive operation that removes
     * all keys from all servers and cannot be undone. The delay is converted from milliseconds
     * to seconds for Memcached (rounded up if not exact).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Schedule a flush to happen in 5 seconds
     * boolean scheduled = cache.flushAll(5000);
     * if (scheduled) {
     *     System.out.println("Flush scheduled for 5 seconds from now");
     * }
     * }</pre>
     *
     * @param delay the delay in milliseconds before the flush operation is executed (converted to seconds, rounded up if not exact). Must be non-negative.
     * @return {@code true} if the flush was scheduled successfully, {@code false} otherwise
     * @throws RuntimeException if the operation times out or encounters an error
     */
    public boolean flushAll(final long delay) {
        return resultOf(mc.flush(toSeconds(delay)));
    }

    /**
     * Asynchronously schedules a flush after a delay.
     * The method returns immediately without blocking. The flush will occur on all connected
     * servers after the delay expires. This is a destructive operation that removes all keys
     * from all servers and cannot be undone. The delay is converted from milliseconds to seconds
     * for Memcached (rounded up if not exact).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Schedule a flush to happen in 10 seconds
     * Future<Boolean> future = cache.asyncFlushAll(10000);
     * boolean scheduled = future.get(); // Wait for scheduling confirmation
     * }</pre>
     *
     * @param delay the delay in milliseconds before the flush operation is executed (converted to seconds, rounded up if not exact). Must be non-negative.
     * @return a Future that will indicate if the flush was scheduled successfully ({@code true}) or failed ({@code false})
     */
    public Future<Boolean> asyncFlushAll(final long delay) {
        return mc.flush(toSeconds(delay));
    }

    /**
     * Disconnects from all Memcached servers and releases resources.
     * After calling this method, the cache client cannot be used anymore and any subsequent
     * operations will fail. Uses the default shutdown timeout from SpyMemcached. This method
     * is idempotent - calling it multiple times has no additional effect.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * try {
     *     // Use cache...
     * } finally {
     *     cache.disconnect();
     *     System.out.println("Cache client disconnected");
     * }
     * }</pre>
     */
    @Override
    public synchronized void disconnect() {
        if (!isShutdown) {
            mc.shutdown();
            isShutdown = true;
        }
    }

    /**
     * Disconnects from all Memcached servers with a specified timeout.
     * Waits up to the timeout for pending operations to complete before forcefully closing
     * connections. After calling this method, the cache client cannot be used anymore and
     * any subsequent operations will fail. This method is idempotent - calling it multiple
     * times has no additional effect.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * try {
     *     // Use cache...
     * } finally {
     *     cache.disconnect(5000); // Wait up to 5 seconds for cleanup
     * }
     * }</pre>
     *
     * @param timeout the maximum time to wait for shutdown in milliseconds. Must be non-negative.
     */
    public synchronized void disconnect(final long timeout) {
        if (!isShutdown) {
            mc.shutdown(timeout, TimeUnit.MILLISECONDS);
            isShutdown = true;
        }
    }

    /**
     * Waits for a Future to complete and returns its result.
     * This method blocks until the Future completes and properly handles interruptions
     * and execution errors by converting checked exceptions to runtime exceptions.
     * When an InterruptedException occurs, the thread's interrupted status is restored
     * before throwing the runtime exception.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Future<Boolean> future = mc.set("key", "value", 3600);
     * Boolean result = resultOf(future);
     * }</pre>
     *
     * @param <R> the type of result returned by the Future
     * @param future the Future whose result is to be retrieved
     * @return the result value from the Future
     * @throws RuntimeException if the Future execution fails or is interrupted
     */
    protected <R> R resultOf(final Future<R> future) {
        try {
            return future.get();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupt status
            throw ExceptionUtil.toRuntimeException(e, true);
        } catch (final ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause != null) {
                throw ExceptionUtil.toRuntimeException(cause, true);
            } else {
                throw ExceptionUtil.toRuntimeException(e, true);
            }
        }
    }

    /**
     * Creates a SpyMemcached client with the specified connection factory.
     * This method handles the IOException that may occur during client creation
     * and converts it to an unchecked exception for easier handling.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * ConnectionFactory factory = new DefaultConnectionFactory();
     * MemcachedClient client = createSpyMemcachedClient("localhost:11211", factory);
     * }</pre>
     *
     * @param serverUrl the Memcached server URL(s) to connect to
     * @param connFactory the connection factory configured with timeout and transcoder settings
     * @return a configured MemcachedClient instance
     * @throws UncheckedIOException if the connection to Memcached servers fails
     */
    protected static MemcachedClient createSpyMemcachedClient(final String serverUrl, final ConnectionFactory connFactory) throws UncheckedIOException {
        try {
            return new MemcachedClient(connFactory, AddrUtil.getAddressList(serverUrl));
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to create Memcached client.", e);
        }
    }
}