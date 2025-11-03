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
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SpyMemcached<String> cache = new SpyMemcached<>("localhost:11211");
     * cache.set("key1", "value1", 3600000);
     * }</pre>
     *
     * @param serverUrl the Memcached server URL(s) in format "host1:port1,host2:port2,..."
     */
    public SpyMemcached(final String serverUrl) {
        this(serverUrl, DEFAULT_TIMEOUT);
    }

    /**
     * Creates a new SpyMemcached instance with a specified timeout.
     * The timeout applies to operation timeouts. If Kryo is available on the classpath
     * (checked via ParserFactory.isKryoAvailable()), it will be used for serialization
     * via KryoTranscoder; otherwise, the default SpyMemcached serialization mechanism is used.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SpyMemcached<User> cache = new SpyMemcached<>("localhost:11211", 5000);
     * cache.set("user:123", user, 3600000);
     * }</pre>
     *
     * @param serverUrl the Memcached server URL(s) in format "host1:port1,host2:port2,..."
     * @param timeout the operation timeout in milliseconds (must be positive)
     * @throws IllegalArgumentException if timeout is not positive
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
     * This is a synchronous operation that blocks until complete or timeout.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = cache.get("user:123");
     * if (user != null) {
     *     System.out.println("Found user: " + user.getName());
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be retrieved
     * @return the cached object of type T, or {@code null} if not found or expired
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
     * and retrieve the result when available.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Future<User> future = cache.asyncGet("user:123");
     * User user = future.get(); // Blocks until complete
     * }</pre>
     *
     * @param key the cache key whose associated value is to be retrieved
     * @return a Future that will contain the cached object of type T, or {@code null} if not found or expired
     */
    @SuppressWarnings("unchecked")
    public Future<T> asyncGet(final String key) {
        return (Future<T>) mc.asyncGet(key);
    }

    /**
     * Retrieves multiple objects from the cache in a single operation.
     * This is more efficient than multiple individual get operations.
     * Keys not found in the cache will not be present in the returned map.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, User> users = cache.getBulk("user:1", "user:2", "user:3");
     * }</pre>
     *
     * @param keys the cache keys whose associated values are to be retrieved
     * @return a map containing the found key-value pairs
     */
    @SuppressWarnings("unchecked")
    @Override
    public final Map<String, T> getBulk(final String... keys) {
        return (Map<String, T>) mc.getBulk(keys);
    }

    /**
     * Asynchronously retrieves multiple objects from the cache.
     * The returned Future contains a map of found key-value pairs.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Future<Map<String, User>> future = cache.asyncGetBulk("user:1", "user:2");
     * Map<String, User> users = future.get();
     * }</pre>
     *
     * @param keys the cache keys whose associated values are to be retrieved
     * @return a Future that will contain the map of found key-value pairs
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public final Future<Map<String, T>> asyncGetBulk(final String... keys) {
        return (Future) mc.asyncGetBulk(keys);
    }

    /**
     * Retrieves multiple objects from the cache using a collection of keys.
     * This is more efficient than multiple individual get operations.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> keys = Arrays.asList("user:1", "user:2", "user:3");
     * Map<String, User> users = cache.getBulk(keys);
     * }</pre>
     *
     * @param keys the collection of cache keys whose associated values are to be retrieved
     * @return a map containing the found key-value pairs
     */
    @SuppressWarnings("unchecked")
    @Override
    public Map<String, T> getBulk(final Collection<String> keys) {
        return (Map<String, T>) mc.getBulk(keys);
    }

    /**
     * Asynchronously retrieves multiple objects using a collection of keys.
     * The returned Future contains a map of found key-value pairs.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Set<String> keys = Set.of("user:1", "user:2");
     * Future<Map<String, User>> future = cache.asyncGetBulk(keys);
     * }</pre>
     *
     * @param keys the collection of cache keys whose associated values are to be retrieved
     * @return a Future that will contain the map of found key-value pairs
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Future<Map<String, T>> asyncGetBulk(final Collection<String> keys) {
        return (Future) mc.asyncGetBulk(keys);
    }

    /**
     * Stores an object in the cache with a specified time-to-live.
     * This operation replaces any existing value for the key.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = new User("John", "john@example.com");
     * boolean success = cache.set("user:123", user, 3600000); // 1 hour TTL
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated
     * @param obj the object value to cache
     * @param liveTime the time-to-live in milliseconds (0 means no expiration)
     * @return {@code true} if the operation was successful, {@code false} otherwise
     */
    @Override
    public boolean set(final String key, final T obj, final long liveTime) {
        return resultOf(mc.set(key, toSeconds(liveTime), obj));
    }

    /**
     * Asynchronously stores an object in the cache with a specified time-to-live.
     * The returned Future can be used to check if the operation succeeded.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Future<Boolean> future = cache.asyncSet("user:123", user, 3600000);
     * boolean success = future.get();
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated
     * @param obj the object value to cache
     * @param liveTime the time-to-live in milliseconds
     * @return a Future that will indicate success ({@code true}) or failure ({@code false})
     */
    public Future<Boolean> asyncSet(final String key, final T obj, final long liveTime) {
        return mc.set(key, toSeconds(liveTime), obj);
    }

    /**
     * Adds an object to the cache only if the key doesn't already exist.
     * This operation is atomic.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * if (cache.add("user:123", user, 3600000)) {
     *     System.out.println("User added successfully");
     * }
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated
     * @param obj the object value to cache
     * @param liveTime the time-to-live in milliseconds
     * @return {@code true} if the object was added, {@code false} if the key already exists
     */
    public boolean add(final String key, final T obj, final long liveTime) {
        return resultOf(mc.add(key, toSeconds(liveTime), obj));
    }

    /**
     * Asynchronously adds an object to the cache only if the key doesn't already exist.
     * This operation is atomic.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Future<Boolean> future = cache.asyncAdd("user:123", user, 3600000);
     * if (future.get()) { System.out.println("Added"); }
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated
     * @param obj the object value to cache
     * @param liveTime the time-to-live in milliseconds
     * @return a Future that will indicate {@code true} if the add succeeded, {@code false} if the key already exists
     */
    public Future<Boolean> asyncAdd(final String key, final T obj, final long liveTime) {
        return mc.add(key, toSeconds(liveTime), obj);
    }

    /**
     * Replaces an object in the cache only if the key already exists.
     * This operation is atomic.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * if (cache.replace("user:123", updatedUser, 3600000)) {
     *     System.out.println("User updated");
     * }
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated
     * @param obj the object value to cache
     * @param liveTime the time-to-live in milliseconds
     * @return {@code true} if the object was replaced, {@code false} if the key doesn't exist
     */
    public boolean replace(final String key, final T obj, final long liveTime) {
        return resultOf(mc.replace(key, toSeconds(liveTime), obj));
    }

    /**
     * Asynchronously replaces an object in the cache only if the key already exists.
     * This operation is atomic.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Future<Boolean> future = cache.asyncReplace("user:123", updatedUser, 3600000);
     * boolean replaced = future.get();
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated
     * @param obj the object value to cache
     * @param liveTime the time-to-live in milliseconds
     * @return a Future that will indicate {@code true} if the replacement succeeded, {@code false} if the key doesn't exist
     */
    public Future<Boolean> asyncReplace(final String key, final T obj, final long liveTime) {
        return mc.replace(key, toSeconds(liveTime), obj);
    }

    /**
     * Removes an object from the cache.
     * The return value indicates whether the operation was acknowledged by the server,
     * not whether the key existed. This operation succeeds whether or not the key exists.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * boolean success = cache.delete("user:123");
     * System.out.println("Delete operation sent: " + success);
     * }</pre>
     *
     * @param key the cache key whose associated value is to be removed
     * @return {@code true} if the delete operation was successfully sent to the server
     */
    @Override
    public boolean delete(final String key) {
        return resultOf(mc.delete(key));
    }

    /**
     * Asynchronously removes an object from the cache.
     * The returned Future indicates if the operation succeeded.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Future<Boolean> future = cache.asyncDelete("user:123");
     * boolean deleted = future.get();
     * }</pre>
     *
     * @param key the cache key whose associated value is to be removed
     * @return a Future that will indicate success ({@code true}) or failure ({@code false})
     */
    public Future<Boolean> asyncDelete(final String key) {
        return mc.delete(key);
    }

    /**
     * Atomically increments a numeric value by 1.
     * If the key doesn't exist, the behavior depends on the Memcached server implementation
     * (typically returns -1 or NOT_FOUND). This operation is atomic and thread-safe across all clients.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long pageViews = cache.incr("page:views");
     * System.out.println("Page views: " + pageViews);
     * }</pre>
     *
     * @param key the cache key whose associated value is to be incremented
     * @return the value after increment, or -1 if the key doesn't exist
     */
    @Override
    public long incr(final String key) {
        return mc.incr(key, 1);
    }

    /**
     * Atomically increments a numeric value by a specified amount.
     * If the key doesn't exist, the behavior depends on the Memcached server implementation
     * (typically returns -1 or NOT_FOUND). This operation is atomic and thread-safe across all clients.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long score = cache.incr("player:score", 10);
     * System.out.println("New score: " + score);
     * }</pre>
     *
     * @param key the cache key whose associated value is to be incremented
     * @param delta the amount by which to increment the value (positive value)
     * @return the value after increment, or -1 if the key doesn't exist
     */
    @Override
    public long incr(final String key, final int delta) {
        return mc.incr(key, delta);
    }

    /**
     * Atomically increments a numeric value with a default value if key doesn't exist.
     * If the key doesn't exist, it is created with the defaultValue, then incremented by delta.
     * The value will not expire (passing -1 as expiration to SpyMemcached means no expiration).
     * This operation is atomic and thread-safe across all clients.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long count = cache.incr("counter:views", 1, 0);
     * System.out.println("View count: " + count);
     * }</pre>
     *
     * @param key the cache key whose associated value is to be incremented
     * @param delta the amount by which to increment the value (must be non-negative)
     * @param defaultValue the initial value to set if the key doesn't exist before incrementing
     * @return the value after increment
     */
    public long incr(final String key, final int delta, final long defaultValue) {
        return mc.incr(key, delta, defaultValue, -1);
    }

    /**
     * Atomically increments a numeric value with default value and expiration.
     * If the key doesn't exist, it's created with the defaultValue, then incremented by delta,
     * and the expiration time is set to the specified liveTime. This operation is atomic and
     * thread-safe across all clients.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long count = cache.incr("counter:hourly", 1, 0, 3600000);
     * }</pre>
     *
     * @param key the cache key whose associated value is to be incremented
     * @param delta the amount by which to increment the value (must be non-negative)
     * @param defaultValue the initial value to set if the key doesn't exist before incrementing
     * @param liveTime the time-to-live in milliseconds for the key (converted to seconds for Memcached)
     * @return the value after increment
     */
    public long incr(final String key, final int delta, final long defaultValue, final long liveTime) {
        return mc.incr(key, delta, defaultValue, toSeconds(liveTime));
    }

    /**
     * Atomically decrements a numeric value by 1.
     * If the key doesn't exist, the behavior depends on the Memcached server implementation
     * (typically returns -1 or NOT_FOUND). Values cannot go below 0 (Memcached prevents underflow).
     * This operation is atomic and thread-safe across all clients.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long remainingTokens = cache.decr("api:tokens");
     * if (remainingTokens <= 0) {
     *     System.out.println("Rate limit exceeded");
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be decremented
     * @return the value after decrement, or -1 if the key doesn't exist
     */
    @Override
    public long decr(final String key) {
        return mc.decr(key, 1);
    }

    /**
     * Atomically decrements a numeric value by a specified amount.
     * If the key doesn't exist, the behavior depends on the Memcached server implementation
     * (typically returns -1 or NOT_FOUND). Values cannot go below 0 (Memcached prevents underflow).
     * This operation is atomic and thread-safe across all clients.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long inventory = cache.decr("product:stock", 5);
     * System.out.println("Remaining inventory: " + inventory);
     * }</pre>
     *
     * @param key the cache key whose associated value is to be decremented
     * @param delta the amount by which to decrement the value (positive value)
     * @return the value after decrement, or -1 if the key doesn't exist
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
     * thread-safe across all clients.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long remaining = cache.decr("inventory:item123", 1, 100);
     * System.out.println("Items remaining: " + remaining);
     * }</pre>
     *
     * @param key the cache key whose associated value is to be decremented
     * @param delta the amount by which to decrement the value (must be non-negative)
     * @param defaultValue the initial value to set if the key doesn't exist before decrementing
     * @return the value after decrement (cannot be negative)
     */
    public long decr(final String key, final int delta, final long defaultValue) {
        return mc.decr(key, delta, defaultValue, -1);
    }

    /**
     * Atomically decrements a numeric value with default value and expiration.
     * If the key doesn't exist, it's created with the defaultValue, then decremented by delta,
     * and the expiration time is set to the specified liveTime. Values cannot go below 0
     * (Memcached prevents underflow). This operation is atomic and thread-safe across all clients.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long remaining = cache.decr("quota:user:123", 1, 1000, 86400000);
     * }</pre>
     *
     * @param key the cache key whose associated value is to be decremented
     * @param delta the amount by which to decrement the value (must be non-negative)
     * @param defaultValue the initial value to set if the key doesn't exist before decrementing
     * @param liveTime the time-to-live in milliseconds for the key (converted to seconds for Memcached)
     * @return the value after decrement (cannot be negative)
     */
    public long decr(final String key, final int delta, final long defaultValue, final long liveTime) {
        return mc.decr(key, delta, defaultValue, toSeconds(liveTime));
    }

    /**
     * Flushes all data from all connected Memcached servers immediately.
     * This operation removes all keys from all servers.
     * Use with extreme caution in production environments as this is a destructive operation.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Warning: This removes ALL data from the cache!
     * cache.flushAll();
     * System.out.println("All cache data cleared");
     * }</pre>
     */
    @Override
    public void flushAll() {
        resultOf(mc.flush());
    }

    /**
     * Asynchronously flushes all data from all servers.
     * The returned Future can be used to check when the operation completes.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Future<Boolean> future = cache.asyncFlushAll();
     * boolean flushed = future.get();
     * }</pre>
     *
     * @return a Future that will indicate when the flush completes
     */
    public Future<Boolean> asyncFlushAll() {
        return mc.flush();
    }

    /**
     * Flushes all data from all servers after a specified delay.
     * The flush will occur on all servers after the delay expires.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * cache.flushAll(5000); // Flush after 5 seconds
     * }</pre>
     *
     * @param delay the delay in milliseconds before the flush operation is executed
     * @return {@code true} if the flush was scheduled successfully, {@code false} otherwise
     */
    public boolean flushAll(final long delay) {
        return resultOf(mc.flush(toSeconds(delay)));
    }

    /**
     * Asynchronously schedules a flush after a delay.
     * The flush will occur on all servers after the delay expires.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Future<Boolean> future = cache.asyncFlushAll(10000);
     * future.get(); // Wait for scheduled flush
     * }</pre>
     *
     * @param delay the delay in milliseconds before the flush operation is executed
     * @return a Future that will indicate if the flush was scheduled successfully
     */
    public Future<Boolean> asyncFlushAll(final long delay) {
        return mc.flush(toSeconds(delay));
    }

    /**
     * Disconnects from all Memcached servers and releases resources.
     * After calling this method, the cache client cannot be used anymore.
     * Uses default shutdown timeout.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * cache.disconnect();
     * System.out.println("Cache client disconnected");
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
     * Disconnects from all servers with a specified timeout.
     * Waits up to the timeout for pending operations to complete.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * cache.disconnect(5000); // Wait up to 5 seconds for cleanup
     * }</pre>
     *
     * @param timeout the maximum time to wait for shutdown in milliseconds
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