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
     * The timeout applies to connection and operation timeouts. If Kryo is available,
     * it will be used for serialization; otherwise, the default Java serialization is used.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * SpyMemcached<User> cache = new SpyMemcached<>("localhost:11211", 5000);
     * cache.set("user:123", user, 3600000);
     * }</pre>
     *
     * @param serverUrl the Memcached server URL(s) in format "host1:port1,host2:port2,..."
     * @param timeout the operation timeout in milliseconds
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
     * This is a synchronous operation that blocks until complete.
     *
     * @param key the cache key
     * @return the cached object, or {@code null} if not found or expired
     */
    @SuppressWarnings("unchecked")
    @Override
    public T get(final String key) {
        return (T) mc.get(key);
    }

    /**
     * Asynchronously retrieves an object from the cache by its key.
     * The operation is executed asynchronously by the Memcached client.
     * The returned Future can be used to check completion and get the result.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Future<User> future = cache.asyncGet("user:123");
     * User user = future.get(); // Blocks until complete
     * }</pre>
     *
     * @param key the cache key
     * @return a Future containing the cached object, or {@code null} if not found or expired
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
     * @param keys the cache keys to retrieve
     * @return a map of found key-value pairs
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
     * @param keys the cache keys to retrieve
     * @return a Future containing the map of found items
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
     * @param keys the collection of cache keys to retrieve
     * @return a map of found key-value pairs
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
     * @param keys the collection of cache keys to retrieve
     * @return a Future containing the map of found items
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Future<Map<String, T>> asyncGetBulk(final Collection<String> keys) {
        return (Future) mc.asyncGetBulk(keys);
    }

    /**
     * Stores an object in the cache with a specified time-to-live.
     * This operation replaces any existing value for the key.
     *
     * @param key the cache key
     * @param obj the object to cache
     * @param liveTime the time-to-live in milliseconds
     * @return {@code true} if the operation was successful
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
     * @param key the cache key
     * @param obj the object to cache
     * @param liveTime the time-to-live in milliseconds
     * @return a Future indicating success or failure
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
     * @param key the cache key
     * @param obj the object to cache
     * @param liveTime the time-to-live in milliseconds
     * @return {@code true} if the object was added, {@code false} if key already exists
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
     * @param key the cache key
     * @param obj the object to cache
     * @param liveTime the time-to-live in milliseconds
     * @return a Future indicating if the add succeeded
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
     * @param key the cache key
     * @param obj the object to cache
     * @param liveTime the time-to-live in milliseconds
     * @return {@code true} if the object was replaced, {@code false} if key doesn't exist
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
     * @param key the cache key
     * @param obj the object to cache
     * @param liveTime the time-to-live in milliseconds
     * @return a Future indicating if the replacement succeeded
     */
    public Future<Boolean> asyncReplace(final String key, final T obj, final long liveTime) {
        return mc.replace(key, toSeconds(liveTime), obj);
    }

    /**
     * Removes an object from the cache.
     * This operation succeeds whether or not the key exists.
     *
     * @param key the cache key to delete
     * @return {@code true} if the operation was successful
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
     * @param key the cache key to delete
     * @return a Future indicating success or failure
     */
    public Future<Boolean> asyncDelete(final String key) {
        return mc.delete(key);
    }

    /**
     * Atomically increments a numeric value by 1.
     * If the key doesn't exist, it will be created with value 1.
     * This operation is atomic and thread-safe across all clients.
     *
     * @param key the cache key
     * @return the value after increment
     */
    @Override
    public long incr(final String key) {
        return mc.incr(key, 1);
    }

    /**
     * Atomically increments a numeric value by a specified amount.
     * If the key doesn't exist, it will be created with the delta value.
     * This operation is atomic and thread-safe across all clients.
     *
     * @param key the cache key
     * @param delta the increment amount
     * @return the value after increment
     */
    @Override
    public long incr(final String key, final int delta) {
        return mc.incr(key, delta);
    }

    /**
     * Atomically increments a numeric value with a default value if key doesn't exist.
     * The created value will not expire unless a liveTime is specified.
     * This operation is atomic and thread-safe across all clients.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long count = cache.incr("counter:views", 1, 0);
     * System.out.println("View count: " + count);
     * }</pre>
     *
     * @param key the cache key
     * @param delta the increment amount
     * @param defaultValue the initial value if key doesn't exist
     * @return the value after increment
     */
    public long incr(final String key, final int delta, final long defaultValue) {
        return mc.incr(key, delta, defaultValue, -1);
    }

    /**
     * Atomically increments a numeric value with default value and expiration.
     * If the key doesn't exist, it's created with the default value and TTL.
     * This operation is atomic and thread-safe across all clients.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long count = cache.incr("counter:hourly", 1, 0, 3600000);
     * }</pre>
     *
     * @param key the cache key
     * @param delta the increment amount
     * @param defaultValue the initial value if key doesn't exist
     * @param liveTime the time-to-live in milliseconds for new keys
     * @return the value after increment
     */
    public long incr(final String key, final int delta, final long defaultValue, final long liveTime) {
        return mc.incr(key, delta, defaultValue, toSeconds(liveTime));
    }

    /**
     * Atomically decrements a numeric value by 1.
     * If the key doesn't exist, it will be created with value 0 (Memcached prevents underflow).
     * This operation is atomic and thread-safe across all clients.
     *
     * @param key the cache key
     * @return the value after decrement (0 if key didn't exist)
     */
    @Override
    public long decr(final String key) {
        return mc.decr(key, 1);
    }

    /**
     * Atomically decrements a numeric value by a specified amount.
     * If the key doesn't exist, it will be created with value 0 (Memcached prevents underflow).
     * This operation is atomic and thread-safe across all clients.
     *
     * @param key the cache key
     * @param delta the decrement amount
     * @return the value after decrement (0 if key didn't exist)
     */
    @Override
    public long decr(final String key, final int delta) {
        return mc.decr(key, delta);
    }

    /**
     * Atomically decrements a numeric value with a default value if key doesn't exist.
     * The created value will not expire unless a liveTime is specified.
     * This operation is atomic and thread-safe across all clients.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long remaining = cache.decr("inventory:item123", 1, 100);
     * System.out.println("Items remaining: " + remaining);
     * }</pre>
     *
     * @param key the cache key
     * @param delta the decrement amount
     * @param defaultValue the initial value if key doesn't exist
     * @return the value after decrement
     */
    public long decr(final String key, final int delta, final long defaultValue) {
        return mc.decr(key, delta, defaultValue, -1);
    }

    /**
     * Atomically decrements a numeric value with default value and expiration.
     * If the key doesn't exist, it's created with the default value and TTL.
     * This operation is atomic and thread-safe across all clients.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long remaining = cache.decr("quota:user:123", 1, 1000, 86400000);
     * }</pre>
     *
     * @param key the cache key
     * @param delta the decrement amount
     * @param defaultValue the initial value if key doesn't exist
     * @param liveTime the time-to-live in milliseconds for new keys
     * @return the value after decrement
     */
    public long decr(final String key, final int delta, final long defaultValue, final long liveTime) {
        return mc.decr(key, delta, defaultValue, toSeconds(liveTime));
    }

    /**
     * Flushes all data from all connected Memcached servers immediately.
     * This operation removes all keys from all servers.
     * Use with extreme caution as this is a destructive operation.
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
     * @return a Future indicating when the flush completes
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
     * @param delay the delay in milliseconds before flushing
     * @return {@code true} if the flush was scheduled successfully
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
     * @param delay the delay in milliseconds before flushing
     * @return a Future indicating if the flush was scheduled
     */
    public Future<Boolean> asyncFlushAll(final long delay) {
        return mc.flush(toSeconds(delay));
    }

    /**
     * Disconnects from all Memcached servers and releases resources.
     * After calling this method, the cache client cannot be used anymore.
     * Uses default shutdown timeout.
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
     * @param timeout the maximum time to wait in milliseconds
     */
    public synchronized void disconnect(final long timeout) {
        if (!isShutdown) {
            mc.shutdown(timeout, TimeUnit.MILLISECONDS);
            isShutdown = true;
        }
    }

    /**
     * Waits for a Future to complete and returns its result.
     * Converts checked exceptions to runtime exceptions.
     *
     * @param <R> the type of the result returned by the Future
     * @param future the Future to wait for
     * @return the Future's result
     * @throws RuntimeException if the Future fails
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
     * This method handles the IOException that may occur during client creation.
     *
     * @param serverUrl the server URL(s) to connect to
     * @param connFactory the connection factory with timeout and transcoder settings
     * @return a configured MemcachedClient instance
     * @throws UncheckedIOException if connection fails
     */
    protected static MemcachedClient createSpyMemcachedClient(final String serverUrl, final ConnectionFactory connFactory) throws UncheckedIOException {
        try {
            return new MemcachedClient(connFactory, AddrUtil.getAddressList(serverUrl));
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to create Memcached client.", e);
        }
    }
}