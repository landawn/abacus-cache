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
 * @param <T> the type of objects to be cached
 * @see AbstractDistributedCacheClient
 * @see MemcachedClient
 */
public class SpyMemcached<T> extends AbstractDistributedCacheClient<T> {

    static final Logger logger = LoggerFactory.getLogger(SpyMemcached.class);

    private final MemcachedClient mc;

    /**
     * Creates a new SpyMemcached instance with the default timeout.
     * The server URL should contain comma-separated host:port pairs for multiple servers.
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
     * @param serverUrl the Memcached server URL(s) in format "host1:port1,host2:port2,..."
     * @param timeout the operation timeout in milliseconds
     */
    public SpyMemcached(final String serverUrl, final long timeout) {
        super(serverUrl);

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

        mc = createSpyMemcachedClient(serverUrl, connFactory);
    }

    /**
     * Retrieves an object from the cache by its key.
     * This is a synchronous operation that blocks until complete.
     *
     * @param key the cache key
     * @return the cached object, or null if not found
     */
    @SuppressWarnings("unchecked")
    @Override
    public T get(final String key) {
        return (T) mc.get(key);
    }

    /**
     * Asynchronously retrieves an object from the cache by its key.
     * The returned Future can be used to check completion and get the result.
     *
     * @param key the cache key
     * @return a Future containing the cached object
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
     * @return true if the operation was successful
     */
    @Override
    public boolean set(final String key, final T obj, final long liveTime) {
        return resultOf(mc.set(key, toSeconds(liveTime), obj));
    }

    /**
     * Asynchronously stores an object in the cache.
     * The returned Future can be used to check if the operation succeeded.
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
     * @param key the cache key
     * @param obj the object to cache
     * @param liveTime the time-to-live in milliseconds
     * @return true if the object was added, false if key already exists
     */
    public boolean add(final String key, final T obj, final long liveTime) {
        return resultOf(mc.add(key, toSeconds(liveTime), obj));
    }

    /**
     * Asynchronously adds an object only if the key doesn't exist.
     * This operation is atomic.
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
     * @param key the cache key
     * @param obj the object to cache
     * @param liveTime the time-to-live in milliseconds
     * @return true if the object was replaced, false if key doesn't exist
     */
    public boolean replace(final String key, final T obj, final long liveTime) {
        return resultOf(mc.replace(key, toSeconds(liveTime), obj));
    }

    /**
     * Asynchronously replaces an object only if the key exists.
     * This operation is atomic.
     *
     * @param key the cache key
     * @param obj the object to cache
     * @param liveTime the time-to-live in milliseconds
     * @return a Future indicating if the replace succeeded
     */
    public Future<Boolean> asyncReplace(final String key, final T obj, final long liveTime) {
        return mc.replace(key, toSeconds(liveTime), obj);
    }

    /**
     * Removes an object from the cache.
     * This operation succeeds whether or not the key exists.
     *
     * @param key the cache key to delete
     * @return true if the operation was successful
     */
    @Override
    public boolean delete(final String key) {
        return resultOf(mc.delete(key));
    }

    /**
     * Asynchronously removes an object from the cache.
     * The returned Future indicates if the operation succeeded.
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
     *
     * @param key the cache key
     * @param deta the increment amount
     * @return the value after increment
     */
    @Override
    public long incr(final String key, final int deta) {
        return mc.incr(key, deta);
    }

    /**
     * Atomically increments a numeric value with a default value if key doesn't exist.
     * The created value will not expire.
     *
     * @param key the cache key
     * @param deta the increment amount
     * @param defaultValue the initial value if key doesn't exist
     * @return the value after increment
     */
    public long incr(final String key, final int deta, final long defaultValue) {
        return mc.incr(key, deta, defaultValue, -1);
    }

    /**
     * Atomically increments a numeric value with default value and expiration.
     * If the key doesn't exist, it's created with the default value and TTL.
     *
     * @param key the cache key
     * @param deta the increment amount
     * @param defaultValue the initial value if key doesn't exist
     * @param liveTime the time-to-live in milliseconds for new keys
     * @return the value after increment
     */
    public long incr(final String key, final int deta, final long defaultValue, final long liveTime) {
        return mc.incr(key, deta, defaultValue, toSeconds(liveTime));
    }

    /**
     * Atomically decrements a numeric value by 1.
     * If the key doesn't exist, it will be created with value 0.
     *
     * @param key the cache key
     * @return the value after decrement
     */
    @Override
    public long decr(final String key) {
        return mc.decr(key, 1);
    }

    /**
     * Atomically decrements a numeric value by a specified amount.
     * If the key doesn't exist, it will be created with value 0.
     *
     * @param key the cache key
     * @param deta the decrement amount
     * @return the value after decrement
     */
    @Override
    public long decr(final String key, final int deta) {
        return mc.decr(key, deta);
    }

    /**
     * Atomically decrements a numeric value with a default value if key doesn't exist.
     * The created value will not expire.
     *
     * @param key the cache key
     * @param deta the decrement amount
     * @param defaultValue the initial value if key doesn't exist
     * @return the value after decrement
     */
    public long decr(final String key, final int deta, final long defaultValue) {
        return mc.decr(key, deta, defaultValue, -1);
    }

    /**
     * Atomically decrements a numeric value with default value and expiration.
     * If the key doesn't exist, it's created with the default value and TTL.
     *
     * @param key the cache key
     * @param deta the decrement amount
     * @param defaultValue the initial value if key doesn't exist
     * @param liveTime the time-to-live in milliseconds for new keys
     * @return the value after decrement
     */
    public long decr(final String key, final int deta, final long defaultValue, final long liveTime) {
        return mc.decr(key, deta, defaultValue, toSeconds(liveTime));
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
     * @return a Future indicating when the flush completes
     */
    public Future<Boolean> asyncFlushAll() {
        return mc.flush();
    }

    /**
     * Flushes all data from all servers after a specified delay.
     * The flush will occur on all servers after the delay expires.
     *
     * @param delay the delay in milliseconds before flushing
     * @return true if the flush was scheduled successfully
     */
    public boolean flushAll(final long delay) {
        return resultOf(mc.flush(toSeconds(delay)));
    }

    /**
     * Asynchronously schedules a flush after a delay.
     * The flush will occur on all servers after the delay expires.
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
    public void disconnect() {
        mc.shutdown();
    }

    /**
     * Disconnects from all servers with a specified timeout.
     * Waits up to the timeout for pending operations to complete.
     *
     * @param timeout the maximum time to wait in milliseconds
     */
    public void disconnect(final long timeout) {
        mc.shutdown(timeout, TimeUnit.MICROSECONDS);
    }

    /**
     * Waits for a Future to complete and returns its result.
     * Converts checked exceptions to runtime exceptions.
     *
     * @param <R> the result type
     * @param future the Future to wait for
     * @return the Future's result
     * @throws RuntimeException if the Future fails
     */
    protected <R> R resultOf(final Future<R> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw ExceptionUtil.toRuntimeException(e, true);
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