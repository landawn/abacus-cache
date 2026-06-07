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
import java.util.concurrent.TimeoutException;

import com.landawn.abacus.exception.UncheckedIOException;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.parser.ParserFactory;
import com.landawn.abacus.util.AddrUtil;
import com.landawn.abacus.util.ExceptionUtil;
import com.landawn.abacus.util.N;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

/**
 * A Memcached distributed cache client implementation backed by the SpyMemcached library.
 * Provides synchronous and asynchronous access to one or more Memcached servers. When the
 * Kryo parser is available on the classpath ({@link ParserFactory#isKryoParserAvailable()}),
 * a {@link KryoTranscoder} is installed for serialization; otherwise the default SpyMemcached
 * transcoder is used.
 *
 * <p>Key features:
 * <ul>
 * <li>Synchronous and {@link Future}-based asynchronous operations.</li>
 * <li>Bulk get operations to reduce network round-trips.</li>
 * <li>Atomic increment/decrement operations.</li>
 * <li>Configurable operation timeout.</li>
 * </ul>
 *
 * <p><b>Thread Safety:</b> Instances of this class are safe for concurrent use by multiple
 * threads. The wrapper delegates to a single shared {@link MemcachedClient}, which performs
 * network I/O on a dedicated selector thread and serializes commands through its internal
 * operation queue; concurrent calls from application threads enqueue operations safely.
 *
 * <p>Example usage:
 * <pre>{@code
 * SpyMemcached<User> cache = new SpyMemcached<>("localhost:11211");
 *
 * // Synchronous operations
 * cache.set("user:123", user, 3600000);   // Cache for 1 hour
 * User cached = cache.get("user:123");
 *
 * // Asynchronous operations
 * Future<Boolean> future = cache.asyncSet("user:456", anotherUser, 3600000);
 * boolean success = future.get();   // Wait for completion
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
    private static final int MEMCACHED_MAX_RELATIVE_EXPIRATION_SECONDS = 30 * 24 * 60 * 60;

    private final MemcachedClient mc;
    private final long operationTimeout;
    private volatile boolean isShutdown = false;

    /**
     * Creates a new {@code SpyMemcached} instance using {@link DistributedCacheClient#DEFAULT_TIMEOUT}
     * as the operation timeout.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * SpyMemcached<String> cache = new SpyMemcached<>("localhost:11211");
     * cache.set("key1", "value1", 3600000);
     * }</pre>
     *
     * @param serverUrl the Memcached server URL(s) in the format
     *                  {@code "host1:port1,host2:port2,..."}; must not be {@code null}, empty, or blank
     * @throws IllegalArgumentException if {@code serverUrl} is {@code null}, empty, blank, or contains
     *         no valid server addresses
     * @throws RuntimeException if {@code serverUrl} cannot be parsed or the connection to the
     *         Memcached server(s) fails
     * @see #SpyMemcached(String, long)
     */
    public SpyMemcached(final String serverUrl) {
        this(serverUrl, DEFAULT_TIMEOUT);
    }

    /**
     * Creates a new {@code SpyMemcached} instance with the specified operation timeout.
     * The timeout applies to all cache operations (get, set, delete, etc.). If Kryo is available
     * on the classpath (checked via {@link ParserFactory#isKryoParserAvailable()}), it is used
     * for object serialization via {@link KryoTranscoder}; otherwise the default SpyMemcached
     * serialization mechanism is used.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Create cache with 5-second operation timeout
     * SpyMemcached<User> cache = new SpyMemcached<>("localhost:11211", 5000);
     * cache.set("user:123", user, 3600000);
     * }</pre>
     *
     * @param serverUrl the Memcached server URL(s) in the format
     *                  {@code "host1:port1,host2:port2,..."}; must not be {@code null}, empty, or blank
     * @param timeout the operation timeout in milliseconds; must be positive. Applies to all cache operations.
     * @throws IllegalArgumentException if {@code timeout} is not positive, or if {@code serverUrl}
     *         is {@code null}, empty, blank, or contains no valid server addresses
     * @throws RuntimeException if {@code serverUrl} cannot be parsed or the connection to the
     *         Memcached server(s) fails
     */
    public SpyMemcached(final String serverUrl, final long timeout) {
        super(serverUrl);

        N.checkArgPositive(timeout, "timeout");

        if (N.isEmpty(AddrUtil.getAddressList(serverUrl))) {
            throw new IllegalArgumentException("No valid server addresses found in: " + serverUrl);
        }

        this.operationTimeout = timeout;

        MemcachedClient tempMc = null;
        try {
            final Transcoder<Object> transcoder = ParserFactory.isKryoParserAvailable() ? new KryoTranscoder<>() : null;

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
            if (logger.isWarnEnabled()) {
                logger.warn("Failed to create SpyMemcached client for server(s): " + serverUrl + " (timeout=" + timeout + "ms)", e);
            }

            if (tempMc != null) {
                tempMc.shutdown();
            }
            throw ExceptionUtil.toRuntimeException(e);
        }
    }

    /**
     * Retrieves an object from the cache by its key.
     * This is a synchronous operation that blocks until complete or timeout is reached.
     * The operation timeout is configured during client construction.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple get operation
     * User user = cache.get("user:123");
     * if (user != null) {
     *     System.out.println("Found user: " + user.getName());
     * } else {
     *     System.out.println("User not found in cache");
     * }
     *
     * // Get with fallback to database
     * User user = cache.get("user:123");
     * if (user == null) {
     *     user = database.findUser(123);
     *     cache.set("user:123", user, 3600000);
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be retrieved; must not be {@code null}
     * @return the cached object of type {@code T}, or {@code null} if not found, expired, or evicted
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    @SuppressWarnings("unchecked")
    @Override
    public T get(final String key) {
        N.checkArgNotNull(key, "key");
        return (T) mc.get(key);
    }

    /**
     * Asynchronously retrieves an object from the cache by its key.
     * The operation is executed asynchronously by the underlying SpyMemcached client
     * and returns immediately. The returned Future can be used to check completion status
     * and retrieve the result when available. The operation timeout is configured during
     * client construction.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple async get
     * Future<User> future = cache.asyncGet("user:123");
     * User user = future.get();   // Blocks until complete
     *
     * // Async get with timeout (requires: import java.util.concurrent.TimeoutException;)
     * Future<User> future = cache.asyncGet("user:123");
     * try {
     *     User user = future.get(1000, TimeUnit.MILLISECONDS);
     * } catch (TimeoutException e) {
     *     System.out.println("Get operation timed out");
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be retrieved; must not be {@code null}
     * @return a {@link Future} that will yield the cached object of type {@code T}, or
     *         {@code null} if not found, expired, or evicted
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the operation fails to initiate
     */
    @SuppressWarnings("unchecked")
    public Future<T> asyncGet(final String key) {
        N.checkArgNotNull(key, "key");
        return (Future<T>) mc.asyncGet(key);
    }

    /**
     * Retrieves multiple objects from the cache in a single operation.
     * This is more efficient than multiple individual get operations as it uses a single
     * network round-trip to fetch all values. Keys not found in the cache or that have expired
     * will not be present in the returned map. This is a synchronous operation that blocks until
     * complete or timeout.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Basic bulk get
     * Map<String, User> users = cache.getBulk("user:1", "user:2", "user:3");
     * users.forEach((key, user) -> System.out.println(key + ": " + user.getName()));
     *
     * // Bulk get with missing key handling
     * Map<String, Product> products = cache.getBulk("prod:1", "prod:2", "prod:3");
     * System.out.println("Found " + products.size() + " out of 3 products");
     *
     * // Identify missing keys
     * String[] requestedKeys = {"user:1", "user:2", "user:3"};
     * Map<String, User> found = cache.getBulk(requestedKeys);
     * Arrays.stream(requestedKeys)
     *       .filter(key -> !found.containsKey(key))
     *       .forEach(key -> System.out.println("Missing: " + key));
     * }</pre>
     *
     * @param keys the cache keys whose associated values are to be retrieved; must not be
     *             {@code null} and must not contain {@code null} elements
     * @return a map containing the found key-value pairs; never {@code null}, but possibly empty
     *         if no keys are found
     * @throws IllegalArgumentException if {@code keys} is {@code null} or contains {@code null} elements
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    @SuppressWarnings("unchecked")
    @Override
    public final Map<String, T> getBulk(final String... keys) {
        checkBulkKeys(keys);
        return (Map<String, T>) mc.getBulk(keys);
    }

    /**
     * Asynchronously retrieves multiple objects from the cache.
     * This method returns immediately without blocking. The returned Future contains a map of
     * found key-value pairs. Keys not found in the cache or that have expired will not be present
     * in the returned map. This operation is more efficient than multiple individual async get
     * operations as it uses a single network round-trip.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple async bulk get
     * Future<Map<String, User>> future = cache.asyncGetBulk("user:1", "user:2");
     * Map<String, User> users = future.get();   // Blocks until complete
     *
     * // Async bulk get with timeout (requires: import java.util.concurrent.TimeoutException;)
     * Future<Map<String, User>> future = cache.asyncGetBulk("user:1", "user:2", "user:3");
     * try {
     *     Map<String, User> users = future.get(2000, TimeUnit.MILLISECONDS);
     *     System.out.println("Retrieved " + users.size() + " users");
     * } catch (TimeoutException e) {
     *     future.cancel(true);
     * }
     * }</pre>
     *
     * @param keys the cache keys whose associated values are to be retrieved; must not be
     *             {@code null} and must not contain {@code null} elements
     * @return a {@link Future} that will yield the map of found key-value pairs; the map is
     *         never {@code null} but may be empty
     * @throws IllegalArgumentException if {@code keys} is {@code null} or contains {@code null} elements
     * @throws RuntimeException if the operation fails to initiate
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public final Future<Map<String, T>> asyncGetBulk(final String... keys) {
        checkBulkKeys(keys);
        return (Future) mc.asyncGetBulk(keys);
    }

    /**
     * Retrieves multiple objects from the cache using a collection of keys.
     * This is more efficient than multiple individual get operations as it uses a single
     * network round-trip to fetch all values. Keys not found in the cache or that have expired
     * will not be present in the returned map. This is a synchronous operation that blocks until
     * complete or timeout.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Using a List (requires: import java.util.List;)
     * List<String> userKeys = Arrays.asList("user:123", "user:456", "user:789");
     * Map<String, User> users = cache.getBulk(userKeys);
     *
     * // Using a Set (requires: import java.util.Set; import java.util.HashSet;)
     * Set<String> keySet = new HashSet<>(Arrays.asList("session:1", "session:2"));
     * Map<String, Session> sessions = cache.getBulk(keySet);
     *
     * // Dynamically built key collection (requires: import java.util.stream.Collectors;)
     * List<Integer> userIds = Arrays.asList(101, 102, 103);
     * List<String> keys = userIds.stream()
     *                            .map(id -> "user:" + id)
     *                            .collect(Collectors.toList());
     * Map<String, User> users = cache.getBulk(keys);
     * }</pre>
     *
     * @param keys the collection of cache keys whose associated values are to be retrieved;
     *             must not be {@code null} and must not contain {@code null} elements
     * @return a map containing the found key-value pairs; never {@code null}, but possibly empty
     *         if no keys are found
     * @throws IllegalArgumentException if {@code keys} is {@code null} or contains {@code null} elements
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    @SuppressWarnings("unchecked")
    @Override
    public Map<String, T> getBulk(final Collection<String> keys) {
        checkBulkKeys(keys);
        return (Map<String, T>) mc.getBulk(keys);
    }

    /**
     * Asynchronously retrieves multiple objects using a collection of keys.
     * This method returns immediately without blocking. The returned Future contains a map of
     * found key-value pairs. Keys not found in the cache or that have expired will not be present
     * in the returned map. This operation is more efficient than multiple individual async get
     * operations as it uses a single network round-trip.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple async bulk get with collection (requires: import java.util.Set;)
     * Set<String> keys = new HashSet<>(Arrays.asList("user:1", "user:2"));
     * Future<Map<String, User>> future = cache.asyncGetBulk(keys);
     * Map<String, User> users = future.get();   // Blocks until complete
     *
     * // Async bulk get from dynamically generated keys (requires: import java.util.concurrent.TimeoutException;)
     * List<String> productKeys = generateProductKeys();
     * Future<Map<String, Product>> future = cache.asyncGetBulk(productKeys);
     * Map<String, Product> products = future.get(3000, TimeUnit.MILLISECONDS);
     * }</pre>
     *
     * @param keys the collection of cache keys whose associated values are to be retrieved;
     *             must not be {@code null} and must not contain {@code null} elements
     * @return a {@link Future} that will yield the map of found key-value pairs; the map is
     *         never {@code null} but may be empty
     * @throws IllegalArgumentException if {@code keys} is {@code null} or contains {@code null} elements
     * @throws RuntimeException if the operation fails to initiate
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Future<Map<String, T>> asyncGetBulk(final Collection<String> keys) {
        checkBulkKeys(keys);
        return (Future) mc.asyncGetBulk(keys);
    }

    /**
     * Stores an object in the cache with a specified time-to-live.
     * This operation replaces any existing value for the key. The method blocks until
     * the operation completes or times out. The liveTime is converted from milliseconds to
     * seconds for Memcached (rounded up if not exact).
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.
     * When multiple clients set the same key concurrently, the last write wins.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Cache with 1 hour TTL
     * User user = new User("John", "john@example.com");
     * boolean success = cache.set("user:123", user, 3600000);
     * if (success) {
     *     System.out.println("User cached successfully");
     * }
     *
     * // Cache session data with 30 minute TTL
     * Session session = new Session("abc123", user);
     * cache.set("session:" + session.getId(), session, 1800000);
     *
     * // Cache with no expiration
     * Config config = loadConfig();
     * cache.set("app:config", config, 0);   // No expiration
     *
     * // Updating existing value
     * Product product = cache.get("product:456");
     * product.setPrice(99.99);
     * cache.set("product:456", product, 7200000);   // 2 hour TTL
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated; must not be {@code null}
     * @param value the value to cache; may be {@code null}
     * @param liveTime the time-to-live in milliseconds ({@code 0} or negative means no expiration); a
     *                 positive value is converted to seconds, rounded up if not exact.
     * @return {@code true} if the operation succeeded; {@code false} otherwise
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    @Override
    public boolean set(final String key, final T value, final long liveTime) {
        N.checkArgNotNull(key, "key");
        return resultOf(mc.set(key, toMemcachedExpiration(liveTime), value));
    }

    /**
     * Asynchronously stores an object in the cache with a specified time-to-live.
     * The returned Future can be used to check if the operation succeeded. This method
     * returns immediately without blocking. The liveTime is converted from milliseconds to
     * seconds for Memcached (rounded up if not exact).
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple async set
     * Future<Boolean> future = cache.asyncSet("user:123", user, 3600000);
     * boolean success = future.get();   // Blocks until complete
     * if (success) {
     *     System.out.println("Set operation succeeded");
     * }
     *
     * // Async set with timeout (requires: import java.util.concurrent.TimeoutException;)
     * Future<Boolean> future = cache.asyncSet("product:456", product, 7200000);
     * try {
     *     boolean success = future.get(1000, TimeUnit.MILLISECONDS);
     * } catch (TimeoutException e) {
     *     future.cancel(true);
     *     System.err.println("Set operation timed out");
     * }
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated; must not be {@code null}
     * @param value the value to cache; may be {@code null}
     * @param liveTime the time-to-live in milliseconds ({@code 0} or negative means no expiration); a
     *                 positive value is converted to seconds, rounded up if not exact.
     * @return a {@link Future} that will yield {@code true} on success or {@code false} on failure
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the operation fails to initiate
     */
    public Future<Boolean> asyncSet(final String key, final T value, final long liveTime) {
        N.checkArgNotNull(key, "key");
        return mc.set(key, toMemcachedExpiration(liveTime), value);
    }

    /**
     * Adds an object to the cache only if the key doesn't already exist.
     * This operation is atomic and thread-safe across all distributed cache clients. The method blocks until
     * the operation completes or times out. If the key already exists, this operation will
     * fail and return {@code false}. The liveTime is converted from milliseconds to seconds
     * for Memcached (rounded up if not exact).
     *
     * <p><b>Thread Safety:</b> This operation is atomic, ensuring that in concurrent scenarios, only one client
     * will successfully add the key while others will receive {@code false}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple conditional add
     * if (cache.add("user:123", user, 3600000)) {
     *     System.out.println("User added successfully");
     * } else {
     *     System.out.println("User already exists in cache");
     * }
     *
     * // Distributed locking pattern
     * String lockKey = "lock:resource:123";
     * if (cache.add(lockKey, "locked", 30000)) { // 30 second lock
     *     try {
     *         // Critical section - only one client can execute this
     *         performCriticalOperation();
     *     } finally {
     *         cache.delete(lockKey);
     *     }
     * } else {
     *     System.out.println("Resource is locked by another process");
     * }
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated; must not be {@code null}
     * @param value the value to cache; may be {@code null}
     * @param liveTime the time-to-live in milliseconds ({@code 0} or negative means no expiration); a
     *                 positive value is converted to seconds, rounded up if not exact.
     * @return {@code true} if the object was added; {@code false} if the key already exists
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    public boolean add(final String key, final T value, final long liveTime) {
        N.checkArgNotNull(key, "key");
        return resultOf(mc.add(key, toMemcachedExpiration(liveTime), value));
    }

    /**
     * Asynchronously adds an object to the cache only if the key doesn't already exist.
     * This operation is atomic and thread-safe across all distributed cache clients. The method returns immediately
     * without blocking. If the key already exists, the Future will contain {@code false}. The liveTime
     * is converted from milliseconds to seconds for Memcached (rounded up if not exact).
     *
     * <p><b>Thread Safety:</b> This operation is atomic, ensuring that in concurrent scenarios, only one client
     * will successfully add the key while others will receive {@code false}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple async add
     * Future<Boolean> future = cache.asyncAdd("user:123", user, 3600000);
     * if (future.get()) {
     *     System.out.println("Added");
     * } else {
     *     System.out.println("Key already exists");
     * }
     *
     * // Async add with timeout (requires: import java.util.concurrent.TimeoutException;)
     * Future<Boolean> future = cache.asyncAdd("session:abc", session, 1800000);
     * try {
     *     boolean added = future.get(500, TimeUnit.MILLISECONDS);
     *     System.out.println("Add successful: " + added);
     * } catch (TimeoutException e) {
     *     future.cancel(true);
     * }
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated; must not be {@code null}
     * @param value the value to cache; may be {@code null}
     * @param liveTime the time-to-live in milliseconds ({@code 0} or negative means no expiration); a
     *                 positive value is converted to seconds, rounded up if not exact.
     * @return a {@link Future} that will yield {@code true} if the add succeeded, or {@code false}
     *         if the key already exists
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the operation fails to initiate
     */
    public Future<Boolean> asyncAdd(final String key, final T value, final long liveTime) {
        N.checkArgNotNull(key, "key");
        return mc.add(key, toMemcachedExpiration(liveTime), value);
    }

    /**
     * Replaces an object in the cache only if the key already exists.
     * This operation is atomic and thread-safe across all distributed cache clients. The method blocks until
     * the operation completes or times out. If the key doesn't exist, this operation will
     * fail and return {@code false}. The liveTime is converted from milliseconds to seconds
     * for Memcached (rounded up if not exact).
     *
     * <p><b>Thread Safety:</b> This operation is atomic, ensuring that updates are applied atomically even in
     * concurrent scenarios.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple replace operation
     * if (cache.replace("user:123", updatedUser, 3600000)) {
     *     System.out.println("User updated");
     * } else {
     *     System.out.println("User not found in cache");
     * }
     *
     * // Update existing cache entry
     * User user = cache.get("user:456");
     * if (user != null) {
     *     user.setLastAccess(System.currentTimeMillis());
     *     if (cache.replace("user:456", user, 7200000)) {
     *         System.out.println("User access time updated");
     *     }
     * }
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated; must not be {@code null}
     * @param value the value to cache; may be {@code null}
     * @param liveTime the time-to-live in milliseconds ({@code 0} or negative means no expiration); a
     *                 positive value is converted to seconds, rounded up if not exact.
     * @return {@code true} if the object was replaced; {@code false} if the key does not exist
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    public boolean replace(final String key, final T value, final long liveTime) {
        N.checkArgNotNull(key, "key");
        return resultOf(mc.replace(key, toMemcachedExpiration(liveTime), value));
    }

    /**
     * Asynchronously replaces an object in the cache only if the key already exists.
     * This operation is atomic and thread-safe across all distributed cache clients. The method returns immediately
     * without blocking. If the key doesn't exist, the Future will contain {@code false}. The liveTime
     * is converted from milliseconds to seconds for Memcached (rounded up if not exact).
     *
     * <p><b>Thread Safety:</b> This operation is atomic, ensuring that updates are applied atomically even in
     * concurrent scenarios.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple async replace
     * Future<Boolean> future = cache.asyncReplace("user:123", updatedUser, 3600000);
     * boolean replaced = future.get();
     * if (replaced) {
     *     System.out.println("Replaced successfully");
     * }
     *
     * // Async replace with timeout (requires: import java.util.concurrent.TimeoutException;)
     * Future<Boolean> future = cache.asyncReplace("config:app", newConfig, 86400000);
     * try {
     *     boolean replaced = future.get(1000, TimeUnit.MILLISECONDS);
     *     System.out.println("Config replaced: " + replaced);
     * } catch (TimeoutException e) {
     *     future.cancel(true);
     * }
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated; must not be {@code null}
     * @param value the value to cache; may be {@code null}
     * @param liveTime the time-to-live in milliseconds ({@code 0} or negative means no expiration); a
     *                 positive value is converted to seconds, rounded up if not exact.
     * @return a {@link Future} that will yield {@code true} if the replacement succeeded, or
     *         {@code false} if the key does not exist
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the operation fails to initiate
     */
    public Future<Boolean> asyncReplace(final String key, final T value, final long liveTime) {
        N.checkArgNotNull(key, "key");
        return mc.replace(key, toMemcachedExpiration(liveTime), value);
    }

    /**
     * Removes an object from the cache.
     * The method blocks until the operation completes or times out. The return value reflects the
     * server's response: {@code true} when the key existed and was removed (a {@code DELETED}
     * response), {@code false} when the key was not found ({@code NOT_FOUND}).
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple delete
     * boolean success = cache.delete("user:123");
     * System.out.println("Key removed: " + success);
     *
     * // Delete after update
     * User user = cache.get("user:456");
     * if (user != null && user.isInactive()) {
     *     cache.delete("user:456");
     * }
     *
     * // Delete multiple keys
     * String[] keysToDelete = {"session:1", "session:2", "session:3"};
     * Arrays.stream(keysToDelete).forEach(cache::delete);
     *
     * // Invalidate cache on entity update
     * void updateUser(User user) {
     *     database.save(user);
     *     cache.delete("user:" + user.getId());   // Invalidate cache
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be removed; must not be {@code null}
     * @return {@code true} if the key existed and was removed; {@code false} if the key was not found
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    @Override
    public boolean delete(final String key) {
        N.checkArgNotNull(key, "key");
        return resultOf(mc.delete(key));
    }

    /**
     * Asynchronously removes an object from the cache.
     * The method returns immediately without blocking. The returned Future yields {@code true} when
     * the key existed and was removed (a {@code DELETED} response), or {@code false} when the key was
     * not found ({@code NOT_FOUND}).
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple async delete
     * Future<Boolean> future = cache.asyncDelete("user:123");
     * boolean deleted = future.get();
     * if (deleted) {
     *     System.out.println("Delete operation acknowledged");
     * }
     *
     * // Async delete with timeout (requires: import java.util.concurrent.TimeoutException;)
     * Future<Boolean> future = cache.asyncDelete("session:abc");
     * try {
     *     boolean deleted = future.get(500, TimeUnit.MILLISECONDS);
     *     System.out.println("Session deleted: " + deleted);
     * } catch (TimeoutException e) {
     *     future.cancel(true);
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be removed; must not be {@code null}
     * @return a {@link Future} that will yield {@code true} if the key existed and was removed, or
     *         {@code false} if the key was not found
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the operation fails to initiate
     */
    public Future<Boolean> asyncDelete(final String key) {
        N.checkArgNotNull(key, "key");
        return mc.delete(key);
    }

    /**
     * Atomically increments a numeric value by 1.
     *
     * <p><b>Memcached-Specific Behavior:</b> If the key doesn't exist, returns -1. Only works with string
     * representations of 64-bit unsigned integers stored in Memcached. The value must be stored as a
     * decimal string representation.
     *
     * <p><b>Thread Safety:</b> This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent increment operations are guaranteed to be serialized correctly,
     * ensuring no increments are lost.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple counter
     * long pageViews = cache.incr("page:views");
     * if (pageViews != -1) {
     *     System.out.println("Page views: " + pageViews);
     * } else {
     *     // Initialize counter if it doesn't exist
     *     cache.set("page:views", "1", 0);
     * }
     *
     * // Rate limiting
     * String key = "rate:limit:" + userId;
     * long attempts = cache.incr(key);
     * if (attempts == -1) {
     *     // Key doesn't exist, initialize it
     *     cache.set(key, "1", 60000);
     *     attempts = 1;
     * }
     * if (attempts > MAX_ATTEMPTS) {
     *     throw new RateLimitException("Too many requests");
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be incremented; must not be {@code null}
     * @return the value after the increment, or {@code -1} if the key does not exist
     *         (Memcached returns {@code -1} for non-existent keys)
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    @Override
    public long incr(final String key) {
        N.checkArgNotNull(key, "key");
        return mc.incr(key, 1);
    }

    /**
     * Atomically increments a numeric value by a specified amount.
     *
     * <p><b>Memcached-Specific Behavior:</b> If the key doesn't exist, returns -1. Only works with string
     * representations of 64-bit unsigned integers stored in Memcached. The value must be stored as a
     * decimal string representation.
     *
     * <p><b>Thread Safety:</b> This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent increment operations are guaranteed to be serialized correctly,
     * ensuring no increments are lost.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Game score increment
     * long score = cache.incr("player:score", 10);
     * if (score != -1) {
     *     System.out.println("New score: " + score);
     * } else {
     *     // Initialize if doesn't exist
     *     cache.set("player:score", "10", 0);
     * }
     *
     * // Batch processing counter
     * long processed = cache.incr("batch:processed", 100);
     *
     * // Points system
     * int points = calculatePoints(action);
     * long totalPoints = cache.incr("user:points:" + userId, points);
     *
     * // Bandwidth tracking
     * long bytesTransferred = cache.incr("bandwidth:today", fileSize);
     * if (bytesTransferred > QUOTA) {
     *     logger.warn("Bandwidth quota exceeded");
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be incremented; must not be {@code null}
     * @param delta the amount by which to increment the value; must be non-negative
     * @return the value after the increment, or {@code -1} if the key does not exist
     *         (Memcached returns {@code -1} for non-existent keys)
     * @throws IllegalArgumentException if {@code key} is {@code null} or {@code delta} is negative
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    @Override
    public long incr(final String key, final int delta) {
        N.checkArgNotNull(key, "key");
        N.checkArgNotNegative(delta, "delta");
        return mc.incr(key, delta);
    }

    /**
     * Atomically increments a numeric value, initializing it to {@code defaultValue} when the key
     * doesn't exist. The newly-created entry will not expire (this implementation passes {@code 0}
     * as the expiration to Memcached, which means "no expiration").
     *
     * <p><b>Memcached-Specific Behavior:</b> Unlike {@link #incr(String)} and {@link #incr(String, int)},
     * which return {@code -1} when the key is absent, this overload first-writes the key with
     * {@code defaultValue} when missing. Per the SpyMemcached / Memcached binary-protocol contract,
     * <b>the increment is NOT applied on the initial insert</b>: when the key is absent the stored
     * value is {@code defaultValue} and that same {@code defaultValue} is returned (not
     * {@code defaultValue + delta}). The {@code delta} only takes effect on subsequent calls when the
     * key already exists. Values are stored as 64-bit unsigned integers in decimal string format.
     *
     * <p><b>Thread Safety:</b> This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent increment operations are guaranteed to be serialized correctly,
     * ensuring no increments are lost.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // First call when key is absent: stores 0, returns 0 (delta is NOT applied on init).
     * long count = cache.incr("counter:views", 1, 0);
     * // Subsequent call: existing value 0 is incremented by 1, returns 1.
     * count = cache.incr("counter:views", 1, 0);
     *
     * // Auto-initializing counter
     * long requestCount = cache.incr("api:requests:" + endpoint, 1, 0);
     * System.out.println("Request " + requestCount + " to " + endpoint);
     * }</pre>
     *
     * @param key the cache key whose associated value is to be incremented; must not be {@code null}
     * @param delta the amount by which to increment the value when the key already exists;
     *              must be non-negative
     * @param defaultValue the initial value to set if the key does not exist; on first-write this
     *                     value is stored verbatim ({@code delta} is NOT added on insert)
     * @return the value after the operation: {@code defaultValue} if the key did not exist (no
     *         delta applied on insert), otherwise the previously stored value plus {@code delta}
     * @throws IllegalArgumentException if {@code key} is {@code null} or {@code delta} is negative
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    public long incr(final String key, final int delta, final long defaultValue) {
        N.checkArgNotNull(key, "key");
        N.checkArgNotNegative(delta, "delta");
        // Memcached's "no expiration" sentinel is 0, NOT -1. When the key is absent, spymemcached
        // seeds it with `add <key> <flags> <exp> <defaultValue>` using THIS exp; memcached treats a
        // negative exp as an absolute time in the past, so a -1 seed is stored already-expired and the
        // counter is re-seeded to defaultValue on every call (never advancing). Pass 0 to truly persist.
        return mc.incr(key, delta, defaultValue, 0);
    }

    /**
     * Atomically increments a numeric value, initializing it to {@code defaultValue} with the given
     * expiration when the key doesn't exist. The {@code liveTime} is converted from milliseconds to
     * seconds for Memcached (rounded up if not exact).
     *
     * <p><b>Memcached-Specific Behavior:</b> Unlike {@link #incr(String)} and {@link #incr(String, int)},
     * which return {@code -1} when the key is absent, this overload first-writes the key with
     * {@code defaultValue} when missing. Per the SpyMemcached / Memcached binary-protocol contract,
     * <b>the increment is NOT applied on the initial insert</b>: when the key is absent the stored
     * value is {@code defaultValue} and that same {@code defaultValue} is returned (not
     * {@code defaultValue + delta}). The {@code delta} only takes effect on subsequent calls when the
     * key already exists. Values are stored as 64-bit unsigned integers in decimal string format.
     *
     * <p><b>Thread Safety:</b> This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent increment operations are guaranteed to be serialized correctly,
     * ensuring no increments are lost.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Hourly counter that expires after 1 hour. First call when key is absent stores 0 and
     * // returns 0; subsequent calls increment by 1.
     * long count = cache.incr("counter:hourly", 1, 0, 3600000);
     * System.out.println("Hourly count: " + count);
     *
     * // Daily quota counter
     * long dailyRequests = cache.incr("quota:daily:" + userId, 1, 0, 86400000);
     * if (dailyRequests > DAILY_LIMIT) {
     *     throw new QuotaExceededException("Daily limit reached");
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be incremented; must not be {@code null}
     * @param delta the amount by which to increment the value when the key already exists;
     *              must be non-negative
     * @param defaultValue the initial value to set if the key does not exist; on first-write this
     *                     value is stored verbatim ({@code delta} is NOT added on insert)
     * @param liveTime the time-to-live in milliseconds for the key ({@code 0} or negative means no
     *                 expiration); a positive value is converted to seconds, rounded up if not exact.
     * @return the value after the operation: {@code defaultValue} if the key did not exist (no
     *         delta applied on insert), otherwise the previously stored value plus {@code delta}
     * @throws IllegalArgumentException if {@code key} is {@code null} or {@code delta} is negative
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    public long incr(final String key, final int delta, final long defaultValue, final long liveTime) {
        N.checkArgNotNull(key, "key");
        N.checkArgNotNegative(delta, "delta");
        return mc.incr(key, delta, defaultValue, toMemcachedExpiration(liveTime));
    }

    /**
     * Atomically decrements a numeric value by 1.
     *
     * <p><b>Memcached-Specific Behavior:</b> If the key doesn't exist, returns -1. Values cannot go below 0
     * (Memcached prevents underflow - attempting to decrement 0 results in 0, not a negative value).
     * Only works with string representations of 64-bit unsigned integers stored in Memcached. The value
     * must be stored as a decimal string representation.
     *
     * <p><b>Thread Safety:</b> This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent decrement operations are guaranteed to be serialized correctly,
     * ensuring no decrements are lost.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Token bucket rate limiting
     * long remainingTokens = cache.decr("api:tokens:" + userId);
     * if (remainingTokens == -1) {
     *     // Key doesn't exist, initialize it
     *     cache.set("api:tokens:" + userId, "100", 60000);
     *     remainingTokens = 100;
     * }
     * if (remainingTokens == 0) {
     *     throw new RateLimitException("Rate limit exceeded");
     * }
     *
     * // Inventory management
     * long stock = cache.decr("product:stock:123");
     * if (stock == -1) {
     *     // Handle key not found
     *     throw new KeyNotFoundException();
     * } else if (stock == 0) {
     *     // Memcached prevents going below 0
     *     throw new OutOfStockException();
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be decremented; must not be {@code null}
     * @return the value after the decrement (cannot be negative due to Memcached's underflow
     *         clamping), or {@code -1} if the key does not exist
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    @Override
    public long decr(final String key) {
        N.checkArgNotNull(key, "key");
        return mc.decr(key, 1);
    }

    /**
     * Atomically decrements a numeric value by a specified amount.
     *
     * <p><b>Memcached-Specific Behavior:</b> If the key doesn't exist, returns -1. Values cannot go below 0
     * (Memcached prevents underflow - if delta is larger than the current value, the result will be 0, not negative).
     * Only works with string representations of 64-bit unsigned integers stored in Memcached. The value
     * must be stored as a decimal string representation.
     *
     * <p><b>Thread Safety:</b> This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent decrement operations are guaranteed to be serialized correctly,
     * ensuring no decrements are lost.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Bulk inventory decrement
     * long inventory = cache.decr("product:stock:456", 5);
     * if (inventory == -1) {
     *     // Key doesn't exist
     *     throw new KeyNotFoundException();
     * } else if (inventory == 0) {
     *     // Either reached 0 or underflow prevented (delta > original value)
     *     System.out.println("Inventory depleted or insufficient");
     * } else {
     *     System.out.println("Remaining inventory: " + inventory);
     * }
     *
     * // API quota management
     * int requestCost = calculateCost(request);
     * long quotaRemaining = cache.decr("quota:" + apiKey, requestCost);
     * if (quotaRemaining == -1) {
     *     throw new KeyNotFoundException();
     * } else if (quotaRemaining == 0) {
     *     // Quota exhausted or exceeded
     *     throw new QuotaExceededException();
     * }
     *
     * // Reservation system (checking before decrement)
     * String key = "event:seats:789";
     * Long currentSeats = cache.get(key);
     * if (currentSeats != null && currentSeats >= numberOfTickets) {
     *     long availableSeats = cache.decr(key, numberOfTickets);
     *     if (availableSeats >= 0) {
     *         System.out.println("Reservation successful");
     *     }
     * } else {
     *     throw new NotEnoughSeatsException();
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be decremented; must not be {@code null}
     * @param delta the amount by which to decrement the value; must be non-negative
     * @return the value after the decrement (cannot be negative due to Memcached's underflow
     *         clamping), or {@code -1} if the key does not exist
     * @throws IllegalArgumentException if {@code key} is {@code null} or {@code delta} is negative
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    @Override
    public long decr(final String key, final int delta) {
        N.checkArgNotNull(key, "key");
        N.checkArgNotNegative(delta, "delta");
        return mc.decr(key, delta);
    }

    /**
     * Atomically decrements a numeric value, initializing it to {@code defaultValue} when the key
     * doesn't exist. The newly-created entry will not expire (this implementation passes {@code 0}
     * as the expiration to Memcached, which means "no expiration").
     *
     * <p><b>Memcached-Specific Behavior:</b> Unlike {@link #decr(String)} and {@link #decr(String, int)},
     * which return {@code -1} when the key is absent, this overload first-writes the key with
     * {@code defaultValue} when missing. Per the SpyMemcached / Memcached binary-protocol contract,
     * <b>the decrement is NOT applied on the initial insert</b>: when the key is absent the stored
     * value is {@code defaultValue} and that same {@code defaultValue} is returned (not
     * {@code defaultValue - delta}). The {@code delta} only takes effect on subsequent calls when the
     * key already exists. Values cannot go below {@code 0} (Memcached clamps underflow). Values are
     * stored as 64-bit unsigned integers in decimal string format.
     *
     * <p><b>Thread Safety:</b> This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent decrement operations are guaranteed to be serialized correctly,
     * ensuring no decrements are lost.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // First call when key is absent: stores 100, returns 100 (delta NOT applied on init).
     * long remaining = cache.decr("inventory:item123", 1, 100);
     * // Subsequent call: existing value 100 is decremented by 1, returns 99.
     * remaining = cache.decr("inventory:item123", 1, 100);
     *
     * // Auto-initializing quota
     * long quotaRemaining = cache.decr("quota:user:" + userId, 1, 1000);
     * if (quotaRemaining == 0) {
     *     System.out.println("Quota exhausted");
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be decremented; must not be {@code null}
     * @param delta the amount by which to decrement the value when the key already exists;
     *              must be non-negative
     * @param defaultValue the initial value to set if the key does not exist; on first-write this
     *                     value is stored verbatim ({@code delta} is NOT subtracted on insert)
     * @return the value after the operation: {@code defaultValue} if the key did not exist (no
     *         delta applied on insert), otherwise the previously stored value minus {@code delta},
     *         clamped at {@code 0}
     * @throws IllegalArgumentException if {@code key} is {@code null} or {@code delta} is negative
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    public long decr(final String key, final int delta, final long defaultValue) {
        N.checkArgNotNull(key, "key");
        N.checkArgNotNegative(delta, "delta");
        // See incr(String, int, long): Memcached's "no expiration" sentinel is 0, not -1. A -1 seed
        // expiration would store the auto-initialized value already-expired, re-seeding every call.
        return mc.decr(key, delta, defaultValue, 0);
    }

    /**
     * Atomically decrements a numeric value, initializing it to {@code defaultValue} with the given
     * expiration when the key doesn't exist. The {@code liveTime} is converted from milliseconds to
     * seconds for Memcached (rounded up if not exact).
     *
     * <p><b>Memcached-Specific Behavior:</b> Unlike {@link #decr(String)} and {@link #decr(String, int)},
     * which return {@code -1} when the key is absent, this overload first-writes the key with
     * {@code defaultValue} when missing. Per the SpyMemcached / Memcached binary-protocol contract,
     * <b>the decrement is NOT applied on the initial insert</b>: when the key is absent the stored
     * value is {@code defaultValue} and that same {@code defaultValue} is returned (not
     * {@code defaultValue - delta}). The {@code delta} only takes effect on subsequent calls when the
     * key already exists. Values cannot go below {@code 0} (Memcached clamps underflow). Values are
     * stored as 64-bit unsigned integers in decimal string format.
     *
     * <p><b>Thread Safety:</b> This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent decrement operations are guaranteed to be serialized correctly,
     * ensuring no decrements are lost.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Daily quota that expires after 24 hours. First call when key is absent stores 1000 and
     * // returns 1000; subsequent calls decrement by 1.
     * long remaining = cache.decr("quota:user:123", 1, 1000, 86400000);
     * System.out.println("Remaining quota: " + remaining);
     *
     * // Hourly rate limit
     * long hourlyLimit = cache.decr("rate:hourly:" + userId, 1, 100, 3600000);
     * if (hourlyLimit == 0) {
     *     throw new RateLimitException("Hourly limit reached");
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be decremented; must not be {@code null}
     * @param delta the amount by which to decrement the value when the key already exists;
     *              must be non-negative
     * @param defaultValue the initial value to set if the key does not exist; on first-write this
     *                     value is stored verbatim ({@code delta} is NOT subtracted on insert)
     * @param liveTime the time-to-live in milliseconds for the key ({@code 0} or negative means no
     *                 expiration); a positive value is converted to seconds, rounded up if not exact.
     * @return the value after the operation: {@code defaultValue} if the key did not exist (no
     *         delta applied on insert), otherwise the previously stored value minus {@code delta},
     *         clamped at {@code 0}
     * @throws IllegalArgumentException if {@code key} is {@code null} or {@code delta} is negative
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    public long decr(final String key, final int delta, final long defaultValue, final long liveTime) {
        N.checkArgNotNull(key, "key");
        N.checkArgNotNegative(delta, "delta");
        return mc.decr(key, delta, defaultValue, toMemcachedExpiration(liveTime));
    }

    /**
     * Flushes all data from all connected Memcached servers immediately.
     * This operation removes all keys from all servers. The method blocks until the
     * flush completes or times out. Use with extreme caution in production environments
     * as this is a destructive operation that cannot be undone.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe but its effects are visible immediately to all clients.
     * Once executed, all cached data will be permanently lost. There is no way to recover
     * the data after this operation completes.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // WARNING: This removes ALL data from ALL cache servers!
     * cache.flushAll();
     * System.out.println("All cache data cleared");
     *
     * // Safe usage in testing
     * @After
     * public void cleanupCache() {
     *     if (isTestEnvironment()) {
     *         cache.flushAll();
     *     }
     * }
     *
     * // Production usage with confirmation
     * public void clearCache(String confirmationToken) {
     *     if (!"CONFIRM_FLUSH_ALL".equals(confirmationToken)) {
     *         throw new IllegalArgumentException("Invalid confirmation");
     *     }
     *     logger.warn("Flushing all cache data");
     *     cache.flushAll();
     *     auditLog.record("CACHE_FLUSH_ALL", user);
     * }
     * }</pre>
     *
     * @throws IllegalStateException if one or more servers do not acknowledge the flush (the underlying
     *         flush operation does not report success)
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    @Override
    public void flushAll() {
        if (!Boolean.TRUE.equals(resultOf(mc.flush()))) {
            throw new IllegalStateException("Failed to flush all Memcached servers");
        }
    }

    /**
     * Asynchronously flushes all data from all connected Memcached servers.
     * The method returns immediately without blocking. The returned Future can be used to
     * check when the operation completes. This is a destructive operation that removes all
     * keys from all servers and cannot be undone. Use with extreme caution in production.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe but its effects are visible immediately to all clients
     * once the flush completes.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple async flush
     * Future<Boolean> future = cache.asyncFlushAll();
     * boolean flushed = future.get();
     * if (flushed) {
     *     System.out.println("All data flushed");
     * }
     *
     * // Async flush with timeout (requires: import java.util.concurrent.TimeoutException;)
     * Future<Boolean> future = cache.asyncFlushAll();
     * try {
     *     boolean flushed = future.get(2000, TimeUnit.MILLISECONDS);
     *     System.out.println("Flush completed: " + flushed);
     * } catch (TimeoutException e) {
     *     logger.warn("Flush operation timed out");
     * }
     * }</pre>
     *
     * @return a {@link Future} that will yield {@code true} when the flush completes successfully,
     *         or {@code false} on failure
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
     * <p><b>Thread Safety:</b> This method is thread-safe but its effects are visible immediately to all clients
     * after the delay period expires.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Schedule a flush to happen in 5 seconds
     * boolean scheduled = cache.flushAll(5000);
     * if (scheduled) {
     *     System.out.println("Flush scheduled for 5 seconds from now");
     * }
     *
     * // Delayed flush for maintenance window
     * long delayUntilMaintenance = calculateDelayToMaintenance();
     * boolean scheduled = cache.flushAll(delayUntilMaintenance);
     * if (scheduled) {
     *     logger.info("Cache flush scheduled for maintenance window");
     * }
     * }</pre>
     *
     * @param delay the delay in milliseconds before the flush operation is executed; a positive value
     *              is converted to seconds, rounded up if not exact. A value of {@code 0} or negative
     *              flushes immediately.
     * @return {@code true} if the flush was scheduled successfully; {@code false} otherwise
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    public boolean flushAll(final long delay) {
        // The memcached `flush_all <delay>` command always interprets its argument as a RELATIVE
        // delay in seconds; unlike storage commands it never treats large values as an absolute
        // Unix timestamp. So convert with toSeconds(), not toMemcachedExpiration().
        return resultOf(mc.flush(toSeconds(delay)));
    }

    /**
     * Asynchronously schedules a flush after a delay.
     * The method returns immediately without blocking. The flush will occur on all connected
     * servers after the delay expires. This is a destructive operation that removes all keys
     * from all servers and cannot be undone. The delay is converted from milliseconds to seconds
     * for Memcached (rounded up if not exact).
     *
     * <p><b>Thread Safety:</b> This method is thread-safe but its effects are visible immediately to all clients
     * after the delay period expires.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Schedule a flush to happen in 10 seconds
     * Future<Boolean> future = cache.asyncFlushAll(10000);
     * boolean scheduled = future.get();   // Wait for scheduling confirmation
     *
     * // Async delayed flush with timeout (requires: import java.util.concurrent.TimeoutException;)
     * Future<Boolean> future = cache.asyncFlushAll(30000);
     * try {
     *     boolean scheduled = future.get(1000, TimeUnit.MILLISECONDS);
     *     System.out.println("Flush scheduled: " + scheduled);
     * } catch (TimeoutException e) {
     *     logger.warn("Failed to schedule flush");
     * }
     * }</pre>
     *
     * @param delay the delay in milliseconds before the flush operation is executed; a positive value
     *              is converted to seconds, rounded up if not exact. A value of {@code 0} or negative
     *              flushes immediately.
     * @return a {@link Future} that will yield {@code true} if the flush was scheduled
     *         successfully, or {@code false} on failure
     */
    public Future<Boolean> asyncFlushAll(final long delay) {
        // See flushAll(long): flush_all's delay is always relative seconds, so use toSeconds().
        return mc.flush(toSeconds(delay));
    }

    /**
     * Disconnects from all Memcached servers and releases resources.
     * After this method returns, the cache client must not be used again; any subsequent
     * operations will fail. This method delegates to {@link MemcachedClient#shutdown()} (the no-arg
     * overload), which initiates an <b>immediate</b> shutdown — in-flight operations are not awaited.
     * Use {@link #disconnect(long)} if you need a bounded graceful shutdown that lets pending
     * operations complete. This method is idempotent: calling it multiple times has no additional effect.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and uses synchronization to ensure only
     * one disconnect occurs. Once called, no other operations should be attempted on this client
     * instance.
     *
     * <p>Call this method when the client is no longer needed to ensure proper cleanup of network
     * connections, thread pools, and other resources. It is safe to call multiple times; subsequent
     * calls have no effect.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Try-finally pattern
     * SpyMemcached<User> cache = new SpyMemcached<>("localhost:11211");
     * try {
     *     cache.set("user:123", user, 3600000);
     *     User cached = cache.get("user:123");
     * } finally {
     *     cache.disconnect();
     *     System.out.println("Cache client disconnected");
     * }
     *
     * // Spring Bean destruction
     * @PreDestroy
     * public void cleanup() {
     *     if (cache != null) {
     *         cache.disconnect();
     *     }
     * }
     *
     * // Application shutdown hook
     * Runtime.getRuntime().addShutdownHook(new Thread(() -> {
     *     logger.info("Shutting down cache client");
     *     cache.disconnect();
     * }));
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
     * Disconnects from all Memcached servers, waiting up to the given timeout for pending
     * operations to complete.
     * After the timeout elapses, any remaining operations are abandoned and connections are
     * closed. After this method returns, the cache client must not be used again; any subsequent
     * operations will fail. This method is idempotent: calling it multiple times has no additional
     * effect.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and uses synchronization to ensure only
     * one disconnect occurs. Once called, no other operations should be attempted on this client
     * instance.
     *
     * <p>Call this method when the client is no longer needed to ensure proper cleanup of network
     * connections, thread pools, and other resources. The timeout allows pending operations to
     * complete gracefully.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Graceful shutdown with timeout
     * try {
     *     // Use cache...
     * } finally {
     *     cache.disconnect(5000);   // Wait up to 5 seconds for cleanup
     * }
     *
     * // Application shutdown with graceful timeout
     * public void shutdown() {
     *     logger.info("Shutting down cache client");
     *     cache.disconnect(10000);   // Wait up to 10 seconds
     *     logger.info("Cache client shutdown complete");
     * }
     * }</pre>
     *
     * @param timeout the maximum time, in milliseconds, to wait for shutdown; must not be negative
     * @throws IllegalArgumentException if {@code timeout} is negative
     */
    public synchronized void disconnect(final long timeout) {
        N.checkArgNotNegative(timeout, "timeout");

        if (!isShutdown) {
            mc.shutdown(timeout, TimeUnit.MILLISECONDS);
            isShutdown = true;
        }
    }

    private int toMemcachedExpiration(final long liveTime) {
        final int seconds = toSeconds(liveTime);

        if (seconds <= MEMCACHED_MAX_RELATIVE_EXPIRATION_SECONDS) {
            return seconds;
        }

        final long expiresAt = System.currentTimeMillis() / 1000L + seconds;

        if (expiresAt > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Time value too large for Memcached expiration: " + liveTime + " ms");
        }

        return (int) expiresAt;
    }

    /**
     * Waits for a {@link Future} to complete and returns its result.
     * Blocks until the Future completes (subject to a bounded wait derived from the configured
     * operation timeout) and converts {@link InterruptedException}, {@link TimeoutException}, and
     * {@link ExecutionException} into runtime exceptions. When an {@link InterruptedException}
     * occurs, the thread's interrupted status is restored before throwing the runtime exception
     * and the Future is cancelled.
     *
     * <p>This is a utility method used internally to convert asynchronous operations to
     * synchronous ones by blocking on the Future's result.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Internal usage pattern
     * Future<Boolean> future = mc.set("key", "value", 3600);
     * Boolean result = resultOf(future);
     * }</pre>
     *
     * @param <R> the type of result returned by the {@link Future}
     * @param future the {@link Future} whose result is to be retrieved; must not be {@code null}
     * @return the result value produced by the {@link Future}
     * @throws IllegalArgumentException if {@code future} is {@code null}
     * @throws RuntimeException if the {@link Future} execution fails, the calling thread is
     *         interrupted, or the bounded wait (a generous multiple of the configured operation
     *         timeout) elapses
     */
    protected <R> R resultOf(final Future<R> future) {
        N.checkArgNotNull(future, "future");

        try {
            // Defense-in-depth bounded wait so a hung/stalled connection cannot pin the
            // calling thread indefinitely. spymemcached enforces its own per-operation
            // timeout internally; this outer bound is intentionally generous (a multiple
            // of the configured operation timeout) so legitimately slower bulk operations
            // are not failed prematurely.
            return future.get(Math.max(operationTimeout * 4, operationTimeout + 5_000L), TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupt status

            future.cancel(true);

            if (logger.isWarnEnabled()) {
                logger.warn("Thread was interrupted while waiting for a Memcached operation to complete", e);
            }

            throw ExceptionUtil.toRuntimeException(e, true);
        } catch (final TimeoutException e) {
            future.cancel(true);

            if (logger.isWarnEnabled()) {
                logger.warn("Timed out waiting for a Memcached operation to complete", e);
            }

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
     * Creates a {@link MemcachedClient} with the specified connection factory.
     * Wraps any {@link IOException} thrown during client construction in an
     * {@link UncheckedIOException} for easier handling by callers.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * ConnectionFactory factory = new DefaultConnectionFactory();
     * MemcachedClient client = createSpyMemcachedClient("localhost:11211", factory);
     * }</pre>
     *
     * @param serverUrl the Memcached server URL(s) to connect to (e.g.,
     *                  {@code "host1:port1,host2:port2,..."})
     * @param connFactory the connection factory configured with timeout and transcoder settings
     * @return a configured {@link MemcachedClient} instance
     * @throws UncheckedIOException if the connection to the Memcached server(s) fails
     */
    protected static MemcachedClient createSpyMemcachedClient(final String serverUrl, final ConnectionFactory connFactory) throws UncheckedIOException {
        try {
            return new MemcachedClient(connFactory, AddrUtil.getAddressList(serverUrl));
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to create Memcached client for server(s): " + serverUrl, e);
        }
    }
}
