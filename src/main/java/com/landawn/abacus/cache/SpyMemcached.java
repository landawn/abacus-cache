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
     * SpyMemcached<String> cache = new SpyMemcached<>("localhost:11211"); // uses DEFAULT_TIMEOUT
     * cache.set("key1", "value1", 3600000);                               // stores with 3600s TTL; returns true on success
     *
     * // A null/blank serverUrl is rejected before any connection attempt.
     * new SpyMemcached<>((String) null); // throws IllegalArgumentException
     * new SpyMemcached<>("   ");         // throws IllegalArgumentException (blank)
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
     * SpyMemcached<User> cache = new SpyMemcached<>("localhost:11211", 5000); // 5000ms operation timeout
     * cache.set("user:123", user, 3600000);                                   // stores with 3600s TTL; returns true on success
     *
     * // timeout must be strictly positive (checkArgPositive).
     * new SpyMemcached<>("localhost:11211", 0);  // throws IllegalArgumentException
     * new SpyMemcached<>("localhost:11211", -1); // throws IllegalArgumentException
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
     * User user = cache.get("user:123"); // returns the cached User, or null if absent
     * if (user != null) {
     *     System.out.println("Found user: " + user.getName()); // prints the cached user's name
     * } else {
     *     System.out.println("User not found in cache"); // printed when get returned null
     * }
     *
     * // Get with fallback to database
     * User user = cache.get("user:123"); // returns null if not cached
     * if (user == null) {
     *     user = database.findUser(123); // load from the source of truth
     *     cache.set("user:123", user, 3600000); // re-populate the cache; returns true on success
     * }
     *
     * cache.get((String) null); // throws IllegalArgumentException (key must not be null)
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
     * Future<User> future = cache.asyncGet("user:123"); // returns immediately; never null
     * User user = future.get();                         // blocks until complete; yields the value or null if absent
     *
     * // Async get with timeout (requires: import java.util.concurrent.TimeoutException;)
     * Future<User> future = cache.asyncGet("user:123"); // dispatches the get; returns the Future
     * try {
     *     User user = future.get(1000, TimeUnit.MILLISECONDS); // waits up to 1s for the result
     * } catch (TimeoutException e) {
     *     System.out.println("Get operation timed out"); // printed if the result was not ready in time
     * }
     *
     * cache.asyncGet((String) null); // throws IllegalArgumentException (key must not be null)
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
     * Map<String, User> users = cache.getBulk("user:1", "user:2", "user:3");         // never null; missing keys are simply absent
     * users.forEach((key, user) -> System.out.println(key + ": " + user.getName())); // iterates only the found entries
     *
     * // Bulk get with missing key handling
     * Map<String, Product> products = cache.getBulk("prod:1", "prod:2", "prod:3"); // returns only the keys present in cache
     * System.out.println("Found " + products.size() + " out of 3 products");       // size() <= number of requested keys
     *
     * // Identify missing keys
     * String[] requestedKeys = {"user:1", "user:2", "user:3"};
     * Map<String, User> found = cache.getBulk(requestedKeys); // returns the subset that was cached
     * Arrays.stream(requestedKeys)
     *       .filter(key -> !found.containsKey(key))
     *       .forEach(key -> System.out.println("Missing: " + key)); // prints each requested key absent from the result
     *
     * cache.getBulk((String[]) null);   // throws IllegalArgumentException (keys must not be null)
     * cache.getBulk("user:1", null);    // throws IllegalArgumentException (no null elements allowed)
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
     * Future<Map<String, User>> future = cache.asyncGetBulk("user:1", "user:2"); // returns immediately; never null
     * Map<String, User> users = future.get();                                    // blocks until complete; yields only the found entries
     *
     * // Async bulk get with timeout (requires: import java.util.concurrent.TimeoutException;)
     * Future<Map<String, User>> future = cache.asyncGetBulk("user:1", "user:2", "user:3"); // dispatches the bulk get
     * try {
     *     Map<String, User> users = future.get(2000, TimeUnit.MILLISECONDS); // waits up to 2s
     *     System.out.println("Retrieved " + users.size() + " users");        // size() <= number of requested keys
     * } catch (TimeoutException e) {
     *     future.cancel(true); // abandon the operation if it did not complete in time
     * }
     *
     * cache.asyncGetBulk((String[]) null);  // throws IllegalArgumentException (keys must not be null)
     * cache.asyncGetBulk("user:1", null);   // throws IllegalArgumentException (no null elements allowed)
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
     * Map<String, User> users = cache.getBulk(userKeys); // never null; missing keys are simply absent
     *
     * // Using a Set (requires: import java.util.Set; import java.util.HashSet;)
     * Set<String> keySet = new HashSet<>(Arrays.asList("session:1", "session:2"));
     * Map<String, Session> sessions = cache.getBulk(keySet); // returns only the keys present in cache
     *
     * // Dynamically built key collection (requires: import java.util.stream.Collectors;)
     * List<Integer> userIds = Arrays.asList(101, 102, 103);
     * List<String> keys = userIds.stream()
     *                            .map(id -> "user:" + id)
     *                            .collect(Collectors.toList()); // builds ["user:101", "user:102", "user:103"]
     * Map<String, User> users = cache.getBulk(keys);            // returns the cached subset
     *
     * cache.getBulk((Collection<String>) null);      // throws IllegalArgumentException (keys must not be null)
     * cache.getBulk(Arrays.asList("user:1", null));  // throws IllegalArgumentException (no null elements allowed)
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
     * Future<Map<String, User>> future = cache.asyncGetBulk(keys); // returns immediately; never null
     * Map<String, User> users = future.get();                      // blocks until complete; yields only the found entries
     *
     * // Async bulk get from dynamically generated keys (requires: import java.util.concurrent.TimeoutException;)
     * List<String> productKeys = generateProductKeys();
     * Future<Map<String, Product>> future = cache.asyncGetBulk(productKeys);   // dispatches the bulk get
     * Map<String, Product> products = future.get(3000, TimeUnit.MILLISECONDS); // waits up to 3s for the result
     *
     * cache.asyncGetBulk((Collection<String>) null);     // throws IllegalArgumentException (keys must not be null)
     * cache.asyncGetBulk(Arrays.asList("user:1", null)); // throws IllegalArgumentException (no null elements allowed)
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
     * seconds for Memcached (rounded up if not exact). TTLs longer than 30 days are stored as an
     * absolute Unix expiration timestamp rather than a relative offset.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.
     * When multiple clients set the same key concurrently, the last write wins.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Cache with 1 hour TTL
     * User user = new User("John", "john@example.com");
     * boolean success = cache.set("user:123", user, 3600000); // 3600000ms -> 3600s TTL; returns true on success
     * if (success) {
     *     System.out.println("User cached successfully"); // printed when the store succeeded
     * }
     *
     * // Cache session data with 30 minute TTL
     * Session session = new Session("abc123", user);
     * cache.set("session:" + session.getId(), session, 1800000); // 1800000ms -> 1800s TTL; returns true on success
     *
     * // Cache with no expiration
     * Config config = loadConfig();
     * cache.set("app:config", config, 0);   // liveTime 0 -> 0s ("no expiration"); returns true on success
     *
     * // A null value is accepted (memcached stores it); only a null key is rejected.
     * cache.set("maybe-null", null, 60000); // returns true on success; stores a null value
     *
     * // Updating existing value
     * Product product = cache.get("product:456");
     * product.setPrice(99.99); // mutate the retrieved object before re-storing
     * cache.set("product:456", product, 7200000);   // 7200000ms -> 7200s (2 hour) TTL; returns true on success
     *
     * cache.set((String) null, user, 3600000); // throws IllegalArgumentException (key must not be null)
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated; must not be {@code null}
     * @param value the value to cache; may be {@code null}
     * @param liveTime the time-to-live in milliseconds ({@code 0} or negative means no expiration); a
     *                 positive value is converted to seconds, rounded up if not exact; values over 30
     *                 days are stored as an absolute expiration timestamp.
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
     * seconds for Memcached (rounded up if not exact). TTLs longer than 30 days are stored as an
     * absolute Unix expiration timestamp rather than a relative offset.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple async set
     * Future<Boolean> future = cache.asyncSet("user:123", user, 3600000); // returns immediately; never null
     * boolean success = future.get();                                     // blocks until complete; yields true on success
     * if (success) {
     *     System.out.println("Set operation succeeded"); // printed when the store succeeded
     * }
     *
     * // Async set with timeout (requires: import java.util.concurrent.TimeoutException;)
     * Future<Boolean> future = cache.asyncSet("product:456", product, 7200000); // dispatches the store
     * try {
     *     boolean success = future.get(1000, TimeUnit.MILLISECONDS); // waits up to 1s for completion
     * } catch (TimeoutException e) {
     *     future.cancel(true); // abandon the operation if it did not complete in time
     *     System.err.println("Set operation timed out"); // printed when the wait elapsed
     * }
     *
     * cache.asyncSet((String) null, user, 3600000); // throws IllegalArgumentException (key must not be null)
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated; must not be {@code null}
     * @param value the value to cache; may be {@code null}
     * @param liveTime the time-to-live in milliseconds ({@code 0} or negative means no expiration); a
     *                 positive value is converted to seconds, rounded up if not exact; values over 30
     *                 days are stored as an absolute expiration timestamp.
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
     * for Memcached (rounded up if not exact). TTLs longer than 30 days are stored as an absolute
     * Unix expiration timestamp rather than a relative offset.
     *
     * <p><b>Thread Safety:</b> This operation is atomic, ensuring that in concurrent scenarios, only one client
     * will successfully add the key while others will receive {@code false}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple conditional add
     * if (cache.add("user:123", user, 3600000)) {        // returns true only if "user:123" was absent
     *     System.out.println("User added successfully"); // printed when the key did not previously exist
     * } else {
     *     System.out.println("User already exists in cache"); // printed when add returned false (key present)
     * }
     *
     * // Distributed locking pattern
     * String lockKey = "lock:resource:123";
     * if (cache.add(lockKey, "locked", 30000)) { // 30000ms -> 30s TTL; true only if we acquired the lock
     *     try {
     *         // Critical section - only one client can execute this
     *         performCriticalOperation(); // runs only after the lock was acquired
     *     } finally {
     *         cache.delete(lockKey); // release the lock; returns true if the key existed
     *     }
     * } else {
     *     System.out.println("Resource is locked by another process"); // add returned false (lock held elsewhere)
     * }
     *
     * cache.add((String) null, user, 3600000); // throws IllegalArgumentException (key must not be null)
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated; must not be {@code null}
     * @param value the value to cache; may be {@code null}
     * @param liveTime the time-to-live in milliseconds ({@code 0} or negative means no expiration); a
     *                 positive value is converted to seconds, rounded up if not exact; values over 30
     *                 days are stored as an absolute expiration timestamp.
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
     * is converted from milliseconds to seconds for Memcached (rounded up if not exact). TTLs longer
     * than 30 days are stored as an absolute Unix expiration timestamp rather than a relative offset.
     *
     * <p><b>Thread Safety:</b> This operation is atomic, ensuring that in concurrent scenarios, only one client
     * will successfully add the key while others will receive {@code false}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple async add
     * Future<Boolean> future = cache.asyncAdd("user:123", user, 3600000); // returns immediately; never null
     * if (future.get()) {                                                 // blocks; yields true only if the key was absent
     *     System.out.println("Added");                                    // printed when the add succeeded
     * } else {
     *     System.out.println("Key already exists"); // printed when add yielded false
     * }
     *
     * // Async add with timeout (requires: import java.util.concurrent.TimeoutException;)
     * Future<Boolean> future = cache.asyncAdd("session:abc", session, 1800000); // dispatches the add
     * try {
     *     boolean added = future.get(500, TimeUnit.MILLISECONDS); // waits up to 500ms
     *     System.out.println("Add successful: " + added); // true if the key was newly added
     * } catch (TimeoutException e) {
     *     future.cancel(true); // abandon the operation if it did not complete in time
     * }
     *
     * cache.asyncAdd((String) null, user, 3600000); // throws IllegalArgumentException (key must not be null)
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated; must not be {@code null}
     * @param value the value to cache; may be {@code null}
     * @param liveTime the time-to-live in milliseconds ({@code 0} or negative means no expiration); a
     *                 positive value is converted to seconds, rounded up if not exact; values over 30
     *                 days are stored as an absolute expiration timestamp.
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
     * for Memcached (rounded up if not exact). TTLs longer than 30 days are stored as an absolute
     * Unix expiration timestamp rather than a relative offset.
     *
     * <p><b>Thread Safety:</b> This operation is atomic, ensuring that updates are applied atomically even in
     * concurrent scenarios.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple replace operation
     * if (cache.replace("user:123", updatedUser, 3600000)) { // returns true only if "user:123" already exists
     *     System.out.println("User updated");                // printed when the key existed and was replaced
     * } else {
     *     System.out.println("User not found in cache"); // printed when replace returned false (key absent)
     * }
     *
     * // Update existing cache entry
     * User user = cache.get("user:456"); // returns null if not cached
     * if (user != null) {
     *     user.setLastAccess(System.currentTimeMillis()); // update the field before replacing
     *     if (cache.replace("user:456", user, 7200000)) { // 7200000ms -> 7200s TTL; true if the key existed
     *         System.out.println("User access time updated"); // printed on successful replace
     *     }
     * }
     *
     * cache.replace((String) null, updatedUser, 3600000); // throws IllegalArgumentException (key must not be null)
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated; must not be {@code null}
     * @param value the value to cache; may be {@code null}
     * @param liveTime the time-to-live in milliseconds ({@code 0} or negative means no expiration); a
     *                 positive value is converted to seconds, rounded up if not exact; values over 30
     *                 days are stored as an absolute expiration timestamp.
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
     * is converted from milliseconds to seconds for Memcached (rounded up if not exact). TTLs longer
     * than 30 days are stored as an absolute Unix expiration timestamp rather than a relative offset.
     *
     * <p><b>Thread Safety:</b> This operation is atomic, ensuring that updates are applied atomically even in
     * concurrent scenarios.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple async replace
     * Future<Boolean> future = cache.asyncReplace("user:123", updatedUser, 3600000); // returns immediately; never null
     * boolean replaced = future.get();                                               // blocks; yields true only if the key already existed
     * if (replaced) {
     *     System.out.println("Replaced successfully"); // printed when the replace succeeded
     * }
     *
     * // Async replace with timeout (requires: import java.util.concurrent.TimeoutException;)
     * Future<Boolean> future = cache.asyncReplace("config:app", newConfig, 86400000); // dispatches the replace
     * try {
     *     boolean replaced = future.get(1000, TimeUnit.MILLISECONDS); // waits up to 1s
     *     System.out.println("Config replaced: " + replaced); // true if the key existed
     * } catch (TimeoutException e) {
     *     future.cancel(true); // abandon the operation if it did not complete in time
     * }
     *
     * cache.asyncReplace((String) null, updatedUser, 3600000); // throws IllegalArgumentException (key must not be null)
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated; must not be {@code null}
     * @param value the value to cache; may be {@code null}
     * @param liveTime the time-to-live in milliseconds ({@code 0} or negative means no expiration); a
     *                 positive value is converted to seconds, rounded up if not exact; values over 30
     *                 days are stored as an absolute expiration timestamp.
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
     * boolean success = cache.delete("user:123");    // true if the key existed and was removed, false if absent
     * System.out.println("Key removed: " + success); // prints "Key removed: true" or "Key removed: false"
     *
     * // Delete after update
     * User user = cache.get("user:456"); // returns null if not cached
     * if (user != null && user.isInactive()) {
     *     cache.delete("user:456"); // returns true if the key existed
     * }
     *
     * // Delete multiple keys
     * String[] keysToDelete = {"session:1", "session:2", "session:3"};
     * Arrays.stream(keysToDelete).forEach(cache::delete); // deletes each key in turn
     *
     * // Invalidate cache on entity update
     * void updateUser(User user) {
     *     database.save(user); // persist to the source of truth first
     *     cache.delete("user:" + user.getId());   // invalidate cache; returns true if the entry existed
     * }
     *
     * cache.delete((String) null); // throws IllegalArgumentException (key must not be null)
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
     * Future<Boolean> future = cache.asyncDelete("user:123"); // returns immediately; never null
     * boolean deleted = future.get();                         // blocks; yields true if the key existed, false if absent
     * if (deleted) {
     *     System.out.println("Delete operation acknowledged"); // printed when the key was removed
     * }
     *
     * // Async delete with timeout (requires: import java.util.concurrent.TimeoutException;)
     * Future<Boolean> future = cache.asyncDelete("session:abc"); // dispatches the delete
     * try {
     *     boolean deleted = future.get(500, TimeUnit.MILLISECONDS); // waits up to 500ms
     *     System.out.println("Session deleted: " + deleted); // true if the key existed
     * } catch (TimeoutException e) {
     *     future.cancel(true); // abandon the operation if it did not complete in time
     * }
     *
     * cache.asyncDelete((String) null); // throws IllegalArgumentException (key must not be null)
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
     * long pageViews = cache.incr("page:views"); // returns value+1, or -1 if "page:views" does not exist
     * if (pageViews != -1) {
     *     System.out.println("Page views: " + pageViews); // printed when the key existed
     * } else {
     *     // Initialize counter if it doesn't exist
     *     cache.set("page:views", "1", 0); // seed the counter with no expiration; returns true on success
     * }
     *
     * // Rate limiting
     * String key = "rate:limit:" + userId;
     * long attempts = cache.incr(key); // returns value+1, or -1 if the key is absent
     * if (attempts == -1) {
     *     // Key doesn't exist, initialize it
     *     cache.set(key, "1", 60000); // seed with 60s TTL; returns true on success
     *     attempts = 1;
     * }
     * if (attempts > MAX_ATTEMPTS) {
     *     throw new RateLimitException("Too many requests");
     * }
     *
     * cache.incr((String) null); // throws IllegalArgumentException (key must not be null)
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
     * long score = cache.incr("player:score", 10); // returns value+10, or -1 if the key is absent
     * if (score != -1) {
     *     System.out.println("New score: " + score); // printed when the key existed
     * } else {
     *     // Initialize if doesn't exist
     *     cache.set("player:score", "10", 0); // seed with no expiration; returns true on success
     * }
     *
     * // Batch processing counter
     * long processed = cache.incr("batch:processed", 100); // returns value+100, or -1 if absent
     *
     * // Points system
     * int points = calculatePoints(action);
     * long totalPoints = cache.incr("user:points:" + userId, points); // returns value+points, or -1 if absent
     *
     * // Bandwidth tracking
     * long bytesTransferred = cache.incr("bandwidth:today", fileSize); // returns value+fileSize, or -1 if absent
     * if (bytesTransferred > QUOTA) {
     *     logger.warn("Bandwidth quota exceeded"); // emitted when the running total exceeds QUOTA
     * }
     *
     * cache.incr("counter", -1);        // throws IllegalArgumentException (delta must be non-negative)
     * cache.incr((String) null, 1);     // throws IllegalArgumentException (key must not be null)
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
     * long count = cache.incr("counter:views", 1, 0); // seeds absent key with 0 (exp 0); returns 0
     * // Subsequent call: existing value 0 is incremented by 1, returns 1.
     * count = cache.incr("counter:views", 1, 0); // returns 1 (existing 0 + delta 1)
     *
     * // Auto-initializing counter
     * long requestCount = cache.incr("api:requests:" + endpoint, 1, 0);  // first call returns defaultValue 0
     * System.out.println("Request " + requestCount + " to " + endpoint); // prints the post-operation count
     *
     * cache.incr("counter", -1, 0L);    // throws IllegalArgumentException (delta must be non-negative)
     * cache.incr((String) null, 1, 0L); // throws IllegalArgumentException (key must not be null)
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
     * seconds for Memcached (rounded up if not exact). TTLs longer than 30 days are stored as an
     * absolute Unix expiration timestamp rather than a relative offset.
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
     * long count = cache.incr("counter:hourly", 1, 0, 3600000); // 3600000ms -> 3600s TTL; first call returns 0
     * System.out.println("Hourly count: " + count);             // prints the post-operation count
     *
     * // Daily quota counter
     * long dailyRequests = cache.incr("quota:daily:" + userId, 1, 0, 86400000); // 86400000ms -> 86400s TTL; first call returns 0
     * if (dailyRequests > DAILY_LIMIT) {
     *     throw new QuotaExceededException("Daily limit reached");
     * }
     *
     * cache.incr("counter", -1, 0L, 1000L);    // throws IllegalArgumentException (delta must be non-negative)
     * cache.incr((String) null, 1, 0L, 1000L); // throws IllegalArgumentException (key must not be null)
     * }</pre>
     *
     * @param key the cache key whose associated value is to be incremented; must not be {@code null}
     * @param delta the amount by which to increment the value when the key already exists;
     *              must be non-negative
     * @param defaultValue the initial value to set if the key does not exist; on first-write this
     *                     value is stored verbatim ({@code delta} is NOT added on insert)
     * @param liveTime the time-to-live in milliseconds for the key ({@code 0} or negative means no
     *                 expiration); a positive value is converted to seconds, rounded up if not exact;
     *                 values over 30 days are stored as an absolute expiration timestamp.
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
     * long remainingTokens = cache.decr("api:tokens:" + userId); // returns value-1 (clamped at 0), or -1 if absent
     * if (remainingTokens == -1) {
     *     // Key doesn't exist, initialize it
     *     cache.set("api:tokens:" + userId, "100", 60000); // seed with 60s TTL; returns true on success
     *     remainingTokens = 100;
     * }
     * if (remainingTokens == 0) {
     *     throw new RateLimitException("Rate limit exceeded");
     * }
     *
     * // Inventory management
     * long stock = cache.decr("product:stock:123"); // returns value-1 (never below 0), or -1 if absent
     * if (stock == -1) {
     *     // Handle key not found
     *     throw new KeyNotFoundException();
     * } else if (stock == 0) {
     *     // Memcached prevents going below 0
     *     throw new OutOfStockException();
     * }
     *
     * cache.decr((String) null); // throws IllegalArgumentException (key must not be null)
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
     * long inventory = cache.decr("product:stock:456", 5); // returns value-5 (clamped at 0), or -1 if absent
     * if (inventory == -1) {
     *     // Key doesn't exist
     *     throw new KeyNotFoundException();
     * } else if (inventory == 0) {
     *     // Either reached 0 or underflow prevented (delta > original value)
     *     System.out.println("Inventory depleted or insufficient"); // printed when the result clamped to 0
     * } else {
     *     System.out.println("Remaining inventory: " + inventory); // prints the post-decrement value
     * }
     *
     * // API quota management
     * int requestCost = calculateCost(request);
     * long quotaRemaining = cache.decr("quota:" + apiKey, requestCost); // returns value-requestCost (>=0), or -1 if absent
     * if (quotaRemaining == -1) {
     *     throw new KeyNotFoundException();
     * } else if (quotaRemaining == 0) {
     *     // Quota exhausted or exceeded
     *     throw new QuotaExceededException();
     * }
     *
     * // Reservation system (checking before decrement)
     * String key = "event:seats:789";
     * Long currentSeats = cache.get(key); // returns null if not cached
     * if (currentSeats != null && currentSeats >= numberOfTickets) {
     *     long availableSeats = cache.decr(key, numberOfTickets); // returns value-numberOfTickets (>=0)
     *     if (availableSeats >= 0) {
     *         System.out.println("Reservation successful"); // printed once seats were decremented
     *     }
     * } else {
     *     throw new NotEnoughSeatsException();
     * }
     *
     * cache.decr("counter", -1);    // throws IllegalArgumentException (delta must be non-negative)
     * cache.decr((String) null, 1); // throws IllegalArgumentException (key must not be null)
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
     * long remaining = cache.decr("inventory:item123", 1, 100); // seeds absent key with 100 (exp 0); returns 100
     * // Subsequent call: existing value 100 is decremented by 1, returns 99.
     * remaining = cache.decr("inventory:item123", 1, 100); // returns 99 (existing 100 - delta 1)
     *
     * // Auto-initializing quota
     * long quotaRemaining = cache.decr("quota:user:" + userId, 1, 1000); // first call returns defaultValue 1000
     * if (quotaRemaining == 0) {
     *     System.out.println("Quota exhausted"); // printed when the value clamped to 0
     * }
     *
     * cache.decr("counter", -1, 0L);    // throws IllegalArgumentException (delta must be non-negative)
     * cache.decr((String) null, 1, 0L); // throws IllegalArgumentException (key must not be null)
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
     * seconds for Memcached (rounded up if not exact). TTLs longer than 30 days are stored as an
     * absolute Unix expiration timestamp rather than a relative offset.
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
     * long remaining = cache.decr("quota:user:123", 1, 1000, 86400000); // 86400000ms -> 86400s TTL; first call returns 1000
     * System.out.println("Remaining quota: " + remaining);              // prints the post-operation value
     *
     * // Hourly rate limit
     * long hourlyLimit = cache.decr("rate:hourly:" + userId, 1, 100, 3600000); // 3600000ms -> 3600s TTL; first call returns 100
     * if (hourlyLimit == 0) {
     *     throw new RateLimitException("Hourly limit reached");
     * }
     *
     * cache.decr("counter", -1, 0L, 1000L);    // throws IllegalArgumentException (delta must be non-negative)
     * cache.decr((String) null, 1, 0L, 1000L); // throws IllegalArgumentException (key must not be null)
     * }</pre>
     *
     * @param key the cache key whose associated value is to be decremented; must not be {@code null}
     * @param delta the amount by which to decrement the value when the key already exists;
     *              must be non-negative
     * @param defaultValue the initial value to set if the key does not exist; on first-write this
     *                     value is stored verbatim ({@code delta} is NOT subtracted on insert)
     * @param liveTime the time-to-live in milliseconds for the key ({@code 0} or negative means no
     *                 expiration); a positive value is converted to seconds, rounded up if not exact;
     *                 values over 30 days are stored as an absolute expiration timestamp.
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
     * cache.flushAll();                             // blocks until the flush is acknowledged; throws IllegalStateException if it fails
     * System.out.println("All cache data cleared"); // printed once the flush returned successfully
     *
     * // Safe usage in testing
     * @After
     * public void cleanupCache() {
     *     if (isTestEnvironment()) {
     *         cache.flushAll(); // clears every server; throws IllegalStateException on non-acknowledgement
     *     }
     * }
     *
     * // Production usage with confirmation
     * public void clearCache(String confirmationToken) {
     *     if (!"CONFIRM_FLUSH_ALL".equals(confirmationToken)) {
     *         throw new IllegalArgumentException("Invalid confirmation");
     *     }
     *     logger.warn("Flushing all cache data"); // audit the impending destructive flush
     *     cache.flushAll(); // removes all keys from all servers
     *     auditLog.record("CACHE_FLUSH_ALL", user); // record the destructive action
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
     * Future<Boolean> future = cache.asyncFlushAll(); // returns immediately; never null
     * boolean flushed = future.get();                 // blocks; yields true when the flush completes
     * if (flushed) {
     *     System.out.println("All data flushed"); // printed when the flush succeeded
     * }
     *
     * // Async flush with timeout (requires: import java.util.concurrent.TimeoutException;)
     * Future<Boolean> future = cache.asyncFlushAll(); // dispatches the flush
     * try {
     *     boolean flushed = future.get(2000, TimeUnit.MILLISECONDS); // waits up to 2s
     *     System.out.println("Flush completed: " + flushed); // true on success
     * } catch (TimeoutException e) {
     *     logger.warn("Flush operation timed out"); // emitted if the result was not ready within 2s
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
     * boolean scheduled = cache.flushAll(5000); // 5000ms -> relative delay of 5s; returns true on success
     * if (scheduled) {
     *     System.out.println("Flush scheduled for 5 seconds from now"); // printed when scheduling succeeded
     * }
     *
     * // The delay is ALWAYS a relative number of seconds; large values are NOT converted to an
     * // absolute timestamp (unlike set/add/replace expirations).
     * cache.flushAll(3_456_000_000L); // 40 days -> relative delay of 3_456_000s (not an epoch timestamp)
     *
     * // Delayed flush for maintenance window
     * long delayUntilMaintenance = calculateDelayToMaintenance();
     * boolean scheduled = cache.flushAll(delayUntilMaintenance); // converts ms to relative seconds (rounded up)
     * if (scheduled) {
     *     logger.info("Cache flush scheduled for maintenance window"); // emitted once scheduling succeeded
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
     * Future<Boolean> future = cache.asyncFlushAll(10000); // 10000ms -> relative delay of 10s; returns immediately
     * boolean scheduled = future.get();                    // blocks; yields true on success
     *
     * // The delay is ALWAYS a relative number of seconds; large values are NOT converted to an
     * // absolute timestamp (unlike set/add/replace expirations).
     * cache.asyncFlushAll(3_456_000_000L); // 40 days -> relative delay of 3_456_000s (not an epoch timestamp)
     *
     * // Async delayed flush with timeout (requires: import java.util.concurrent.TimeoutException;)
     * Future<Boolean> future = cache.asyncFlushAll(30000); // 30000ms -> relative delay of 30s
     * try {
     *     boolean scheduled = future.get(1000, TimeUnit.MILLISECONDS); // waits up to 1s for confirmation
     *     System.out.println("Flush scheduled: " + scheduled);         // true on success
     * } catch (TimeoutException e) {
     *     logger.warn("Failed to schedule flush"); // emitted if confirmation was not received within 1s
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
     *     cache.set("user:123", user, 3600000); // 3600s TTL; returns true on success
     *     User cached = cache.get("user:123");  // returns the cached User, or null if absent
     * } finally {
     *     cache.disconnect();                              // immediate shutdown; safe to call again (idempotent)
     *     System.out.println("Cache client disconnected"); // printed after shutdown
     * }
     *
     * // Spring Bean destruction
     * @PreDestroy
     * public void cleanup() {
     *     if (cache != null) {
     *         cache.disconnect(); // releases connections and threads; no-op if already shut down
     *     }
     * }
     *
     * // Application shutdown hook
     * Runtime.getRuntime().addShutdownHook(new Thread(() -> {
     *     logger.info("Shutting down cache client"); // emitted as the JVM shuts down
     *     cache.disconnect(); // immediate shutdown of the shared client
     * }));
     * }</pre>
     *
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
     *     cache.disconnect(5000);   // waits up to 5000ms for pending ops; safe to call again (idempotent)
     * }
     *
     * // Application shutdown with graceful timeout
     * public void shutdown() {
     *     logger.info("Shutting down cache client"); // emitted before the graceful shutdown
     *     cache.disconnect(10000);   // waits up to 10000ms for pending ops to finish
     *     logger.info("Cache client shutdown complete"); // emitted after disconnect returns
     * }
     *
     * cache.disconnect(-1); // throws IllegalArgumentException (timeout must not be negative)
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
