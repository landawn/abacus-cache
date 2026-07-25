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
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.ExceptionUtil;
import com.landawn.abacus.util.N;

import net.spy.memcached.CachedData;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

/**
 * Preserves the pre-2.8.5 JVM descriptors of asynchronous methods whose source-level return type
 * was refined from {@link Future} to {@link ContinuableFuture}. Implementing this contract causes
 * javac to emit the covariant bridge methods needed by already-compiled clients while the public
 * implementation continues to expose the richer return type to newly compiled code.
 */
interface LegacySpyMemcachedAsyncApi<T> {

    Future<T> asyncGet(String key);

    Future<Map<String, T>> asyncGetBulk(String... keys);

    Future<Map<String, T>> asyncGetBulk(Collection<String> keys);

    Future<Boolean> asyncAdd(String key, T value, long liveTime);

    Future<Boolean> asyncReplace(String key, T value, long liveTime);

    Future<Boolean> asyncFlushAll();

    Future<Boolean> asyncFlushAll(long delay);
}

/**
 * A Memcached distributed cache client implementation backed by the SpyMemcached library.
 * Provides synchronous and asynchronous access to one or more Memcached servers. When the
 * Kryo parser is available on the classpath ({@link ParserFactory#isKryoParserAvailable()}),
 * a {@link KryoTranscoder} is installed for serialization; otherwise the default SpyMemcached
 * transcoder is used.
 *
 * <p>Key features:
 * <ul>
 * <li>Synchronous and {@link ContinuableFuture}-based asynchronous operations.</li>
 * <li>Bulk get operations to reduce network round-trips.</li>
 * <li>Atomic increment/decrement operations.</li>
 * <li>Configurable operation timeout.</li>
 * </ul>
 *
 * <p><b>Thread Safety:</b> Instances of this class are safe for concurrent use by multiple
 * threads. The wrapper delegates to a single shared {@link MemcachedClient}, which performs
 * network I/O on a dedicated selector thread and serializes commands through its internal
 * operation queue; concurrent calls from application threads enqueue operations safely.
 * Disconnect publishes a terminal lifecycle flag before delegate shutdown starts. Cache
 * operations begun afterward fail deterministically with {@link IllegalStateException}; an
 * operation already past its lifecycle check can still race with shutdown and surface the
 * delegate's own shutdown exception.
 *
 * <p><b>Lifecycle contract for all operations:</b> every public cache operation in this class
 * ({@code get}, bulk gets, {@code put}/{@code add}/{@code replace}/{@code remove}, counters and
 * flushes, including their asynchronous and compatibility aliases) checks the terminal flag before
 * argument validation, TTL conversion, value serialization, or delegate dispatch. Once either
 * {@code disconnect} overload begins, each of those methods throws {@link IllegalStateException}.
 * This common lifecycle exception is stated here instead of repeated in every operation's
 * {@code @throws} list. {@link #serverUrl()} remains an immutable configuration accessor and can be
 * queried after disconnect. Repeated no-argument disconnects, and repeated timed disconnects with
 * a non-negative timeout, are no-ops; {@link #disconnect(long)} always validates its argument, so a
 * negative timeout is rejected even after shutdown.
 * Instances own network resources and are intended to be long-lived and application-scoped;
 * optionally disconnect a shared instance once during application shutdown, not after each use.
 *
 * <p><b>Key validation:</b> beyond the {@code null} checks documented per method, the underlying
 * memcached client validates every key on the calling thread and throws
 * {@link IllegalArgumentException} for keys that are empty, longer than 250 bytes (UTF-8), or that
 * contain a space, CR, LF, or NUL byte. This applies to every keyed operation and is stated here
 * instead of repeated in each method's {@code @throws} list.
 *
 * <p><b>Counter encoding:</b> Memcached's native increment/decrement commands operate only on raw
 * ASCII decimal values. Ordinary values written by {@link #put(String, Object, long)} use the
 * configured transcoder and therefore are not valid native counters when Kryo is active. The
 * increment/decrement overloads with a default value seed a missing key as raw ASCII; continue to
 * access such keys through the counter methods because a Kryo-backed {@link #get(String)} cannot
 * decode that raw representation.
 *
 * <p><b>Asynchronous completion:</b> asynchronous methods return after validation, any required
 * serialization, and operation enqueueing; they do not wait for a server response. Enqueueing can
 * still block briefly when the bounded operation queue is full. Calling no-argument {@code get()}
 * on a returned future is bounded by the configured operation timeout. In particular, this class
 * wraps spymemcached's otherwise effectively-unbounded bulk-get {@code Future#get()} so a written
 * request to an unresponsive server cannot pin the caller indefinitely.
 *
 * <p><b>Subclassing:</b> the four bulk-get methods ({@link #getBulk(String...)},
 * {@link #getBulk(Collection)}, {@link #asyncGetBulk(String...)}, and
 * {@link #asyncGetBulk(Collection)}) are {@code final}. They copy the caller-supplied key container
 * before validating it and then issue commands only from that private copy, so validation cannot be
 * defeated by mutating the argument afterward; sealing them keeps that guarantee intact for every
 * instance of this type. Every other operation remains overridable.
 *
 * <p>Example usage:
 * <pre>{@code
 * // Construct once during application setup and share across requests/threads.
 * SpyMemcached<User> cache = new SpyMemcached<>("localhost:11211");
 * // Synchronous operations
 * cache.put("user:123", user, 3600000);   // Cache for 1 hour
 * User cached = cache.get("user:123");
 *
 * // Asynchronous operations
 * ContinuableFuture<Boolean> future = cache.asyncPut("user:456", anotherUser, 3600000);
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
public class SpyMemcached<T> extends AbstractDistributedCacheClient<T> implements LegacySpyMemcachedAsyncApi<T> {

    private static final Logger logger = LoggerFactory.getLogger(SpyMemcached.class);
    private static final int MEMCACHED_MAX_RELATIVE_EXPIRATION_SECONDS = 30 * 24 * 60 * 60;

    /**
     * Upper bound for the effective operation timeout. spymemcached converts the configured
     * operation timeout to nanoseconds (roughly {@code creationTime + MILLISECONDS.toNanos(timeout)})
     * when checking whether an in-flight operation has timed out; a huge configured timeout (e.g.
     * {@code Long.MAX_VALUE} as a "no timeout" sentinel) overflows that arithmetic and makes every
     * operation appear instantly timed out. Clamping to ~146 years is "effectively no timeout"
     * without the overflow.
     */
    private static final long MAX_SAFE_OPERATION_TIMEOUT_MILLIS = Long.MAX_VALUE / 2_000_000L;

    /**
     * Stores counter seeds as raw ASCII decimal bytes (flags 0) — the only representation
     * memcached's native incr/decr can mutate. The client's default transcoder (Kryo when
     * available) must never be used for counter seeds: a Kryo-encoded seed makes every
     * subsequent native incr/decr fail with {@code CLIENT_ERROR cannot increment or decrement
     * non-numeric value}, which also tears down and re-establishes the connection.
     */
    private static final Transcoder<Object> ASCII_COUNTER_TRANSCODER = new Transcoder<>() {
        @Override
        public boolean asyncDecode(final CachedData d) {
            return false;
        }

        @Override
        public CachedData encode(final Object o) {
            return new CachedData(0, String.valueOf(o).getBytes(StandardCharsets.US_ASCII), getMaxSize());
        }

        @Override
        public Object decode(final CachedData d) {
            return new String(d.getData(), StandardCharsets.US_ASCII);
        }

        @Override
        public int getMaxSize() {
            return CachedData.MAX_SIZE;
        }
    };

    private final MemcachedClient mc;

    /**
     * The effective (clamped) operation timeout, used by {@link #resultOf(Future)} as the bound
     * for synchronous waits. spymemcached enforces its internal per-operation timeout only for
     * operations still queued for write; once a request has been written to a wedged server, the
     * caller's wait is the only bound — so this must be the configured timeout itself, matching
     * both the documented contract and the delegate's own synchronous methods.
     */
    private final long operationTimeoutMillis;

    private volatile boolean isShutdown = false;

    /**
     * Creates a new {@code SpyMemcached} instance using {@link DistributedCacheClient#DEFAULT_TIMEOUT}
     * as the operation timeout.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * SpyMemcached<String> cache = new SpyMemcached<>("localhost:11211"); // uses DEFAULT_TIMEOUT
     * cache.put("key1", "value1", 3600000); // stores with 3600s TTL; returns true on success
     * // Retain and share cache; optionally disconnect it during application shutdown.
     *
     * // A null/blank serverUrl is rejected before any connection attempt.
     * new SpyMemcached<>((String) null); // throws IllegalArgumentException
     * new SpyMemcached<>("   ");         // throws IllegalArgumentException (blank)
     * }</pre>
     *
     * @param serverUrl one or more {@code host:port} addresses separated by commas, whitespace,
     *                  or both; must not be {@code null}, empty, or blank
     * @throws IllegalArgumentException if {@code serverUrl} is {@code null}, empty, blank, or contains
     *         no valid server addresses
     * @throws RuntimeException if {@code serverUrl} cannot be parsed (e.g., an unresolvable hostname)
     *         or local client/socket setup fails. Note: connections are established asynchronously by
     *         the SpyMemcached IO thread — a resolvable but unreachable or down server does <b>not</b>
     *         fail construction; operations against it fail later with timeouts.
     * @see #SpyMemcached(String, long)
     */
    public SpyMemcached(final String serverUrl) {
        this(serverUrl, DEFAULT_TIMEOUT);
    }

    /**
     * Creates a new {@code SpyMemcached} instance with the specified operation timeout.
     * The timeout applies to all cache operations (get, put, remove, etc.). If Kryo is available
     * on the classpath (checked via {@link ParserFactory#isKryoParserAvailable()}), it is used
     * for object serialization via {@link KryoTranscoder}; otherwise the default SpyMemcached
     * serialization mechanism is used.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Create cache with 5-second operation timeout
     * SpyMemcached<User> cache = new SpyMemcached<>("localhost:11211", 5000); // 5000ms operation timeout
     * cache.put("user:123", user, 3600000); // stores with 3600s TTL; returns true on success
     * // Retain and share cache; optionally disconnect it during application shutdown.
     *
     * // timeout must be strictly positive (checkArgPositive).
     * new SpyMemcached<>("localhost:11211", 0);  // throws IllegalArgumentException
     * new SpyMemcached<>("localhost:11211", -1); // throws IllegalArgumentException
     * }</pre>
     *
     * @param serverUrl one or more {@code host:port} addresses separated by commas, whitespace,
     *                  or both; must not be {@code null}, empty, or blank
     * @param timeout the operation timeout in milliseconds; must be positive. Applies to all cache
     *                operations. Extremely large values (beyond a ~146-year safety cap) are clamped,
     *                because spymemcached's internal nanosecond arithmetic would otherwise overflow
     *                and fail every operation instantly.
     * @throws IllegalArgumentException if {@code timeout} is not positive, or if {@code serverUrl}
     *         is {@code null}, empty, blank, or contains no valid server addresses
     * @throws RuntimeException if {@code serverUrl} cannot be parsed (e.g., an unresolvable hostname)
     *         or local client/socket setup fails. Note: connections are established asynchronously by
     *         the SpyMemcached IO thread — a resolvable but unreachable or down server does <b>not</b>
     *         fail construction; operations against it fail later with timeouts.
     */
    public SpyMemcached(final String serverUrl, final long timeout) {
        super(serverUrl);

        N.checkArgPositive(timeout, "timeout");

        // The getAddressList call is the load-bearing part: it fails fast with a descriptive
        // IllegalArgumentException on any malformed serverUrl before any client resources exist.
        // Parsed once (each parse eagerly resolves every hostname) and reused for client creation.
        // The isEmpty branch is currently unreachable (getAddressList throws rather than returning
        // an empty list) and is kept deliberately as a guard against that contract changing in a
        // future abacus-common release.
        final List<InetSocketAddress> serverAddresses = AddrUtil.getAddressList(serverUrl);

        if (N.isEmpty(serverAddresses)) {
            throw new IllegalArgumentException("No valid server addresses found in: " + serverUrl);
        }

        // Clamp to the spymemcached-safe maximum: see MAX_SAFE_OPERATION_TIMEOUT_MILLIS.
        final long effectiveTimeout = Math.min(timeout, MAX_SAFE_OPERATION_TIMEOUT_MILLIS);
        operationTimeoutMillis = effectiveTimeout;

        MemcachedClient tempMc = null;
        try {
            final Transcoder<Object> transcoder = ParserFactory.isKryoParserAvailable() ? new KryoTranscoder<>() : null;

            final ConnectionFactory connFactory = new DefaultConnectionFactory() {
                @Override
                public long getOperationTimeout() {
                    return effectiveTimeout;
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

            tempMc = createSpyMemcachedClient(serverUrl, serverAddresses, connFactory);
            this.mc = tempMc;
        } catch (final Exception e) {
            // NOTE: with the current try-block body the shutdown below is unreachable (the only
            // statement after client creation is the field assignment, which cannot throw). It is
            // kept deliberately so that any code later added between creation and the end of the
            // try block cannot silently leak a connected client and its IO thread.
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
     * User loaded = cache.get("user:123"); // returns null if not cached
     * if (loaded == null) {
     *     loaded = database.findUser(123); // load from the source of truth
     *     cache.put("user:123", loaded, 3600000); // re-populate the cache; returns true on success
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
        assertNotShutdown();
        N.checkArgNotNull(key, "key");
        // Route through resultOf (rather than the client's synchronous get) so an interrupt
        // restores the thread's interrupt flag: spymemcached's sync methods wrap
        // InterruptedException in a RuntimeException WITHOUT restoring the flag, silently
        // defeating cooperative cancellation in thread pools.
        return (T) resultOf(mc.asyncGet(key));
    }

    /**
     * Asynchronously retrieves an object from the cache by its key.
     * The operation is executed asynchronously by the underlying SpyMemcached client. This method
     * returns after validation and enqueueing without waiting for the server response; enqueueing
     * can still block briefly if the operation queue is full. The returned Future can be used to
     * check completion status and retrieve the result when available. The operation timeout is
     * configured during client construction.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple async get
     * ContinuableFuture<User> getFuture = cache.asyncGet("user:123"); // dispatched; never null
     * User user = getFuture.get();                            // blocks until complete; yields the value or null if absent
     *
     * // Async get with timeout (requires: import java.util.concurrent.TimeoutException;)
     * ContinuableFuture<User> timedGet = cache.asyncGet("user:123"); // dispatches the get; returns the Future
     * try {
     *     User timedUser = timedGet.get(1000, TimeUnit.MILLISECONDS); // waits up to 1s for the result
     * } catch (TimeoutException e) {
     *     timedGet.cancel(true); // abandon the operation if it did not complete in time
     *     System.out.println("Get operation timed out"); // printed if the result was not ready in time
     * }
     *
     * cache.asyncGet((String) null); // throws IllegalArgumentException (key must not be null)
     * }</pre>
     *
     * @param key the cache key whose associated value is to be retrieved; must not be {@code null}
     * @return a {@link ContinuableFuture} that will yield the cached object of type {@code T}, or
     *         {@code null} if not found, expired, or evicted
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the operation fails to initiate
     */
    @SuppressWarnings("unchecked")
    @Override
    public ContinuableFuture<T> asyncGet(final String key) {
        assertNotShutdown();
        N.checkArgNotNull(key, "key");
        return ContinuableFuture.wrap((Future<T>) mc.asyncGet(key));
    }

    /**
     * Retrieves multiple objects in one batched request. The client generally sends one operation
     * to each server owning at least one requested key, reducing round-trips versus individual gets.
     * Keys not found in the cache or that have expired
     * will not be present in the returned map. This is a synchronous operation that blocks until
     * complete or timeout. The caller's array is copied before validation and dispatch, so later
     * mutation cannot change the in-flight request.
     *
     * <p><b>&#9888;&#65039; A missing key is not always a cache miss:</b> when an operation sits
     * unwritten longer than the operation timeout (server stalled or reconnecting, write-queue
     * backlog), the underlying client completes it as timed out without cancelling or erroring it,
     * and the returned map silently lacks that server's keys instead of this method throwing. The
     * single-key {@code get} throws in the identical scenario; only the bulk path is silent.
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
        assertNotShutdown();
        final String[] keySnapshot = snapshotBulkKeys(keys);
        // See get(String): resultOf preserves the interrupt flag, unlike the sync client call.
        return (Map<String, T>) resultOf(mc.asyncGetBulk(keySnapshot));
    }

    /**
     * Asynchronously retrieves multiple objects from the cache.
     * This method returns after validation and enqueueing without waiting for server responses;
     * enqueueing can still block briefly if the operation queue is full. The returned Future
     * contains a map of found key-value pairs. Keys not found in the cache or that have expired
     * will not be present in the returned map. The client generally sends one operation to each
     * involved server. The caller's array is copied before validation and dispatch.
     *
     * <p><b>&#9888;&#65039; A missing key is not always a cache miss:</b> an operation that sits
     * unwritten longer than the operation timeout is completed as timed out without error, and the
     * future's map silently lacks that server's keys (see {@link #getBulk(String...)}).
     * A no-argument {@link Future#get()} is bounded by the configured operation timeout; if that
     * outer wait expires, the delegate is cancelled and {@link ExecutionException} is thrown with
     * a {@link TimeoutException} cause. A caller-supplied timed {@code get} retains the standard
     * {@code TimeoutException} behavior.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple async bulk get
     * ContinuableFuture<Map<String, User>> bulkFuture = cache.asyncGetBulk("user:1", "user:2"); // dispatched; never null
     * Map<String, User> users = bulkFuture.get();                                      // bounded by the configured operation timeout
     *
     * // Async bulk get with timeout (requires: import java.util.concurrent.TimeoutException;)
     * ContinuableFuture<Map<String, User>> timedBulk = cache.asyncGetBulk("user:1", "user:2", "user:3"); // dispatches the bulk get
     * try {
     *     Map<String, User> timedUsers = timedBulk.get(2000, TimeUnit.MILLISECONDS); // waits up to 2s
     *     System.out.println("Retrieved " + timedUsers.size() + " users");          // size() <= number of requested keys
     * } catch (TimeoutException e) {
     *     timedBulk.cancel(true); // abandon the operation if it did not complete in time
     * }
     *
     * cache.asyncGetBulk((String[]) null);  // throws IllegalArgumentException (keys must not be null)
     * cache.asyncGetBulk("user:1", null);   // throws IllegalArgumentException (no null elements allowed)
     * }</pre>
     *
     * @param keys the cache keys whose associated values are to be retrieved; must not be
     *             {@code null} and must not contain {@code null} elements
     * @return a {@link ContinuableFuture} that will yield the map of found key-value pairs; the map is
     *         never {@code null} but may be empty
     * @throws IllegalArgumentException if {@code keys} is {@code null} or contains {@code null} elements
     * @throws RuntimeException if the operation fails to initiate
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public final ContinuableFuture<Map<String, T>> asyncGetBulk(final String... keys) {
        assertNotShutdown();
        final String[] keySnapshot = snapshotBulkKeys(keys);
        return wrapBulkFuture((Future) mc.asyncGetBulk(keySnapshot));
    }

    /**
     * Retrieves multiple objects from the cache using a batched collection request. The client
     * generally sends one operation to each server owning at least one requested key. Keys not
     * found in the cache or that have expired
     * will not be present in the returned map. This is a synchronous operation that blocks until
     * complete or timeout. The collection is copied and validated in one pass before dispatch;
     * later caller mutation cannot affect the request.
     *
     * <p><b>&#9888;&#65039; A missing key is not always a cache miss:</b> an operation that sits
     * unwritten longer than the operation timeout is completed as timed out without error, and the
     * returned map silently lacks that server's keys (see {@link #getBulk(String...)}).
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Using a List (requires: import java.util.List;)
     * List<String> userKeys = Arrays.asList("user:123", "user:456", "user:789");
     * Map<String, User> listUsers = cache.getBulk(userKeys); // never null; missing keys are simply absent
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
     * Map<String, User> generatedUsers = cache.getBulk(keys);   // returns the cached subset
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
    public final Map<String, T> getBulk(final Collection<String> keys) {
        assertNotShutdown();
        final List<String> keySnapshot = snapshotBulkKeys(keys);
        // See get(String): resultOf preserves the interrupt flag, unlike the sync client call.
        return (Map<String, T>) resultOf(mc.asyncGetBulk(keySnapshot));
    }

    /**
     * Asynchronously retrieves multiple objects using a collection of keys.
     * This method returns after validation and enqueueing without waiting for server responses;
     * enqueueing can still block briefly if the operation queue is full. The returned Future
     * contains a map of found key-value pairs. Keys not found in the cache or that have expired
     * will not be present in the returned map. The client generally sends one operation to each
     * involved server. The collection is copied and validated in one pass before dispatch.
     *
     * <p><b>&#9888;&#65039; A missing key is not always a cache miss:</b> an operation that sits
     * unwritten longer than the operation timeout is completed as timed out without error, and the
     * future's map silently lacks that server's keys (see {@link #getBulk(String...)}).
     * No-argument and caller-timed {@code get} behavior is the same as documented by
     * {@link #asyncGetBulk(String...)}.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple async bulk get with collection (requires: import java.util.Set;)
     * Set<String> keys = new HashSet<>(Arrays.asList("user:1", "user:2"));
     * ContinuableFuture<Map<String, User>> userFuture = cache.asyncGetBulk(keys); // dispatched; never null
     * Map<String, User> users = userFuture.get();                        // bounded by the configured operation timeout
     *
     * // Async bulk get from dynamically generated keys (requires: import java.util.concurrent.TimeoutException;)
     * List<String> productKeys = generateProductKeys();
     * ContinuableFuture<Map<String, Product>> productFuture = cache.asyncGetBulk(productKeys); // dispatches the bulk get
     * Map<String, Product> products = productFuture.get(3000, TimeUnit.MILLISECONDS);           // waits up to 3s
     *
     * cache.asyncGetBulk((Collection<String>) null);     // throws IllegalArgumentException (keys must not be null)
     * cache.asyncGetBulk(Arrays.asList("user:1", null)); // throws IllegalArgumentException (no null elements allowed)
     * }</pre>
     *
     * @param keys the collection of cache keys whose associated values are to be retrieved;
     *             must not be {@code null} and must not contain {@code null} elements
     * @return a {@link ContinuableFuture} that will yield the map of found key-value pairs; the map is
     *         never {@code null} but may be empty
     * @throws IllegalArgumentException if {@code keys} is {@code null} or contains {@code null} elements
     * @throws RuntimeException if the operation fails to initiate
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public final ContinuableFuture<Map<String, T>> asyncGetBulk(final Collection<String> keys) {
        assertNotShutdown();
        final List<String> keySnapshot = snapshotBulkKeys(keys);
        return wrapBulkFuture((Future) mc.asyncGetBulk(keySnapshot));
    }

    /**
     * Takes ownership of a stable key-array view before validation and dispatch. A caller retains
     * ownership of a varargs array and may mutate it as soon as this method starts; dispatching the
     * live array after validation would create a time-of-check/time-of-use gap.
     */
    private static String[] snapshotBulkKeys(final String... keys) {
        N.checkArgNotNull(keys, "keys");

        final String[] keySnapshot = keys.clone();
        checkBulkKeys(keySnapshot);
        return keySnapshot;
    }

    /**
     * Copies the collection, then validates the stable copy. The copy insulates the asynchronous
     * request from later caller mutations, and validating the copy (rather than the live
     * collection) means the validated view is exactly the dispatched view even for
     * custom/concurrent collections.
     */
    private static List<String> snapshotBulkKeys(final Collection<String> keys) {
        N.checkArgNotNull(keys, "keys");

        final List<String> keySnapshot = new ArrayList<>(keys);
        checkBulkKeys(keySnapshot);
        return keySnapshot;
    }

    /**
     * Applies this client's configured timeout to a bulk future's no-argument {@code get()}.
     * spymemcached's {@code BulkGetFuture#get()} waits for {@link Long#MAX_VALUE} milliseconds,
     * unlike its single-operation futures, so a request already written to an unresponsive server
     * can otherwise wait effectively forever. Explicitly timed {@code get} calls are passed through
     * unchanged so the caller's timeout remains authoritative.
     */
    private <R> ContinuableFuture<R> wrapBulkFuture(final Future<R> future) {
        return ContinuableFuture.wrap(new DefaultTimeoutFuture<>(future, operationTimeoutMillis));
    }

    /** Delegating future that changes only the default wait bound. */
    private static final class DefaultTimeoutFuture<R> implements Future<R> {

        private final Future<R> delegate;
        private final long timeoutMillis;

        DefaultTimeoutFuture(final Future<R> delegate, final long timeoutMillis) {
            this.delegate = delegate;
            this.timeoutMillis = timeoutMillis;
        }

        @Override
        public boolean cancel(final boolean mayInterruptIfRunning) {
            return delegate.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return delegate.isCancelled();
        }

        @Override
        public boolean isDone() {
            return delegate.isDone();
        }

        @Override
        public R get() throws InterruptedException, ExecutionException {
            try {
                return delegate.get(timeoutMillis, TimeUnit.MILLISECONDS);
            } catch (final TimeoutException e) {
                // A timed-out bulk request is no longer useful to this caller. Cancellation also
                // keeps reconnect queues from retaining work that may never receive a response.
                delegate.cancel(true);
                throw new ExecutionException(e);
            }
        }

        @Override
        public R get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return delegate.get(timeout, unit);
        }
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
     * boolean success = cache.put("user:123", user, 3600000); // 3600000ms -> 3600s TTL; returns true on success
     * if (success) {
     *     System.out.println("User cached successfully"); // printed when the store succeeded
     * }
     *
     * // Cache session data with 30 minute TTL
     * Session session = new Session("abc123", user);
     * cache.put("session:" + session.getId(), session, 1800000); // 1800000ms -> 1800s TTL; returns true on success
     *
     * // Cache with no expiration
     * Config config = loadConfig();
     * cache.put("app:config", config, 0);   // liveTime 0 -> 0s ("no expiration"); returns true on success
     *
     * // A null value is accepted when the Kryo transcoder is active (Kryo on the classpath, the
     * // default for this wrapper); the stock SerializingTranscoder rejects null with a
     * // NullPointerException. Only a null key is always rejected.
     * cache.put("maybe-null", null, 60000); // returns true on success; stores a null value
     *
     * // Updating existing value
     * Product product = cache.get("product:456");
     * product.setPrice(99.99); // mutate the retrieved object before re-storing
     * cache.put("product:456", product, 7200000);   // 7200000ms -> 7200s (2-hour) TTL; returns true on success
     *
     * cache.put((String) null, user, 3600000); // throws IllegalArgumentException (key must not be null)
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated; must not be {@code null}
     * @param value the value to cache; may be {@code null} when the Kryo transcoder is active
     *              (the default with Kryo on the classpath). With the stock spymemcached
     *              {@code SerializingTranscoder} (Kryo absent), a {@code null} value throws
     *              {@code NullPointerException}
     * @param liveTime the time-to-live in milliseconds ({@code 0} or negative means no expiration); a
     *                 positive value is converted to seconds, rounded up if not exact; values over 30
     *                 days are stored as an absolute expiration timestamp. A TTL whose absolute
     *                 expiration would exceed epoch second 2^31-1 (January 2038) is rejected with
     *                 {@code IllegalArgumentException} (memcached expirations are 32-bit).
     * @return {@code true} if the operation succeeded; {@code false} otherwise
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    @Override
    public boolean put(final String key, final T value, final long liveTime) {
        assertNotShutdown();
        N.checkArgNotNull(key, "key");
        return Boolean.TRUE.equals(resultOf(mc.set(key, toMemcachedExpiration(liveTime), value)));
    }

    /**
     * Asynchronously stores an object in the cache with a specified time-to-live.
     * The returned Future can be used to check if the operation succeeded. This method returns
     * after validation, synchronous value serialization, and enqueueing without waiting for the
     * server response; enqueueing can still block briefly if the operation queue is full. The
     * liveTime is converted from milliseconds to seconds for Memcached (rounded up if not exact).
     * TTLs longer than 30 days are stored as an absolute Unix expiration timestamp rather than a
     * relative offset.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple async set
     * ContinuableFuture<Boolean> storeFuture = cache.asyncPut("user:123", user, 3600000); // dispatched; never null
     * boolean stored = storeFuture.get();                                       // blocks until complete; yields true on success
     * if (stored) {
     *     System.out.println("Set operation succeeded"); // printed when the store succeeded
     * }
     *
     * // Async set with timeout (requires: import java.util.concurrent.TimeoutException;)
     * ContinuableFuture<Boolean> timedStore = cache.asyncPut("product:456", product, 7200000); // dispatches the store
     * try {
     *     boolean storedInTime = timedStore.get(1000, TimeUnit.MILLISECONDS); // waits up to 1s for completion
     * } catch (TimeoutException e) {
     *     timedStore.cancel(true); // abandon the operation if it did not complete in time
     *     System.err.println("Set operation timed out"); // printed when the wait elapsed
     * }
     *
     * cache.asyncPut((String) null, user, 3600000); // throws IllegalArgumentException (key must not be null)
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated; must not be {@code null}
     * @param value the value to cache; may be {@code null} when the Kryo transcoder is active
     *              (the default with Kryo on the classpath). With the stock spymemcached
     *              {@code SerializingTranscoder} (Kryo absent), a {@code null} value throws
     *              {@code NullPointerException}
     * @param liveTime the time-to-live in milliseconds ({@code 0} or negative means no expiration); a
     *                 positive value is converted to seconds, rounded up if not exact; values over 30
     *                 days are stored as an absolute expiration timestamp. A TTL whose absolute
     *                 expiration would exceed epoch second 2^31-1 (January 2038) is rejected with
     *                 {@code IllegalArgumentException} (memcached expirations are 32-bit).
     * @return a {@link ContinuableFuture} that will yield {@code true} on success or {@code false} on failure
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the operation fails to initiate
     */
    public ContinuableFuture<Boolean> asyncPut(final String key, final T value, final long liveTime) {
        assertNotShutdown();
        N.checkArgNotNull(key, "key");
        return ContinuableFuture.wrap(mc.set(key, toMemcachedExpiration(liveTime), value));
    }

    /**
     * Compatibility alias for {@link #asyncPut(String, Object, long)}.
     *
     * @param key the cache key; must not be {@code null}
     * @param value the value to cache
     * @param liveTime the time-to-live in milliseconds
     * @return a future that completes with the storage result
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @deprecated use {@link #asyncPut(String, Object, long)}; retained with its original
     *             {@link Future} return descriptor for source and binary compatibility
     */
    @Deprecated(since = "2.8.5")
    public Future<Boolean> asyncSet(final String key, final T value, final long liveTime) {
        return asyncPut(key, value, liveTime);
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
     * // Install an immutable marker only once (no read-then-write race)
     * boolean installed = cache.add("schema:version", "v2", 0); // no expiration; false when already present
     * System.out.println(installed ? "Version marker installed" : "Version marker already exists");
     *
     * cache.add((String) null, user, 3600000); // throws IllegalArgumentException (key must not be null)
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated; must not be {@code null}
     * @param value the value to cache; may be {@code null} when the Kryo transcoder is active
     *              (the default with Kryo on the classpath). With the stock spymemcached
     *              {@code SerializingTranscoder} (Kryo absent), a {@code null} value throws
     *              {@code NullPointerException}
     * @param liveTime the time-to-live in milliseconds ({@code 0} or negative means no expiration); a
     *                 positive value is converted to seconds, rounded up if not exact; values over 30
     *                 days are stored as an absolute expiration timestamp. A TTL whose absolute
     *                 expiration would exceed epoch second 2^31-1 (January 2038) is rejected with
     *                 {@code IllegalArgumentException} (memcached expirations are 32-bit).
     * @return {@code true} if the object was added; {@code false} if the key already exists
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    public boolean add(final String key, final T value, final long liveTime) {
        assertNotShutdown();
        N.checkArgNotNull(key, "key");
        return Boolean.TRUE.equals(resultOf(mc.add(key, toMemcachedExpiration(liveTime), value)));
    }

    /**
     * Asynchronously adds an object to the cache only if the key doesn't already exist.
     * This operation is atomic and thread-safe across all distributed cache clients. The method
     * returns after validation, synchronous value serialization, and enqueueing without waiting for
     * the server response; enqueueing can still block briefly if the operation queue is full. If the
     * key already exists, the Future will contain {@code false}. The liveTime is converted from
     * milliseconds to seconds for Memcached (rounded up if not exact). TTLs longer than 30 days are
     * stored as an absolute Unix expiration timestamp rather than a relative offset.
     *
     * <p><b>Thread Safety:</b> This operation is atomic, ensuring that in concurrent scenarios, only one client
     * will successfully add the key while others will receive {@code false}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple async add
     * ContinuableFuture<Boolean> addFuture = cache.asyncAdd("user:123", user, 3600000); // dispatched; never null
     * if (addFuture.get()) {                                                    // blocks; yields true only if the key was absent
     *     System.out.println("Added");                                    // printed when the add succeeded
     * } else {
     *     System.out.println("Key already exists"); // printed when add yielded false
     * }
     *
     * // Async add with timeout (requires: import java.util.concurrent.TimeoutException;)
     * ContinuableFuture<Boolean> timedAdd = cache.asyncAdd("session:abc", session, 1800000); // dispatches the add
     * try {
     *     boolean added = timedAdd.get(500, TimeUnit.MILLISECONDS); // waits up to 500ms
     *     System.out.println("Add successful: " + added); // true if the key was newly added
     * } catch (TimeoutException e) {
     *     timedAdd.cancel(true); // abandon the operation if it did not complete in time
     * }
     *
     * cache.asyncAdd((String) null, user, 3600000); // throws IllegalArgumentException (key must not be null)
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated; must not be {@code null}
     * @param value the value to cache; may be {@code null} when the Kryo transcoder is active
     *              (the default with Kryo on the classpath). With the stock spymemcached
     *              {@code SerializingTranscoder} (Kryo absent), a {@code null} value throws
     *              {@code NullPointerException}
     * @param liveTime the time-to-live in milliseconds ({@code 0} or negative means no expiration); a
     *                 positive value is converted to seconds, rounded up if not exact; values over 30
     *                 days are stored as an absolute expiration timestamp. A TTL whose absolute
     *                 expiration would exceed epoch second 2^31-1 (January 2038) is rejected with
     *                 {@code IllegalArgumentException} (memcached expirations are 32-bit).
     * @return a {@link ContinuableFuture} that will yield {@code true} if the add succeeded, or {@code false}
     *         if the key already exists
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the operation fails to initiate
     */
    @Override
    public ContinuableFuture<Boolean> asyncAdd(final String key, final T value, final long liveTime) {
        assertNotShutdown();
        N.checkArgNotNull(key, "key");
        return ContinuableFuture.wrap(mc.add(key, toMemcachedExpiration(liveTime), value));
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
     * @param value the value to cache; may be {@code null} when the Kryo transcoder is active
     *              (the default with Kryo on the classpath). With the stock spymemcached
     *              {@code SerializingTranscoder} (Kryo absent), a {@code null} value throws
     *              {@code NullPointerException}
     * @param liveTime the time-to-live in milliseconds ({@code 0} or negative means no expiration); a
     *                 positive value is converted to seconds, rounded up if not exact; values over 30
     *                 days are stored as an absolute expiration timestamp. A TTL whose absolute
     *                 expiration would exceed epoch second 2^31-1 (January 2038) is rejected with
     *                 {@code IllegalArgumentException} (memcached expirations are 32-bit).
     * @return {@code true} if the object was replaced; {@code false} if the key does not exist
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    public boolean replace(final String key, final T value, final long liveTime) {
        assertNotShutdown();
        N.checkArgNotNull(key, "key");
        return Boolean.TRUE.equals(resultOf(mc.replace(key, toMemcachedExpiration(liveTime), value)));
    }

    /**
     * Asynchronously replaces an object in the cache only if the key already exists.
     * This operation is atomic and thread-safe across all distributed cache clients. The method
     * returns after validation, synchronous value serialization, and enqueueing without waiting for
     * the server response; enqueueing can still block briefly if the operation queue is full. If the
     * key doesn't exist, the Future will contain {@code false}. The liveTime is converted from
     * milliseconds to seconds for Memcached (rounded up if not exact). TTLs longer than 30 days are
     * stored as an absolute Unix expiration timestamp rather than a relative offset.
     *
     * <p><b>Thread Safety:</b> This operation is atomic, ensuring that updates are applied atomically even in
     * concurrent scenarios.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple async replace
     * ContinuableFuture<Boolean> replaceFuture = cache.asyncReplace("user:123", updatedUser, 3600000); // dispatched; never null
     * boolean replaced = replaceFuture.get();                                              // true only if the key already existed
     * if (replaced) {
     *     System.out.println("Replaced successfully"); // printed when the replace succeeded
     * }
     *
     * // Async replace with timeout (requires: import java.util.concurrent.TimeoutException;)
     * ContinuableFuture<Boolean> timedReplace = cache.asyncReplace("config:app", newConfig, 86400000); // dispatches the replace
     * try {
     *     boolean replacedInTime = timedReplace.get(1000, TimeUnit.MILLISECONDS); // waits up to 1s
     *     System.out.println("Config replaced: " + replacedInTime); // true if the key existed
     * } catch (TimeoutException e) {
     *     timedReplace.cancel(true); // abandon the operation if it did not complete in time
     * }
     *
     * cache.asyncReplace((String) null, updatedUser, 3600000); // throws IllegalArgumentException (key must not be null)
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated; must not be {@code null}
     * @param value the value to cache; may be {@code null} when the Kryo transcoder is active
     *              (the default with Kryo on the classpath). With the stock spymemcached
     *              {@code SerializingTranscoder} (Kryo absent), a {@code null} value throws
     *              {@code NullPointerException}
     * @param liveTime the time-to-live in milliseconds ({@code 0} or negative means no expiration); a
     *                 positive value is converted to seconds, rounded up if not exact; values over 30
     *                 days are stored as an absolute expiration timestamp. A TTL whose absolute
     *                 expiration would exceed epoch second 2^31-1 (January 2038) is rejected with
     *                 {@code IllegalArgumentException} (memcached expirations are 32-bit).
     * @return a {@link ContinuableFuture} that will yield {@code true} if the replacement succeeded, or
     *         {@code false} if the key does not exist
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the operation fails to initiate
     */
    @Override
    public ContinuableFuture<Boolean> asyncReplace(final String key, final T value, final long liveTime) {
        assertNotShutdown();
        N.checkArgNotNull(key, "key");
        return ContinuableFuture.wrap(mc.replace(key, toMemcachedExpiration(liveTime), value));
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
     * boolean success = cache.remove("user:123");    // true if the key existed and was removed, false if absent
     * System.out.println("Key removed: " + success); // prints "Key removed: true" or "Key removed: false"
     *
     * // Delete after update
     * User user = cache.get("user:456"); // returns null if not cached
     * if (user != null && user.isInactive()) {
     *     cache.remove("user:456"); // returns true if the key existed
     * }
     *
     * // Delete multiple keys
     * String[] keysToDelete = {"session:1", "session:2", "session:3"};
     * Arrays.stream(keysToDelete).forEach(cache::remove); // deletes each key in turn
     *
     * // Invalidate cache on entity update
     * void updateUser(User user) {
     *     database.save(user); // persist to the source of truth first
     *     cache.remove("user:" + user.getId());   // invalidate cache; returns true if the entry existed
     * }
     *
     * cache.remove((String) null); // throws IllegalArgumentException (key must not be null)
     * }</pre>
     *
     * @param key the cache key whose associated value is to be removed; must not be {@code null}
     * @return {@code true} if the key existed and was removed; {@code false} if the key was not found
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    @Override
    public boolean remove(final String key) {
        assertNotShutdown();
        N.checkArgNotNull(key, "key");
        return Boolean.TRUE.equals(resultOf(mc.delete(key)));
    }

    /**
     * Asynchronously removes an object from the cache.
     * The method returns after validation and enqueueing without waiting for the server response;
     * enqueueing can still block briefly if the operation queue is full. The returned Future yields
     * {@code true} when the key existed and was removed (a {@code DELETED} response), or
     * {@code false} when the key was not found ({@code NOT_FOUND}).
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple async delete
     * ContinuableFuture<Boolean> removeFuture = cache.asyncRemove("user:123"); // dispatched; never null
     * boolean deleted = removeFuture.get();                         // true if the key existed, false if absent
     * if (deleted) {
     *     System.out.println("Delete operation acknowledged"); // printed when the key was removed
     * }
     *
     * // Async delete with timeout (requires: import java.util.concurrent.TimeoutException;)
     * ContinuableFuture<Boolean> timedRemove = cache.asyncRemove("session:abc"); // dispatches the delete
     * try {
     *     boolean deletedInTime = timedRemove.get(500, TimeUnit.MILLISECONDS); // waits up to 500ms
     *     System.out.println("Session deleted: " + deletedInTime); // true if the key existed
     * } catch (TimeoutException e) {
     *     timedRemove.cancel(true); // abandon the operation if it did not complete in time
     * }
     *
     * cache.asyncRemove((String) null); // throws IllegalArgumentException (key must not be null)
     * }</pre>
     *
     * @param key the cache key whose associated value is to be removed; must not be {@code null}
     * @return a {@link ContinuableFuture} that will yield {@code true} if the key existed and was removed, or
     *         {@code false} if the key was not found
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the operation fails to initiate
     */
    public ContinuableFuture<Boolean> asyncRemove(final String key) {
        assertNotShutdown();
        N.checkArgNotNull(key, "key");
        return ContinuableFuture.wrap(mc.delete(key));
    }

    /**
     * Compatibility alias for {@link #asyncRemove(String)}.
     *
     * @param key the cache key to remove; must not be {@code null}
     * @return a future that completes with {@code true} when the key was removed, or {@code false}
     *         when it was absent
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @deprecated use {@link #asyncRemove(String)}; retained with its original {@link Future}
     *             return descriptor for source and binary compatibility
     */
    @Deprecated(since = "2.8.5")
    public Future<Boolean> asyncDelete(final String key) {
        return asyncRemove(key);
    }

    /**
     * Atomically increments a numeric value by 1.
     *
     * <p><b>Memcached-Specific Behavior:</b> If the key doesn't exist, returns -1. Only works with string
     * representations of 64-bit unsigned integers stored in Memcached. The value must be stored as a
     * decimal string representation. Although memcached counters are unsigned 64-bit, this client
     * parses responses as a signed {@code long}: keep counter values below 2<sup>63</sup> — a stored
     * value at or above that is valid to the server but unparseable by the client, and every
     * subsequent access fails and tears down the connection.
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
     *     // Initialize the counter via the auto-seeding overload. Do NOT seed counters with
     *     // put(key, "1", ...): put() serializes through the configured transcoder (Kryo when
     *     // available), producing bytes that memcached's native incr/decr cannot mutate.
     *     pageViews = cache.incr("page:views", 1, 1); // seeds absent key with 1; returns 1
     * }
     *
     * // Rate limiting (auto-seeding overload handles the absent-key case atomically)
     * String key = "rate:limit:" + userId;
     * long attempts = cache.incr(key, 1, 1, 60000); // seeds absent key with 1 (60s TTL); else returns value+1
     * if (attempts > MAX_ATTEMPTS) {
     *     throw new RateLimitException("Too many requests");
     * }
     *
     * cache.incr((String) null); // throws IllegalArgumentException (key must not be null)
     * }</pre>
     *
     * @param key the cache key whose associated value is to be incremented; must not be {@code null}
     * @return the value after the increment, or {@code -1} if the key does not exist
     *         (the client maps memcached's {@code NOT_FOUND} response to {@code -1})
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    @Override
    public long incr(final String key) {
        assertNotShutdown();
        N.checkArgNotNull(key, "key");
        // See get(String): resultOf preserves the interrupt flag, unlike the sync client call.
        // It also surfaces errored/cancelled operations as RuntimeException instead of the
        // ambiguous -1 the sync client returns for ANY failure, so -1 reliably means "absent".
        return resultOf(mc.asyncIncr(key, 1));
    }

    /**
     * Atomically increments a numeric value by a specified amount.
     *
     * <p><b>Memcached-Specific Behavior:</b> If the key doesn't exist, returns -1. Only works with string
     * representations of 64-bit unsigned integers stored in Memcached. The value must be stored as a
     * decimal string representation. Although memcached counters are unsigned 64-bit, this client
     * parses responses as a signed {@code long}: keep counter values below 2<sup>63</sup> — a stored
     * value at or above that is valid to the server but unparseable by the client, and every
     * subsequent access fails and tears down the connection.
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
     *     // Initialize via the auto-seeding overload. Do NOT seed counters with put(): it
     *     // serializes through the configured transcoder (Kryo when available), producing
     *     // bytes that memcached's native incr/decr cannot mutate.
     *     score = cache.incr("player:score", 10, 10); // seeds absent key with 10; returns 10
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
     *         (the client maps memcached's {@code NOT_FOUND} response to {@code -1})
     * @throws IllegalArgumentException if {@code key} is {@code null} or {@code delta} is negative
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    @Override
    public long incr(final String key, final long delta) {
        assertNotShutdown();
        N.checkArgNotNull(key, "key");
        N.checkArgNotNegative(delta, "delta");
        // See incr(String): resultOf preserves the interrupt flag and disambiguates -1.
        return resultOf(mc.asyncIncr(key, delta));
    }

    /**
     * Atomically increments a numeric value, initializing it to {@code defaultValue} when the key
     * doesn't exist. The newly-created entry will not expire (this implementation passes {@code 0}
     * as the expiration to Memcached, which means "no expiration").
     *
     * <p><b>Memcached-Specific Behavior:</b> Unlike {@link #incr(String)} and {@link #incr(String, long)},
     * which return {@code -1} when the key is absent, this overload first-writes the key with
     * {@code defaultValue} when missing. Per this wrapper's ASCII-protocol seeding contract,
     * <b>the increment is NOT applied on the initial insert</b>: when the key is absent the stored
     * value is {@code defaultValue} and that same {@code defaultValue} is returned (not
     * {@code defaultValue + delta}). The {@code delta} only takes effect on subsequent calls when the
     * key already exists. Values are stored as 64-bit unsigned integers in decimal string format;
     * keep seeds and accumulated values below 2<sup>63</sup> (see {@link #incr(String, long)}).
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
     *         delta applied on insert), otherwise the previously stored value plus {@code delta};
     *         {@code -1} if the key could neither be found nor seeded (e.g., deleted concurrently
     *         between the seeding attempt and the retry)
     * @throws IllegalArgumentException if {@code key} is {@code null}, {@code delta} is negative,
     *         or {@code defaultValue} is negative (memcached counters are unsigned 64-bit decimals;
     *         a negative seed would be unmutatable by native incr/decr)
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    public long incr(final String key, final long delta, final long defaultValue) {
        assertNotShutdown();
        N.checkArgNotNull(key, "key");
        N.checkArgNotNegative(delta, "delta");
        // Memcached's "no expiration" sentinel is 0, NOT -1: memcached treats a negative seed
        // expiration as an absolute time in the past, so the counter would be re-seeded on every call.
        return mutateWithAsciiSeed(true, key, delta, defaultValue, 0);
    }

    /**
     * Atomically increments a numeric value, initializing it to {@code defaultValue} with the given
     * expiration when the key doesn't exist. The {@code liveTime} is converted from milliseconds to
     * seconds for Memcached (rounded up if not exact). TTLs longer than 30 days are stored as an
     * absolute Unix expiration timestamp rather than a relative offset.
     *
     * <p><b>Memcached-Specific Behavior:</b> Unlike {@link #incr(String)} and {@link #incr(String, long)},
     * which return {@code -1} when the key is absent, this overload first-writes the key with
     * {@code defaultValue} when missing. Per this wrapper's ASCII-protocol seeding contract,
     * <b>the increment is NOT applied on the initial insert</b>: when the key is absent the stored
     * value is {@code defaultValue} and that same {@code defaultValue} is returned (not
     * {@code defaultValue + delta}). The {@code delta} only takes effect on subsequent calls when the
     * key already exists. Values are stored as 64-bit unsigned integers in decimal string format;
     * keep seeds and accumulated values below 2<sup>63</sup> (see {@link #incr(String, long)}).
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
     *                 values over 30 days are stored as an absolute expiration timestamp. A TTL whose
     *                 absolute expiration would exceed epoch second 2^31-1 (January 2038) is rejected
     *                 with {@code IllegalArgumentException} (memcached expirations are 32-bit).
     * @return the value after the operation: {@code defaultValue} if the key did not exist (no
     *         delta applied on insert), otherwise the previously stored value plus {@code delta};
     *         {@code -1} if the key could neither be found nor seeded (e.g., deleted concurrently
     *         between the seeding attempt and the retry)
     * @throws IllegalArgumentException if {@code key} is {@code null}, {@code delta} is negative,
     *         or {@code defaultValue} is negative (memcached counters are unsigned 64-bit decimals;
     *         a negative seed would be unmutatable by native incr/decr)
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    public long incr(final String key, final long delta, final long defaultValue, final long liveTime) {
        assertNotShutdown();
        N.checkArgNotNull(key, "key");
        N.checkArgNotNegative(delta, "delta");
        return mutateWithAsciiSeed(true, key, delta, defaultValue, toMemcachedExpiration(liveTime));
    }

    /**
     * Atomically decrements a numeric value by 1.
     *
     * <p><b>Memcached-Specific Behavior:</b> If the key doesn't exist, returns -1. Values cannot go below 0
     * (Memcached prevents underflow - attempting to decrement 0 results in 0, not a negative value).
     * Only works with string representations of 64-bit unsigned integers stored in Memcached. The value
     * must be stored as a decimal string representation, and must be below 2<sup>63</sup> — a stored
     * value at or above that is valid to the server but unparseable by this client, and every
     * subsequent access fails and tears down the connection.
     *
     * <p><b>Thread Safety:</b> This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent decrement operations are guaranteed to be serialized correctly,
     * ensuring no decrements are lost.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Token bucket rate limiting (the auto-seeding overload initializes absent keys; never
     * // seed counters with put() - it stores transcoder-encoded bytes that incr/decr cannot mutate)
     * long remainingTokens = cache.decr("api:tokens:" + userId, 1, 100, 60000); // seeds absent key with 100 (60s TTL)
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
        assertNotShutdown();
        N.checkArgNotNull(key, "key");
        // See incr(String): resultOf preserves the interrupt flag and disambiguates -1.
        return resultOf(mc.asyncDecr(key, 1));
    }

    /**
     * Atomically decrements a numeric value by a specified amount.
     *
     * <p><b>Memcached-Specific Behavior:</b> If the key doesn't exist, returns -1. Values cannot go below 0
     * (Memcached prevents underflow - if delta is larger than the current value, the result will be 0, not negative).
     * Only works with string representations of 64-bit unsigned integers stored in Memcached. The value
     * must be stored as a decimal string representation, and must be below 2<sup>63</sup> — a stored
     * value at or above that is valid to the server but unparseable by this client, and every
     * subsequent access fails and tears down the connection.
     *
     * <p><b>Thread Safety:</b> This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent decrement operations are guaranteed to be serialized correctly,
     * ensuring no decrements are lost.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Bulk inventory decrement (the auto-seeding overload initializes absent keys; never seed
     * // counters with put() - it stores transcoder-encoded bytes that incr/decr cannot mutate, and
     * // counters cannot be read back with get())
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
    public long decr(final String key, final long delta) {
        assertNotShutdown();
        N.checkArgNotNull(key, "key");
        N.checkArgNotNegative(delta, "delta");
        // See incr(String): resultOf preserves the interrupt flag and disambiguates -1.
        return resultOf(mc.asyncDecr(key, delta));
    }

    /**
     * Atomically decrements a numeric value, initializing it to {@code defaultValue} when the key
     * doesn't exist. The newly-created entry will not expire (this implementation passes {@code 0}
     * as the expiration to Memcached, which means "no expiration").
     *
     * <p><b>Memcached-Specific Behavior:</b> Unlike {@link #decr(String)} and {@link #decr(String, long)},
     * which return {@code -1} when the key is absent, this overload first-writes the key with
     * {@code defaultValue} when missing. Per this wrapper's ASCII-protocol seeding contract,
     * <b>the decrement is NOT applied on the initial insert</b>: when the key is absent the stored
     * value is {@code defaultValue} and that same {@code defaultValue} is returned (not
     * {@code defaultValue - delta}). The {@code delta} only takes effect on subsequent calls when the
     * key already exists. Values cannot go below {@code 0} (Memcached clamps underflow). Values are
     * stored as 64-bit unsigned integers in decimal string format; keep seeds and accumulated values
     * below 2<sup>63</sup> (see {@link #decr(String, long)}).
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
     *         clamped at {@code 0}; {@code -1} if the key could neither be found nor seeded
     *         (e.g., deleted concurrently between the seeding attempt and the retry)
     * @throws IllegalArgumentException if {@code key} is {@code null}, {@code delta} is negative,
     *         or {@code defaultValue} is negative (memcached counters are unsigned 64-bit decimals;
     *         a negative seed would be unmutatable by native incr/decr)
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    public long decr(final String key, final long delta, final long defaultValue) {
        assertNotShutdown();
        N.checkArgNotNull(key, "key");
        N.checkArgNotNegative(delta, "delta");
        // See incr(String, long, long): Memcached's "no expiration" sentinel is 0, not -1. A -1 seed
        // expiration would store the auto-initialized value already-expired, re-seeding every call.
        return mutateWithAsciiSeed(false, key, delta, defaultValue, 0);
    }

    /**
     * Atomically decrements a numeric value, initializing it to {@code defaultValue} with the given
     * expiration when the key doesn't exist. The {@code liveTime} is converted from milliseconds to
     * seconds for Memcached (rounded up if not exact). TTLs longer than 30 days are stored as an
     * absolute Unix expiration timestamp rather than a relative offset.
     *
     * <p><b>Memcached-Specific Behavior:</b> Unlike {@link #decr(String)} and {@link #decr(String, long)},
     * which return {@code -1} when the key is absent, this overload first-writes the key with
     * {@code defaultValue} when missing. Per this wrapper's ASCII-protocol seeding contract,
     * <b>the decrement is NOT applied on the initial insert</b>: when the key is absent the stored
     * value is {@code defaultValue} and that same {@code defaultValue} is returned (not
     * {@code defaultValue - delta}). The {@code delta} only takes effect on subsequent calls when the
     * key already exists. Values cannot go below {@code 0} (Memcached clamps underflow). Values are
     * stored as 64-bit unsigned integers in decimal string format; keep seeds and accumulated values
     * below 2<sup>63</sup> (see {@link #decr(String, long)}).
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
     *                 values over 30 days are stored as an absolute expiration timestamp. A TTL whose
     *                 absolute expiration would exceed epoch second 2^31-1 (January 2038) is rejected
     *                 with {@code IllegalArgumentException} (memcached expirations are 32-bit).
     * @return the value after the operation: {@code defaultValue} if the key did not exist (no
     *         delta applied on insert), otherwise the previously stored value minus {@code delta},
     *         clamped at {@code 0}; {@code -1} if the key could neither be found nor seeded
     *         (e.g., deleted concurrently between the seeding attempt and the retry)
     * @throws IllegalArgumentException if {@code key} is {@code null}, {@code delta} is negative,
     *         or {@code defaultValue} is negative (memcached counters are unsigned 64-bit decimals;
     *         a negative seed would be unmutatable by native incr/decr)
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    public long decr(final String key, final long delta, final long defaultValue, final long liveTime) {
        assertNotShutdown();
        N.checkArgNotNull(key, "key");
        N.checkArgNotNegative(delta, "delta");
        return mutateWithAsciiSeed(false, key, delta, defaultValue, toMemcachedExpiration(liveTime));
    }

    /**
     * Implements incr/decr-with-default on the ASCII protocol without going through spymemcached's
     * {@code mutateWithDefault}, which seeds an absent key via the client's <em>default</em>
     * transcoder. With the {@link KryoTranscoder} installed (the default when Kryo is on the
     * classpath), that seed is stored as a Kryo-encoded blob that memcached's native incr/decr
     * cannot mutate — every subsequent call then fails with {@code CLIENT_ERROR cannot increment
     * or decrement non-numeric value} and tears down the connection. Instead, the seed is written
     * here with an atomic {@code add} using {@link #ASCII_COUNTER_TRANSCODER} (raw ASCII decimal),
     * the only representation native incr/decr can operate on.
     *
     * <p>Semantics mirror memcached's incr-with-default: when the key is absent, the stored value
     * is {@code defaultValue} and that same value is returned — the delta is NOT applied on the
     * initial insert.
     *
     * <p>Bound: memcached counters are unsigned 64-bit, but spymemcached parses every mutate
     * response with {@code Long.parseLong}; a counter at or above 2<sup>63</sup> throws a
     * {@code NumberFormatException} inside the client's IO thread, which tears down (and
     * reconnects) the whole connection on every access to that key. Validation here cannot prevent
     * accumulated increments from crossing that bound, so the public counter docs instruct callers
     * to keep seeds and values below {@code Long.MAX_VALUE}.
     *
     * @param isIncrement {@code true} for incr, {@code false} for decr
     * @param key the counter key
     * @param delta the mutation amount applied when the key already exists
     * @param defaultValue the seed stored when the key is absent
     * @param expiration the memcached expiration for the seed (already converted via
     *                   {@link #toMemcachedExpiration(long)}; {@code 0} = no expiration)
     * @return the post-operation value, or {@code -1} if the key could neither be found nor seeded
     *         (e.g., deleted concurrently between the seeding attempt and the retry)
     */
    private long mutateWithAsciiSeed(final boolean isIncrement, final String key, final long delta, final long defaultValue, final int expiration) {
        // Memcached counters are unsigned 64-bit decimals; a negative seed would be stored as
        // e.g. "-5", which native incr/decr cannot mutate - the same counter-poisoning (plus
        // connection-teardown-per-call) failure mode this ASCII seeding exists to prevent.
        N.checkArgNotNegative(defaultValue, "defaultValue");

        // The async variants + resultOf preserve the interrupt flag and surface errored or
        // cancelled operations as RuntimeException, so -1 here reliably means "key absent".
        long result = resultOf(isIncrement ? mc.asyncIncr(key, delta) : mc.asyncDecr(key, delta));

        if (result == -1) {
            // Key absent: seed it atomically (add succeeds only if still absent).
            if (Boolean.TRUE.equals(resultOf(mc.add(key, expiration, String.valueOf(defaultValue), ASCII_COUNTER_TRANSCODER)))) {
                return defaultValue;
            }

            // Another thread seeded the key concurrently; apply the mutation to the existing value.
            result = resultOf(isIncrement ? mc.asyncIncr(key, delta) : mc.asyncDecr(key, delta));
        }

        return result;
    }

    /**
     * Requests an immediate flush from every connected Memcached server.
     * Each server that accepts the operation removes all of its keys. The method blocks until the
     * flush completes or times out. Use with extreme caution in production environments
     * as this is a destructive operation that cannot be undone.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe. A server that accepts the command makes
     * the flush visible to all of its clients immediately; the invalidated data cannot be recovered.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // WARNING: This can remove ALL data from every configured cache server!
     * cache.flushAll(); // blocks for spymemcached's aggregate result; see multi-server caveat below
     * System.out.println("Flush command completed"); // verify servers individually if full-cluster proof is required
     *
     * // Safe usage in testing
     * @AfterEach
     * public void cleanupCache() {
     *     if (isTestEnvironment()) {
     *         cache.flushAll(); // requests a flush from every configured server
     *     }
     * }
     *
     * // Production usage with confirmation
     * public void clearCache(String confirmationToken) {
     *     if (!"CONFIRM_FLUSH_ALL".equals(confirmationToken)) {
     *         throw new IllegalArgumentException("Invalid confirmation");
     *     }
     *     logger.warn("Flushing all cache data"); // audit the impending destructive flush
     *     cache.flushAll(); // requests a flush from every configured server
     *     auditLog.record("CACHE_FLUSH_ALL", user); // record the destructive action
     * }
     * }</pre>
     *
     * @throws IllegalStateException if this client has been disconnected/is being disconnected, or
     *         if the flush is not reported successful. Note that with multiple
     *         servers, spymemcached aggregates the per-server outcomes into a single last-writer-wins
     *         flag, so one server's failure can be masked by a later server's success; the exception is
     *         therefore guaranteed only for single-server configurations
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    @Override
    public void flushAll() {
        assertNotShutdown();
        if (!Boolean.TRUE.equals(resultOf(mc.flush()))) {
            throw new IllegalStateException("The Memcached flush was not reported successful");
        }
    }

    /**
     * Asynchronously requests a flush from every connected Memcached server. The method returns
     * after enqueueing without waiting for server responses; enqueueing can still block briefly if
     * the operation queue is full. The returned Future can be used to check when the operation
     * completes. Each server that accepts the destructive command invalidates all of its keys, and
     * the operation cannot be undone. Use with extreme caution in production.
     *
     * <p><b>Multi-server result limitation:</b> spymemcached stores each server callback into one
     * shared result flag, so the last callback wins. In a multi-server configuration, {@code true}
     * means the final response was successful; it does not prove that every server accepted the
     * flush. A {@code false} result likewise reflects the final response rather than a full tally.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe but its effects are visible immediately to all clients
     * once the flush completes.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple async flush
     * ContinuableFuture<Boolean> flushFuture = cache.asyncFlushAll(); // dispatched; never null
     * boolean flushed = flushFuture.get();                   // see the multi-server result limitation above
     * if (flushed) {
     *     System.out.println("Final server callback reported success");
     * }
     *
     * // Async flush with timeout (requires: import java.util.concurrent.TimeoutException;)
     * ContinuableFuture<Boolean> timedFlush = cache.asyncFlushAll(); // dispatches the flush
     * try {
     *     boolean flushedInTime = timedFlush.get(2000, TimeUnit.MILLISECONDS); // waits up to 2s
     *     System.out.println("Flush completed: " + flushedInTime); // final callback's result
     * } catch (TimeoutException e) {
     *     timedFlush.cancel(true); // abandon the wait if confirmation did not arrive in time
     *     logger.warn("Flush operation timed out"); // emitted if the result was not ready within 2s
     * }
     * }</pre>
     *
     * @return a {@link ContinuableFuture} that yields the final server callback's result; for one
     *         server, {@code true} means the flush was accepted and {@code false} means it was not
     * @throws RuntimeException if the operation fails to initiate (e.g., the operation queue is
     *         full or the client is shutting down)
     */
    @Override
    public ContinuableFuture<Boolean> asyncFlushAll() {
        assertNotShutdown();
        return ContinuableFuture.wrap(mc.flush());
    }

    /**
     * Requests a delayed flush from every connected Memcached server.
     * The method blocks until the scheduling completes or times out. The flush will occur on each
     * server that accepts the command after the delay expires. This destructive operation removes
     * all keys from those servers and cannot be undone. The delay is converted from milliseconds
     * to seconds for Memcached (rounded up if not exact).
     * In a multi-server configuration, the boolean is spymemcached's last-callback-wins aggregate
     * and does not prove that every server accepted the request (see {@link #asyncFlushAll()}).
     *
     * <p><b>Thread Safety:</b> This method is thread-safe but its effects are visible immediately to all clients
     * after the delay period expires.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Schedule a flush to happen in 5 seconds
     * boolean scheduled = cache.flushAll(5000); // 5000ms -> relative delay of 5s; single-server result
     * if (scheduled) {
     *     System.out.println("Flush scheduled for 5 seconds from now"); // printed when scheduling succeeded
     * }
     *
     * // Like set/add/replace expirations, delays over 30 days are sent as an absolute
     * // now+delay Unix timestamp (memcached's protocol rule for all expiration values).
     * cache.flushAll(3_456_000_000L); // 40 days -> absolute timestamp 40 days from now
     *
     * // Delayed flush for maintenance window
     * long delayUntilMaintenance = calculateDelayToMaintenance();
     * boolean maintenanceScheduled = cache.flushAll(delayUntilMaintenance); // converts ms to seconds (rounded up)
     * if (maintenanceScheduled) {
     *     logger.info("Cache flush scheduled for maintenance window"); // emitted once scheduling succeeded
     * }
     * }</pre>
     *
     * @param delay the delay in milliseconds before the flush operation is executed; a positive value
     *              is converted to seconds, rounded up if not exact. A value of {@code 0} or negative
     *              flushes immediately.
     * @return the final server callback's result; in a single-server configuration, {@code true}
     *         means the flush was scheduled successfully and {@code false} means it was not
     * @throws IllegalArgumentException if {@code delay} is large enough that its absolute expiration
     *              timestamp would exceed memcached's 32-bit expiration limit (roughly beyond the year 2038)
     * @throws RuntimeException if the operation times out or encounters a network error
     */
    public boolean flushAll(final long delay) {
        assertNotShutdown();
        // The memcached `flush_all <delay>` argument goes through the server's realtime()
        // conversion just like storage expirations: values above 30 days are interpreted as
        // ABSOLUTE Unix timestamps, not relative seconds. Sending a raw 40-day delay (3456000s)
        // would be read as an epoch time in Feb 1970 - i.e., flush (nearly) immediately instead
        // of in 40 days. toMemcachedExpiration() converts >30-day delays to now+delay timestamps.
        return Boolean.TRUE.equals(resultOf(mc.flush(toMemcachedExpiration(delay))));
    }

    /**
     * Asynchronously schedules a flush after a delay. The method returns after enqueueing without
     * waiting for server responses; enqueueing can still block briefly if the operation queue is
     * full. The flush will occur on each server that accepted it after the delay expires. This is a
     * destructive operation that removes all keys from those servers and cannot be undone. The
     * delay is converted from milliseconds to seconds for Memcached (rounded up if not exact).
     * The returned boolean has the same last-callback-wins limitation as {@link #asyncFlushAll()}.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe but its effects are visible immediately to all clients
     * after the delay period expires.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Schedule a flush to happen in 10 seconds
     * ContinuableFuture<Boolean> scheduleFuture = cache.asyncFlushAll(10000); // 10000ms -> relative delay of 10s
     * boolean scheduled = scheduleFuture.get();                       // final callback's result
     *
     * // Like set/add/replace expirations, delays over 30 days are sent as an absolute
     * // now+delay Unix timestamp (memcached's protocol rule for all expiration values).
     * cache.asyncFlushAll(3_456_000_000L); // 40 days -> absolute timestamp 40 days from now
     *
     * // Async delayed flush with timeout (requires: import java.util.concurrent.TimeoutException;)
     * ContinuableFuture<Boolean> timedSchedule = cache.asyncFlushAll(30000); // 30000ms -> relative delay of 30s
     * try {
     *     boolean scheduledInTime = timedSchedule.get(1000, TimeUnit.MILLISECONDS); // waits up to 1s
     *     System.out.println("Flush scheduled: " + scheduledInTime);   // final callback's result
     * } catch (TimeoutException e) {
     *     timedSchedule.cancel(true); // abandon the wait if confirmation did not arrive in time
     *     logger.warn("Failed to schedule flush"); // emitted if confirmation was not received within 1s
     * }
     * }</pre>
     *
     * @param delay the delay in milliseconds before the flush operation is executed; a positive value
     *              is converted to seconds, rounded up if not exact. A value of {@code 0} or negative
     *              flushes immediately.
     * @return a {@link ContinuableFuture} that yields the final server callback's result; for one
     *         server, {@code true} means the flush was scheduled and {@code false} means it was not
     * @throws IllegalArgumentException if {@code delay} is large enough that its absolute expiration
     *              timestamp would exceed memcached's 32-bit expiration limit (roughly beyond the year 2038)
     * @throws RuntimeException if the operation fails to initiate (e.g., the operation queue is
     *         full or the client is shutting down)
     */
    @Override
    public ContinuableFuture<Boolean> asyncFlushAll(final long delay) {
        assertNotShutdown();
        // See flushAll(long): flush_all's delay is subject to the server's 30-day absolute-vs-
        // relative rule, so it must be converted exactly like storage expirations.
        return ContinuableFuture.wrap(mc.flush(toMemcachedExpiration(delay)));
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
     * one disconnect occurs. It publishes the terminal state before delegate shutdown begins, so
     * new cache operations fail with {@link IllegalStateException} while teardown is in progress.
     * Once called, no other operations should be attempted on this client instance.
     *
     * <p>This optional terminal operation is intended for the lifecycle shutdown of the application
     * or component that owns this shared client. It releases network connections, thread pools, and
     * other client resources; it should not be invoked after an individual cache operation or request.
     * It is safe to call multiple times; subsequent calls have no effect.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Optional Spring Bean destruction for the shared application client
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
            // Publish the terminal state before entering a potentially slow or failing shutdown.
            // Operations beginning after this write fail locally instead of serializing values or
            // allocating delegate futures against a client whose IO thread is being torn down.
            isShutdown = true;
            mc.shutdown();
        }
    }

    /**
     * Disconnects from all Memcached servers, waiting up to the given timeout for pending
     * operations to complete.
     * After the timeout elapses, any remaining operations are abandoned and connections are
     * closed. After this method returns, the cache client must not be used again; any subsequent
     * operations will fail. For a non-negative timeout this method is idempotent: calling it
     * multiple times has no additional effect.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and uses synchronization to ensure only
     * one disconnect occurs. It publishes the terminal state before the graceful queue wait begins,
     * so new cache operations fail with {@link IllegalStateException} during teardown. Once called,
     * no other operations should be attempted on this client instance.
     *
     * <p>This optional terminal operation is intended for lifecycle shutdown of the application or
     * component that owns this shared client, not per-operation cleanup. It releases connections,
     * thread pools, and other resources while allowing pending operations to complete within the
     * supplied bound. Repeated calls with a valid timeout are no-ops after the first shutdown; the
     * timeout argument is still validated on every call, so a negative value always throws.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Optional application shutdown with graceful timeout
     * public void shutdown() {
     *     logger.info("Shutting down cache client"); // emitted before the graceful shutdown
     *     cache.disconnect(10000);   // waits up to 10000ms for pending ops to finish
     *     logger.info("Cache client shutdown complete"); // emitted after disconnect returns
     * }
     *
     * cache.disconnect(-1); // throws IllegalArgumentException (timeout must not be negative)
     * }</pre>
     *
     * @param timeout the maximum time, in milliseconds, to wait for shutdown; must not be negative.
     *                A value of {@code 0} returns immediately without waiting for pending operations
     *                (unlike the constructor's operation {@code timeout}, which must be strictly positive).
     * @throws IllegalArgumentException if {@code timeout} is negative
     * @throws RuntimeException if the graceful wait is interrupted or shutdown otherwise fails. If
     *         the underlying cause is {@link InterruptedException}, this method restores the calling
     *         thread's interrupt status before propagating the exception.
     */
    public synchronized void disconnect(final long timeout) {
        N.checkArgNotNegative(timeout, "timeout");

        if (!isShutdown) {
            // See disconnect(): publish first so new operations cannot enter during the graceful
            // queue wait. The delegate's shutdown sequence is terminal even if it later throws.
            isShutdown = true;

            try {
                mc.shutdown(timeout, TimeUnit.MILLISECONDS);
            } catch (final RuntimeException e) {
                // spymemcached wraps InterruptedException from its graceful queue wait without
                // restoring the flag. Preserve cooperative cancellation just as resultOf does.
                if (e.getCause() instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }

                throw e;
            }
        }
    }

    /**
     * Rejects cache operations once either disconnect overload has begun. The explicit wrapper
     * check is intentionally performed before key/TTL validation and value serialization, giving
     * every operation the same deterministic lifecycle failure instead of relying on where the
     * underlying client happens to check its connection state.
     *
     * @throws IllegalStateException if this client has been disconnected or is being disconnected
     */
    private void assertNotShutdown() {
        if (isShutdown) {
            throw new IllegalStateException("This Memcached client has been disconnected");
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
     * Blocks for at most the configured (clamped) operation timeout and converts
     * {@link InterruptedException}, {@link TimeoutException}, and {@link ExecutionException} into
     * runtime exceptions. On both interrupt and timeout the Future is cancelled; additionally,
     * when an {@link InterruptedException} occurs the thread's interrupted status is restored
     * before the runtime exception is thrown.
     *
     * <p>This is a utility method used internally to convert asynchronous operations to
     * synchronous ones by blocking on the Future's result.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Internal usage pattern
     * Future<Boolean> future = mc.set("key", 3600, "value");
     * Boolean result = resultOf(future);
     * }</pre>
     *
     * @param <R> the type of result returned by the {@link Future}
     * @param future the {@link Future} whose result is to be retrieved; must not be {@code null}
     * @return the result value produced by the {@link Future}
     * @throws IllegalArgumentException if {@code future} is {@code null}
     * @throws RuntimeException if the {@link Future} execution fails, the calling thread is
     *         interrupted, or the configured operation timeout elapses
     */
    protected <R> R resultOf(final Future<R> future) {
        N.checkArgNotNull(future, "future");

        try {
            // spymemcached enforces its internal per-operation timeout only for operations still
            // queued for write; once a request has been written to a connected-but-unresponsive
            // server, this wait is the only bound. Waiting exactly the configured timeout matches
            // the delegate's own synchronous methods and the documented contract.
            return future.get(operationTimeoutMillis, TimeUnit.MILLISECONDS);
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
     * @param serverUrl one or more {@code host:port} addresses separated by commas, whitespace,
     *                  or both
     * @param connFactory the connection factory configured with timeout and transcoder settings
     * @return a configured {@link MemcachedClient} instance
     * @throws UncheckedIOException if local client/socket setup fails. Connections are established
     *         asynchronously by the SpyMemcached IO thread, so an unreachable or down server does
     *         not cause this method to fail
     */
    protected static MemcachedClient createSpyMemcachedClient(final String serverUrl, final ConnectionFactory connFactory) throws UncheckedIOException {
        return createSpyMemcachedClient(serverUrl, AddrUtil.getAddressList(serverUrl), connFactory);
    }

    private static MemcachedClient createSpyMemcachedClient(final String serverUrl, final List<InetSocketAddress> serverAddresses,
            final ConnectionFactory connFactory) throws UncheckedIOException {
        try {
            return new MemcachedClient(connFactory, serverAddresses);
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to create Memcached client for server(s): " + serverUrl, e);
        }
    }
}
