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

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.util.Charsets;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Strings;

/**
 * A wrapper cache implementation that provides a standardized Cache interface for distributed cache clients.
 * This class adds key prefixing, error handling with circuit breaker pattern, and adapts distributed cache
 * client operations to the standard Cache interface. It's designed to work with any
 * DistributedCacheClient implementation like Memcached or Redis.
 *
 * <p><b>Key Features:</b>
 * <ul>
 * <li>Automatic key prefixing for namespace isolation</li>
 * <li>Base64 encoding of keys for compatibility with cache systems that restrict key characters</li>
 * <li>Circuit breaker pattern on read operations to protect against cascading failures</li>
 * <li>Transparent error recovery via a configurable fail-fast window after repeated failures</li>
 * <li>Adapts the TTL+idle Cache interface to the TTL-only model used by most distributed caches ({@code maxIdleTime} is ignored)</li>
 * </ul>
 *
 * <p><b>Thread Safety:</b>
 * This class is thread-safe. The circuit breaker state (failure counter and last failure time)
 * is managed using atomic variables, and the close operation is synchronized. Multiple threads
 * can safely perform concurrent cache operations.
 *
 * <p><b>Construction:</b> Constructors are {@code protected}; obtain instances through
 * {@link CacheFactory#createDistributedCache(DistributedCacheClient, String, int, long)} and its overloads.
 *
 * <p><b>Usage Examples:</b>
 * <pre>{@code
 * // Create cache with full configuration via the factory
 * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
 * DistributedCache<String, User> cache = CacheFactory.createDistributedCache(
 *     client,
 *     "myapp:",           // key prefix for namespace isolation
 *     100,                // max consecutive failures before circuit opens
 *     1000                // retry delay in ms after circuit opens
 * );
 *
 * // Basic operations
 * User user = new User("John");
 * cache.put("user:123", user, 3600000, 0);   // 1 hour TTL, maxIdleTime ignored
 * User cached = cache.getOrNull("user:123");
 *
 * // Always close to release resources
 * cache.close();
 * }</pre>
 *
 * @param <K> the type of keys used to identify cache entries
 * @param <V> the type of values stored in the cache
 * @see AbstractCache
 * @see DistributedCacheClient
 * @see CacheFactory
 */
public class DistributedCache<K, V> extends AbstractCache<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(DistributedCache.class);

    /**
     * Default maximum number of consecutive failures before opening the circuit breaker.
     * When failures exceed this threshold, the circuit opens and subsequent operations
     * return immediately without attempting cache access until the retry delay expires.
     */
    protected static final int DEFAULT_MAX_FAILED_NUMBER = 100;

    /**
     * Default delay in milliseconds before attempting to retry after the circuit breaker opens.
     * When the circuit is open, operations fail fast until this delay has elapsed since the last failure.
     */
    protected static final long DEFAULT_RETRY_DELAY = 1000;

    // ...
    private final DistributedCacheClient<V> dcc;

    private final String keyPrefix;

    // Precomputed once: keyPrefix is normalized to Strings.EMPTY in the constructor,
    // so the empty-vs-prefixed branch in generateKey() is fixed for the instance's
    // lifetime and need not be re-evaluated on every cache operation.
    private final boolean hasKeyPrefix;

    private final int maxFailedNumForRetry;

    private final long retryDelay;

    // Sentinel for "no failure recorded" in lastFailedTime. System.nanoTime() values are
    // arbitrary (and may be negative), so a real timestamp can't double as the sentinel.
    private static final long NEVER_FAILED = Long.MIN_VALUE;

    // Precomputed from retryDelay; TimeUnit.toNanos saturates at Long.MAX_VALUE for huge delays.
    private final long retryDelayNanos;

    // ...
    private final AtomicInteger failedCounter = new AtomicInteger();

    // Monotonic (System.nanoTime()) timestamp of the most recent failure, or NEVER_FAILED when
    // none is recorded. A monotonic clock is used instead of the wall clock so a backward
    // NTP/manual clock adjustment cannot extend the fail-fast window: with the wall clock, a
    // jump of -10 minutes kept the open circuit returning null for those extra 10 minutes.
    private final AtomicLong lastFailedTime = new AtomicLong(NEVER_FAILED);

    private volatile boolean isClosed = false;

    /**
     * Creates a DistributedCache with default retry configuration.
     * Uses an empty key prefix and default circuit breaker parameters
     * ({@link #DEFAULT_MAX_FAILED_NUMBER} consecutive failures, {@link #DEFAULT_RETRY_DELAY} ms retry delay).
     *
     * <p>This constructor is {@code protected}; external callers should obtain instances
     * via {@link CacheFactory#createDistributedCache(DistributedCacheClient)}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = CacheFactory.createDistributedCache(client);
     *
     * // Cache operations will use circuit breaker protection
     * cache.put("key", new User("John"), 3600000, 0);
     * User user = cache.getOrNull("key");
     * }</pre>
     *
     * @param dcc the distributed cache client to wrap (must not be {@code null})
     * @throws IllegalArgumentException if {@code dcc} is {@code null}
     */
    protected DistributedCache(final DistributedCacheClient<V> dcc) {
        this(dcc, Strings.EMPTY, DEFAULT_MAX_FAILED_NUMBER, DEFAULT_RETRY_DELAY);
    }

    /**
     * Creates a DistributedCache with a key prefix and default circuit breaker configuration.
     * All keys will be prefixed for namespace isolation. Uses default circuit breaker parameters
     * ({@link #DEFAULT_MAX_FAILED_NUMBER} consecutive failures, {@link #DEFAULT_RETRY_DELAY} ms retry delay).
     *
     * <p>This constructor is {@code protected}; external callers should obtain instances
     * via {@link CacheFactory#createDistributedCache(DistributedCacheClient, String)}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = CacheFactory.createDistributedCache(client, "myapp:");
     *
     * // All keys will be prefixed with "myapp:" and Base64 encoded
     * cache.put("user:123", new User("John"), 3600000, 0);
     * // Actual cache key: "myapp:" + Base64("user:123")
     * }</pre>
     *
     * @param dcc the distributed cache client to wrap (must not be {@code null})
     * @param keyPrefix the key prefix to prepend to all keys (may be empty string or {@code null} for no prefix)
     * @throws IllegalArgumentException if {@code dcc} is {@code null}
     */
    protected DistributedCache(final DistributedCacheClient<V> dcc, final String keyPrefix) {
        this(dcc, keyPrefix, DEFAULT_MAX_FAILED_NUMBER, DEFAULT_RETRY_DELAY);
    }

    /**
     * Creates a DistributedCache with full configuration including circuit breaker parameters.
     * This is the most flexible constructor allowing full control over key prefixing and
     * circuit breaker behavior. Use this constructor when you need fine-grained control over
     * error handling and retry logic.
     *
     * <p><b>Circuit Breaker Parameters:</b>
     * <ul>
     * <li>{@code maxFailedNumForRetry}: Number of consecutive failures before circuit opens</li>
     * <li>{@code retryDelay}: Milliseconds to wait before attempting retry after circuit opens</li>
     * </ul>
     *
     * <p>This constructor is {@code protected}; external callers should obtain instances
     * via {@link CacheFactory#createDistributedCache(DistributedCacheClient, String, int, long)}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     *
     * // More aggressive circuit breaker (opens faster, retries sooner)
     * DistributedCache<String, User> aggressiveCache = CacheFactory.createDistributedCache(
     *     client,
     *     "myapp:",  // key prefix for namespace isolation
     *     10,        // open circuit after just 10 consecutive failures
     *     500        // retry after 500ms
     * );
     *
     * // More tolerant circuit breaker (slower to open, longer retry delay)
     * DistributedCache<String, User> tolerantCache = CacheFactory.createDistributedCache(
     *     client,
     *     "myapp:",
     *     200,       // allow up to 200 consecutive failures
     *     5000       // wait 5 seconds before retry
     * );
     * }</pre>
     *
     * @param dcc the distributed cache client to wrap (must not be {@code null})
     * @param keyPrefix the key prefix to prepend to all keys (may be empty string or {@code null} for no prefix).
     *        The prefix is prepended verbatim (only the key part is Base64-encoded), so it must consist of
     *        printable ASCII characters without spaces or control characters. Including at least one character
     *        outside the Base64 alphabet (such as {@code ':'}) is recommended: it guarantees prefixed and
     *        unprefixed namespaces can never collide
     * @param maxFailedNumForRetry maximum consecutive failures before opening circuit breaker (must be non-negative).
     *        A value of {@code 0} is a degenerate configuration in which the circuit opens on every failure for
     *        {@code retryDelay} ms; in that mode the failure counter stays at 0 and the closed-to-open WARN
     *        transition log is not emitted
     * @param retryDelay delay in milliseconds before attempting retry after circuit opens (must be non-negative)
     * @throws IllegalArgumentException if {@code dcc} is {@code null}, {@code maxFailedNumForRetry} is negative,
     *         {@code retryDelay} is negative, or {@code keyPrefix} contains a non-printable-ASCII character,
     *         a space, or a control character
     */
    protected DistributedCache(final DistributedCacheClient<V> dcc, final String keyPrefix, final int maxFailedNumForRetry, final long retryDelay) {
        N.checkArgNotNull(dcc, "dcc");
        N.checkArgNotNegative(maxFailedNumForRetry, "maxFailedNumForRetry");
        N.checkArgNotNegative(retryDelay, "retryDelay");

        this.keyPrefix = Strings.isEmpty(keyPrefix) ? Strings.EMPTY : keyPrefix;

        // The prefix is prepended verbatim to generated keys (only the key part is Base64-encoded),
        // so it must itself be key-safe for the backing store: printable ASCII without spaces or
        // control characters. Anything else would make every generated key invalid at the server -
        // every write would throw and every read would silently feed the circuit breaker.
        for (int i = 0, len = this.keyPrefix.length(); i < len; i++) {
            final char ch = this.keyPrefix.charAt(i);

            if (ch <= ' ' || ch >= 127) {
                throw new IllegalArgumentException("keyPrefix must contain only printable ASCII characters without spaces or control characters; found char "
                        + (int) ch + " at index " + i);
            }
        }
        this.hasKeyPrefix = Strings.isNotEmpty(this.keyPrefix);
        this.dcc = dcc;
        this.maxFailedNumForRetry = maxFailedNumForRetry;
        this.retryDelay = retryDelay;
        this.retryDelayNanos = TimeUnit.MILLISECONDS.toNanos(retryDelay);
    }

    /**
     * Retrieves a value from the distributed cache by its key.
     * This method implements circuit breaker pattern to protect against cascading failures
     * when the distributed cache becomes unavailable. Keys are automatically prefixed and
     * Base64 encoded for compatibility with distributed cache systems that have key format restrictions.
     *
     * <p><b>Circuit Breaker Behavior:</b>
     * The circuit breaker has two effective states (there is no canonical "half-open" single-probe gate):
     * <ul>
     * <li><b>Closed (Normal):</b> Operations proceed normally. Each failure increments the counter
     *     and updates the last-failure timestamp; each success resets both.</li>
     * <li><b>Open (Failing Fast):</b> When {@code failedCounter >= maxFailedNumForRetry} AND the time
     *     since the last failure is less than {@code retryDelay} milliseconds, this method returns
     *     {@code null} immediately without attempting cache access. Once the retry window elapses, ALL
     *     subsequent reads attempt the cache again — there is no single-probe restriction, so a still-
     *     unavailable cache may briefly cause a burst of failures before re-opening the circuit.</li>
     * </ul>
     *
     * <p><b>State Transitions:</b>
     * <ul>
     * <li>Successful operation: Resets {@code failedCounter} to 0 and clears the last-failure timestamp (closes circuit)</li>
     * <li>Failed operation: Increments {@code failedCounter} (capped at the threshold) and records the failure
     *     time using a monotonic clock ({@code System.nanoTime()}), so wall-clock adjustments cannot affect the window</li>
     * <li>All exceptions from the underlying cache client are caught and treated as failures, except
     *     {@link IllegalArgumentException} (a deterministic client-side validation error, e.g. an over-long
     *     generated key), which is rethrown without touching the breaker state; the closed-to-open
     *     transition is logged once at WARN level</li>
     * </ul>
     *
     * <p><b>Thread Safety:</b>
     * This method is thread-safe. The failure counter and last failure time are managed using
     * {@link AtomicInteger} and {@link AtomicLong} respectively, allowing safe concurrent access.
     *
     * <p><b>Key Processing:</b>
     * The key is transformed as follows: {@code key -> keyPrefix + Base64(UTF8(toString(key)))}.
     * Base64 encoding ensures compatibility with cache systems that restrict key characters
     * (e.g., spaces, special characters, Unicode).
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = CacheFactory.createDistributedCache(client, "myapp:", 100, 1000);
     *
     * // Basic retrieval with null check
     * User user = cache.getOrNull("user:123");          // returns the cached User, or null on miss
     * if (user != null) {
     *     System.out.println("Found: " + user.getName());   // prints when present
     * } else {
     *     // Could be: not found, expired, evicted, circuit open, or network error
     *     System.out.println("Cache miss - loading from database");   // prints on null result
     *     user = loadFromDatabase("user:123");
     *     cache.put("user:123", user, 3600000, 0);      // returns true on successful store
     * }
     *
     * // Cache-aside pattern with circuit breaker protection
     * User getUser(String userId) {
     *     User user = cache.getOrNull("user:" + userId);   // returns cached value or null
     *     if (user == null) {
     *         // Fallback to database (could be cache miss or circuit breaker open)
     *         user = database.findUser(userId);
     *         if (user != null) {
     *             cache.put("user:" + userId, user, 3600000, 0);   // returns true; re-populates cache
     *         }
     *     }
     *     return user;
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @return the cached value, or {@code null} if not found, expired, evicted, circuit breaker is open, or on error
     * @throws IllegalStateException if the cache has been closed
     * @throws IllegalArgumentException if the key is null (validated up-front, before the circuit breaker
     *         check and before {@link #generateKey(Object)}), or if the underlying client rejects the
     *         generated cache key (e.g. it exceeds memcached's 250-character key limit after prefixing
     *         and Base64 expansion); such validation errors are rethrown and do not affect the circuit
     *         breaker state
     * @see #generateKey(Object)
     * @see DistributedCacheClient#get(String)
     */
    @Override
    public V getOrNull(final K key) {
        assertNotClosed();

        // Validate the key up-front (cheap null check) so the documented IllegalArgumentException
        // for a null key is thrown consistently, even when the circuit breaker would otherwise
        // short-circuit to null before generateKey(key) is reached.
        N.checkArgNotNull(key, "key");

        // Read the breaker state: two independent volatile reads, not an atomic pair snapshot.
        // Every interleaving with a concurrent reset/failure fails safe (worst case one extra
        // cache attempt or one extra fast-fail), so no pairing synchronization is needed.
        // Elapsed time is measured with System.nanoTime(): the wall clock previously used here
        // let a backward NTP/manual clock jump extend the fail-fast window by the jump size.
        final int currentFailedCount = failedCounter.get();
        final long currentLastFailedTime = lastFailedTime.get();

        if ((currentFailedCount >= maxFailedNumForRetry) && (currentLastFailedTime != NEVER_FAILED)
                && ((System.nanoTime() - currentLastFailedTime) < retryDelayNanos)) {
            return null;
        }

        final String cacheKey = generateKey(key);

        V result = null;
        // TRUE = success, FALSE = availability failure, null = client-side validation error
        // (rethrown; must neither reset nor advance the breaker).
        Boolean isOK = null;

        try {
            result = dcc.get(cacheKey);
            isOK = Boolean.TRUE;
        } catch (final IllegalArgumentException e) {
            // A deterministic client-side validation error (e.g. the generated key exceeding
            // memcached's 250-character limit after prefixing and Base64 expansion) indicates a
            // bad key, not an unavailable cache. Rethrow instead of counting it toward the
            // availability breaker: a hot invalid key would otherwise open the circuit and blank
            // reads for ALL keys, with only debug-level evidence.
            throw e;
        } catch (final Exception e) {
            isOK = Boolean.FALSE;

            // Swallowed by design: the circuit breaker (updated in the finally block) handles
            // recovery, so callers see a cache miss rather than an exception. Logged at debug
            // to aid diagnosis without flooding logs while the circuit is open.
            if (logger.isDebugEnabled()) {
                // Report the current count rather than guessing the post-increment value: the
                // increment happens in the finally block below and is capped at maxFailedNumForRetry,
                // so "current + 1" would over-report once the breaker threshold has been reached.
                logger.debug("Distributed cache read failed (treated as cache miss); consecutive failure count is ~" + failedCounter.get(), e);
            }
        } finally {
            if (isOK == null) {
                // Validation error: leave the breaker state untouched.
            } else if (isOK) {
                // Steady-state fast path: when there have been no recent failures the counter
                // is already 0, so skip the two contended atomic writes and only pay a volatile
                // read. Reset order (counter first, then time) is preserved for the rare
                // recovery case to avoid the counter=0 / stale-timestamp race.
                if (failedCounter.get() != 0) {
                    failedCounter.set(0);
                    lastFailedTime.set(NEVER_FAILED);
                }
            } else {
                // Update timestamp BEFORE incrementing counter to prevent race conditions
                // This ensures that when another thread sees the incremented counter,
                // it will also see the corresponding timestamp
                lastFailedTime.set(System.nanoTime());

                // Stop incrementing once the threshold is reached: the circuit-open condition
                // (failedCounter >= maxFailedNumForRetry) is already satisfied, and letting the
                // counter grow without bound would eventually overflow to a negative value and
                // silently disable the breaker after Integer.MAX_VALUE consecutive failures.
                // Use an atomic compare-and-cap (getAndUpdate) instead of a separate get()/
                // incrementAndGet(): the latter is a check-then-act race in which N concurrent
                // failing reads can all observe (cap - 1), all pass the guard, and overshoot the cap.
                final int previousFailedCount = failedCounter.getAndUpdate(current -> current < maxFailedNumForRetry ? current + 1 : current);

                // Surface the closed->open transition once at WARN: while the circuit is open
                // every read returns null with only debug-level logging, so without this the
                // cache can be silently disabled in production.
                if (previousFailedCount == maxFailedNumForRetry - 1 && logger.isWarnEnabled()) {
                    logger.warn("Distributed cache circuit breaker opened after " + maxFailedNumForRetry
                            + " consecutive read failures; reads will fail fast (returning null) until " + retryDelay
                            + " ms have elapsed since the most recent failure");
                }
            }
        }

        return result;
    }

    /**
     * Stores a key-value pair in the distributed cache with custom expiration settings.
     * If the key already exists, its value and expiration time will be replaced.
     * Keys are automatically prefixed and Base64 encoded for compatibility.
     *
     * <p><b>Important - maxIdleTime Limitation:</b>
     * Most distributed cache systems (Memcached, Redis) only support time-to-live (TTL) expiration
     * and do not track last access time. Therefore, the {@code maxIdleTime} parameter is
     * <b>ignored</b> by this implementation. Only the {@code liveTime} parameter affects expiration.
     * If you need idle timeout functionality, consider using {@link LocalCache} instead.
     *
     * <p><b>Behavior:</b>
     * <ul>
     * <li>Overwrites existing entries with the same key</li>
     * <li>Resets TTL for existing entries</li>
     * <li>Only {@code liveTime} affects expiration ({@code maxIdleTime} is ignored)</li>
     * <li>Returns the underlying client's success result; network errors or timeouts typically
     *     propagate as a {@link RuntimeException} rather than returning {@code false}</li>
     * <li>Does not implement circuit breaker logic (unlike {@link #getOrNull(Object)})</li>
     * <li>Does not affect or reset the circuit breaker failure counter</li>
     * </ul>
     *
     * <p><b>Thread Safety:</b>
     * This method is thread-safe and can be called concurrently from multiple threads.
     * The underlying distributed cache client handles thread-safe operations.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = CacheFactory.createDistributedCache(client, "myapp:");
     *
     * // Cache with 1 hour TTL (maxIdleTime ignored)
     * User user = new User("John");
     * boolean success = cache.put("user:123", user, 3600000, 0);   // returns true on success
     * if (success) {
     *     System.out.println("User cached successfully");   // prints when put returned true
     * } else {
     *     System.out.println("Failed to cache user (underlying client reported failure, e.g. cache full / write rejected)");   // prints when put returned false
     * }
     *
     * // Session caching with 30 minute TTL
     * Session session = new Session("abc123");
     * cache.put("session:" + session.getId(), session, 1800000, 1800000);   // returns true; idle time ignored
     *
     * // No expiration (permanent until manually deleted or evicted)
     * Config config = loadConfig();
     * cache.put("app:config", config, 0, 0);            // returns true; liveTime 0 = no expiration
     *
     * // Update existing cache entry with new TTL
     * User updated = cache.getOrNull("user:123");       // returns the previously cached User
     * if (updated != null) {
     *     updated.setLastLogin(System.currentTimeMillis());   // mutate the loaded value before re-caching
     *     cache.put("user:123", updated, 7200000, 0);   // returns true; overwrites with 2 hour TTL
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @param value the value to cache (null handling depends on underlying cache client implementation)
     * @param liveTime the time-to-live in milliseconds (0 or negative for no expiration)
     * @param maxIdleTime the maximum idle time in milliseconds (<b>IGNORED - not supported by distributed caches</b>)
     * @return {@code true} if the operation was successful, {@code false} otherwise (as reported
     *         by the underlying cache client)
     * @throws IllegalStateException if the cache has been closed
     * @throws IllegalArgumentException if the key is null, if the underlying client rejects the generated
     *         cache key (e.g. it exceeds memcached's 250-character key limit after prefixing and Base64
     *         expansion), or if {@code liveTime} exceeds {@link Integer#MAX_VALUE} seconds (~68 years,
     *         rejected by the bundled clients' millisecond-to-second conversion)
     * @throws RuntimeException if a network error or timeout occurs (propagated from the underlying cache client)
     * @see #generateKey(Object)
     * @see DistributedCacheClient#set(String, Object, long)
     */
    @Override
    public boolean put(final K key, final V value, final long liveTime, final long maxIdleTime) {
        assertNotClosed();

        N.checkArgNotNull(key, "key");

        return dcc.set(generateKey(key), value, liveTime);
    }

    /**
     * Removes a key-value pair from the distributed cache.
     * This operation is idempotent - it succeeds whether the key exists or not.
     * Keys are automatically prefixed and Base64 encoded before deletion.
     *
     * <p><b>Behavior:</b>
     * <ul>
     * <li>Removes the entry if the key exists</li>
     * <li>Silently succeeds if the key does not exist (no error)</li>
     * <li>Returns void (does not indicate success/failure or key existence)</li>
     * <li>Safe to call multiple times with the same key (idempotent)</li>
     * <li>Network errors or timeouts typically propagate as a {@link RuntimeException} from the underlying client</li>
     * <li>Does not implement circuit breaker logic</li>
     * </ul>
     *
     * <p><b>Thread Safety:</b>
     * This method is thread-safe and can be called concurrently from multiple threads.
     * However, in a distributed environment, concurrent removes from different clients
     * may race, which is inherent to distributed systems.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = CacheFactory.createDistributedCache(client, "myapp:");
     *
     * // Basic removal
     * cache.remove("user:123");   // removes if present, no-op if absent; returns void
     *
     * // Safe to call multiple times (idempotent)
     * cache.remove("user:123");   // removes the entry
     * cache.remove("user:123");   // no-op; no exception thrown
     *
     * // Cache invalidation on update (write-through pattern)
     * void updateUser(User user) {
     *     database.save(user);                    // persist the authoritative copy first
     *     cache.remove("user:" + user.getId());   // invalidates cache; returns void
     * }
     *
     * // Batch removal
     * List<String> userIds = Arrays.asList("123", "456", "789");
     * userIds.forEach(id -> cache.remove("user:" + id));   // removes each key in turn
     *
     * // Remove on business logic event
     * void logoutUser(String sessionId) {
     *     cache.remove("session:" + sessionId);   // evicts the session entry; returns void
     *     database.updateLastLogout(sessionId);   // record the logout in the backing store
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @throws IllegalStateException if the cache has been closed
     * @throws IllegalArgumentException if the key is null
     * @throws RuntimeException if a network error or timeout occurs (propagated from the underlying cache client)
     * @see #clear()
     * @see #generateKey(Object)
     * @see DistributedCacheClient#delete(String)
     */
    @Override
    public void remove(final K key) {
        assertNotClosed();

        N.checkArgNotNull(key, "key");

        dcc.delete(generateKey(key));
    }

    /**
     * Checks if the cache contains a specific key with a non-null value.
     * This is implemented by attempting to retrieve the value using {@link #getOrNull(Object)},
     * so it inherits all the same behaviors including circuit breaker logic, key transformation,
     * and error handling.
     *
     * <p><b>Implementation Note:</b>
     * This method calls {@code getOrNull(key) != null}, which means it:
     * <ul>
     * <li>Performs a full GET operation (not a lightweight existence check)</li>
     * <li>May return {@code false} if circuit breaker is open</li>
     * <li>May return {@code false} on network errors (exceptions are caught)</li>
     * <li>Returns {@code false} for expired entries</li>
     * <li>Returns {@code false} if the cached value is null</li>
     * <li>Subject to circuit breaker state (same as {@link #getOrNull(Object)})</li>
     * </ul>
     *
     * <p><b>Thread Safety:</b>
     * This method is thread-safe and inherits the thread-safety guarantees of {@link #getOrNull(Object)}.
     *
     * <p><b>Performance Consideration:</b>
     * Since this method performs a full GET operation, if you need the value afterward,
     * it's more efficient to call {@link #getOrNull(Object)} directly and check for null
     * rather than calling {@code containsKey()} followed by {@code getOrNull()}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = CacheFactory.createDistributedCache(client, "myapp:");
     *
     * // Basic existence check
     * if (cache.containsKey("user:123")) {              // returns true only if value present and non-null
     *     System.out.println("User exists in cache");   // prints when containsKey returned true
     * } else {
     *     System.out.println("User not in cache (or circuit breaker open)");   // prints when containsKey returned false
     * }
     *
     * // Conditional caching
     * if (!cache.containsKey("config:settings")) {      // returns false when absent
     *     Config config = loadConfigFromFile();
     *     cache.put("config:settings", config, 0, 0);   // returns true; populates the missing entry
     * }
     *
     * // INEFFICIENT - performs GET twice:
     * if (cache.containsKey("user:123")) {              // first GET (returns true)
     *     User user = cache.getOrNull("user:123");      // second GET operation!
     *     processUser(user);                            // consume the fetched value
     * }
     *
     * // EFFICIENT - performs GET once:
     * User user = cache.getOrNull("user:123");          // single GET; returns value or null
     * if (user != null) {
     *     processUser(user);                            // consume the fetched value
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @return {@code true} if the key exists and has a non-null value, {@code false} otherwise,
     *         on error, or when circuit breaker is open
     * @throws IllegalStateException if the cache has been closed
     * @throws IllegalArgumentException if the key is null (thrown by {@link #getOrNull(Object)})
     * @see #getOrNull(Object)
     */
    @Override
    public boolean containsKey(final K key) {
        return getOrNull(key) != null;
    }

    /**
     * Returns the set of keys in the cache.
     * This operation is <b>not supported</b> for distributed caches due to
     * performance and consistency concerns in distributed systems.
     *
     * <p><b>Rationale for not supporting this operation:</b>
     * <ul>
     * <li>Distributed caches like Memcached and Redis do not efficiently support key enumeration</li>
     * <li>Retrieving all keys would require scanning entire cache servers, causing severe performance degradation</li>
     * <li>The result would be inconsistent across concurrent operations in distributed environments</li>
     * <li>Key prefixing and Base64 encoding makes key filtering complex and inefficient</li>
     * <li>Would return keys from all applications sharing the cache servers, not just this instance</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = CacheFactory.createDistributedCache(client, "myapp:");
     *
     * // keySet() is never supported on a distributed cache.
     * cache.keySet();                                  // throws UnsupportedOperationException
     *
     * // It throws even on an empty cache (nothing has been put yet).
     * try {
     *     Set<String> keys = cache.keySet();           // throws UnsupportedOperationException
     * } catch (UnsupportedOperationException e) {
     *     // Use containsKey(key) to test a specific key instead of enumerating keys.
     *     boolean present = cache.containsKey("user:123");   // returns false (nothing cached yet)
     * }
     * }</pre>
     *
     * @return this method never returns normally; it always throws
     * @throws UnsupportedOperationException always thrown for distributed caches
     * @see #size()
     * @see #containsKey(Object)
     */
    @Override
    public Set<K> keySet() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the number of entries in the cache.
     * This operation is <b>not supported</b> for distributed caches due to
     * performance and consistency concerns in distributed systems.
     *
     * <p><b>Rationale for not supporting this operation:</b>
     * <ul>
     * <li>Distributed caches typically do not track total entry count</li>
     * <li>Counting entries would require scanning all cache servers, causing severe performance degradation</li>
     * <li>The count would include entries from all applications sharing the cache servers</li>
     * <li>Key prefixing makes it impossible to count only this cache instance's entries efficiently</li>
     * <li>The result would be immediately stale in concurrent distributed environments</li>
     * <li>No efficient way to distinguish this instance's keys from others without full server scan</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = CacheFactory.createDistributedCache(client, "myapp:");
     *
     * // size() is never supported on a distributed cache.
     * cache.size();                                    // throws UnsupportedOperationException
     *
     * // It throws even right after a successful put (the count is not tracked).
     * cache.put("user:123", new User("John"), 3600000, 0);   // returns true
     * try {
     *     int count = cache.size();                    // throws UnsupportedOperationException
     * } catch (UnsupportedOperationException e) {
     *     // No entry count is available; probe individual keys with containsKey(key) instead.
     *     boolean present = cache.containsKey("user:123");   // returns true (just cached above)
     * }
     * }</pre>
     *
     * @return this method never returns normally; it always throws
     * @throws UnsupportedOperationException always thrown for distributed caches
     * @see #keySet()
     * @see #containsKey(Object)
     */
    @Override
    public int size() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Removes all entries from all connected cache servers.
     * This is a destructive operation that affects <b>ALL data across ALL servers</b>,
     * not just entries with the configured key prefix. Use with extreme caution in production environments.
     *
     * <p><b>WARNING - Global Impact:</b>
     * <ul>
     * <li>Flushes <b>ALL</b> data from all cache servers (not just keys with this instance's prefix)</li>
     * <li>Affects all applications sharing the same cache servers</li>
     * <li>Cannot be undone - all cached data is permanently lost</li>
     * <li>May cause sudden load spike on backend systems as all caches become cold simultaneously</li>
     * <li>Should typically only be used in testing or with dedicated cache servers</li>
     * <li>Does not reset the circuit breaker state</li>
     * </ul>
     *
     * <p><b>Thread Safety:</b>
     * This method is thread-safe. However, due to the global impact, concurrent calls from
     * multiple threads will all trigger flush operations on the distributed cache servers.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = CacheFactory.createDistributedCache(client, "myapp:");
     *
     * // WARNING: This will flush ALL data from all cache servers!
     * // Not just "myapp:" prefixed keys, but EVERYTHING!
     * cache.clear();   // flushes every entry on all servers; returns void
     *
     * // Safe usage in testing (with dedicated test cache server)
     * // @After
     * public void cleanupCache() {
     *     if (isTestEnvironment()) {
     *         testCache.clear();   // OK - dedicated test server; flushes all and returns void
     *     }
     * }
     *
     * // Production usage - AVOID or require explicit confirmation
     * public void clearProductionCache(String confirmationToken) {
     *     if (!"CONFIRM_FLUSH_ALL_PRODUCTION".equals(confirmationToken)) {
     *         throw new IllegalArgumentException("Invalid confirmation");   // throws on bad token
     *     }
     *     logger.warn("DANGEROUS: Flushing ALL cache data from production servers");   // audit warning before flush
     *     cache.clear();   // affects all apps sharing these servers; returns void
     *     auditLog.record("CACHE_FLUSH_ALL", getCurrentUser());   // records the destructive action
     * }
     * }</pre>
     *
     * @throws IllegalStateException if the cache has been closed
     * @throws UnsupportedOperationException if the underlying client does not support {@code flushAll}
     *         (the {@link AbstractDistributedCacheClient} base class throws this by default)
     * @throws RuntimeException if a network error or timeout occurs (propagated from the underlying cache client)
     * @see DistributedCacheClient#flushAll()
     * @see #remove(Object)
     */
    @Override
    public void clear() {
        assertNotClosed();

        dcc.flushAll();
    }

    /**
     * Closes the cache and disconnects from all distributed cache servers.
     * After closing, the cache cannot be used - subsequent operations will throw {@link IllegalStateException}.
     * This method is idempotent and thread-safe - calling multiple times has no additional effect after the first call.
     *
     * <p><b>Resource Cleanup:</b>
     * <ul>
     * <li>Disconnects from all cache servers</li>
     * <li>Releases network connections and thread pools managed by the underlying cache client</li>
     * <li>Marks cache as closed ({@link #isClosed()} returns true)</li>
     * <li>Does not flush cached data from servers (data remains until expired or evicted)</li>
     * <li>Does not reset circuit breaker state (failure counters remain in their current state)</li>
     * <li>Safe to call multiple times (idempotent)</li>
     * </ul>
     *
     * <p><b>Thread Safety:</b>
     * This method is synchronized to ensure thread-safe closure. Multiple threads calling {@code close()}
     * concurrently will be serialized, and only the first call will perform the actual disconnection.
     * Subsequent calls return immediately without additional work.
     *
     * <p><b>Post-Close Behavior:</b>
     * Operations <em>started</em> after closing (except {@link #isClosed()} and {@link #close()}) throw
     * {@link IllegalStateException} via {@link #assertNotClosed()}. An operation already in flight when
     * {@code close()} runs is not excluded: it may complete normally, or surface the underlying client's
     * own exception (e.g., a "shutting down" error) instead of {@code IllegalStateException}. For the
     * asynchronous wrappers ({@code asyncGet}, {@code asyncPut}, ...), the closed-check runs inside the
     * submitted task, so the call site receives a normally-returned future that <em>completes
     * exceptionally</em> with the {@code IllegalStateException} rather than throwing directly.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Try-with-resources (recommended - implements AutoCloseable)
     * try (DistributedCache<String, User> cache = CacheFactory.createDistributedCache(client, "myapp:")) {
     *     cache.put("user:123", user, 3600000, 0);      // returns true
     *     User cached = cache.getOrNull("user:123");    // returns the cached User
     *     // Cache automatically closed when exiting try block
     * }
     *
     * // Try-finally pattern
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = CacheFactory.createDistributedCache(client, "myapp:");
     * try {
     *     cache.put("key", value, 3600000, 0);          // returns true
     *     // ... use cache
     * } finally {
     *     cache.close();   // releases resources; disconnects the client
     * }
     *
     * // Application shutdown hook
     * Runtime.getRuntime().addShutdownHook(new Thread(() -> {
     *     if (!cache.isClosed()) {                       // returns false until close() runs
     *         cache.close();                             // disconnects; sets isClosed() to true
     *     }
     * }));
     *
     * // Safe to call multiple times (idempotent)
     * cache.close();   // first call disconnects the client
     * cache.close();   // no-op; no exception thrown, disconnect not repeated
     *
     * // Verify operations fail after close
     * cache.close();              // marks the cache closed
     * cache.getOrNull("key");     // throws IllegalStateException (cache is closed)
     * }</pre>
     *
     * @see DistributedCacheClient#disconnect()
     * @see #isClosed()
     * @see #assertNotClosed()
     */
    @Override
    public synchronized void close() {
        if (isClosed()) {
            return;
        }

        // Mark as closed first to prevent new operations from starting
        // even if disconnect() throws an exception
        isClosed = true;

        try {
            dcc.disconnect();
        } catch (final Exception e) {
            // Even if disconnect fails, the cache remains closed; not rethrown to keep close() idempotent.
            // Logged at warn because a failed disconnect may leak client-side connections/resources.
            if (logger.isWarnEnabled()) {
                logger.warn("Error while disconnecting the distributed cache client during close()", e);
            }
        }
    }

    /**
     * Checks if the cache has been closed.
     * Once a cache is closed via {@link #close()}, it cannot be reopened and should not be used.
     * This method is thread-safe and returns immediately without blocking.
     *
     * <p><b>Behavior:</b>
     * <ul>
     * <li>Returns {@code true} if {@link #close()} has been called, {@code false} otherwise</li>
     * <li>Thread-safe and can be called from multiple threads concurrently</li>
     * <li>Does not throw exceptions even if the cache is in an error state</li>
     * <li>Once true, will always remain true (caches cannot be reopened)</li>
     * <li>Reads the volatile {@code isClosed} field, ensuring visibility across threads</li>
     * </ul>
     *
     * <p><b>Thread Safety:</b>
     * This method is thread-safe. The {@code isClosed} field is declared as {@code volatile},
     * ensuring that changes made by {@link #close()} are immediately visible to all threads
     * calling this method.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = CacheFactory.createDistributedCache(client, "myapp:");
     *
     * // Check before operations
     * if (!cache.isClosed()) {                          // returns false while the cache is open
     *     cache.put("user:123", user, 3600000, 0);      // returns true
     * } else {
     *     logger.warn("Cache is closed, skipping cache operation");   // logged when isClosed() is true
     * }
     *
     * // Verify state after closing
     * cache.close();                                    // disconnects the client
     * System.out.println("Is closed: " + cache.isClosed());   // isClosed() returns true
     *
     * // Safe guard in long-running operations
     * public void processUsers(List<User> users) {
     *     for (User user : users) {
     *         if (cache.isClosed()) {                   // returns true once close() has been called
     *             logger.warn("Cache closed during processing");   // logged and loop aborted
     *             break;
     *         }
     *         cache.put("user:" + user.getId(), user, 3600000, 0);   // returns true while open
     *     }
     * }
     *
     * // Conditional cleanup in shutdown hooks
     * public void shutdown() {
     *     if (!cache.isClosed()) {                      // returns false if not yet closed
     *         cache.close();                            // disconnects; isClosed() becomes true
     *     }
     * }
     * }</pre>
     *
     * @return {@code true} if {@link #close()} has been called, {@code false} otherwise
     * @see #close()
     */
    @Override
    public boolean isClosed() {
        return isClosed;
    }

    /**
     * Generates the actual cache key by applying prefix and Base64 encoding.
     * This method transforms the application-level key into a format suitable for distributed cache systems
     * that may have restrictions on key characters (e.g., spaces, special characters, Unicode).
     *
     * <p><b>Transformation Process:</b>
     * <ol>
     * <li>Validate key is not null (throws {@link IllegalArgumentException} if null)</li>
     * <li>Convert key to string: the key itself if it is already a {@code String}, otherwise {@code N.stringOf(k)}</li>
     * <li>Encode to UTF-8 bytes: {@code toString(k).getBytes(Charsets.UTF_8)}</li>
     * <li>Base64 encode: {@code Strings.base64Encode(bytes)}</li>
     * <li>Prepend prefix if configured: {@code keyPrefix + base64Key}</li>
     * </ol>
     *
     * <p><b>Rationale for Base64 Encoding:</b>
     * <ul>
     * <li>Ensures the encoded key part is ASCII-safe for Memcached (which restricts key characters);
     *     the prefix is prepended verbatim and is validated at construction time to be printable ASCII</li>
     * <li>Handles Unicode characters in keys safely</li>
     * <li>Avoids issues with special characters (spaces, newlines, control characters, etc.)</li>
     * <li>Provides consistent key format across different cache systems</li>
     * <li>Eliminates risk of key injection or parsing issues in cache protocols</li>
     * </ul>
     *
     * <p><b>Length Expansion:</b> Base64 expands the key by a factor of 4/3 (plus the prefix length).
     * Memcached's client-side key limit is 250 characters, so an original key longer than roughly 186
     * characters (less when a prefix is configured) produces a generated key the underlying client
     * rejects with {@link IllegalArgumentException} on every operation. Redis has no comparable limit.
     *
     * <p><b>Namespace Isolation:</b> a prefix containing at least one character outside the Base64
     * alphabet (such as {@code ':'}) guarantees prefixed and unprefixed key spaces can never collide;
     * a prefix made purely of Base64-alphabet characters with a length divisible by 4 could in theory
     * collide with the Base64 encoding of a different unprefixed key.
     *
     * <p><b>Thread Safety:</b>
     * This method is thread-safe and stateless. Multiple threads can call this method
     * concurrently without synchronization.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // With prefix "myapp:"
     * DistributedCache<String, User> cache = CacheFactory.createDistributedCache(client, "myapp:");
     * String cacheKey = cache.generateKey("user:123");
     * // Result: "myapp:dXNlcjoxMjM=" (prefix + Base64 of "user:123")
     *
     * // Without prefix
     * DistributedCache<String, User> cache2 = CacheFactory.createDistributedCache(client, "");
     * String cacheKey2 = cache2.generateKey("user:123");
     * // Result: "dXNlcjoxMjM=" (just Base64 of "user:123")
     *
     * // With Unicode key
     * String unicodeKey = cache.generateKey("用户:123");
     * // Result: "myapp:55So5oi3OjEyMw==" (prefix + Base64 of UTF-8 encoded Unicode)
     *
     * // With spaces and special characters
     * String specialKey = cache.generateKey("my key with spaces!");
     * // Result: "myapp:bXkga2V5IHdpdGggc3BhY2VzIQ==" (safe for all cache systems)
     *
     * // Integer key (converted to string first)
     * String intKey = cache.generateKey(12345);
     * // Result: "myapp:MTIzNDU=" (prefix + Base64 of "12345")
     * }</pre>
     *
     * @param key the original key, must not be null
     * @return the prefixed and Base64-encoded cache key suitable for distributed cache systems
     * @throws IllegalArgumentException if key is null, or if its string representation
     *         (via {@link N#stringOf(Object)}) is null
     * @see Strings#base64Encode(byte[])
     * @see N#stringOf(Object)
     */
    protected String generateKey(final K key) {
        N.checkArgNotNull(key, "key");

        // Fast path for String keys: skip N.stringOf (which may do reflection/formatting
        // for arbitrary types) when the key is already a String.
        final String keyStr = (key instanceof final String s) ? s : N.stringOf(key);
        if (keyStr == null) {
            throw new IllegalArgumentException("Key string representation cannot be null");
        }

        final String encodedKey = Strings.base64Encode(keyStr.getBytes(Charsets.UTF_8));

        return hasKeyPrefix ? (keyPrefix + encodedKey) : encodedKey;
    }

    /**
     * Ensures the cache is not closed before performing operations.
     * This method is called at the beginning of most cache operations to prevent
     * operations on a closed cache, which could lead to resource leaks, network errors,
     * or undefined behavior.
     *
     * <p><b>Usage:</b>
     * This is a protected utility method used internally by cache operation methods
     * ({@link #getOrNull(Object)}, {@link #put(Object, Object, long, long)}, {@link #remove(Object)}, {@link #clear()})
     * to validate cache state before proceeding with operations.
     *
     * <p><b>Thread Safety:</b>
     * This method is thread-safe and reads the volatile {@code isClosed} field to ensure
     * visibility of close operations across threads.
     *
     * @throws IllegalStateException if the cache has been closed (i.e., {@link #isClosed()} returns true)
     * @see #isClosed()
     * @see #close()
     */
    protected void assertNotClosed() {
        if (isClosed) {
            throw new IllegalStateException("This cache has been closed");
        }
    }
}
