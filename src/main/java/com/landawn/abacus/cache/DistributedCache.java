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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.landawn.abacus.util.Charsets;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Strings;

/**
 * A wrapper cache implementation that provides a standardized Cache interface for distributed cache clients.
 * This class adds key prefixing, error handling with circuit breaker pattern, and adapts distributed cache
 * client operations to the standard Cache interface. It's designed to work with any
 * DistributedCacheClient implementation like Memcached or Redis.
 *
 * <p><b>Key Features:</b></p>
 * <ul>
 * <li>Automatic key prefixing for namespace isolation</li>
 * <li>Base64 encoding of keys for compatibility with cache systems that restrict key characters</li>
 * <li>Circuit breaker pattern on read operations to protect against cascading failures</li>
 * <li>Transparent error recovery with configurable retry behavior</li>
 * <li>Adaptation of TTL-only expiration to TTL+idle interface (maxIdleTime ignored)</li>
 * </ul>
 *
 * <p><b>Thread Safety:</b></p>
 * This class is thread-safe. The circuit breaker state (failure counter and last failure time)
 * is managed using atomic variables, and the close operation is synchronized. Multiple threads
 * can safely perform concurrent cache operations.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Create cache with full configuration
 * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
 * DistributedCache<String, User> cache = new DistributedCache<>(
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
 * @param <K> the key type
 * @param <V> the value type
 * @see AbstractCache
 * @see DistributedCacheClient
 * @see CacheFactory
 */
public class DistributedCache<K, V> extends AbstractCache<K, V> {

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

    private final int maxFailedNumForRetry;

    private final long retryDelay;

    // ...
    private final AtomicInteger failedCounter = new AtomicInteger();

    private final AtomicLong lastFailedTime = new AtomicLong(0);

    private volatile boolean isClosed = false;

    /**
     * Creates a DistributedCache with default retry configuration.
     * Uses an empty key prefix and default circuit breaker parameters (max 100 consecutive failures, 1000ms retry delay).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = new DistributedCache<>(client);
     *
     * // Cache operations will use circuit breaker protection
     * cache.put("key", new User("John"), 3600000, 0);
     * User user = cache.getOrNull("key");
     * }</pre>
     *
     * @param dcc the distributed cache client to wrap (must not be null)
     * @throws IllegalArgumentException if dcc is null
     */
    protected DistributedCache(final DistributedCacheClient<V> dcc) {
        this(dcc, Strings.EMPTY, DEFAULT_MAX_FAILED_NUMBER, DEFAULT_RETRY_DELAY);
    }

    /**
     * Creates a DistributedCache with a key prefix and default circuit breaker configuration.
     * All keys will be prefixed for namespace isolation. Uses default circuit breaker parameters
     * (max 100 consecutive failures, 1000ms retry delay).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = new DistributedCache<>(client, "myapp:");
     *
     * // All keys will be prefixed with "myapp:" and Base64 encoded
     * cache.put("user:123", new User("John"), 3600000, 0);
     * // Actual cache key: "myapp:" + Base64("user:123")
     * }</pre>
     *
     * @param dcc the distributed cache client to wrap (must not be null)
     * @param keyPrefix the key prefix to prepend to all keys (can be empty string or null for no prefix)
     * @throws IllegalArgumentException if dcc is null
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
     * <p><b>Circuit Breaker Parameters:</b></p>
     * <ul>
     * <li>{@code maxFailedNumForRetry}: Number of consecutive failures before circuit opens</li>
     * <li>{@code retryDelay}: Milliseconds to wait before attempting retry after circuit opens</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     *
     * // More aggressive circuit breaker (opens faster, retries sooner)
     * DistributedCache<String, User> aggressiveCache = new DistributedCache<>(
     *     client,
     *     "myapp:",  // key prefix for namespace isolation
     *     10,        // open circuit after just 10 consecutive failures
     *     500        // retry after 500ms
     * );
     *
     * // More tolerant circuit breaker (slower to open, longer retry delay)
     * DistributedCache<String, User> tolerantCache = new DistributedCache<>(
     *     client,
     *     "myapp:",
     *     200,       // allow up to 200 consecutive failures
     *     5000       // wait 5 seconds before retry
     * );
     * }</pre>
     *
     * @param dcc the distributed cache client to wrap (must not be null)
     * @param keyPrefix the key prefix to prepend to all keys (can be empty string or null for no prefix)
     * @param maxFailedNumForRetry maximum consecutive failures before opening circuit breaker (must be non-negative)
     * @param retryDelay delay in milliseconds before attempting retry after circuit opens (must be non-negative)
     * @throws IllegalArgumentException if dcc is null, maxFailedNumForRetry is negative, or retryDelay is negative
     */
    protected DistributedCache(final DistributedCacheClient<V> dcc, final String keyPrefix, final int maxFailedNumForRetry, final long retryDelay) {
        if (dcc == null) {
            throw new IllegalArgumentException("DistributedCacheClient cannot be null");
        }

        if (maxFailedNumForRetry < 0) {
            throw new IllegalArgumentException("maxFailedNumForRetry cannot be negative: " + maxFailedNumForRetry);
        }

        if (retryDelay < 0) {
            throw new IllegalArgumentException("retryDelay cannot be negative: " + retryDelay);
        }

        this.keyPrefix = Strings.isEmpty(keyPrefix) ? Strings.EMPTY : keyPrefix;
        this.dcc = dcc;
        this.maxFailedNumForRetry = maxFailedNumForRetry;
        this.retryDelay = retryDelay;
    }

    /**
     * Retrieves a value from the distributed cache by its key.
     * This method implements circuit breaker pattern to protect against cascading failures
     * when the distributed cache becomes unavailable. Keys are automatically prefixed and
     * Base64 encoded for compatibility with distributed cache systems that have key format restrictions.
     *
     * <p><b>Circuit Breaker Behavior:</b></p>
     * The circuit breaker has three states:
     * <ul>
     * <li><b>Closed (Normal):</b> Operations proceed normally. Tracks failure count.</li>
     * <li><b>Open (Failing Fast):</b> When {@code failedCounter > maxFailedNumForRetry} AND within {@code retryDelay}
     *     milliseconds of last failure, returns {@code null} immediately without attempting cache access.</li>
     * <li><b>Half-Open (Testing):</b> After {@code retryDelay} expires, attempts one operation to test if cache recovered.</li>
     * </ul>
     *
     * <p><b>State Transitions:</b></p>
     * <ul>
     * <li>Successful operation: Resets {@code failedCounter} to 0 and {@code lastFailedTime} to 0 (closes circuit)</li>
     * <li>Failed operation: Increments {@code failedCounter} and updates {@code lastFailedTime} to current time</li>
     * <li>All exceptions from underlying cache client are caught and treated as failures</li>
     * </ul>
     *
     * <p><b>Thread Safety:</b></p>
     * This method is thread-safe. The failure counter and last failure time are managed using
     * {@link AtomicInteger} and {@link AtomicLong} respectively, allowing safe concurrent access.
     *
     * <p><b>Key Processing:</b></p>
     * The key is transformed as follows: {@code key -> keyPrefix + Base64(UTF8(toString(key)))}.
     * Base64 encoding ensures compatibility with cache systems that restrict key characters
     * (e.g., spaces, special characters, Unicode).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = new DistributedCache<>(client, "myapp:", 100, 1000);
     *
     * // Basic retrieval with null check
     * User user = cache.getOrNull("user:123");
     * if (user != null) {
     *     System.out.println("Found: " + user.getName());
     * } else {
     *     // Could be: not found, expired, evicted, circuit open, or network error
     *     System.out.println("Cache miss - loading from database");
     *     user = loadFromDatabase("user:123");
     *     cache.put("user:123", user, 3600000, 0);
     * }
     *
     * // Cache-aside pattern with circuit breaker protection
     * User getUser(String userId) {
     *     User user = cache.getOrNull("user:" + userId);
     *     if (user == null) {
     *         // Fallback to database (could be cache miss or circuit breaker open)
     *         user = database.findUser(userId);
     *         if (user != null) {
     *             cache.put("user:" + userId, user, 3600000, 0);
     *         }
     *     }
     *     return user;
     * }
     * }</pre>
     *
     * @param key the cache key, must not be null
     * @return the cached value, or {@code null} if not found, expired, evicted, circuit breaker is open, or on error
     * @throws IllegalStateException if the cache has been closed
     * @throws IllegalArgumentException if the key is null (thrown by {@link #generateKey(Object)})
     * @see #generateKey(Object)
     * @see DistributedCacheClient#get(String)
     */
    @Override
    public V getOrNull(final K key) {
        assertNotClosed();

        // Read circuit breaker state atomically to avoid race conditions
        final int currentFailedCount = failedCounter.get();
        final long currentLastFailedTime = lastFailedTime.get();
        final long currentTime = System.currentTimeMillis();

        // Check circuit breaker state with atomic snapshot to prevent time-based race conditions
        if ((currentFailedCount > maxFailedNumForRetry) && ((currentTime - currentLastFailedTime) < retryDelay)) {
            return null;
        }

        V result = null;
        boolean isOK = false;

        try {
            result = dcc.get(generateKey(key));
            isOK = true;
        } catch (final Exception e) {
            // Log the exception if needed, but don't rethrow
            // isOK = false;
        } finally {
            if (isOK) {
                // Reset in the correct order: counter first, then time
                // This prevents race conditions where another thread might see counter=0 but old timestamp
                failedCounter.set(0);
                lastFailedTime.set(0);
            } else {
                // Update timestamp BEFORE incrementing counter to prevent race conditions
                // This ensures that when another thread sees the incremented counter,
                // it will also see the corresponding timestamp
                lastFailedTime.set(System.currentTimeMillis());
                failedCounter.incrementAndGet();
            }
        }

        return result;
    }

    /**
     * Stores a key-value pair in the distributed cache with custom expiration settings.
     * If the key already exists, its value and expiration time will be replaced.
     * Keys are automatically prefixed and Base64 encoded for compatibility.
     *
     * <p><b>Important - maxIdleTime Limitation:</b></p>
     * Most distributed cache systems (Memcached, Redis) only support time-to-live (TTL) expiration
     * and do not track last access time. Therefore, the {@code maxIdleTime} parameter is
     * <b>ignored</b> by this implementation. Only the {@code liveTime} parameter affects expiration.
     * If you need idle timeout functionality, consider using {@link LocalCache} instead.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>Overwrites existing entries with the same key</li>
     * <li>Resets TTL for existing entries</li>
     * <li>Only {@code liveTime} affects expiration ({@code maxIdleTime} is ignored)</li>
     * <li>Returns {@code false} on network errors or timeouts</li>
     * <li>Does not implement circuit breaker logic (unlike {@link #getOrNull(Object)})</li>
     * <li>Does not affect or reset the circuit breaker failure counter</li>
     * </ul>
     *
     * <p><b>Thread Safety:</b></p>
     * This method is thread-safe and can be called concurrently from multiple threads.
     * The underlying distributed cache client handles thread-safe operations.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = new DistributedCache<>(client, "myapp:");
     *
     * // Cache with 1 hour TTL (maxIdleTime ignored)
     * User user = new User("John");
     * boolean success = cache.put("user:123", user, 3600000, 0);
     * if (success) {
     *     System.out.println("User cached successfully");
     * } else {
     *     System.out.println("Failed to cache user (network error or timeout)");
     * }
     *
     * // Session caching with 30 minute TTL
     * Session session = new Session("abc123");
     * cache.put("session:" + session.getId(), session, 1800000, 1800000);   // idle time ignored
     *
     * // No expiration (permanent until manually deleted or evicted)
     * Config config = loadConfig();
     * cache.put("app:config", config, 0, 0);
     *
     * // Update existing cache entry with new TTL
     * User updated = cache.getOrNull("user:123");
     * if (updated != null) {
     *     updated.setLastLogin(System.currentTimeMillis());
     *     cache.put("user:123", updated, 7200000, 0);   // 2 hour TTL
     * }
     * }</pre>
     *
     * @param key the cache key, must not be null
     * @param value the value to cache (null handling depends on underlying cache client implementation)
     * @param liveTime the time-to-live in milliseconds (0 or negative for no expiration)
     * @param maxIdleTime the maximum idle time in milliseconds (<b>IGNORED - not supported by distributed caches</b>)
     * @return {@code true} if the operation was successful, {@code false} on network errors or timeouts
     * @throws IllegalStateException if the cache has been closed
     * @throws IllegalArgumentException if the key is null (thrown by {@link #generateKey(Object)})
     * @see #generateKey(Object)
     * @see DistributedCacheClient#set(String, Object, long)
     */
    @Override
    public boolean put(final K key, final V value, final long liveTime, final long maxIdleTime) {
        assertNotClosed();

        return dcc.set(generateKey(key), value, liveTime);
    }

    /**
     * Removes a key-value pair from the distributed cache.
     * This operation is idempotent - it succeeds whether the key exists or not.
     * Keys are automatically prefixed and Base64 encoded before deletion.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>Removes the entry if the key exists</li>
     * <li>Silently succeeds if the key does not exist (no error)</li>
     * <li>Returns void (does not indicate success/failure or key existence)</li>
     * <li>Safe to call multiple times with the same key (idempotent)</li>
     * <li>Network errors are silently ignored (no exception thrown)</li>
     * <li>Does not implement circuit breaker logic</li>
     * </ul>
     *
     * <p><b>Thread Safety:</b></p>
     * This method is thread-safe and can be called concurrently from multiple threads.
     * However, in a distributed environment, concurrent removes from different clients
     * may race, which is inherent to distributed systems.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = new DistributedCache<>(client, "myapp:");
     *
     * // Basic removal
     * cache.remove("user:123");   // Removes if exists, no-op if doesn't exist
     *
     * // Safe to call multiple times (idempotent)
     * cache.remove("user:123");
     * cache.remove("user:123");   // No exception thrown
     *
     * // Cache invalidation on update (write-through pattern)
     * void updateUser(User user) {
     *     database.save(user);
     *     cache.remove("user:" + user.getId());   // Invalidate cache
     * }
     *
     * // Batch removal
     * List<String> userIds = Arrays.asList("123", "456", "789");
     * userIds.forEach(id -> cache.remove("user:" + id));
     *
     * // Remove on business logic event
     * void logoutUser(String sessionId) {
     *     cache.remove("session:" + sessionId);
     *     database.updateLastLogout(sessionId);
     * }
     * }</pre>
     *
     * @param key the cache key, must not be null
     * @throws IllegalStateException if the cache has been closed
     * @throws IllegalArgumentException if the key is null (thrown by {@link #generateKey(Object)})
     * @see #clear()
     * @see #generateKey(Object)
     * @see DistributedCacheClient#delete(String)
     */
    @Override
    public void remove(final K key) {
        assertNotClosed();

        dcc.delete(generateKey(key));
    }

    /**
     * Checks if the cache contains a specific key with a non-null value.
     * This is implemented by attempting to retrieve the value using {@link #getOrNull(Object)},
     * so it inherits all the same behaviors including circuit breaker logic, key transformation,
     * and error handling.
     *
     * <p><b>Implementation Note:</b></p>
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
     * <p><b>Thread Safety:</b></p>
     * This method is thread-safe and inherits the thread-safety guarantees of {@link #getOrNull(Object)}.
     *
     * <p><b>Performance Consideration:</b></p>
     * Since this method performs a full GET operation, if you need the value afterward,
     * it's more efficient to call {@link #getOrNull(Object)} directly and check for null
     * rather than calling {@code containsKey()} followed by {@code getOrNull()}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = new DistributedCache<>(client, "myapp:");
     *
     * // Basic existence check
     * if (cache.containsKey("user:123")) {
     *     System.out.println("User exists in cache");
     * } else {
     *     System.out.println("User not in cache (or circuit breaker open)");
     * }
     *
     * // Conditional caching
     * if (!cache.containsKey("config:settings")) {
     *     Config config = loadConfigFromFile();
     *     cache.put("config:settings", config, 0, 0);
     * }
     *
     * // INEFFICIENT - performs GET twice:
     * if (cache.containsKey("user:123")) {
     *     User user = cache.getOrNull("user:123");   // Second GET operation!
     *     processUser(user);
     * }
     *
     * // EFFICIENT - performs GET once:
     * User user = cache.getOrNull("user:123");
     * if (user != null) {
     *     processUser(user);
     * }
     * }</pre>
     *
     * @param key the cache key, must not be null
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
     * <p><b>Rationale for not supporting this operation:</b></p>
     * <ul>
     * <li>Distributed caches like Memcached and Redis do not efficiently support key enumeration</li>
     * <li>Retrieving all keys would require scanning entire cache servers, causing severe performance degradation</li>
     * <li>The result would be inconsistent across concurrent operations in distributed environments</li>
     * <li>Key prefixing and Base64 encoding makes key filtering complex and inefficient</li>
     * <li>Would return keys from all applications sharing the cache servers, not just this instance</li>
     * </ul>
     *
     * @return never returns normally
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
     * <p><b>Rationale for not supporting this operation:</b></p>
     * <ul>
     * <li>Distributed caches typically do not track total entry count</li>
     * <li>Counting entries would require scanning all cache servers, causing severe performance degradation</li>
     * <li>The count would include entries from all applications sharing the cache servers</li>
     * <li>Key prefixing makes it impossible to count only this cache instance's entries efficiently</li>
     * <li>The result would be immediately stale in concurrent distributed environments</li>
     * <li>No efficient way to distinguish this instance's keys from others without full server scan</li>
     * </ul>
     *
     * @return never returns normally
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
     * <p><b>WARNING - Global Impact:</b></p>
     * <ul>
     * <li>Flushes <b>ALL</b> data from all cache servers (not just keys with this instance's prefix)</li>
     * <li>Affects all applications sharing the same cache servers</li>
     * <li>Cannot be undone - all cached data is permanently lost</li>
     * <li>May cause sudden load spike on backend systems as all caches become cold simultaneously</li>
     * <li>Should typically only be used in testing or with dedicated cache servers</li>
     * <li>Does not reset the circuit breaker state</li>
     * </ul>
     *
     * <p><b>Thread Safety:</b></p>
     * This method is thread-safe. However, due to the global impact, concurrent calls from
     * multiple threads will all trigger flush operations on the distributed cache servers.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = new DistributedCache<>(client, "myapp:");
     *
     * // WARNING: This will flush ALL data from all cache servers!
     * // Not just "myapp:" prefixed keys, but EVERYTHING!
     * cache.clear();
     *
     * // Safe usage in testing (with dedicated test cache server)
     * @After
     * public void cleanupCache() {
     *     if (isTestEnvironment()) {
     *         testCache.clear();   // OK - dedicated test server
     *     }
     * }
     *
     * // Production usage - AVOID or require explicit confirmation
     * public void clearProductionCache(String confirmationToken) {
     *     if (!"CONFIRM_FLUSH_ALL_PRODUCTION".equals(confirmationToken)) {
     *         throw new IllegalArgumentException("Invalid confirmation");
     *     }
     *     logger.warn("DANGEROUS: Flushing ALL cache data from production servers");
     *     cache.clear();   // Will affect all apps using these cache servers!
     *     auditLog.record("CACHE_FLUSH_ALL", getCurrentUser());
     * }
     * }</pre>
     *
     * @throws IllegalStateException if the cache has been closed
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
     * <p><b>Resource Cleanup:</b></p>
     * <ul>
     * <li>Disconnects from all cache servers</li>
     * <li>Releases network connections and thread pools managed by the underlying cache client</li>
     * <li>Marks cache as closed ({@link #isClosed()} returns true)</li>
     * <li>Does not flush cached data from servers (data remains until expired or evicted)</li>
     * <li>Does not reset circuit breaker state (failure counters remain in their current state)</li>
     * <li>Safe to call multiple times (idempotent)</li>
     * </ul>
     *
     * <p><b>Thread Safety:</b></p>
     * This method is synchronized to ensure thread-safe closure. Multiple threads calling {@code close()}
     * concurrently will be serialized, and only the first call will perform the actual disconnection.
     * Subsequent calls return immediately without additional work.
     *
     * <p><b>Post-Close Behavior:</b></p>
     * After closing, all operations except {@link #isClosed()} and {@link #close()} will throw
     * {@link IllegalStateException} via {@link #assertNotClosed()}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Try-with-resources (recommended - implements AutoCloseable)
     * try (DistributedCache<String, User> cache = new DistributedCache<>(client, "myapp:")) {
     *     cache.put("user:123", user, 3600000, 0);
     *     User cached = cache.getOrNull("user:123");
     *     // Cache automatically closed when exiting try block
     * }
     *
     * // Try-finally pattern
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = new DistributedCache<>(client, "myapp:");
     * try {
     *     cache.put("key", value, 3600000, 0);
     *     // ... use cache
     * } finally {
     *     cache.close();   // Always close to release resources
     * }
     *
     * // Application shutdown hook
     * Runtime.getRuntime().addShutdownHook(new Thread(() -> {
     *     if (!cache.isClosed()) {
     *         cache.close();
     *     }
     * }));
     *
     * // Safe to call multiple times (idempotent)
     * cache.close();
     * cache.close();   // No exception thrown
     *
     * // Verify operations fail after close
     * cache.close();
     * cache.getOrNull("key");   // Throws IllegalStateException
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
            // Even if disconnect fails, the cache should remain closed
            // Log the exception if needed, but don't rethrow to maintain idempotent behavior
        }
    }

    /**
     * Checks if the cache has been closed.
     * Once a cache is closed via {@link #close()}, it cannot be reopened and should not be used.
     * This method is thread-safe and returns immediately without blocking.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>Returns {@code true} if {@link #close()} has been called, {@code false} otherwise</li>
     * <li>Thread-safe and can be called from multiple threads concurrently</li>
     * <li>Does not throw exceptions even if the cache is in an error state</li>
     * <li>Once true, will always remain true (caches cannot be reopened)</li>
     * <li>Reads the volatile {@code isClosed} field, ensuring visibility across threads</li>
     * </ul>
     *
     * <p><b>Thread Safety:</b></p>
     * This method is thread-safe. The {@code isClosed} field is declared as {@code volatile},
     * ensuring that changes made by {@link #close()} are immediately visible to all threads
     * calling this method.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = new DistributedCache<>(client, "myapp:");
     *
     * // Check before operations
     * if (!cache.isClosed()) {
     *     cache.put("user:123", user, 3600000, 0);
     * } else {
     *     logger.warn("Cache is closed, skipping cache operation");
     * }
     *
     * // Verify state after closing
     * cache.close();
     * System.out.println("Is closed: " + cache.isClosed());   // true
     *
     * // Safe guard in long-running operations
     * public void processUsers(List<User> users) {
     *     for (User user : users) {
     *         if (cache.isClosed()) {
     *             logger.warn("Cache closed during processing");
     *             break;
     *         }
     *         cache.put("user:" + user.getId(), user, 3600000, 0);
     *     }
     * }
     *
     * // Conditional cleanup in shutdown hooks
     * public void shutdown() {
     *     if (!cache.isClosed()) {
     *         cache.close();
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
     * <p><b>Transformation Process:</b></p>
     * <ol>
     * <li>Validate key is not null (throws {@link IllegalArgumentException} if null)</li>
     * <li>Convert key to string: {@code N.stringOf(k)}</li>
     * <li>Encode to UTF-8 bytes: {@code toString(k).getBytes(Charsets.UTF_8)}</li>
     * <li>Base64 encode: {@code Strings.base64Encode(bytes)}</li>
     * <li>Prepend prefix if configured: {@code keyPrefix + base64Key}</li>
     * </ol>
     *
     * <p><b>Rationale for Base64 Encoding:</b></p>
     * <ul>
     * <li>Ensures keys are ASCII-safe for Memcached (which restricts key characters)</li>
     * <li>Handles Unicode characters in keys safely</li>
     * <li>Avoids issues with special characters (spaces, newlines, control characters, etc.)</li>
     * <li>Provides consistent key format across different cache systems</li>
     * <li>Eliminates risk of key injection or parsing issues in cache protocols</li>
     * </ul>
     *
     * <p><b>Thread Safety:</b></p>
     * This method is thread-safe and stateless. Multiple threads can call this method
     * concurrently without synchronization.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // With prefix "myapp:"
     * DistributedCache<String, User> cache = new DistributedCache<>(client, "myapp:");
     * String cacheKey = cache.generateKey("user:123");
     * // Result: "myapp:dXNlcjoxMjM=" (prefix + Base64 of "user:123")
     *
     * // Without prefix
     * DistributedCache<String, User> cache2 = new DistributedCache<>(client, "");
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
     * @throws IllegalArgumentException if key is null
     * @see Strings#base64Encode(byte[])
     * @see N#stringOf(Object)
     */
    protected String generateKey(final K key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        final String keyStr = N.stringOf(key);
        if (keyStr == null) {
            throw new IllegalArgumentException("Key string representation cannot be null");
        }

        final String encodedKey = Strings.base64Encode(keyStr.getBytes(Charsets.UTF_8));

        return Strings.isEmpty(keyPrefix) ? encodedKey : (keyPrefix + encodedKey);
    }

    /**
     * Ensures the cache is not closed before performing operations.
     * This method is called at the beginning of most cache operations to prevent
     * operations on a closed cache, which could lead to resource leaks, network errors,
     * or undefined behavior.
     *
     * <p><b>Usage:</b></p>
     * This is a protected utility method used internally by cache operation methods
     * ({@link #getOrNull(Object)}, {@link #put(Object, Object, long, long)}, {@link #remove(Object)}, {@link #clear()})
     * to validate cache state before proceeding with operations.
     *
     * <p><b>Thread Safety:</b></p>
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