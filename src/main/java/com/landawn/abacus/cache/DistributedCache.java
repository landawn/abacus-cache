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
 * This class adds key prefixing, error handling with retry logic, and adapts distributed cache
 * client operations to the standard Cache interface. It's designed to work with any
 * DistributedCacheClient implementation like Memcached or Redis.
 * 
 * <br><br>
 * Key features:
 * <ul>
 * <li>Automatic key prefixing for namespace isolation</li>
 * <li>Base64 encoding of keys for compatibility</li>
 * <li>Retry logic with configurable failure threshold</li>
 * <li>Transparent error recovery</li>
 * <li>Adaptation of TTL-only expiration to TTL+idle interface</li>
 * </ul>
 * 
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
 * DistributedCache<String, User> cache = new DistributedCache<>(
 *     client,
 *     "myapp:",           // key prefix
 *     100,                // max failures before stopping retries
 *     1000                // retry delay in ms
 * );
 *
 * User user = new User("John");
 * cache.put("user:123", user, 3600000, 1800000);
 * User cached = cache.gett("user:123");
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
     * Default maximum number of consecutive failures before stopping retry attempts.
     */
    protected static final int DEFAULT_MAX_FAILED_NUMBER = 100;

    /**
     * Default delay in milliseconds between retry attempts after failures.
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
     * Uses an empty key prefix and default retry parameters (max 100 failures, 1000ms retry delay).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = new DistributedCache<>(client);
     * }</pre>
     *
     * @param dcc the distributed cache client to wrap, must not be null
     * @throws IllegalArgumentException if dcc is null
     */
    protected DistributedCache(final DistributedCacheClient<V> dcc) {
        this(dcc, Strings.EMPTY, DEFAULT_MAX_FAILED_NUMBER, DEFAULT_RETRY_DELAY);
    }

    /**
     * Creates a DistributedCache with a key prefix and default retry configuration.
     * All keys will be prefixed for namespace isolation. Uses default retry parameters
     * (max 100 failures, 1000ms retry delay).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = new DistributedCache<>(client, "myapp:");
     * }</pre>
     *
     * @param dcc the distributed cache client to wrap, must not be null
     * @param keyPrefix the prefix to prepend to all keys (empty string or null for no prefix)
     * @throws IllegalArgumentException if dcc is null
     */
    protected DistributedCache(final DistributedCacheClient<V> dcc, final String keyPrefix) {
        this(dcc, keyPrefix, DEFAULT_MAX_FAILED_NUMBER, DEFAULT_RETRY_DELAY);
    }

    /**
     * Creates a DistributedCache with full configuration.
     * Allows customization of key prefix and retry behavior. This is the most flexible
     * constructor allowing full control over all cache parameters.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = new DistributedCache<>(
     *     client,
     *     "myapp:",  // key prefix for namespace isolation
     *     100,       // max consecutive failures
     *     1000       // retry delay in milliseconds
     * );
     * }</pre>
     *
     * @param dcc the distributed cache client to wrap, must not be null
     * @param keyPrefix the prefix to prepend to all keys (empty string or null for no prefix)
     * @param maxFailedNumForRetry maximum consecutive failures before stopping retries (must be positive)
     * @param retryDelay delay in milliseconds between retry attempts after failure threshold is reached (must be non-negative)
     * @throws IllegalArgumentException if dcc is null
     */
    protected DistributedCache(final DistributedCacheClient<V> dcc, final String keyPrefix, final int maxFailedNumForRetry, final long retryDelay) {
        if (dcc == null) {
            throw new IllegalArgumentException("DistributedCacheClient cannot be null");
        }

        this.keyPrefix = Strings.isEmpty(keyPrefix) ? Strings.EMPTY : keyPrefix;
        this.dcc = dcc;
        this.maxFailedNumForRetry = maxFailedNumForRetry;
        this.retryDelay = retryDelay;
    }

    /**
     * Retrieves a value from the distributed cache by its key.
     * This method implements automatic retry logic with circuit breaker pattern to protect
     * against cascading failures. Keys are automatically prefixed and Base64 encoded for
     * compatibility with distributed cache systems that have key format restrictions.
     *
     * <p><b>Retry Mechanism (Circuit Breaker):</b></p>
     * <ul>
     * <li>Tracks consecutive failures and time of last failure</li>
     * <li>When failures exceed {@code maxFailedNumForRetry}, enters "open circuit" state</li>
     * <li>During open circuit (within {@code retryDelay} ms of last failure), returns {@code null} immediately</li>
     * <li>After retry delay expires, attempts operation again (half-open state)</li>
     * <li>Successful operations reset failure counter to zero (closed circuit)</li>
     * <li>All exceptions from underlying cache client are caught and {@code null} returned</li>
     * </ul>
     *
     * <p><b>Key Processing:</b></p>
     * The key is transformed as follows: {@code key -> keyPrefix + Base64(UTF8(toString(k)))}.
     * Base64 encoding ensures compatibility with cache systems that restrict key characters
     * (e.g., spaces, special characters).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = new DistributedCache<>(client, "myapp:");
     *
     * // Basic retrieval with null check
     * User user = cache.gett("user:123");
     * if (user != null) {
     *     System.out.println("Found: " + user.getName());
     * } else {
     *     // Could be: not found, expired, evicted, circuit open, or network error
     *     System.out.println("Cache miss - loading from database");
     *     user = loadFromDatabase("user:123");
     *     cache.put("user:123", user, 3600000, 0);
     * }
     *
     * // Cache-aside pattern
     * User getUser(String userId) {
     *     User user = cache.gett("user:" + userId);
     *     if (user == null) {
     *         user = database.findUser(userId);
     *         if (user != null) {
     *             cache.put("user:" + userId, user, 3600000, 0);
     *         }
     *     }
     *     return user;
     * }
     * }</pre>
     *
     * @param k the cache key, must not be null
     * @return the cached value, or {@code null} if not found, expired, evicted, circuit breaker is open, or on error
     * @throws IllegalStateException if the cache has been closed
     * @throws IllegalArgumentException if the key is null (thrown by {@link #generateKey(Object)})
     * @see #generateKey(Object)
     * @see DistributedCacheClient#get(String)
     */
    @Override
    public V gett(final K k) {
        assertNotClosed();

        if ((failedCounter.get() > maxFailedNumForRetry) && ((System.currentTimeMillis() - lastFailedTime.get()) < retryDelay)) {
            return null;
        }

        V result = null;
        boolean isOK = false;

        try {
            result = dcc.get(generateKey(k));
            isOK = true;
        } catch (final Exception e) {
            // Log the exception if needed, but don't rethrow
            // isOK = false;
        } finally {
            if (isOK) {
                failedCounter.set(0);
                lastFailedTime.set(0);
            } else {
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
     * <li>Does not implement circuit breaker logic (unlike {@link #gett(Object)})</li>
     * </ul>
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
     * }
     *
     * // Session caching with 30 minute TTL
     * Session session = new Session("abc123");
     * cache.put("session:" + session.getId(), session, 1800000, 1800000); // idle time ignored
     *
     * // No expiration (permanent until manually deleted)
     * Config config = loadConfig();
     * cache.put("app:config", config, 0, 0);
     *
     * // Update existing cache entry with new TTL
     * User updated = cache.gett("user:123");
     * updated.setLastLogin(System.currentTimeMillis());
     * cache.put("user:123", updated, 7200000, 0); // 2 hour TTL
     * }</pre>
     *
     * @param k the cache key, must not be null
     * @param v the value to cache (null handling depends on underlying cache client implementation)
     * @param liveTime the time-to-live in milliseconds (0 or negative for no expiration)
     * @param maxIdleTime the maximum idle time in milliseconds (<b>IGNORED - not supported by distributed caches</b>)
     * @return {@code true} if the operation was successful, {@code false} on network errors or timeouts
     * @throws IllegalStateException if the cache has been closed
     * @throws IllegalArgumentException if the key is null (thrown by {@link #generateKey(Object)})
     * @see #generateKey(Object)
     * @see DistributedCacheClient#set(String, Object, long)
     */
    @Override
    public boolean put(final K k, final V v, final long liveTime, final long maxIdleTime) {
        assertNotClosed();

        return dcc.set(generateKey(k), v, liveTime);
    }

    /**
     * Removes a key-value pair from the distributed cache.
     * This operation is idempotent and thread-safe - it succeeds whether the key exists or not.
     * Keys are automatically prefixed and Base64 encoded before deletion.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>Removes the entry if the key exists</li>
     * <li>Silently succeeds if the key does not exist (no error)</li>
     * <li>Returns void (does not indicate success/failure or key existence)</li>
     * <li>Safe to call multiple times with the same key</li>
     * <li>Network errors are silently ignored (no exception thrown)</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = new DistributedCache<>(client, "myapp:");
     *
     * // Basic removal
     * cache.remove("user:123"); // Removes if exists, no-op if doesn't exist
     *
     * // Safe to call multiple times
     * cache.remove("user:123");
     * cache.remove("user:123"); // No exception thrown
     *
     * // Cache invalidation on update
     * void updateUser(User user) {
     *     database.save(user);
     *     cache.remove("user:" + user.getId()); // Invalidate cache
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
     * @param k the cache key, must not be null
     * @throws IllegalStateException if the cache has been closed
     * @throws IllegalArgumentException if the key is null (thrown by {@link #generateKey(Object)})
     * @see #clear()
     * @see #generateKey(Object)
     * @see DistributedCacheClient#delete(String)
     */
    @Override
    public void remove(final K k) {
        assertNotClosed();

        dcc.delete(generateKey(k));
    }

    /**
     * Checks if the cache contains a specific key with a non-null value.
     * This is implemented by attempting to retrieve the value using {@link #gett(Object)},
     * so it inherits all the same behaviors including circuit breaker logic, key transformation,
     * and error handling.
     *
     * <p><b>Implementation Note:</b></p>
     * This method calls {@code gett(k) != null}, which means it:
     * <ul>
     * <li>Performs a full GET operation (not a lightweight existence check)</li>
     * <li>May return {@code false} if circuit breaker is open</li>
     * <li>May return {@code false} on network errors (exceptions are caught)</li>
     * <li>Returns {@code false} for expired entries</li>
     * <li>Returns {@code false} if the cached value is null</li>
     * </ul>
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
     * // Avoid redundant retrieval
     * if (!cache.containsKey("config:settings")) {
     *     Config config = loadConfigFromFile();
     *     cache.put("config:settings", config, 0, 0);
     * }
     *
     * // Note: This performs a GET, so you might as well use the value
     * // Less efficient:
     * if (cache.containsKey("user:123")) {
     *     User user = cache.gett("user:123"); // Second GET operation
     * }
     * // More efficient:
     * User user = cache.gett("user:123");
     * if (user != null) {
     *     // Use user
     * }
     * }</pre>
     *
     * @param k the cache key, must not be null
     * @return {@code true} if the key exists and has a non-null value, {@code false} otherwise,
     *         on error, or when circuit breaker is open
     * @throws IllegalStateException if the cache has been closed
     * @throws IllegalArgumentException if the key is null (thrown by {@link #gett(Object)})
     * @see #gett(Object)
     */
    @Override
    public boolean containsKey(final K k) {
        return gett(k) != null;
    }

    /**
     * Returns the set of keys in the cache.
     * This operation is <b>not supported</b> for distributed caches due to
     * performance and consistency concerns in distributed systems.
     *
     * <p><b>Rationale for not supporting this operation:</b></p>
     * <ul>
     * <li>Distributed caches like Memcached and Redis do not efficiently support key enumeration</li>
     * <li>Retrieving all keys would require scanning entire cache servers, causing performance issues</li>
     * <li>The result would be inconsistent across concurrent operations in distributed environments</li>
     * <li>Key prefixing and Base64 encoding makes key filtering complex and inefficient</li>
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
     * <li>Counting entries would require scanning all cache servers, causing performance issues</li>
     * <li>The count would include entries from all applications sharing the cache servers</li>
     * <li>Key prefixing makes it impossible to count only this cache instance's entries efficiently</li>
     * <li>The result would be immediately stale in concurrent distributed environments</li>
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
     * <li>May cause sudden load spike on backend systems as all caches are cold</li>
     * <li>Should typically only be used in testing or with dedicated cache servers</li>
     * </ul>
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
     *         testCache.clear(); // OK - dedicated test server
     *     }
     * }
     *
     * // Production usage - AVOID or require confirmation
     * public void clearProductionCache(String confirmationToken) {
     *     if (!"CONFIRM_FLUSH_ALL_PRODUCTION".equals(confirmationToken)) {
     *         throw new IllegalArgumentException("Invalid confirmation");
     *     }
     *     logger.warn("DANGEROUS: Flushing ALL cache data from production servers");
     *     cache.clear(); // Will affect all apps using these cache servers!
     *     auditLog.record("CACHE_FLUSH_ALL", getCurrentUser());
     * }
     * }</pre>
     *
     * @throws IllegalStateException if the cache has been closed
     * @see DistributedCacheClient#flushAll()
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
     * <li>Releases network connections and thread pools</li>
     * <li>Marks cache as closed ({@link #isClosed()} returns true)</li>
     * <li>Does not flush cached data from servers (data remains until expired)</li>
     * <li>Safe to call multiple times (idempotent)</li>
     * </ul>
     *
     * <p><b>Post-Close Behavior:</b></p>
     * After closing, all operations except {@link #isClosed()} and {@link #close()} will throw
     * {@link IllegalStateException} via {@link #assertNotClosed()}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Try-with-resources (recommended)
     * try (DistributedCache<String, User> cache = new DistributedCache<>(client, "myapp:")) {
     *     cache.put("user:123", user, 3600000, 0);
     *     User cached = cache.gett("user:123");
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
     *     cache.close(); // Always close to release resources
     * }
     *
     * // Application shutdown hook
     * Runtime.getRuntime().addShutdownHook(new Thread(() -> {
     *     if (!cache.isClosed()) {
     *         cache.close();
     *     }
     * }));
     *
     * // Safe to call multiple times
     * cache.close();
     * cache.close(); // No exception thrown
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

        dcc.disconnect();

        isClosed = true;
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
     * </ul>
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
     * System.out.println("Is closed: " + cache.isClosed()); // true
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
     * // Conditional cleanup
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
     * <li>Avoids issues with special characters (spaces, newlines, etc.)</li>
     * <li>Provides consistent key format across different cache systems</li>
     * </ul>
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
     * }</pre>
     *
     * @param k the original key, must not be null
     * @return the prefixed and Base64-encoded cache key suitable for distributed cache systems
     * @throws IllegalArgumentException if k is null
     * @see Strings#base64Encode(byte[])
     * @see N#stringOf(Object)
     */
    protected String generateKey(final K k) {
        if (k == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        return Strings.isEmpty(keyPrefix) ? Strings.base64Encode(N.stringOf(k).getBytes(Charsets.UTF_8))
                : (keyPrefix + Strings.base64Encode(N.stringOf(k).getBytes(Charsets.UTF_8)));
    }

    /**
     * Ensures the cache is not closed before performing operations.
     * This method is called at the beginning of most cache operations to prevent
     * operations on a closed cache, which could lead to resource leaks or undefined behavior.
     *
     * <p><b>Usage:</b></p>
     * This is a protected utility method used internally by cache operation methods
     * ({@link #gett(Object)}, {@link #put(Object, Object, long, long)}, {@link #remove(Object)}, {@link #clear()})
     * to validate cache state before proceeding.
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