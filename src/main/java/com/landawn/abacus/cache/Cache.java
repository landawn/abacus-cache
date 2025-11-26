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

import java.io.Closeable;
import java.util.Set;

import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.Properties;
import com.landawn.abacus.util.u.Optional;

/**
 * The core interface for all cache implementations in the Abacus framework.
 * This interface defines the contract for caching systems, providing both synchronous
 * and asynchronous operations, configurable expiration policies, and property management.
 * 
 * <br><br>
 * Key features:
 * <ul>
 * <li>Synchronous and asynchronous operations</li>
 * <li>Time-to-live (TTL) and idle timeout support</li>
 * <li>Optional-based API for null-safe operations</li>
 * <li>Property bag for custom configuration</li>
 * <li>Resource management via Closeable</li>
 * </ul>
 * 
 * <br>
 * Example usage:
 * <pre>{@code
 * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
 * 
 * // Synchronous operations
 * cache.put("user:123", user);
 * Optional<User> cached = cache.get("user:123");
 * 
 * // Asynchronous operations
 * ContinuableFuture<Boolean> future = cache.asyncPut("user:456", anotherUser);
 * future.thenAccept(success -> System.out.println("Cached: " + success));
 * 
 * // Custom expiration
 * cache.put("temp:data", data, 5000, 2000); // 5s TTL, 2s idle timeout
 * }</pre>
 *
 * @param <K> the key type
 * @param <V> the value type
 * @see LocalCache
 * @see DistributedCache
 * @see CacheFactory
 */
public interface Cache<K, V> extends Closeable {

    /**
     * Default time-to-live for cache entries: 3 hours (in milliseconds).
     */
    long DEFAULT_LIVE_TIME = 3 * 60 * 60 * 1000L;

    /**
     * Default maximum idle time for cache entries: 30 minutes (in milliseconds).
     */
    long DEFAULT_MAX_IDLE_TIME = 30 * 60 * 1000L;

    /**
     * Retrieves a value from the cache wrapped in an Optional.
     * This method provides a null-safe way to handle cache misses and follows functional programming patterns.
     * The operation is thread-safe and does not block other cache operations.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>Returns {@code Optional.empty()} if the key does not exist</li>
     * <li>Returns {@code Optional.empty()} if the entry has expired (TTL or idle timeout exceeded)</li>
     * <li>May update the last access time for idle timeout tracking (implementation-specific)</li>
     * <li>Does not throw exceptions for missing keys - returns empty Optional instead</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Basic retrieval with functional approach
     * Optional<User> user = cache.get("user:123");
     * user.ifPresent(u -> System.out.println("Found: " + u.getName()));
     *
     * // With default value
     * User result = cache.get("user:123").orElse(defaultUser);
     *
     * // Chaining operations
     * String userName = cache.get("user:123")
     *     .map(User::getName)
     *     .orElse("Unknown");
     * }</pre>
     *
     * @param k the cache key to look up (must not be null for most implementations)
     * @return an Optional containing the cached value if present and not expired, or an empty Optional otherwise
     * @throws IllegalStateException if the cache has been closed
     * @see #gett(Object)
     * @see #asyncGet(Object)
     */
    Optional<V> get(final K k);

    /**
     * Retrieves a value from the cache directly without wrapping in Optional.
     * This method returns null for cache misses rather than using Optional, providing a more
     * traditional API for scenarios where Optional overhead is not desired or when working
     * with legacy code. The operation is thread-safe and does not block other cache operations.
     *
     * <p><b>Note:</b> The method name uses double 't' (gett) to distinguish it from {@link #get(Object)},
     * which returns {@code Optional<V>}. This naming convention allows both APIs to coexist
     * while clearly indicating their different return types.</p>
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>Returns {@code null} if the key does not exist in the cache</li>
     * <li>Returns {@code null} if the entry has expired (TTL or idle timeout exceeded)</li>
     * <li>May update the last access time for idle timeout tracking (implementation-specific)</li>
     * <li>Does not throw exceptions for missing keys - returns null instead</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Basic retrieval with null check
     * User user = cache.gett("user:123");
     * if (user != null) {
     *     System.out.println(user.getName());
     * }
     *
     * // With default value fallback
     * User result = cache.gett("user:123");
     * if (result == null) {
     *     result = loadFromDatabase("user:123");
     *     cache.put("user:123", result);
     * }
     * }</pre>
     *
     * @param k the cache key to look up (must not be null for most implementations)
     * @return the cached value if present and not expired, or null if the key is not found or has expired
     * @throws IllegalStateException if the cache has been closed
     * @see #get(Object)
     * @see #asyncGett(Object)
     */
    V gett(final K k);

    /**
     * Stores a key-value pair in the cache using default expiration settings.
     * The default TTL ({@link #DEFAULT_LIVE_TIME}) and idle time ({@link #DEFAULT_MAX_IDLE_TIME})
     * are used unless overridden by the implementation. If the key already exists, its value will
     * be updated and its expiration time will be reset. The operation is thread-safe and atomic.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>Overwrites existing entries with the same key</li>
     * <li>Resets TTL and idle timeout for existing entries</li>
     * <li>May trigger eviction of old entries if cache capacity is reached</li>
     * <li>Returns false if the operation fails (e.g., cache full and eviction not possible)</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Basic put operation
     * boolean success = cache.put("user:123", user);
     * if (success) {
     *     System.out.println("User cached successfully");
     * }
     *
     * // Update existing entry
     * cache.put("user:123", updatedUser); // Replaces previous value
     *
     * // Cache-aside pattern
     * User user = cache.gett("user:123");
     * if (user == null) {
     *     user = loadFromDatabase("user:123");
     *     cache.put("user:123", user);
     * }
     * }</pre>
     *
     * @param k the cache key to store the value under (must not be null for most implementations)
     * @param v the value to cache (may be null depending on implementation, check implementation docs)
     * @return true if the operation was successful, false otherwise (e.g., cache full, closed, or write failure)
     * @throws IllegalStateException if the cache has been closed
     * @see #put(Object, Object, long, long)
     * @see #asyncPut(Object, Object)
     */
    boolean put(final K k, final V v);

    /**
     * Stores a key-value pair in the cache with custom expiration settings.
     * If the key already exists, its value and expiration settings will be replaced.
     * The entry will be evicted when either the TTL expires (measured from the time of insertion)
     * or the idle time is exceeded (measured from the last access). The operation is thread-safe and atomic.
     *
     * <p><b>Expiration Semantics:</b></p>
     * <ul>
     * <li><b>liveTime (TTL):</b> Absolute expiration time from insertion. Entry is removed after this duration
     *     regardless of access patterns. Use 0 or negative to disable TTL expiration.</li>
     * <li><b>maxIdleTime:</b> Relative expiration based on last access. Entry is removed if not accessed within
     *     this duration. Use 0 or negative to disable idle timeout. <b>Note:</b> Not supported by all
     *     implementations, particularly distributed caches - check implementation documentation.</li>
     * <li>If both are set, the entry expires when either condition is met (whichever comes first).</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Session with 1 hour TTL, 30 minute idle timeout
     * cache.put("session:abc", session, 3600000, 1800000);
     *
     * // Temporary data with 5 second TTL, no idle timeout
     * cache.put("temp:data", data, 5000, 0);
     *
     * // Short-lived data that expires quickly if unused
     * cache.put("otp:token", token, 300000, 60000); // 5min TTL, 1min idle
     *
     * // No expiration (use with caution to avoid memory leaks)
     * cache.put("permanent:config", config, 0, 0);
     * }</pre>
     *
     * @param k the cache key to store the value under (must not be null for most implementations)
     * @param v the value to cache (may be null depending on implementation, check implementation docs)
     * @param liveTime the time-to-live in milliseconds from insertion (0 or negative for no TTL expiration)
     * @param maxIdleTime the maximum idle time in milliseconds since last access (0 or negative for no idle timeout).
     *                    <b>Note:</b> Not supported by all implementations - check implementation documentation.
     * @return true if the operation was successful, false otherwise (e.g., cache full, closed, or write failure)
     * @throws IllegalStateException if the cache has been closed
     * @see #put(Object, Object)
     * @see #asyncPut(Object, Object, long, long)
     */
    boolean put(final K k, final V v, long liveTime, long maxIdleTime);

    /**
     * Removes an entry from the cache.
     * This operation is idempotent and thread-safe - it succeeds whether the key exists or not.
     * If the key exists, the entry is removed immediately; if not, the operation has no effect and
     * returns normally. The operation is atomic and does not block other cache operations.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>Removes the entry if the key exists</li>
     * <li>Does nothing if the key does not exist (no error)</li>
     * <li>Safe to call multiple times with the same key</li>
     * <li>Does not throw exceptions for non-existent keys</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Basic removal
     * cache.remove("user:123"); // Removes if exists, no error if not
     *
     * // Safe to call multiple times
     * cache.remove("user:123");
     * cache.remove("user:123"); // No exception thrown
     *
     * // Remove on update failure
     * boolean updated = updateUser(userId);
     * if (!updated) {
     *     cache.remove("user:" + userId); // Invalidate stale cache entry
     * }
     *
     * // Batch removal
     * for (String key : keysToRemove) {
     *     cache.remove(key);
     * }
     * }</pre>
     *
     * @param k the cache key to remove (must not be null for most implementations)
     * @throws IllegalStateException if the cache has been closed
     * @see #clear()
     * @see #asyncRemove(Object)
     */
    void remove(final K k);

    /**
     * Checks if the cache contains a specific key.
     * This operation is thread-safe and does not block other cache operations. For most implementations,
     * this method checks for the presence of the key but does not affect the access time or LRU ordering,
     * and it does not reset idle timeout counters. However, expired entries may or may not be considered
     * present depending on the implementation - check implementation documentation for specifics.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>Returns true if the key exists and is not expired</li>
     * <li>Returns false if the key does not exist or has expired</li>
     * <li>Does not update access time or reset idle timeout (implementation-specific)</li>
     * <li>Does not affect LRU eviction ordering (for LRU-based caches)</li>
     * <li>May be faster than {@link #get(Object)} for existence checks</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Basic existence check
     * if (cache.containsKey("user:123")) {
     *     System.out.println("User is cached");
     * } else {
     *     System.out.println("User not found in cache");
     * }
     *
     * // Avoid unnecessary retrieval
     * if (!cache.containsKey("config:settings")) {
     *     Config config = loadConfigFromFile();
     *     cache.put("config:settings", config);
     * }
     *
     * // Check before removal
     * if (cache.containsKey(key)) {
     *     cache.remove(key);
     *     log("Removed cached entry for: " + key);
     * }
     * }</pre>
     *
     * @param k the cache key to check for (must not be null for most implementations)
     * @return true if the key exists in the cache and is not expired, false otherwise
     * @throws IllegalStateException if the cache has been closed
     * @see #get(Object)
     * @see #asyncContainsKey(Object)
     */
    boolean containsKey(final K k);

    /**
     * Asynchronously retrieves a value from the cache wrapped in an Optional.
     * The operation is executed on a background thread from the shared async executor pool,
     * allowing non-blocking cache access. This is the asynchronous version of {@link #get(Object)}.
     * The returned ContinuableFuture provides functional composition capabilities for chaining
     * dependent operations.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>Returns immediately with a ContinuableFuture that will complete with the result</li>
     * <li>The actual cache operation is executed asynchronously on a background thread</li>
     * <li>Completes with {@code Optional.empty()} if the key does not exist or has expired</li>
     * <li>Thread-safe and non-blocking</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Basic async retrieval
     * cache.asyncGet("user:123")
     *      .thenAccept(opt -> opt.ifPresent(u -> System.out.println("Found: " + u.getName())));
     *
     * // Chain multiple operations
     * cache.asyncGet("user:123")
     *      .thenApply(opt -> opt.map(User::getName).orElse("Unknown"))
     *      .thenAccept(name -> log("User name: " + name));
     *
     * // Exception handling
     * cache.asyncGet("user:123")
     *      .thenAccept(opt -> process(opt))
     *      .exceptionally(e -> {
     *          log("Error retrieving from cache", e);
     *          return null;
     *      });
     * }</pre>
     *
     * @param k the cache key to look up (must not be null for most implementations)
     * @return a ContinuableFuture that will complete with an Optional containing the cached value if present,
     *         or an empty Optional if the key is not found or has expired. The future may complete exceptionally
     *         if an error occurs during the operation (e.g., NullPointerException for null keys,
     *         IllegalStateException if the cache is closed).
     * @see #get(Object)
     * @see #asyncGett(Object)
     */
    ContinuableFuture<Optional<V>> asyncGet(final K k);

    /**
     * Asynchronously retrieves a value from the cache directly without wrapping in Optional.
     * The operation is executed on a background thread from the shared async executor pool,
     * allowing non-blocking cache access. This is the asynchronous version of {@link #gett(Object)}.
     * The returned ContinuableFuture provides functional composition capabilities for chaining
     * dependent operations.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>Returns immediately with a ContinuableFuture that will complete with the result</li>
     * <li>The actual cache operation is executed asynchronously on a background thread</li>
     * <li>Completes with null if the key does not exist or has expired</li>
     * <li>Thread-safe and non-blocking</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Basic async retrieval with null check
     * cache.asyncGett("user:123")
     *      .thenAccept(user -> {
     *          if (user != null) {
     *              process(user);
     *          }
     *      });
     *
     * // Chain multiple async operations
     * cache.asyncGett("user:123")
     *      .thenCompose(user -> user != null ? updateUser(user) : loadFromDatabase("user:123"))
     *      .thenAccept(user -> cache.put("user:123", user));
     *
     * // Exception handling
     * cache.asyncGett("user:123")
     *      .exceptionally(e -> {
     *          log("Error retrieving from cache", e);
     *          return null;
     *      });
     * }</pre>
     *
     * @param k the cache key to look up (must not be null for most implementations)
     * @return a ContinuableFuture that will complete with the cached value if present,
     *         or null if the key is not found or has expired. The future may complete exceptionally
     *         if an error occurs during the operation (e.g., NullPointerException for null keys,
     *         IllegalStateException if the cache is closed).
     * @see #gett(Object)
     * @see #asyncGet(Object)
     */
    ContinuableFuture<V> asyncGett(final K k);

    /**
     * Asynchronously stores a key-value pair using default expiration settings.
     * The operation is executed on a background thread from the shared async executor pool,
     * allowing non-blocking cache writes. This is the asynchronous version of {@link #put(Object, Object)}.
     * The returned ContinuableFuture provides functional composition capabilities for chaining
     * dependent operations.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>Returns immediately with a ContinuableFuture that will complete with the result</li>
     * <li>The actual cache operation is executed asynchronously on a background thread</li>
     * <li>Overwrites existing entries with the same key</li>
     * <li>Resets TTL and idle timeout for existing entries</li>
     * <li>Thread-safe and non-blocking</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Basic async put
     * cache.asyncPut("user:123", user)
     *      .thenAccept(success -> {
     *          if (success) {
     *              log("User cached successfully");
     *          }
     *      });
     *
     * // Chain multiple operations
     * cache.asyncPut("user:123", user)
     *      .thenCompose(success -> success ? notifySubscribers(user) : handleFailure())
     *      .thenRun(() -> log("Operation complete"));
     *
     * // Exception handling
     * cache.asyncPut("user:123", user)
     *      .exceptionally(e -> {
     *          log("Error caching user", e);
     *          return false;
     *      });
     * }</pre>
     *
     * @param k the cache key to store the value under (must not be null for most implementations)
     * @param v the value to cache (may be null depending on implementation, check implementation docs)
     * @return a ContinuableFuture that will complete with true if the operation was successful, false otherwise
     *         (e.g., cache full, closed, or write failure). The future may complete exceptionally if an error
     *         occurs during the operation (e.g., NullPointerException for null keys, IllegalStateException
     *         if the cache is closed).
     * @see #put(Object, Object)
     * @see #asyncPut(Object, Object, long, long)
     */
    ContinuableFuture<Boolean> asyncPut(final K k, final V v);

    /**
     * Asynchronously stores a key-value pair with custom expiration settings.
     * The operation is executed on a background thread from the shared async executor pool,
     * allowing non-blocking cache writes with custom TTL and idle timeout. This is the asynchronous
     * version of {@link #put(Object, Object, long, long)}. The returned ContinuableFuture provides
     * functional composition capabilities for chaining dependent operations.
     *
     * <p><b>Expiration Semantics:</b></p>
     * <ul>
     * <li><b>liveTime (TTL):</b> Absolute expiration time from insertion. Entry is removed after this duration
     *     regardless of access patterns. Use 0 or negative to disable TTL expiration.</li>
     * <li><b>maxIdleTime:</b> Relative expiration based on last access. Entry is removed if not accessed within
     *     this duration. Use 0 or negative to disable idle timeout. <b>Note:</b> Not supported by all
     *     implementations, particularly distributed caches - check implementation documentation.</li>
     * <li>If both are set, the entry expires when either condition is met (whichever comes first).</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Session with 1 hour TTL, 30 minute idle timeout
     * cache.asyncPut("session:abc", session, 3600000, 1800000)
     *      .thenAccept(success -> {
     *          if (success) {
     *              log("Session cached with 1h TTL, 30min idle");
     *          }
     *      });
     *
     * // Short-lived temporary data
     * cache.asyncPut("temp:data", data, 5000, 0)
     *      .thenRun(() -> log("Temporary data cached for 5 seconds"));
     *
     * // Exception handling
     * cache.asyncPut("otp:token", token, 300000, 60000)
     *      .exceptionally(e -> {
     *          log("Error caching token", e);
     *          return false;
     *      });
     * }</pre>
     *
     * @param k the cache key to store the value under (must not be null for most implementations)
     * @param v the value to cache (may be null depending on implementation, check implementation docs)
     * @param liveTime the time-to-live in milliseconds from insertion (0 or negative for no TTL expiration)
     * @param maxIdleTime the maximum idle time in milliseconds since last access (0 or negative for no idle timeout).
     *                    <b>Note:</b> Not supported by all implementations - check implementation documentation.
     * @return a ContinuableFuture that will complete with true if the operation was successful, false otherwise
     *         (e.g., cache full, closed, or write failure). The future may complete exceptionally if an error
     *         occurs during the operation (e.g., NullPointerException for null keys, IllegalStateException
     *         if the cache is closed).
     * @see #put(Object, Object, long, long)
     * @see #asyncPut(Object, Object)
     */
    ContinuableFuture<Boolean> asyncPut(final K k, final V v, long liveTime, long maxIdleTime);

    /**
     * Asynchronously removes an entry from the cache.
     * The operation is executed on a background thread from the shared async executor pool,
     * allowing non-blocking cache removals. This is the asynchronous version of {@link #remove(Object)}.
     * The operation is idempotent - it succeeds whether the key exists or not.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>Returns immediately with a ContinuableFuture that will complete when the operation finishes</li>
     * <li>The actual cache operation is executed asynchronously on a background thread</li>
     * <li>Removes the entry if the key exists</li>
     * <li>Does nothing if the key does not exist (no error)</li>
     * <li>Thread-safe and non-blocking</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Basic async removal
     * cache.asyncRemove("user:123")
     *      .thenRun(() -> log("User removed from cache"));
     *
     * // Chain with other operations
     * cache.asyncRemove("user:123")
     *      .thenCompose(v -> notifySubscribers("user:123"))
     *      .thenRun(() -> log("Cache invalidation complete"));
     *
     * // Exception handling
     * cache.asyncRemove("user:123")
     *      .exceptionally(e -> {
     *          log("Error removing from cache", e);
     *          return null;
     *      });
     * }</pre>
     *
     * @param k the cache key to remove (must not be null for most implementations)
     * @return a ContinuableFuture that completes when the operation finishes. The future may complete
     *         exceptionally if an error occurs during the operation (e.g., NullPointerException for null keys,
     *         IllegalStateException if the cache is closed).
     * @see #remove(Object)
     * @see #asyncPut(Object, Object)
     */
    ContinuableFuture<Void> asyncRemove(final K k);

    /**
     * Asynchronously checks if the cache contains a specific key.
     * The operation is executed on a background thread from the shared async executor pool,
     * allowing non-blocking existence checks. This is the asynchronous version of {@link #containsKey(Object)}.
     * For most implementations, this method checks for the presence of the key but does not affect
     * the access time or reset idle timeout counters.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>Returns immediately with a ContinuableFuture that will complete with the result</li>
     * <li>The actual cache operation is executed asynchronously on a background thread</li>
     * <li>Completes with true if the key exists and is not expired</li>
     * <li>Completes with false if the key does not exist or has expired</li>
     * <li>Thread-safe and non-blocking</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Basic async existence check
     * cache.asyncContainsKey("user:123")
     *      .thenAccept(exists -> log("User exists in cache: " + exists));
     *
     * // Conditional operations based on existence
     * cache.asyncContainsKey("config:settings")
     *      .thenCompose(exists -> exists ? cache.asyncGett("config:settings") : loadConfigAsync())
     *      .thenAccept(config -> processConfig(config));
     *
     * // Exception handling
     * cache.asyncContainsKey("user:123")
     *      .exceptionally(e -> {
     *          log("Error checking cache", e);
     *          return false;
     *      });
     * }</pre>
     *
     * @param k the cache key to check for (must not be null for most implementations)
     * @return a ContinuableFuture that will complete with true if the key exists in the cache and is not expired,
     *         false otherwise. The future may complete exceptionally if an error occurs during the operation
     *         (e.g., NullPointerException for null keys, IllegalStateException if the cache is closed).
     * @see #containsKey(Object)
     * @see #asyncGet(Object)
     */
    ContinuableFuture<Boolean> asyncContainsKey(final K k);

    /**
     * Returns a set of all keys currently in the cache.
     * The returned set may be a snapshot (immutable) or a live view (reflecting cache changes)
     * depending on the implementation. The set may or may not include expired entries that have
     * not yet been evicted. Some cache implementations may throw UnsupportedOperationException.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>May return a snapshot (immutable) or live view (implementation-specific)</li>
     * <li>May or may not include expired but not yet evicted entries</li>
     * <li>The operation may be expensive for large caches</li>
     * <li>Thread-safety of the returned set depends on implementation</li>
     * <li>Some implementations (e.g., certain distributed caches) may not support this operation</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Iterate over all keys
     * Set<String> keys = cache.keySet();
     * keys.forEach(key -> System.out.println("Cached key: " + key));
     *
     * // Check if any keys exist
     * if (!cache.keySet().isEmpty()) {
     *     System.out.println("Cache has entries");
     * }
     *
     * // Bulk operations (if set is modifiable and live)
     * Set<String> keys = cache.keySet();
     * keys.removeIf(key -> key.startsWith("temp:"));
     *
     * // Pattern matching
     * cache.keySet().stream()
     *     .filter(key -> key.startsWith("user:"))
     *     .forEach(key -> processUser(cache.gett(key)));
     * }</pre>
     *
     * @return a set of all cache keys (may or may not include expired entries depending on implementation)
     * @throws UnsupportedOperationException if the operation is not supported by this cache implementation
     * @throws IllegalStateException if the cache has been closed (implementation-specific)
     * @see #size()
     * @see #containsKey(Object)
     */
    Set<K> keySet();

    /**
     * Returns the number of entries currently in the cache.
     * The count may or may not include expired entries that have not yet been evicted, depending
     * on the implementation. Some implementations may return an estimate rather than an exact count
     * for performance reasons, and some cache implementations may throw UnsupportedOperationException.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>May or may not include expired but not yet evicted entries</li>
     * <li>May return an estimate rather than exact count (particularly for distributed caches)</li>
     * <li>The operation is typically fast but may be expensive for certain implementations</li>
     * <li>Thread-safe but the count may change immediately after being retrieved</li>
     * <li>Some implementations (e.g., certain distributed caches) may not support this operation</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Basic size check
     * int count = cache.size();
     * System.out.println("Cache contains " + count + " entries");
     *
     * // Check if cache is empty
     * if (cache.size() == 0) {
     *     System.out.println("Cache is empty");
     * }
     *
     * // Monitor cache usage
     * int size = cache.size();
     * if (size > 900) {
     *     log("Cache nearing capacity: " + size + "/1000");
     * }
     *
     * // Conditional clearing
     * if (cache.size() > threshold) {
     *     cache.clear();
     * }
     * }</pre>
     *
     * @return the number of cache entries (may be an estimate depending on implementation)
     * @throws UnsupportedOperationException if the operation is not supported by this cache implementation
     * @throws IllegalStateException if the cache has been closed (implementation-specific)
     * @see #keySet()
     * @see #clear()
     */
    int size();

    /**
     * Removes all entries from the cache.
     * After this operation, the cache will be empty ({@link #size()} returns 0).
     * This operation is thread-safe and atomic, though it may be expensive for distributed caches
     * or caches with very large numbers of entries. The operation may block briefly while clearing.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>Removes all entries including expired and non-expired</li>
     * <li>After completion, {@link #size()} returns 0 and {@link #keySet()} returns an empty set</li>
     * <li>Thread-safe and atomic operation</li>
     * <li>May be expensive for large caches, particularly distributed caches</li>
     * <li>Does not affect cache configuration or properties</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Basic clear operation
     * cache.clear(); // Removes all cached entries
     *
     * // Clear and verify
     * cache.clear();
     * System.out.println("Cache size after clear: " + cache.size()); // Should be 0
     *
     * // Periodic cache refresh
     * cache.clear();
     * reloadAllDataIntoCache();
     *
     * // Clear on configuration change
     * if (configChanged()) {
     *     cache.clear();
     *     log("Cache cleared due to configuration change");
     * }
     * }</pre>
     *
     * @throws IllegalStateException if the cache has been closed (implementation-specific)
     * @see #remove(Object)
     * @see #size()
     */
    void clear();

    /**
     * Closes the cache and releases all associated resources.
     * After closing, the cache cannot be used - subsequent operations may throw exceptions
     * (typically {@link IllegalStateException}) or have undefined behavior depending on the implementation.
     * This method is idempotent and thread-safe - multiple calls have no additional effect and will not
     * throw exceptions.
     *
     * <p><b>Resource Cleanup:</b></p>
     * <ul>
     * <li>Releases all cached entries and associated memory</li>
     * <li>Stops background eviction threads (if any)</li>
     * <li>Closes connections to remote cache servers (for distributed caches)</li>
     * <li>Releases file handles or other system resources</li>
     * <li>Does not throw exceptions even if already closed</li>
     * </ul>
     *
     * <p><b>Important:</b> It is recommended to always close the cache when it's no longer needed to prevent
     * resource leaks. Use try-with-resources or try-finally blocks to ensure proper cleanup.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Try-with-resources (recommended approach)
     * try (Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000)) {
     *     cache.put("key", value);
     *     User user = cache.gett("key");
     *     // Cache is automatically closed when exiting the try block
     * }
     *
     * // Try-finally (when try-with-resources is not suitable)
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * try {
     *     cache.put("key", value);
     *     processData(cache);
     * } finally {
     *     cache.close(); // Always close to release resources
     * }
     *
     * // Safe to call multiple times
     * cache.close();
     * cache.close(); // No exception thrown
     * }</pre>
     *
     * @see Closeable#close()
     * @see #isClosed()
     */
    @Override
    void close();

    /**
     * Checks if the cache has been closed.
     * Once a cache is closed via {@link #close()}, it cannot be reopened and should not be used.
     * This method is thread-safe and returns immediately without blocking.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>Returns true if {@link #close()} has been called, false otherwise</li>
     * <li>Thread-safe and can be called from multiple threads</li>
     * <li>Does not throw exceptions even if the cache is in an error state</li>
     * <li>Once true, will always remain true (caches cannot be reopened)</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Check before operations
     * if (!cache.isClosed()) {
     *     cache.put("key", value);
     * }
     *
     * // Verify state after closing
     * cache.close();
     * System.out.println("Is closed: " + cache.isClosed()); // true
     *
     * // Safe operations with closed check
     * public void cacheUser(User user) {
     *     if (cache.isClosed()) {
     *         log("Cache is closed, skipping caching");
     *         return;
     *     }
     *     cache.put("user:" + user.getId(), user);
     * }
     *
     * // Guard against closed cache in long-running process
     * while (!cache.isClosed() && hasMoreData()) {
     *     Data data = fetchNextData();
     *     cache.put(data.getKey(), data);
     * }
     * }</pre>
     *
     * @return true if {@link #close()} has been called, false otherwise
     * @see #close()
     */
    boolean isClosed();

    /**
     * Returns the properties bag for this cache instance.
     * Properties can be used to store custom configuration, metadata, or application-specific data
     * associated with this cache. The returned Properties object is mutable and changes are reflected
     * in the cache. The properties are independent of cache entries and persist until explicitly removed
     * or the cache is closed.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>Returns the same Properties instance for the lifetime of the cache</li>
     * <li>Properties are mutable and changes affect the cache</li>
     * <li>Properties are not persisted and are lost when the cache is closed</li>
     * <li>Thread-safe access depends on the Properties implementation</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Set properties directly
     * Properties<String, Object> props = cache.getProperties();
     * props.put("description", "User cache for active sessions");
     * props.put("region", "us-west-2");
     * props.put("maxRetries", 3);
     *
     * // Later retrieve the properties
     * String description = (String) cache.getProperties().get("description");
     * System.out.println("Cache: " + description);
     *
     * // Check if property exists
     * if (cache.getProperties().containsKey("region")) {
     *     String region = (String) cache.getProperties().get("region");
     *     log("Operating in region: " + region);
     * }
     * }</pre>
     *
     * @return the properties container for this cache, never null
     * @throws IllegalStateException if the cache has been closed (implementation-specific)
     * @see #getProperty(String)
     * @see #setProperty(String, Object)
     */
    Properties<String, Object> getProperties();

    /**
     * Retrieves a property value by name.
     * This is a convenience method equivalent to calling {@code getProperties().get(propName)}.
     * Returns null if the property doesn't exist. The method performs an unchecked cast to the
     * expected type T, so the caller is responsible for ensuring the correct type is used.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>Returns the property value if it exists</li>
     * <li>Returns null if the property doesn't exist</li>
     * <li>Performs unchecked cast to type T (caller must ensure correct type)</li>
     * <li>Thread-safe access depends on the Properties implementation</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * cache.setProperty("description", "User cache");
     * cache.setProperty("maxRetries", 3);
     * cache.setProperty("enableMetrics", true);
     *
     * // Basic retrieval with correct types
     * String description = cache.getProperty("description");
     * Integer retries = cache.getProperty("maxRetries");
     * Boolean metrics = cache.getProperty("enableMetrics");
     *
     * // Returns null for non-existent properties
     * String unknown = cache.getProperty("nonExistent"); // null
     *
     * // Use with null check
     * Integer maxRetries = cache.getProperty("maxRetries");
     * if (maxRetries != null && maxRetries > 0) {
     *     setRetryCount(maxRetries);
     * }
     * }</pre>
     *
     * @param <T> the type of the property value to be returned (caller should ensure correct type)
     * @param propName the property name to look up (must not be null)
     * @return the property value cast to type T, or null if not found
     * @throws ClassCastException if the property exists but cannot be cast to type T
     * @throws IllegalStateException if the cache has been closed (implementation-specific)
     * @see #getProperties()
     * @see #setProperty(String, Object)
     */
    <T> T getProperty(String propName);

    /**
     * Sets a property value.
     * This is a convenience method equivalent to calling {@code getProperties().put(propName, propValue)}.
     * Properties can be used for custom configuration, metadata, or application-specific data.
     * The method returns the previous value if one existed, or null otherwise.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>Sets the property value, overwriting any existing value</li>
     * <li>Returns the previous value if one existed, null otherwise</li>
     * <li>Accepts any object type as the property value</li>
     * <li>Performs unchecked cast on the previous value (caller must ensure correct type)</li>
     * <li>Thread-safe access depends on the Properties implementation</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Set various property types
     * cache.setProperty("description", "User cache for session data");
     * cache.setProperty("maxRetries", 3);
     * cache.setProperty("enableMetrics", true);
     * cache.setProperty("lastRefresh", System.currentTimeMillis());
     *
     * // Update an existing property and get the old value
     * String oldDescription = cache.setProperty("description", "Updated description");
     * System.out.println("Old: " + oldDescription); // "User cache for session data"
     *
     * // First set returns null
     * String prev = cache.setProperty("newProperty", "value"); // prev is null
     *
     * // Use returned value to check if update was needed
     * Integer oldRetries = cache.setProperty("maxRetries", 5);
     * if (oldRetries == null || !oldRetries.equals(5)) {
     *     log("Retry count updated from " + oldRetries + " to 5");
     * }
     * }</pre>
     *
     * @param <T> the type of the previous property value to be returned (caller should ensure correct type)
     * @param propName the property name to set (must not be null)
     * @param propValue the property value to set (can be any object type, including null)
     * @return the previous value associated with the property, or null if there was no previous value
     * @throws ClassCastException if a previous value exists but cannot be cast to type T
     * @throws IllegalStateException if the cache has been closed (implementation-specific)
     * @see #getProperty(String)
     * @see #removeProperty(String)
     */
    <T> T setProperty(String propName, Object propValue);

    /**
     * Removes a property from the cache.
     * This is a convenience method equivalent to calling {@code getProperties().remove(propName)}.
     * This operation is idempotent - it succeeds whether the property exists or not, returning
     * the previous value if one existed, or null otherwise.
     *
     * <p><b>Behavior:</b></p>
     * <ul>
     * <li>Removes the property if it exists</li>
     * <li>Returns the removed value if the property existed, null otherwise</li>
     * <li>Does nothing if the property doesn't exist (no error)</li>
     * <li>Performs unchecked cast on the removed value (caller must ensure correct type)</li>
     * <li>Thread-safe access depends on the Properties implementation</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * cache.setProperty("description", "User cache");
     * cache.setProperty("tempFlag", true);
     *
     * // Remove and get the old value
     * String oldValue = cache.removeProperty("description");
     * System.out.println("Removed: " + oldValue); // "User cache"
     *
     * // Removing non-existent property returns null
     * String notFound = cache.removeProperty("nonExistent"); // null
     *
     * // Safe to call multiple times
     * cache.removeProperty("tempFlag");
     * cache.removeProperty("tempFlag"); // No error, returns null
     *
     * // Use returned value to check if removal was necessary
     * Boolean wasSet = cache.removeProperty("enableMetrics");
     * if (wasSet != null) {
     *     log("Metrics were " + (wasSet ? "enabled" : "disabled") + ", now removed");
     * }
     * }</pre>
     *
     * @param <T> the type of the property value to be returned (caller should ensure correct type)
     * @param propName the property name to remove (must not be null)
     * @return the removed value, or null if the property didn't exist
     * @throws ClassCastException if the property exists but cannot be cast to type T
     * @throws IllegalStateException if the cache has been closed (implementation-specific)
     * @see #getProperty(String)
     * @see #setProperty(String, Object)
     */
    <T> T removeProperty(String propName);
}