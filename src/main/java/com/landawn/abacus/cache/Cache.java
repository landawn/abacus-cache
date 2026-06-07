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
 * Defines the contract for caching systems, providing both synchronous
 * and asynchronous operations, configurable expiration policies, and property management.
 *
 * <p>Key features:
 * <ul>
 * <li>Synchronous and asynchronous operations</li>
 * <li>Time-to-live (TTL) and idle timeout support</li>
 * <li>Optional-based API for null-safe operations</li>
 * <li>Property bag for custom configuration</li>
 * <li>Resource management via Closeable</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
 * 
 * // Synchronous operations
 * cache.put("user:123", user);
 * Optional<User> cached = cache.get("user:123");
 * 
 * // Asynchronous operations
 * ContinuableFuture<Boolean> future = cache.asyncPut("user:456", anotherUser);
 * future.thenAcceptAsync(success -> System.out.println("Cached: " + success));
 * 
 * // Custom expiration
 * cache.put("temp:data", data, 5000, 2000);   // 5s TTL, 2s idle timeout
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
     * This is the null-safe alternative to {@link #getOrNull(Object)}, which returns the value
     * directly (or {@code null} when the key is absent or expired).
     *
     * <p><b>Behavior:</b>
     * <ul>
     * <li>Returns {@code Optional.empty()} if the key does not exist</li>
     * <li>Returns {@code Optional.empty()} if the entry has expired (TTL or idle timeout exceeded)</li>
     * <li>May update the last-access time for idle-timeout tracking (implementation-specific)</li>
     * <li>Does not throw for missing keys - returns an empty Optional instead</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b>
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
     * @param key the cache key to look up; null-handling is implementation-defined (most implementations reject null)
     * @return an Optional containing the cached value if present and not expired, or an empty Optional otherwise
     * @see #getOrNull(Object)
     * @see #asyncGet(Object)
     */
    Optional<V> get(final K key);

    /**
     * Retrieves a value from the cache directly, returning {@code null} on a miss.
     * This method is named {@code getOrNull} to coexist with {@link #get(Object)},
     * which returns {@code Optional<V>}, and avoids the overhead of allocating an
     * {@link Optional} wrapper.
     *
     * <p><b>Behavior:</b>
     * <ul>
     * <li>Returns {@code null} if the key does not exist</li>
     * <li>Returns {@code null} if the entry has expired (TTL or idle timeout exceeded)</li>
     * <li>May update the last-access time for idle-timeout tracking (implementation-specific)</li>
     * <li>Does not throw for missing keys - returns {@code null} instead</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Basic retrieval with null check
     * User user = cache.getOrNull("user:123");
     * if (user != null) {
     *     System.out.println(user.getName());
     * }
     *
     * // Cache-aside with database fallback
     * User result = cache.getOrNull("user:123");
     * if (result == null) {
     *     result = loadFromDatabase("user:123");
     *     cache.put("user:123", result);
     * }
     * }</pre>
     *
     * @param key the cache key to look up; null-handling is implementation-defined (most implementations reject null)
     * @return the cached value if present and not expired, or {@code null} if the key is not found or has expired
     * @see #get(Object)
     * @see #asyncGetOrNull(Object)
     */
    V getOrNull(final K key);

    /**
     * Stores a key-value pair in the cache using the implementation's default expiration settings.
     * If the key already exists, its value is replaced and its expiration is reset.
     *
     * <p><b>Behavior:</b>
     * <ul>
     * <li>Overwrites any existing entry with the same key</li>
     * <li>Resets TTL and idle-timeout counters for the entry</li>
     * <li>May trigger eviction of other entries if cache capacity is reached</li>
     * <li>Returns {@code false} if the operation fails (e.g., cache full and eviction not possible)</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b>
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
     * cache.put("user:123", updatedUser);   // Replaces previous value
     *
     * // Cache-aside pattern
     * User user = cache.getOrNull("user:123");
     * if (user == null) {
     *     user = loadFromDatabase("user:123");
     *     cache.put("user:123", user);
     * }
     * }</pre>
     *
     * @param key the cache key to store the value under; null-handling is implementation-defined (most implementations reject null)
     * @param value the value to cache; null-handling is implementation-defined
     * @return {@code true} if the entry was stored, {@code false} otherwise (e.g., cache full, closed, or write failure)
     * @see #put(Object, Object, long, long)
     * @see #asyncPut(Object, Object)
     */
    boolean put(final K key, final V value);

    /**
     * Stores a key-value pair in the cache with custom expiration settings.
     * If the key already exists, its value and expiration settings are replaced.
     * The entry is evicted when either the TTL elapses (measured from insertion)
     * or the idle time is exceeded (measured from last access), whichever happens first.
     *
     * <p><b>Expiration Semantics:</b>
     * <ul>
     * <li><b>liveTime (TTL):</b> Absolute expiration time from insertion. Entry is removed after this duration
     *     regardless of access patterns. The exact handling of 0 / negative values is implementation-defined
     *     (some implementations treat them as "no TTL", others reject them).</li>
     * <li><b>maxIdleTime:</b> Expiration based on last access. Entry is removed if not accessed within this duration.
     *     <b>Not supported by all implementations</b> — distributed caches (e.g. Memcached, Redis) typically ignore
     *     this parameter. Check the implementing class's documentation.</li>
     * <li>If both are set, the entry expires when either condition is met (whichever comes first).</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b>
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
     * cache.put("otp:token", token, 300000, 60000);   // 5min TTL, 1min idle
     * }</pre>
     *
     * @param key the cache key to store the value under; null-handling is implementation-defined (most implementations reject null)
     * @param value the value to cache; null-handling is implementation-defined
     * @param liveTime the time-to-live in milliseconds from insertion; handling of {@code <= 0} is implementation-defined
     * @param maxIdleTime the maximum idle time in milliseconds since last access; handling of {@code <= 0} is
     *                    implementation-defined and the parameter may be ignored entirely by distributed caches
     * @return {@code true} if the entry was stored, {@code false} otherwise (e.g., cache full, closed, or write failure)
     * @see #put(Object, Object)
     * @see #asyncPut(Object, Object, long, long)
     */
    boolean put(final K key, final V value, long liveTime, long maxIdleTime);

    /**
     * Removes an entry from the cache.
     * Idempotent: removing a key that is not present has no effect and does not throw.
     *
     * <p><b>Behavior:</b>
     * <ul>
     * <li>Removes the entry if the key exists</li>
     * <li>Does nothing if the key does not exist (no error)</li>
     * <li>Safe to call multiple times with the same key</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Basic removal
     * cache.remove("user:123");   // Removes if exists, no error if not
     *
     * // Invalidate on update failure
     * boolean updated = updateUser(userId);
     * if (!updated) {
     *     cache.remove("user:" + userId);
     * }
     * }</pre>
     *
     * @param key the cache key to remove; null-handling is implementation-defined (most implementations reject null)
     * @see #clear()
     * @see #containsKey(Object)
     * @see #asyncRemove(Object)
     */
    void remove(final K key);

    /**
     * Returns whether the cache contains an entry for the specified key.
     *
     * <p><b>Behavior:</b>
     * <ul>
     * <li>Returns {@code true} if the key has a live (non-expired) entry</li>
     * <li>Returns {@code false} if the key does not exist or has expired</li>
     * <li>Whether the call updates access time / LRU ordering / idle counters is implementation-defined</li>
     * <li>Whether expired-but-not-yet-evicted entries are visible here is implementation-defined</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * if (cache.containsKey("user:123")) {
     *     System.out.println("User is cached");
     * }
     *
     * if (!cache.containsKey("config:settings")) {
     *     cache.put("config:settings", loadConfigFromFile());
     * }
     * }</pre>
     *
     * @param key the cache key to check for; null-handling is implementation-defined (most implementations reject null)
     * @return {@code true} if an entry for the key exists and is not expired, {@code false} otherwise
     * @see #get(Object)
     * @see #asyncContainsKey(Object)
     */
    boolean containsKey(final K key);

    /**
     * Asynchronously retrieves a value from the cache wrapped in an Optional.
     * Asynchronous counterpart of {@link #get(Object)}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * cache.asyncGet("user:123")
     *      .thenAcceptAsync(opt -> opt.ifPresent(u -> System.out.println("Found: " + u.getName())));
     *
     * // Block and transform the result
     * String name = cache.asyncGet("user:123")
     *      .get()
     *      .map(User::getName)
     *      .orElse("Unknown");
     * }</pre>
     *
     * @param key the cache key to look up; null-handling is implementation-defined (most implementations reject null)
     * @return a ContinuableFuture that completes with an Optional containing the cached value,
     *         or with {@code Optional.empty()} if the key is absent or has expired. The future
     *         completes exceptionally if the underlying {@code get} call throws.
     * @see #get(Object)
     * @see #asyncGetOrNull(Object)
     */
    ContinuableFuture<Optional<V>> asyncGet(final K key);

    /**
     * Asynchronously retrieves a value from the cache, returning {@code null} on a miss.
     * Asynchronous counterpart of {@link #getOrNull(Object)}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * cache.asyncGetOrNull("user:123")
     *      .thenAcceptAsync(user -> {
     *          if (user != null) {
     *              process(user);
     *          }
     *      });
     * }</pre>
     *
     * @param key the cache key to look up; null-handling is implementation-defined (most implementations reject null)
     * @return a ContinuableFuture that completes with the cached value, or with {@code null}
     *         if the key is absent or has expired. The future completes exceptionally if the
     *         underlying {@code getOrNull} call throws.
     * @see #getOrNull(Object)
     * @see #asyncGet(Object)
     */
    ContinuableFuture<V> asyncGetOrNull(final K key);

    /**
     * Asynchronously stores a key-value pair using the implementation's default expiration settings.
     * Asynchronous counterpart of {@link #put(Object, Object)}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * cache.asyncPut("user:123", user)
     *      .thenAcceptAsync(success -> {
     *          if (success) {
     *              log("User cached successfully");
     *          }
     *      });
     * }</pre>
     *
     * @param key the cache key to store the value under; null-handling is implementation-defined (most implementations reject null)
     * @param value the value to cache; null-handling is implementation-defined
     * @return a ContinuableFuture that completes with {@code true} on success, {@code false} otherwise.
     *         The future completes exceptionally if the underlying {@code put} call throws.
     * @see #put(Object, Object)
     * @see #asyncPut(Object, Object, long, long)
     */
    ContinuableFuture<Boolean> asyncPut(final K key, final V value);

    /**
     * Asynchronously stores a key-value pair with custom expiration settings.
     * Asynchronous counterpart of {@link #put(Object, Object, long, long)}; see that method for
     * the semantics of {@code liveTime} and {@code maxIdleTime}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * // Session with 1 hour TTL, 30 minute idle timeout
     * cache.asyncPut("session:abc", session, 3600000, 1800000)
     *      .thenAcceptAsync(success -> log("Session cached: " + success));
     *
     * // Short-lived temporary data
     * cache.asyncPut("temp:data", data, 5000, 0)
     *      .thenRunAsync(() -> log("Temporary data cached for 5 seconds"));
     * }</pre>
     *
     * @param key the cache key to store the value under; null-handling is implementation-defined (most implementations reject null)
     * @param value the value to cache; null-handling is implementation-defined
     * @param liveTime the time-to-live in milliseconds from insertion; handling of {@code <= 0} is implementation-defined
     * @param maxIdleTime the maximum idle time in milliseconds since last access; handling of {@code <= 0} is
     *                    implementation-defined and the parameter may be ignored entirely by distributed caches
     * @return a ContinuableFuture that completes with {@code true} on success, {@code false} otherwise.
     *         The future completes exceptionally if the underlying {@code put} call throws.
     * @see #put(Object, Object, long, long)
     * @see #asyncPut(Object, Object)
     */
    ContinuableFuture<Boolean> asyncPut(final K key, final V value, long liveTime, long maxIdleTime);

    /**
     * Asynchronously removes an entry from the cache.
     * Asynchronous counterpart of {@link #remove(Object)}; idempotent.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * cache.asyncRemove("user:123")
     *      .thenRunAsync(() -> log("User removed from cache"));
     * }</pre>
     *
     * @param key the cache key to remove; null-handling is implementation-defined (most implementations reject null)
     * @return a ContinuableFuture that completes (with a {@code null} result) when the removal
     *         has finished. The future completes exceptionally if the underlying {@code remove}
     *         call throws.
     * @see #remove(Object)
     * @see #asyncPut(Object, Object)
     */
    ContinuableFuture<Void> asyncRemove(final K key);

    /**
     * Asynchronously checks if the cache contains a specific key.
     * Asynchronous counterpart of {@link #containsKey(Object)}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * cache.asyncContainsKey("user:123")
     *      .thenAcceptAsync(exists -> log("User exists in cache: " + exists));
     * }</pre>
     *
     * @param key the cache key to check for; null-handling is implementation-defined (most implementations reject null)
     * @return a ContinuableFuture that completes with {@code true} if a live entry for the key
     *         exists, {@code false} otherwise. The future completes exceptionally if the
     *         underlying {@code containsKey} call throws.
     * @see #containsKey(Object)
     * @see #asyncGet(Object)
     */
    ContinuableFuture<Boolean> asyncContainsKey(final K key);

    /**
     * Returns a set of the keys currently in the cache.
     * Whether the returned set is a live view or a snapshot, whether it includes
     * expired-but-not-yet-evicted entries, and the cost of producing it are all
     * implementation-defined.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * Set<String> keys = cache.keySet();
     * keys.forEach(key -> System.out.println("Cached key: " + key));
     *
     * // Most implementations return a snapshot, so mutating the returned Set does NOT
     * // mutate the cache itself - call remove() to actually evict.
     * cache.keySet().stream()
     *     .filter(key -> key.startsWith("temp:"))
     *     .forEach(cache::remove);
     * }</pre>
     *
     * @return a set of cache keys; whether expired entries are included is implementation-defined
     * @throws UnsupportedOperationException if the operation is not supported by this implementation
     *         (e.g., some distributed cache backends)
     * @see #size()
     * @see #containsKey(Object)
     */
    Set<K> keySet();

    /**
     * Returns the number of entries currently in the cache.
     * Whether the count includes expired-but-not-yet-evicted entries and whether it is
     * exact or approximate are implementation-defined.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * int count = cache.size();
     * System.out.println("Cache contains " + count + " entries");
     *
     * if (cache.size() > threshold) {
     *     cache.clear();
     * }
     * }</pre>
     *
     * @return the number of cache entries (may be an estimate depending on implementation)
     * @throws UnsupportedOperationException if the operation is not supported by this implementation
     *         (e.g., some distributed cache backends)
     * @see #keySet()
     * @see #clear()
     */
    int size();

    /**
     * Removes all entries from the cache. After this call, {@link #size()} returns 0
     * (subject to concurrent insertions). Cache configuration and {@link #getProperties() properties}
     * are not affected.
     *
     * <p>May be expensive for very large caches and for distributed cache backends.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * cache.clear();
     * System.out.println("Cache size after clear: " + cache.size());   // typically 0
     *
     * if (configChanged()) {
     *     cache.clear();
     *     log("Cache cleared due to configuration change");
     * }
     * }</pre>
     *
     * @see #remove(Object)
     * @see #size()
     */
    void clear();

    /**
     * Closes the cache and releases its resources. After closing, the cache cannot be reopened;
     * the behavior of subsequent operations is implementation-defined (typically they throw or
     * return as if empty). This method is expected to be idempotent — calling it more than once
     * has no additional effect.
     *
     * <p><b>Typical resource cleanup:</b>
     * <ul>
     * <li>Releases cached entries and associated memory</li>
     * <li>Stops background eviction threads (if any)</li>
     * <li>Closes connections to remote cache servers (for distributed caches)</li>
     * <li>Releases file handles or other system resources</li>
     * </ul>
     *
     * <p><b>Override:</b> Although {@link Closeable#close()} declares {@code throws IOException},
     * this override drops the checked exception so a cache can be used in try-with-resources
     * without forcing callers to handle IOException.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Try-with-resources (recommended)
     * try (Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000)) {
     *     cache.put("key", value);
     *     User user = cache.getOrNull("key");
     * }
     * }</pre>
     *
     * @see Closeable#close()
     * @see #isClosed()
     */
    @Override
    void close();

    /**
     * Returns whether the cache has been closed. Once {@link #close()} has been called,
     * this method returns {@code true} and the cache cannot be reopened.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * if (!cache.isClosed()) {
     *     cache.put("key", value);
     * }
     *
     * cache.close();
     * System.out.println("Is closed: " + cache.isClosed());   // true
     * }</pre>
     *
     * @return {@code true} if {@link #close()} has been called, {@code false} otherwise
     * @see #close()
     */
    boolean isClosed();

    /**
     * Returns the mutable property bag for this cache instance. Properties are independent of
     * cache entries: they live only in memory and are useful for custom configuration or
     * metadata attached to the cache. The same instance is returned for the lifetime of the cache.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * Properties<String, Object> props = cache.getProperties();
     * props.put("description", "User cache for active sessions");
     * props.put("region", "us-west-2");
     *
     * String description = (String) cache.getProperties().get("description");
     * }</pre>
     *
     * @return the properties container for this cache; never {@code null}
     * @see #getProperty(String)
     * @see #setProperty(String, Object)
     */
    Properties<String, Object> getProperties();

    /**
     * Retrieves a property value by name. Equivalent to {@code getProperties().get(propName)}.
     * The return type is an unchecked cast to {@code T}; because the cast is erased at runtime,
     * any {@link ClassCastException} surfaces at the call site, not inside this method.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * cache.setProperty("description", "User cache");
     * cache.setProperty("maxRetries", 3);
     *
     * String description = cache.getProperty("description");
     * Integer retries = cache.getProperty("maxRetries");
     *
     * String unknown = cache.getProperty("nonExistent");   // null
     * }</pre>
     *
     * @param <T> the expected type of the property value (caller responsibility)
     * @param propName the property name to look up
     * @return the property value cast to {@code T}, or {@code null} if no such property is set
     * @see #getProperties()
     * @see #setProperty(String, Object)
     */
    <T> T getProperty(String propName);

    /**
     * Sets a property value. Equivalent to {@code getProperties().put(propName, propValue)}.
     * The previous value (if any) is returned as an unchecked cast to {@code T}; any
     * {@link ClassCastException} surfaces at the call site, not inside this method.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     *
     * cache.setProperty("description", "User cache for session data");
     * cache.setProperty("maxRetries", 3);
     *
     * // Update returns the old value
     * String oldDescription = cache.setProperty("description", "Updated description");
     * // oldDescription == "User cache for session data"
     *
     * // First set returns null
     * String prev = cache.setProperty("newProperty", "value");   // prev is null
     * }</pre>
     *
     * @param <T> the expected type of the previous property value (caller responsibility)
     * @param propName the property name to set
     * @param propValue the property value to set (may be {@code null})
     * @return the previous value associated with this property name, or {@code null} if there was none
     * @see #getProperty(String)
     * @see #removeProperty(String)
     */
    <T> T setProperty(String propName, Object propValue);

    /**
     * Removes a property by name. Equivalent to {@code getProperties().remove(propName)}.
     * Idempotent: removing an absent property returns {@code null} and has no other effect.
     * The removed value is returned as an unchecked cast to {@code T}; any
     * {@link ClassCastException} surfaces at the call site, not inside this method.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
     * cache.setProperty("description", "User cache");
     *
     * String oldValue = cache.removeProperty("description");   // "User cache"
     * String notFound = cache.removeProperty("nonExistent");   // null
     * }</pre>
     *
     * @param <T> the expected type of the removed property value (caller responsibility)
     * @param propName the property name to remove
     * @return the previous value associated with this property name, or {@code null} if there was none
     * @see #getProperty(String)
     * @see #setProperty(String, Object)
     */
    <T> T removeProperty(String propName);
}