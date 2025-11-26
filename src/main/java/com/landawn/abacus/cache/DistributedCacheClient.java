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

import java.util.Collection;
import java.util.Map;

/**
 * Interface for distributed cache client implementations.
 * This interface defines the contract for distributed caching systems like Memcached and Redis,
 * providing basic cache operations and atomic counter functionality. Implementations handle
 * network communication, serialization, and distributed data management.
 * 
 * <br><br>
 * Key features:
 * <ul>
 * <li>Basic CRUD operations (get, set, delete)</li>
 * <li>Bulk operations for efficiency</li>
 * <li>Atomic increment/decrement operations</li>
 * <li>Time-based expiration support</li>
 * </ul>
 * 
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
 * User user = new User("John", "john@example.com");
 * client.set("user:123", user, 3600000); // Cache for 1 hour
 * User cached = client.get("user:123");
 * long visits = client.incr("visits:123");
 * }</pre>
 *
 * @param <T> the type of objects to be cached
 * @see SpyMemcached
 * @see JRedis
 * @see DistributedCache
 */
public interface DistributedCacheClient<T> {

    /**
     * Default timeout for network operations in milliseconds (1000ms).
     */
    long DEFAULT_TIMEOUT = 1000;

    /**
     * Constant identifier for Memcached client type.
     */
    String MEMCACHED = "Memcached";

    /**
     * Constant identifier for Redis client type.
     */
    String REDIS = "Redis";

    /**
     * Returns the server URL(s) this client is connected to.
     * For multiple servers, the format is implementation-specific
     * (e.g., comma-separated for some implementations).
     *
     * <p>This method is thread-safe and can be called concurrently from multiple threads.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * String url = client.serverUrl();
     * System.out.println("Connected to: " + url);
     * }</pre>
     *
     * @return the server URL(s), never {@code null}
     */
    String serverUrl();

    /**
     * Retrieves a value from the cache by its key.
     *
     * <p>This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = client.get("user:123");
     * if (user != null) {
     *     System.out.println("Found user: " + user.getName());
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @return the cached value, or {@code null} if not found, expired, or evicted
     * @throws IllegalArgumentException if {@code key} is {@code null}
     */
    T get(String key);

    /**
     * Retrieves multiple values from the cache in a single operation.
     * This is more efficient than multiple individual get operations.
     * Keys not found in the cache will not be present in the returned map.
     *
     * <p>This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Map<String, User> users = client.getBulk("user:123", "user:456", "user:789");
     * users.forEach((key, user) -> System.out.println(key + ": " + user.getName()));
     * }</pre>
     *
     * @param keys the cache keys to retrieve, must not be {@code null} or contain {@code null} elements
     * @return a map of found key-value pairs, never {@code null} (may be empty if no keys are found)
     * @throws IllegalArgumentException if {@code keys} is {@code null} or contains {@code null} elements
     */
    Map<String, T> getBulk(String... keys);

    /**
     * Retrieves multiple values from the cache in a single operation.
     * This is more efficient than multiple individual get operations.
     * Keys not found in the cache will not be present in the returned map.
     *
     * <p>This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * List<String> userKeys = Arrays.asList("user:123", "user:456");
     * Map<String, User> users = client.getBulk(userKeys);
     * }</pre>
     *
     * @param keys the collection of cache keys to retrieve, must not be {@code null} or contain {@code null} elements
     * @return a map of found key-value pairs, never {@code null} (may be empty if no keys are found)
     * @throws IllegalArgumentException if {@code keys} is {@code null} or contains {@code null} elements
     */
    Map<String, T> getBulk(Collection<String> keys);

    /**
     * Stores a key-value pair in the cache with a specified time-to-live.
     * If the key already exists, its value will be replaced.
     *
     * <p>This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.
     * When multiple clients set the same key concurrently, the last write wins.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = new User("John", "john@example.com");
     * boolean success = client.set("user:123", user, 3600000); // 1 hour TTL
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @param obj the value to cache, may be {@code null} (if supported by the implementation)
     * @param liveTime the time-to-live in milliseconds (0 means no expiration), must not be negative
     * @return {@code true} if the operation was successful, {@code false} otherwise
     * @throws IllegalArgumentException if {@code key} is {@code null} or {@code liveTime} is negative
     */
    boolean set(String key, T obj, long liveTime);

    /**
     * Removes a key-value pair from the cache.
     * The return value indicates whether the operation was acknowledged by the server,
     * not whether the key existed. In most implementations, this returns {@code true}
     * if the delete command was successfully sent, regardless of key existence.
     *
     * <p>This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * boolean success = client.delete("user:123");
     * System.out.println("Delete operation sent: " + success);
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @return {@code true} if the delete operation was successfully sent to the server, {@code false} otherwise
     * @throws IllegalArgumentException if {@code key} is {@code null}
     */
    boolean delete(String key);

    /**
     * Atomically increments a numeric value by 1.
     * If the key doesn't exist, behavior is implementation-dependent (some implementations
     * may initialize it to 0 before incrementing, others may throw an error).
     *
     * <p>This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent increment operations are guaranteed to be serialized correctly,
     * ensuring no increments are lost.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long pageViews = client.incr("page:views");
     * System.out.println("Page views: " + pageViews);
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @return the value after increment
     * @throws IllegalArgumentException if {@code key} is {@code null}
     */
    long incr(String key);

    /**
     * Atomically increments a numeric value by a specified amount.
     * If the key doesn't exist, behavior is implementation-dependent (some implementations
     * may initialize it to 0 before incrementing, others may throw an error).
     *
     * <p>This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent increment operations are guaranteed to be serialized correctly,
     * ensuring no increments are lost.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long score = client.incr("player:score", 10);
     * System.out.println("New score: " + score);
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @param delta the increment amount (positive value), must be non-negative
     * @return the value after increment
     * @throws IllegalArgumentException if {@code key} is {@code null} or {@code delta} is negative
     */
    long incr(String key, int delta);

    /**
     * Atomically decrements a numeric value by 1.
     * If the key doesn't exist, behavior is implementation-dependent (some implementations
     * may initialize it to 0 before decrementing, others may throw an error).
     *
     * <p>This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent decrement operations are guaranteed to be serialized correctly,
     * ensuring no decrements are lost.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long remainingTokens = client.decr("api:tokens");
     * if (remainingTokens <= 0) {
     *     System.out.println("Rate limit exceeded");
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @return the value after decrement
     * @throws IllegalArgumentException if {@code key} is {@code null}
     */
    long decr(String key);

    /**
     * Atomically decrements a numeric value by a specified amount.
     * If the key doesn't exist, behavior is implementation-dependent (some implementations
     * may initialize it to 0 before decrementing, others may throw an error).
     *
     * <p>This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent decrement operations are guaranteed to be serialized correctly,
     * ensuring no decrements are lost.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long inventory = client.decr("product:stock", 5);
     * System.out.println("Remaining inventory: " + inventory);
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @param delta the decrement amount (positive value), must be non-negative
     * @return the value after decrement
     * @throws IllegalArgumentException if {@code key} is {@code null} or {@code delta} is negative
     */
    long decr(String key, int delta);

    /**
     * Removes all keys from all connected cache servers.
     * This is a destructive operation that affects all data across all servers.
     * Use with extreme caution in production environments.
     *
     * <p>This method is thread-safe but its effects are visible immediately to all clients.
     * Once executed, all cached data will be permanently lost.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Warning: This removes ALL data from the cache!
     * client.flushAll();
     * System.out.println("All cache data cleared");
     * }</pre>
     */
    void flushAll();

    /**
     * Disconnects from all cache servers and releases resources.
     * After calling this method, the client cannot be used anymore and any subsequent
     * operations will fail or throw exceptions.
     *
     * <p>This method should be called when the client is no longer needed to ensure
     * proper cleanup of network connections and other resources. It is safe to call
     * this method multiple times; subsequent calls will have no effect.</p>
     *
     * <p>This method is thread-safe, but once called, no other operations should be
     * attempted on this client instance.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * try {
     *     // Use the client
     *     client.set("key", value, 3600000);
     * } finally {
     *     client.disconnect();
     *     System.out.println("Cache client disconnected");
     * }
     * }</pre>
     */
    void disconnect();
}