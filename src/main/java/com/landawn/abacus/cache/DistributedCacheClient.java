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
 * <br>
 * Example usage:
 * <pre>{@code
 * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
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
     * @return the server URL(s)
     */
    String serverUrl();

    /**
     * Retrieves an object from the cache by its key.
     *
     * @param key the cache key
     * @return the cached object, or null if not found or expired
     */
    T get(String key);

    /**
     * Retrieves multiple objects from the cache in a single operation.
     * This is more efficient than multiple individual get operations.
     * Keys not found in the cache will not be present in the returned map.
     *
     * @param keys the cache keys to retrieve
     * @return a map of found key-value pairs
     */
    Map<String, T> getBulk(String... keys);

    /**
     * Retrieves multiple objects from the cache in a single operation.
     * This is more efficient than multiple individual get operations.
     * Keys not found in the cache will not be present in the returned map.
     *
     * @param keys the collection of cache keys to retrieve
     * @return a map of found key-value pairs
     */
    Map<String, T> getBulk(Collection<String> keys);

    /**
     * Stores an object in the cache with a specified time-to-live.
     * If the key already exists, its value will be replaced.
     *
     * @param key the cache key
     * @param obj the object to cache
     * @param liveTime the time-to-live in milliseconds (0 means no expiration)
     * @return true if the operation was successful
     */
    boolean set(String key, T obj, long liveTime);

    /**
     * Removes an object from the cache.
     * This operation succeeds whether the key exists.
     *
     * @param key the cache key to delete
     * @return true if the operation was successful
     */
    boolean delete(String key);

    /**
     * Atomically increments a numeric value by 1.
     * If the key doesn't exist, behavior depends on the implementation:
     * - Memcached: Creates key with value 1
     * - Redis: Creates key with value 1
     * This operation is atomic and thread-safe across all clients.
     *
     * @param key the cache key
     * @return the value after increment
     */
    long incr(String key);

    /**
     * Atomically increments a numeric value by a specified amount.
     * If the key doesn't exist, behavior depends on the implementation:
     * - Memcached: Creates key with the delta value
     * - Redis: Creates key with the delta value
     * This operation is atomic and thread-safe across all clients.
     *
     * @param key the cache key
     * @param delta the increment amount (can be negative for decrement)
     * @return the value after increment
     */
    long incr(String key, int delta);

    /**
     * Atomically decrements a numeric value by 1.
     * If the key doesn't exist, behavior depends on the implementation:
     * - Memcached: Creates key with value 0, then decrements (result: 0, as underflow is prevented)
     * - Redis: Creates key with value -1
     * This operation is atomic and thread-safe across all clients.
     *
     * @param key the cache key
     * @return the value after decrement
     */
    long decr(String key);

    /**
     * Atomically decrements a numeric value by a specified amount.
     * If the key doesn't exist, behavior depends on the implementation:
     * - Memcached: Creates key with value 0, then decrements (result: 0, as underflow is prevented)
     * - Redis: Creates key with value -(delta value)
     * This operation is atomic and thread-safe across all clients.
     *
     * @param key the cache key
     * @param delta the decrement amount (positive value)
     * @return the value after decrement
     */
    long decr(String key, int delta);

    /**
     * Removes all keys from all connected cache servers.
     * This is a destructive operation that affects all data across all servers.
     * Use with extreme caution in production environments.
     */
    void flushAll();

    /**
     * Disconnects from all cache servers and releases resources.
     * After calling this method, the client cannot be used anymore.
     */
    void disconnect();
}