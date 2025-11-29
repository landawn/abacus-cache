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
 * Abstract base class for distributed cache client implementations.
 * This class provides common functionality and default implementations for methods
 * that are not universally supported across all distributed cache systems.
 * Concrete implementations like SpyMemcached and JRedis extend this class.
 *
 * <p>This abstract class serves as a foundation for distributed cache clients,
 * providing server URL management, default implementations for optional bulk
 * operations and flush functionality, and utility methods for time conversion.</p>
 *
 * <p><b>Thread Safety:</b> This class is thread-safe. The {@code serverUrl} field is
 * immutable after construction. Subclasses must ensure their implementations of abstract
 * methods are thread-safe to maintain the overall thread safety guarantee.</p>
 *
 * <br><br>
 * Key features:
 * <ul>
 * <li>Stores and provides access to server URL(s)</li>
 * <li>Default implementations for optional operations (getBulk, flushAll)</li>
 * <li>Utility method for time conversion (milliseconds to seconds)</li>
 * <li>Template method pattern for consistent behavior across implementations</li>
 * </ul>
 *
 * <br>
 * Subclasses must implement the following abstract methods:
 * <ul>
 * <li>{@link #get(String)} - retrieves a single value from cache</li>
 * <li>{@link #set(String, Object, long)} - stores a value with TTL</li>
 * <li>{@link #delete(String)} - removes a key from cache</li>
 * <li>{@link #incr(String)} and {@link #incr(String, int)} - atomic increment operations</li>
 * <li>{@link #decr(String)} and {@link #decr(String, int)} - atomic decrement operations</li>
 * <li>{@link #disconnect()} - releases resources and closes connections</li>
 * </ul>
 *
 * <br>
 * Subclasses may optionally override:
 * <ul>
 * <li>{@link #getBulk(String...)} and {@link #getBulk(Collection)} - for bulk retrieval support</li>
 * <li>{@link #flushAll()} - for clearing all cache data</li>
 * </ul>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Example custom implementation
 * public class MyDistributedCache<T> extends AbstractDistributedCacheClient<T> {
 *     private final MyClient client;
 *
 *     public MyDistributedCache(String serverUrl) {
 *         super(serverUrl);
 *         this.client = new MyClient(serverUrl);
 *     }
 *
 *     @Override
 *     public T get(String key) {
 *         // Implementation-specific logic
 *         return client.retrieve(key);
 *     }
 *
 *     @Override
 *     public boolean set(String key, T obj, long liveTime) {
 *         int ttl = toSeconds(liveTime);   // Use utility method
 *         return client.store(key, obj, ttl);
 *     }
 *
 *     @Override
 *     public boolean delete(String key) {
 *         return client.remove(key);
 *     }
 *
 *     @Override
 *     public long incr(String key) {
 *         return client.increment(key, 1);
 *     }
 *
 *     @Override
 *     public long incr(String key, int delta) {
 *         return client.increment(key, delta);
 *     }
 *
 *     @Override
 *     public long decr(String key) {
 *         return client.decrement(key, 1);
 *     }
 *
 *     @Override
 *     public long decr(String key, int delta) {
 *         return client.decrement(key, delta);
 *     }
 *
 *     @Override
 *     public void disconnect() {
 *         client.close();
 *     }
 *
 *     // Optionally override bulk operations if supported
 *     @Override
 *     public Map<String, T> getBulk(String... keys) {
 *         return client.multiGet(keys);
 *     }
 * }
 * }</pre>
 *
 * @param <T> the type of objects to be cached
 * @see DistributedCacheClient
 * @see SpyMemcached
 * @see JRedis
 */
public abstract class AbstractDistributedCacheClient<T> implements DistributedCacheClient<T> {

    private final String serverUrl;

    /**
     * Constructs an AbstractDistributedCacheClient with the specified server URL.
     * The server URL format is implementation-specific but typically includes
     * host and port information (e.g., "localhost:11211" for Memcached or
     * "localhost:6379" for Redis).
     *
     * <p>For multiple servers, implementations may use comma-separated values,
     * space-separated values, or other formats. Consult the specific implementation
     * documentation for the exact format.</p>
     *
     * <p><b>Thread Safety:</b> This constructor is safe to call from any thread.
     * The {@code serverUrl} parameter is stored as an immutable field.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Single server
     * AbstractDistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     *
     * // Multiple servers (format depends on implementation)
     * AbstractDistributedCacheClient<User> client = new SpyMemcached<>("server1:11211,server2:11211");
     * }</pre>
     *
     * @param serverUrl the server URL(s) for connecting to the distributed cache, must not be {@code null}
     * @throws IllegalArgumentException if {@code serverUrl} is {@code null} (implementation-specific)
     */
    protected AbstractDistributedCacheClient(final String serverUrl) {
        this.serverUrl = serverUrl;
    }

    /**
     * Returns the server URL(s) this client is connected to.
     * The format is implementation-specific and may include multiple servers.
     * For multiple servers, the format depends on the implementation
     * (e.g., comma-separated for some implementations).
     *
     * <p>This method is thread-safe and can be called concurrently from multiple threads.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * AbstractDistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * String url = client.serverUrl();
     * System.out.println("Connected to: " + url);   // Output: "Connected to: localhost:11211"
     * }</pre>
     *
     * @return the server URL(s) for this client, never {@code null}
     */
    @Override
    public String serverUrl() {
        return serverUrl;
    }

    /**
     * Retrieves multiple objects from the cache using varargs.
     * This is more efficient than multiple individual get operations.
     * Keys not found in the cache will not be present in the returned map.
     *
     * <p><b>Default Implementation:</b> This default implementation throws {@code UnsupportedOperationException}.
     * Subclasses should override this method if the underlying cache system supports bulk retrieval
     * operations (such as Memcached's getMulti or Redis's MGET). If the underlying system does not
     * provide native bulk operations, subclasses may choose to implement this by performing multiple
     * individual get operations, though this defeats the performance benefit.</p>
     *
     * <p><b>When to Override:</b>
     * <ul>
     * <li>The underlying cache system provides native bulk retrieval operations</li>
     * <li>Performance optimization is needed for multi-key retrieval scenarios</li>
     * <li>Your use case frequently requires retrieving multiple related cache entries</li>
     * </ul>
     *
     * <p><b>Thread Safety:</b> Implementations must be thread-safe and handle
     * concurrent access safely across distributed cache clients.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // In a subclass that implements this method:
     * AbstractDistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * Map<String, User> users = client.getBulk("user:123", "user:456", "user:789");
     * users.forEach((key, user) -> System.out.println(key + ": " + user.getName()));
     *
     * // Handling missing keys
     * String[] requestedKeys = {"user:1", "user:2", "user:3"};
     * Map<String, User> result = client.getBulk(requestedKeys);
     * System.out.println("Retrieved " + result.size() + " out of " + requestedKeys.length + " users");
     * }</pre>
     *
     * @param keys the cache keys to retrieve values for (variable number of String arguments),
     *             must not be {@code null} or contain {@code null} elements
     * @return a map of found key-value pairs, never {@code null} (may be empty if no keys are found)
     * @throws UnsupportedOperationException if this operation is not supported by the implementation (default behavior)
     * @throws IllegalArgumentException if {@code keys} is {@code null} or contains {@code null} elements (implementation-specific)
     * @throws RuntimeException if a network error or timeout occurs (implementation-specific)
     * @see #getBulk(Collection)
     */
    @Override
    public Map<String, T> getBulk(final String... keys) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieves multiple objects from the cache using a collection.
     * This is more efficient than multiple individual get operations.
     * Keys not found in the cache will not be present in the returned map.
     *
     * <p><b>Default Implementation:</b> This default implementation throws {@code UnsupportedOperationException}.
     * Subclasses should override this method if the underlying cache system supports bulk retrieval
     * operations (such as Memcached's getMulti or Redis's MGET). If the underlying system does not
     * provide native bulk operations, subclasses may choose to implement this by performing multiple
     * individual get operations, though this defeats the performance benefit.</p>
     *
     * <p><b>When to Override:</b>
     * <ul>
     * <li>The underlying cache system provides native bulk retrieval operations</li>
     * <li>Performance optimization is needed for multi-key retrieval scenarios</li>
     * <li>Your use case frequently requires retrieving multiple related cache entries</li>
     * <li>You need to accept dynamically generated key collections (Lists, Sets, etc.)</li>
     * </ul>
     *
     * <p><b>Implementation Note:</b> If you override this method, you should typically also
     * override {@link #getBulk(String...)} and potentially implement it by delegating to this
     * method using {@code Arrays.asList(keys)} for consistency.</p>
     *
     * <p><b>Thread Safety:</b> Implementations must be thread-safe and handle
     * concurrent access safely across distributed cache clients.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // In a subclass that implements this method:
     * AbstractDistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * List<String> userKeys = Arrays.asList("user:123", "user:456", "user:789");
     * Map<String, User> users = client.getBulk(userKeys);
     * System.out.println("Retrieved " + users.size() + " users");
     *
     * // Using with dynamically generated keys
     * List<Integer> productIds = Arrays.asList(101, 102, 103);
     * Set<String> productKeys = productIds.stream()
     *         .map(id -> "product:" + id)
     *         .collect(Collectors.toSet());
     * Map<String, Product> products = client.getBulk(productKeys);
     * }</pre>
     *
     * @param keys the collection of cache keys to retrieve values for,
     *             must not be {@code null} or contain {@code null} elements
     * @return a map of found key-value pairs, never {@code null} (may be empty if no keys are found)
     * @throws UnsupportedOperationException if this operation is not supported by the implementation (default behavior)
     * @throws IllegalArgumentException if {@code keys} is {@code null} or contains {@code null} elements (implementation-specific)
     * @throws RuntimeException if a network error or timeout occurs (implementation-specific)
     * @see #getBulk(String...)
     */
    @Override
    public Map<String, T> getBulk(final Collection<String> keys) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Removes all keys from all connected cache servers.
     * This is a destructive operation that affects all data across all servers.
     * Use with extreme caution in production environments.
     *
     * <p><b>Default Implementation:</b> This default implementation throws {@code UnsupportedOperationException}.
     * Subclasses should override this method if the underlying cache system supports flush/clear operations
     * (such as Memcached's flush_all or Redis's FLUSHALL/FLUSHDB). This operation is typically used for
     * testing purposes or administrative maintenance.</p>
     *
     * <p><b>When to Override:</b>
     * <ul>
     * <li>The underlying cache system provides flush/clear operations</li>
     * <li>You need to support testing scenarios that require a clean cache state</li>
     * <li>Administrative operations require the ability to clear all cached data</li>
     * </ul>
     *
     * <p><b>Implementation Note:</b> Implementations should ensure this operation affects all
     * connected servers when using a multi-server configuration. The operation should be idempotent
     * (safe to call multiple times).</p>
     *
     * <p><b>Thread Safety:</b> Implementations must be thread-safe. However, note that
     * once executed, all cached data will be permanently lost and the effects are
     * visible immediately to all clients connected to the same cache servers.</p>
     *
     * <p><b>Warning:</b> This is a destructive operation that removes all data
     * from all connected cache servers. There is no way to undo this operation.
     * Use with extreme caution in production environments.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // In a subclass that implements this method:
     * AbstractDistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * // WARNING: This removes ALL data from all cache servers!
     * client.flushAll();
     * System.out.println("All cache data cleared");
     *
     * // Safe usage in testing
     * @After
     * public void cleanupCache() {
     *     try {
     *         cacheClient.flushAll();
     *     } catch (UnsupportedOperationException e) {
     *         // Cache implementation doesn't support flush
     *     }
     * }
     * }</pre>
     *
     * @throws UnsupportedOperationException if this operation is not supported by the implementation (default behavior)
     * @throws RuntimeException if a network error or timeout occurs (implementation-specific)
     */
    @Override
    public void flushAll() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Converts milliseconds to seconds for cache operations.
     * Most distributed caches (like Memcached and Redis) use seconds for time-to-live (TTL),
     * so this utility method converts milliseconds (used by the Cache interface) to seconds.
     * The method rounds up to ensure the TTL is not shorter than requested.
     *
     * <p><b>Rounding Behavior:</b>
     * <ul>
     * <li>Exact seconds (e.g., 2000ms) are converted exactly (2000ms → 2s)</li>
     * <li>Fractional seconds are rounded up (e.g., 1500ms → 2s, 999ms → 1s)</li>
     * <li>This ensures cached items live at least as long as requested</li>
     * <li>Zero milliseconds returns zero seconds (no expiration)</li>
     * </ul>
     * </p>
     *
     * <p><b>Implementation Details:</b> The conversion algorithm uses integer division
     * and modulo operations to efficiently round up fractional seconds. If the millisecond
     * value is evenly divisible by 1000, it performs exact division. Otherwise, it adds 1
     * to round up. This ensures that a TTL of 1ms becomes 1s, not 0s.</p>
     *
     * <p><b>Thread Safety:</b> This method is thread-safe as it has no side effects
     * and operates only on method parameters.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // In a subclass implementation
     * @Override
     * public boolean set(String key, T obj, long liveTime) {
     *     int ttlSeconds = toSeconds(liveTime);
     *     // Use ttlSeconds with cache system
     *     return cacheClient.set(key, obj, ttlSeconds);
     * }
     *
     * // Example conversions:
     * int seconds1 = toSeconds(1500);       // Returns 2 (1.5s rounds up)
     * int seconds2 = toSeconds(2000);       // Returns 2 (exactly 2s)
     * int seconds3 = toSeconds(999);        // Returns 1 (rounds up to 1s)
     * int seconds4 = toSeconds(0);          // Returns 0 (no expiration)
     * int seconds5 = toSeconds(3600000);    // Returns 3600 (1 hour)
     *
     * // Common TTL values:
     * int oneMinute = toSeconds(60000);     // 60 seconds
     * int oneHour = toSeconds(3600000);     // 3600 seconds
     * int oneDay = toSeconds(86400000);     // 86400 seconds
     * int oneWeek = toSeconds(604800000);   // 604800 seconds
     * }</pre>
     *
     * @param liveTime the time-to-live in milliseconds, must not be negative
     * @return the time-to-live in seconds, rounded up if there's a fractional second
     * @throws IllegalArgumentException if the time value exceeds Integer.MAX_VALUE seconds
     *         (approximately 68 years when converted from milliseconds)
     */
    protected int toSeconds(final long liveTime) {
        final long seconds = (liveTime % 1000 == 0) ? (liveTime / 1000) : (liveTime / 1000) + 1;

        if (seconds > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Time value too large: " + liveTime + " ms (exceeds max integer seconds)");
        }

        return (int) seconds;
    }
}
