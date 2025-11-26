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
     * // Simple get operation
     * User user = client.get("user:123");
     * if (user != null) {
     *     System.out.println("Found user: " + user.getName());
     * } else {
     *     System.out.println("User not found in cache");
     * }
     *
     * // Get with fallback to database
     * User user = client.get("user:123");
     * if (user == null) {
     *     user = database.findUser(123);
     *     client.set("user:123", user, 3600000);
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @return the cached value, or {@code null} if not found, expired, or evicted
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error or timeout occurs
     */
    T get(String key);

    /**
     * Retrieves multiple values from the cache in a single operation.
     * This is more efficient than multiple individual get operations as it reduces
     * network round-trips. Keys not found in the cache will not be present in the returned map.
     *
     * <p>This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Basic bulk get
     * Map<String, User> users = client.getBulk("user:123", "user:456", "user:789");
     * users.forEach((key, user) -> System.out.println(key + ": " + user.getName()));
     *
     * // Bulk get with missing key handling
     * Map<String, Product> products = client.getBulk("prod:1", "prod:2", "prod:3");
     * System.out.println("Found " + products.size() + " out of 3 products");
     *
     * // Identify missing keys
     * String[] requestedKeys = {"user:1", "user:2", "user:3"};
     * Map<String, User> found = client.getBulk(requestedKeys);
     * Arrays.stream(requestedKeys)
     *       .filter(key -> !found.containsKey(key))
     *       .forEach(key -> System.out.println("Missing: " + key));
     * }</pre>
     *
     * @param keys the cache keys to retrieve, must not be {@code null} or contain {@code null} elements
     * @return a map of found key-value pairs, never {@code null} (may be empty if no keys are found)
     * @throws IllegalArgumentException if {@code keys} is {@code null} or contains {@code null} elements
     * @throws RuntimeException if a network error or timeout occurs
     */
    Map<String, T> getBulk(String... keys);

    /**
     * Retrieves multiple values from the cache in a single operation.
     * This is more efficient than multiple individual get operations as it reduces
     * network round-trips. Keys not found in the cache will not be present in the returned map.
     *
     * <p>This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Using a List
     * List<String> userKeys = Arrays.asList("user:123", "user:456", "user:789");
     * Map<String, User> users = client.getBulk(userKeys);
     *
     * // Using a Set (useful when keys come from various sources)
     * Set<String> keySet = new HashSet<>(Arrays.asList("session:1", "session:2"));
     * Map<String, Session> sessions = client.getBulk(keySet);
     *
     * // Dynamically built key collection
     * List<Integer> userIds = List.of(101, 102, 103);
     * List<String> keys = userIds.stream()
     *                            .map(id -> "user:" + id)
     *                            .collect(Collectors.toList());
     * Map<String, User> users = client.getBulk(keys);
     * }</pre>
     *
     * @param keys the collection of cache keys to retrieve, must not be {@code null} or contain {@code null} elements
     * @return a map of found key-value pairs, never {@code null} (may be empty if no keys are found)
     * @throws IllegalArgumentException if {@code keys} is {@code null} or contains {@code null} elements
     * @throws RuntimeException if a network error or timeout occurs
     */
    Map<String, T> getBulk(Collection<String> keys);

    /**
     * Stores a key-value pair in the cache with a specified time-to-live.
     * If the key already exists, its value will be replaced. The {@code liveTime} parameter
     * is converted from milliseconds to seconds (rounded up if not exact) for storage.
     *
     * <p>This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.
     * When multiple clients set the same key concurrently, the last write wins.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Cache with 1 hour TTL
     * User user = new User("John", "john@example.com");
     * boolean success = client.set("user:123", user, 3600000);
     * if (success) {
     *     System.out.println("User cached successfully");
     * }
     *
     * // Cache session data with 30 minute TTL
     * Session session = new Session("abc123", user);
     * client.set("session:" + session.getId(), session, 1800000);
     *
     * // Cache with no expiration
     * Config config = loadConfig();
     * client.set("app:config", config, 0); // No expiration
     *
     * // Updating existing value
     * Product product = client.get("product:456");
     * product.setPrice(99.99);
     * client.set("product:456", product, 7200000); // 2 hour TTL
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @param obj the value to cache, may be {@code null} (if supported by the implementation)
     * @param liveTime the time-to-live in milliseconds (0 means no expiration), must not be negative
     * @return {@code true} if the operation was successful, {@code false} otherwise
     * @throws IllegalArgumentException if {@code key} is {@code null} or {@code liveTime} is negative
     * @throws RuntimeException if a network error or timeout occurs
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
     * // Simple delete
     * boolean success = client.delete("user:123");
     * System.out.println("Delete operation sent: " + success);
     *
     * // Delete after update
     * User user = client.get("user:456");
     * if (user != null && user.isInactive()) {
     *     client.delete("user:456");
     * }
     *
     * // Delete multiple keys
     * String[] keysToDelete = {"session:1", "session:2", "session:3"};
     * Arrays.stream(keysToDelete).forEach(client::delete);
     *
     * // Invalidate cache on entity update
     * void updateUser(User user) {
     *     database.save(user);
     *     client.delete("user:" + user.getId()); // Invalidate cache
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @return {@code true} if the delete operation was successfully sent to the server, {@code false} otherwise
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error or timeout occurs
     */
    boolean delete(String key);

    /**
     * Atomically increments a numeric value by 1.
     *
     * <p><b>Implementation-specific behavior when key doesn't exist:</b></p>
     * <ul>
     * <li><b>Memcached (SpyMemcached):</b> Returns -1 if key doesn't exist</li>
     * <li><b>Redis (JRedis):</b> Creates key with value 1 (initializes to 0, then increments)</li>
     * </ul>
     *
     * <p>This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent increment operations are guaranteed to be serialized correctly,
     * ensuring no increments are lost.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Simple counter
     * long pageViews = client.incr("page:views");
     * System.out.println("Page views: " + pageViews);
     *
     * // Request counter with Redis (auto-initializes)
     * long requestCount = client.incr("api:requests");
     *
     * // Memcached requires initialization
     * long count = client.incr("counter:visits");
     * if (count == -1) {
     *     client.set("counter:visits", 1L, 0);
     *     count = 1;
     * }
     *
     * // Rate limiting
     * String key = "rate:limit:" + userId;
     * long attempts = client.incr(key);
     * if (attempts > MAX_ATTEMPTS) {
     *     throw new RateLimitException("Too many requests");
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @return the value after increment, or -1 if key doesn't exist (Memcached)
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error or timeout occurs
     */
    long incr(String key);

    /**
     * Atomically increments a numeric value by a specified amount.
     *
     * <p><b>Implementation-specific behavior when key doesn't exist:</b></p>
     * <ul>
     * <li><b>Memcached (SpyMemcached):</b> Returns -1 if key doesn't exist</li>
     * <li><b>Redis (JRedis):</b> Creates key with delta value (initializes to 0, then increments by delta)</li>
     * </ul>
     *
     * <p>This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent increment operations are guaranteed to be serialized correctly,
     * ensuring no increments are lost.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Game score increment
     * long score = client.incr("player:score", 10);
     * System.out.println("New score: " + score);
     *
     * // Batch processing counter
     * long processed = client.incr("batch:processed", 100);
     *
     * // Points system
     * int points = calculatePoints(action);
     * long totalPoints = client.incr("user:points:" + userId, points);
     *
     * // Bandwidth tracking
     * long bytesTransferred = client.incr("bandwidth:today", fileSize);
     * if (bytesTransferred > QUOTA) {
     *     logger.warn("Bandwidth quota exceeded");
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @param delta the increment amount (positive value), must be non-negative
     * @return the value after increment, or -1 if key doesn't exist (Memcached)
     * @throws IllegalArgumentException if {@code key} is {@code null} or {@code delta} is negative
     * @throws RuntimeException if a network error or timeout occurs
     */
    long incr(String key, int delta);

    /**
     * Atomically decrements a numeric value by 1.
     *
     * <p><b>Implementation-specific behavior when key doesn't exist:</b></p>
     * <ul>
     * <li><b>Memcached (SpyMemcached):</b> Returns -1 if key doesn't exist. Values cannot go below 0.</li>
     * <li><b>Redis (JRedis):</b> Creates key with value -1 (initializes to 0, then decrements)</li>
     * </ul>
     *
     * <p>This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent decrement operations are guaranteed to be serialized correctly,
     * ensuring no decrements are lost.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Token bucket rate limiting
     * long remainingTokens = client.decr("api:tokens:" + userId);
     * if (remainingTokens <= 0) {
     *     throw new RateLimitException("Rate limit exceeded");
     * }
     *
     * // Inventory management
     * long stock = client.decr("product:stock:123");
     * if (stock < 0) {
     *     // Handle out of stock
     *     client.incr("product:stock:123"); // Revert
     *     throw new OutOfStockException();
     * }
     *
     * // Download counter
     * long remaining = client.decr("downloads:remaining:" + userId);
     * if (remaining >= 0) {
     *     processDownload();
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @return the value after decrement (cannot be negative in Memcached), or -1 if key doesn't exist (Memcached)
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error or timeout occurs
     */
    long decr(String key);

    /**
     * Atomically decrements a numeric value by a specified amount.
     *
     * <p><b>Implementation-specific behavior when key doesn't exist:</b></p>
     * <ul>
     * <li><b>Memcached (SpyMemcached):</b> Returns -1 if key doesn't exist. Values cannot go below 0.</li>
     * <li><b>Redis (JRedis):</b> Creates key with negative delta value (initializes to 0, then decrements by delta)</li>
     * </ul>
     *
     * <p>This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent decrement operations are guaranteed to be serialized correctly,
     * ensuring no decrements are lost.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Bulk inventory decrement
     * long inventory = client.decr("product:stock:456", 5);
     * System.out.println("Remaining inventory: " + inventory);
     *
     * // API quota management
     * int requestCost = calculateCost(request);
     * long quotaRemaining = client.decr("quota:" + apiKey, requestCost);
     * if (quotaRemaining < 0) {
     *     throw new QuotaExceededException();
     * }
     *
     * // Reservation system
     * long availableSeats = client.decr("event:seats:789", numberOfTickets);
     * if (availableSeats < 0) {
     *     // Revert the decrement
     *     client.incr("event:seats:789", numberOfTickets);
     *     throw new NotEnoughSeatsException();
     * }
     *
     * // Resource pool management
     * long availableConnections = client.decr("pool:connections", 1);
     * if (availableConnections >= 0) {
     *     return acquireConnection();
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @param delta the decrement amount (positive value), must be non-negative
     * @return the value after decrement (cannot be negative in Memcached), or -1 if key doesn't exist (Memcached)
     * @throws IllegalArgumentException if {@code key} is {@code null} or {@code delta} is negative
     * @throws RuntimeException if a network error or timeout occurs
     */
    long decr(String key, int delta);

    /**
     * Removes all keys from all connected cache servers.
     * This is a destructive operation that affects all data across all servers.
     * Use with extreme caution in production environments.
     *
     * <p>This method is thread-safe but its effects are visible immediately to all clients.
     * Once executed, all cached data will be permanently lost. There is no way to recover
     * the data after this operation completes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // WARNING: This removes ALL data from ALL cache servers!
     * client.flushAll();
     * System.out.println("All cache data cleared");
     *
     * // Safe usage in testing
     * @After
     * public void cleanupCache() {
     *     if (isTestEnvironment()) {
     *         cacheClient.flushAll();
     *     }
     * }
     *
     * // Production usage with confirmation
     * public void clearCache(String confirmationToken) {
     *     if (!"CONFIRM_FLUSH_ALL".equals(confirmationToken)) {
     *         throw new IllegalArgumentException("Invalid confirmation");
     *     }
     *     logger.warn("Flushing all cache data");
     *     client.flushAll();
     *     auditLog.record("CACHE_FLUSH_ALL", user);
     * }
     *
     * // Application reset
     * public void resetApplication() {
     *     client.flushAll();
     *     database.resetToDefaults();
     *     logger.info("Application reset complete");
     * }
     * }</pre>
     *
     * @throws RuntimeException if a network error or timeout occurs
     */
    void flushAll();

    /**
     * Disconnects from all cache servers and releases resources.
     * After calling this method, the client cannot be used anymore and any subsequent
     * operations will fail or throw exceptions.
     *
     * <p>This method should be called when the client is no longer needed to ensure
     * proper cleanup of network connections, thread pools, and other resources. It is
     * safe to call this method multiple times; subsequent calls will have no effect.</p>
     *
     * <p>This method is thread-safe, but once called, no other operations should be
     * attempted on this client instance.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Try-finally pattern
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * try {
     *     client.set("user:123", user, 3600000);
     *     User cached = client.get("user:123");
     * } finally {
     *     client.disconnect();
     *     System.out.println("Cache client disconnected");
     * }
     *
     * // Try-with-resources pattern (if implementing AutoCloseable)
     * try (AutoCloseable closeable = () -> client.disconnect()) {
     *     client.set("key", value, 3600000);
     *     // Client will be disconnected automatically
     * }
     *
     * // Application shutdown hook
     * Runtime.getRuntime().addShutdownHook(new Thread(() -> {
     *     logger.info("Shutting down cache client");
     *     cacheClient.disconnect();
     * }));
     *
     * // Spring Bean destruction
     * @PreDestroy
     * public void cleanup() {
     *     if (cacheClient != null) {
     *         cacheClient.disconnect();
     *     }
     * }
     *
     * // Graceful shutdown with timeout (implementation-specific)
     * public void shutdownGracefully() {
     *     logger.info("Disconnecting from cache servers");
     *     client.disconnect();
     *     logger.info("Disconnection complete");
     * }
     * }</pre>
     */
    void disconnect();
}