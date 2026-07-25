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
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Interface for distributed cache client implementations.
 * This interface defines the contract for distributed caching systems like Memcached and Redis,
 * providing basic cache operations and atomic counter functionality. Implementations handle
 * network communication, serialization, and distributed data management.
 *
 * <p>Key features:
 * <ul>
 * <li>Basic CRUD operations (get, put, remove)</li>
 * <li>Optional bulk and flush operations (unsupported implementations may throw)</li>
 * <li>Atomic increment/decrement operations</li>
 * <li>Time-based expiration support</li>
 * </ul>
 *
 * <p><b>Lifecycle:</b> Implementations commonly own connection pools and are designed to be shared
 * for an application's lifetime. Do not create and disconnect a client per request or cache
 * operation. An owning application may invoke {@link #disconnect()} once during component or
 * application shutdown.
 *
 * <p><b>Usage Examples:</b>
 * <pre>{@code
 * // Construct once during application setup and share across requests/threads.
 * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
 * User user = new User("John", "john@example.com");
 * client.put("user:123", user, 3600000);   // Cache for 1 hour
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
     * Constant identifier for Redis Cluster client type.
     */
    String REDIS_CLUSTER = "RedisCluster";

    /**
     * Returns the server URL(s) this client is configured to use.
     * For multiple servers, the format is implementation-specific
     * (e.g., comma-separated for some implementations).
     *
     * <p>This method is thread-safe and can be called concurrently from multiple threads.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * String url = client.serverUrl(); // client is the application's shared cache client
     * System.out.println("Connected to: " + url);
     * }</pre>
     *
     * @return the server URL(s), never {@code null} or blank
     */
    String serverUrl();

    /**
     * Retrieves a value from the cache by its key.
     *
     * <p>This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple get operation
     * User cachedUser = client.get("user:123");
     * if (cachedUser != null) {
     *     System.out.println("Found user: " + cachedUser.getName());
     * } else {
     *     System.out.println("User not found in cache");
     * }
     *
     * // Get with fallback to database
     * User user = client.get("user:123");
     * if (user == null) {
     *     user = database.findUser(123);
     *     client.put("user:123", user, 3600000);
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
     * Retrieves multiple values from the cache. Implementations may batch requests by server or
     * issue individual gets when backend topology requires it. Keys not found in the cache will
     * not be present in the returned map. An implementation that represents a cached {@code null}
     * as a miss may also omit keys explicitly stored with a null value; consult the implementation's
     * null-value contract.
     *
     * <p>This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.
     *
     * <p><b>Usage Examples:</b>
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
     * @param keys the cache keys to retrieve; a supporting implementation requires a non-null array
     *             with no null elements
     * @return a map of found key-value pairs, never {@code null} (may be empty if no keys are found)
     * @throws IllegalArgumentException if bulk retrieval is supported and {@code keys} is
     *         {@code null} or contains a null element
     * @throws RuntimeException if a network error or timeout occurs
     * @throws UnsupportedOperationException if the implementation does not support bulk retrieval
     *         (the {@link AbstractDistributedCacheClient} base class throws this by default)
     */
    Map<String, T> getBulk(String... keys);

    /**
     * Retrieves multiple values from the cache. Implementations may batch requests by server or
     * issue individual gets when backend topology requires it. Keys not found in the cache will
     * not be present in the returned map. An implementation that represents a cached {@code null}
     * as a miss may also omit keys explicitly stored with a null value; consult the implementation's
     * null-value contract.
     *
     * <p>This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.
     *
     * <p><b>Usage Examples:</b>
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
     * Map<String, User> dynamicUsers = client.getBulk(keys);
     * }</pre>
     *
     * @param keys the cache keys to retrieve; a supporting implementation requires a non-null
     *             collection with no null elements
     * @return a map of found key-value pairs, never {@code null} (may be empty if no keys are found)
     * @throws IllegalArgumentException if bulk retrieval is supported and {@code keys} is
     *         {@code null} or contains a null element
     * @throws RuntimeException if a network error or timeout occurs
     * @throws UnsupportedOperationException if the implementation does not support bulk retrieval
     *         (the {@link AbstractDistributedCacheClient} base class throws this by default)
     */
    Map<String, T> getBulk(Collection<String> keys);

    /**
     * Stores a key-value pair in the cache with a specified time-to-live.
     * If the key already exists, its value will be replaced. Distributed cache backends express TTL with
     * differing precision, so the conversion of {@code liveTime} (always supplied in milliseconds) is
     * implementation-specific: the bundled Memcached client converts it to whole seconds, rounded up
     * (see {@link AbstractDistributedCacheClient#toSeconds(long)}). Because Memcached interprets any
     * expiration beyond 30 days as an absolute Unix timestamp, the Memcached client automatically
     * converts such longer TTLs to an absolute expiration time, rejecting (with
     * {@link IllegalArgumentException}) only values large enough to overflow Memcached's 32-bit
     * expiration field (roughly beyond the year 2038). The bundled Redis clients instead honor the
     * millisecond {@code liveTime} exactly via the {@code SET ... PX} command.
     *
     * <p>This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.
     * When multiple clients set the same key concurrently, the last write wins.
     *
     * <p><b>Implementation compatibility:</b> the default implementation delegates to the legacy
     * {@link #set(String, Object, long)} name when the concrete client overrides it. A concrete
     * client must provide a non-recursive implementation of either this method (recommended) or
     * {@code set}. Omitting both methods, or routing both names back through these defaults, fails
     * fast with {@link UnsupportedOperationException}. The cycle guard is per-thread and
     * per-client, not per-key: while this default is delegating to {@code set}, re-entering either
     * name of the pair on the same thread — even for a different key — is treated as a delegation
     * cycle and also fails fast, so an overriding {@code set} must not call back into
     * {@code this.put(...)}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Cache with 1 hour TTL
     * User user = new User("John", "john@example.com");
     * boolean success = client.put("user:123", user, 3600000);
     * if (success) {
     *     System.out.println("User cached successfully");
     * }
     *
     * // Cache session data with 30 minute TTL
     * Session session = new Session("abc123", user);
     * client.put("session:" + session.getId(), session, 1800000);
     *
     * // Cache with no expiration
     * Config config = loadConfig();
     * client.put("app:config", config, 0);   // No expiration
     *
     * // Updating existing value
     * Product product = client.get("product:456");
     * product.setPrice(99.99);
     * client.put("product:456", product, 7200000);   // 2 hour TTL
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @param value the value to cache, may be {@code null} (if supported by the implementation)
     * @param liveTime the time-to-live in milliseconds (0 or negative for no expiration)
     * @return {@code true} if the operation was successful, {@code false} otherwise
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error or timeout occurs
     * @throws UnsupportedOperationException if the concrete client supplies neither a
     *         non-recursive {@code put} nor a non-recursive {@code set} implementation
     */
    @SuppressWarnings("deprecation")
    default boolean put(final String key, final T value, final long liveTime) {
        if (!isOverridden(this, "set")) {
            throw missingCompatibilityImplementation("put", "set");
        }

        if (!DistributedCacheClientCompatibilitySupport.tryEnter(this, DistributedCacheClientCompatibilitySupport.WRITE_PAIR)) {
            throw missingCompatibilityImplementation("put", "set");
        }

        try {
            return set(key, value, liveTime);
        } finally {
            DistributedCacheClientCompatibilitySupport.exit(this, DistributedCacheClientCompatibilitySupport.WRITE_PAIR);
        }
    }

    /**
     * Legacy name for {@link #put(String, Object, long)}.
     *
     * <p>This compatibility bridge is deliberately a default method. Implementations compiled
     * against versions through 2.8.4 may override only {@code set}, while newer implementations
     * override {@code put}; the two defaults let either implementation style serve callers using
     * the other name. Every concrete client must provide a non-recursive implementation of at
     * least one member of the pair; otherwise this method fails fast with
     * {@link UnsupportedOperationException}. The cycle guard is per-thread and per-client, not
     * per-key: while this default is delegating to {@code put}, re-entering either name of the
     * pair on the same thread — even for a different key — is treated as a delegation cycle and
     * also fails fast, so an overriding {@code put} must not call back into {@code this.set(...)}.
     *
     * @param key the cache key, must not be {@code null}
     * @param value the value to cache, may be {@code null} if supported by the implementation
     * @param liveTime the time-to-live in milliseconds ({@code 0} or negative for no expiration)
     * @return {@code true} if the operation was successful, {@code false} otherwise
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error or timeout occurs
     * @throws UnsupportedOperationException if the concrete client supplies neither a
     *         non-recursive {@code put} nor a non-recursive {@code set} implementation
     * @deprecated Use {@link #put(String, Object, long)}. Retained for source and binary
     *             compatibility with clients compiled against version 2.8.4 and earlier.
     */
    @Deprecated(since = "2.8.5", forRemoval = false)
    default boolean set(final String key, final T value, final long liveTime) {
        if (!isOverridden(this, "put")) {
            throw missingCompatibilityImplementation("put", "set");
        }

        if (!DistributedCacheClientCompatibilitySupport.tryEnter(this, DistributedCacheClientCompatibilitySupport.WRITE_PAIR)) {
            throw missingCompatibilityImplementation("put", "set");
        }

        try {
            return put(key, value, liveTime);
        } finally {
            DistributedCacheClientCompatibilitySupport.exit(this, DistributedCacheClientCompatibilitySupport.WRITE_PAIR);
        }
    }

    /**
     * Removes a key-value pair from the cache.
     * The exact meaning of the return value is implementation-specific:
     * <ul>
     * <li><b>Memcached (SpyMemcached):</b> Returns the server's acknowledgement of the delete
     *     operation. Returns {@code false} when the key did not exist.</li>
     * <li><b>Redis ({@link JRedis}/{@link JRedisCluster}):</b> Returns {@code true} when the key existed and was removed,
     *     {@code false} when the key did not exist (based on the count returned by the DEL command).</li>
     * </ul>
     *
     * <p>This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.
     *
     * <p><b>Implementation compatibility:</b> the default implementation delegates to the legacy
     * {@link #delete(String)} name when the concrete client overrides it. A concrete client must
     * provide a non-recursive implementation of either this method (recommended) or
     * {@code delete}. Omitting both methods, or routing both names back through these defaults,
     * fails fast with {@link UnsupportedOperationException}. The cycle guard is per-thread and
     * per-client, not per-key: while this default is delegating to {@code delete}, re-entering
     * either name of the pair on the same thread — even for a different key — is treated as a
     * delegation cycle and also fails fast, so an overriding {@code delete} must not call back
     * into {@code this.remove(...)}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple delete
     * boolean success = client.remove("user:123");
     * System.out.println("Key existed and was removed: " + success);
     *
     * // Delete after update
     * User user = client.get("user:456");
     * if (user != null && user.isInactive()) {
     *     client.remove("user:456");
     * }
     *
     * // Delete multiple keys
     * String[] keysToDelete = {"session:1", "session:2", "session:3"};
     * Arrays.stream(keysToDelete).forEach(client::remove);
     *
     * // Invalidate cache on entity update
     * void updateUser(User user) {
     *     database.save(user);
     *     client.remove("user:" + user.getId());   // Invalidate cache
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @return {@code true} if the key existed and was removed; {@code false} if the key did not exist
     *         when the command was issued (see the per-implementation notes above for exact semantics).
     *         A network error or timeout is thrown rather than reported as a {@code false} return.
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error or timeout occurs
     * @throws UnsupportedOperationException if the concrete client supplies neither a
     *         non-recursive {@code remove} nor a non-recursive {@code delete} implementation
     */
    @SuppressWarnings("deprecation")
    default boolean remove(final String key) {
        if (!isOverridden(this, "delete")) {
            throw missingCompatibilityImplementation("remove", "delete");
        }

        if (!DistributedCacheClientCompatibilitySupport.tryEnter(this, DistributedCacheClientCompatibilitySupport.REMOVE_PAIR)) {
            throw missingCompatibilityImplementation("remove", "delete");
        }

        try {
            return delete(key);
        } finally {
            DistributedCacheClientCompatibilitySupport.exit(this, DistributedCacheClientCompatibilitySupport.REMOVE_PAIR);
        }
    }

    /**
     * Legacy name for {@link #remove(String)}.
     *
     * <p>As with {@link #set(String, Object, long)}, this default bridge supports both legacy
     * implementations that override {@code delete} and current implementations that override
     * {@code remove}. Every concrete client must provide a non-recursive implementation of at
     * least one member of the pair; otherwise this method fails fast with
     * {@link UnsupportedOperationException}. The cycle guard is per-thread and per-client, not
     * per-key: while this default is delegating to {@code remove}, re-entering either name of the
     * pair on the same thread — even for a different key — is treated as a delegation cycle and
     * also fails fast, so an overriding {@code remove} must not call back into
     * {@code this.delete(...)}.
     *
     * @param key the cache key, must not be {@code null}
     * @return {@code true} if the key existed and was removed; {@code false} otherwise
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error or timeout occurs
     * @throws UnsupportedOperationException if the concrete client supplies neither a
     *         non-recursive {@code remove} nor a non-recursive {@code delete} implementation
     * @deprecated Use {@link #remove(String)}. Retained for source and binary compatibility with
     *             clients compiled against version 2.8.4 and earlier.
     */
    @Deprecated(since = "2.8.5", forRemoval = false)
    default boolean delete(final String key) {
        if (!isOverridden(this, "remove")) {
            throw missingCompatibilityImplementation("remove", "delete");
        }

        if (!DistributedCacheClientCompatibilitySupport.tryEnter(this, DistributedCacheClientCompatibilitySupport.REMOVE_PAIR)) {
            throw missingCompatibilityImplementation("remove", "delete");
        }

        try {
            return remove(key);
        } finally {
            DistributedCacheClientCompatibilitySupport.exit(this, DistributedCacheClientCompatibilitySupport.REMOVE_PAIR);
        }
    }

    /**
     * Returns whether the runtime implementation supplies a method outside this interface. This
     * check is paid only when a compatibility default is invoked; concrete clients that override
     * the called method dispatch directly and incur no reflection.
     */
    private static boolean isOverridden(final DistributedCacheClient<?> client, final String methodName) {
        return DistributedCacheClientCompatibilitySupport.isOverridden(client.getClass(), methodName);
    }

    private static UnsupportedOperationException missingCompatibilityImplementation(final String currentName, final String legacyName) {
        return new UnsupportedOperationException("A DistributedCacheClient implementation must provide a non-recursive implementation of either " + currentName
                + "(...) or " + legacyName + "(...)");
    }

    /**
     * Atomically increments a numeric value by 1.
     *
     * <p><b>Implementation-specific behavior when key doesn't exist:</b>
     * <ul>
     * <li><b>Memcached (SpyMemcached):</b> Returns -1 if key doesn't exist</li>
     * <li><b>Redis ({@link JRedis}/{@link JRedisCluster}):</b> Creates key with value 1 (initializes to 0, then increments)</li>
     * </ul>
     *
     * <p>This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent increment operations are guaranteed to be serialized correctly,
     * ensuring no increments are lost.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple counter
     * long pageViews = client.incr("page:views");
     * System.out.println("Page views: " + pageViews);
     *
     * // Request counter with Redis (auto-initializes)
     * long requestCount = client.incr("api:requests");
     *
     * // Memcached returns -1 for a counter that was never seeded. Do NOT seed it with put(...):
     * // the bundled SpyMemcached transcoder serializes the value, and native incr/decr cannot
     * // mutate those bytes. Seed Memcached counters through a path that writes a plain
     * // ASCII-decimal value (see the SpyMemcached implementation's counter handling). Redis, by
     * // contrast, auto-initializes the counter on the first incr.
     * long count = client.incr("counter:visits");   // -1 on Memcached if the counter was never seeded
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
     * @return the value after increment. For non-existent keys: Memcached returns -1
     *         (no auto-initialization); Redis creates the key (effective value after
     *         increment is 1).
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error or timeout occurs, or if the key holds a value that is
     *         not a valid integer counter (e.g. a value previously stored via {@code put})
     */
    long incr(String key);

    /**
     * Atomically increments a numeric value by a specified amount.
     *
     * <p><b>Implementation-specific behavior when key doesn't exist:</b>
     * <ul>
     * <li><b>Memcached (SpyMemcached):</b> Returns -1 if key doesn't exist</li>
     * <li><b>Redis ({@link JRedis}/{@link JRedisCluster}):</b> Creates key with delta value (initializes to 0, then increments by delta)</li>
     * </ul>
     *
     * <p>This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent increment operations are guaranteed to be serialized correctly,
     * ensuring no increments are lost.
     *
     * <p><b>Usage Examples:</b>
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
     * @param delta the increment amount, must be non-negative
     * @return the value after increment. For non-existent keys: Memcached returns -1
     *         (no auto-initialization); Redis creates the key (effective value after
     *         increment is {@code delta}).
     * @throws IllegalArgumentException if {@code key} is {@code null} or {@code delta} is negative
     * @throws RuntimeException if a network error or timeout occurs, or if the key holds a value that is
     *         not a valid integer counter (e.g. a value previously stored via {@code put})
     */
    long incr(String key, long delta);

    /**
     * Atomically decrements a numeric value by 1.
     *
     * <p><b>Implementation-specific behavior when key doesn't exist:</b>
     * <ul>
     * <li><b>Memcached (SpyMemcached):</b> Returns -1 if key doesn't exist. Values cannot go below 0.</li>
     * <li><b>Redis ({@link JRedis}/{@link JRedisCluster}):</b> Creates key with value -1 (initializes to 0, then decrements)</li>
     * </ul>
     *
     * <p>This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent decrement operations are guaranteed to be serialized correctly,
     * ensuring no decrements are lost.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Token bucket rate limiting
     * long remainingTokens = client.decr("api:tokens:" + userId);
     * if (remainingTokens <= 0) {
     *     throw new RateLimitException("Rate limit exceeded");
     * }
     *
     * // Inventory management. Branch on == -1 (key never seeded) and == 0 (depleted): on
     * // Memcached an existing counter is clamped at 0 and never goes negative; -1 is returned
     * // only as the absent-key sentinel, so a "< 0" guard fires only for a never-seeded key
     * // and can never detect depletion.
     * long stock = client.decr("product:stock:123");
     * if (stock == -1) {
     *     throw new IllegalStateException("Stock counter was never seeded");   // Memcached: absent key
     * } else if (stock == 0) {
     *     // Depleted; on Memcached this also fires when an already-zero counter was clamped
     *     throw new OutOfStockException();
     * }
     *
     * // Download counter: seed the counter at (limit + 1) so that "> 0" cleanly means "allowed".
     * // Decrementing an exhausted (0) counter stays 0 on Memcached (clamped), so it remains denied.
     * long remaining = client.decr("downloads:remaining:" + userId);
     * if (remaining > 0) {
     *     processDownload();
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @return the value after decrement. Memcached clamps at 0 (values cannot go negative)
     *         and returns -1 if the key doesn't exist; Redis allows negative values and
     *         creates non-existent keys (effective value after decrement is -1).
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error or timeout occurs, or if the key holds a value that is
     *         not a valid integer counter (e.g. a value previously stored via {@code put})
     */
    long decr(String key);

    /**
     * Atomically decrements a numeric value by a specified amount.
     *
     * <p><b>Implementation-specific behavior when key doesn't exist:</b>
     * <ul>
     * <li><b>Memcached (SpyMemcached):</b> Returns -1 if key doesn't exist. Values cannot go below 0.</li>
     * <li><b>Redis ({@link JRedis}/{@link JRedisCluster}):</b> Creates key with negative delta value (initializes to 0, then decrements by delta)</li>
     * </ul>
     *
     * <p>This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent decrement operations are guaranteed to be serialized correctly,
     * ensuring no decrements are lost.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Bulk inventory decrement
     * long inventory = client.decr("product:stock:456", 5);
     * System.out.println("Remaining inventory: " + inventory);
     *
     * // API quota management. Note the guards below are written for Redis semantics (negative
     * // results allowed). On Memcached an existing counter is clamped at 0, so a multi-unit
     * // decrement CANNOT detect "not enough quota left" this way - and the "< 0" guard would
     * // fire for a never-seeded key (-1 sentinel), misreporting "counter absent" as "quota
     * // exceeded". Seed the counter and treat 0 as exhausted instead (see the single-decrement
     * // example on decr(String)).
     * int requestCost = calculateCost(request);
     * long quotaRemaining = client.decr("quota:" + apiKey, requestCost);   // Redis-style guard below
     * if (quotaRemaining < 0) {
     *     throw new QuotaExceededException();
     * }
     *
     * // Reservation system (Redis semantics: a negative result reveals over-reservation)
     * long availableSeats = client.decr("event:seats:789", numberOfTickets);
     * if (availableSeats < 0) {
     *     // Revert the decrement
     *     client.incr("event:seats:789", numberOfTickets);
     *     throw new NotEnoughSeatsException();
     * }
     *
     * // Resource pool management: seed the counter at (poolSize + 1) and use a strict "> 0"
     * // guard - it is portable across both backends. On Memcached, decrementing an exhausted (0)
     * // counter stays 0 (clamped), so a ">= 0" guard would hand out connections forever once
     * // the pool hit zero.
     * long availableConnections = client.decr("pool:connections", 1);
     * if (availableConnections > 0) {
     *     return acquireConnection();
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @param delta the decrement amount, must be non-negative
     * @return the value after decrement. Memcached clamps at 0 (values cannot go negative)
     *         and returns -1 if the key doesn't exist; Redis allows negative values and
     *         creates non-existent keys (effective value after decrement is {@code -delta}).
     * @throws IllegalArgumentException if {@code key} is {@code null} or {@code delta} is negative
     * @throws RuntimeException if a network error or timeout occurs, or if the key holds a value that is
     *         not a valid integer counter (e.g. a value previously stored via {@code put})
     */
    long decr(String key, long delta);

    /**
     * Requests removal of cached data in the implementation-defined backend scope.
     * This may affect every configured server, database, or namespace, including data belonging
     * to other applications. Use with extreme caution in production environments.
     *
     * <p><b>&#9888;&#65039; Destructive operation:</b> The scope is implementation-specific and may
     * include databases or namespaces used by other applications on the same servers.
     *
     * <p>This method is thread-safe. Data successfully flushed from the implementation-defined
     * scope cannot be recovered, and the invalidation is normally visible to every client sharing
     * that scope.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // WARNING: This may remove every entry in the implementation-defined backend scope!
     * client.flushAll();
     * System.out.println("Configured cache scope cleared");
     *
     * // Safe usage in testing
     * @AfterEach
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
     * @throws UnsupportedOperationException if the implementation does not support flushing all entries
     *         (the {@link AbstractDistributedCacheClient} base class throws this by default)
     */
    void flushAll();

    /**
     * Disconnects from all cache servers and releases resources.
     * After calling this method, the client cannot be used anymore and any subsequent
     * operations will fail or throw exceptions.
     *
     * <p>This is an optional, terminal lifecycle operation for the application or component that
     * owns the client. Implementations commonly pool connections, so callers should normally retain
     * and share a client and invoke this method only when that owner is permanently shutting down,
     * not after an individual request or cache operation. Implementations should make repeated calls
     * harmless; consult the concrete implementation for exact idempotence and failure behavior.
     *
     * <p>This method is thread-safe, but once called, no other operations should be
     * attempted on this client instance.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Optional application shutdown hook for a shared client
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
     * // Explicit component lifecycle callback
     * public void shutdownCacheClient() {
     *     logger.info("Disconnecting from cache servers");
     *     client.disconnect();
     *     logger.info("Disconnection complete");
     * }
     * }</pre>
     */
    void disconnect();
}

/**
 * Support for the current/legacy compatibility bridges. Per-runtime-class metadata keeps
 * reflection off repeated calls, while a per-thread, per-client guard catches delegation cycles
 * that runtime-generated proxies and subinterface defaults can otherwise make look like genuine
 * overrides.
 */
final class DistributedCacheClientCompatibilitySupport {

    static final int WRITE_PAIR = 1;
    static final int REMOVE_PAIR = 1 << 1;

    private static final int PUT = 1;
    private static final int SET = 1 << 1;
    private static final int REMOVE = 1 << 2;
    private static final int DELETE = 1 << 3;

    private static final ClassValue<Integer> OVERRIDE_MASK = new ClassValue<>() {
        @Override
        protected Integer computeValue(final Class<?> type) {
            int mask = 0;

            if (isDeclaredOutsideInterface(type, "put", String.class, Object.class, long.class)) {
                mask |= PUT;
            }

            if (isDeclaredOutsideInterface(type, "set", String.class, Object.class, long.class)) {
                mask |= SET;
            }

            if (isDeclaredOutsideInterface(type, "remove", String.class)) {
                mask |= REMOVE;
            }

            if (isDeclaredOutsideInterface(type, "delete", String.class)) {
                mask |= DELETE;
            }

            return mask;
        }
    };

    /**
     * Active default bridges for the current thread, keyed by client identity rather than
     * {@code equals}. Entries exist only for the duration of a default-method delegation and the
     * empty map is retained so repeated compatibility calls on the same thread do not allocate.
     * Client references themselves are always removed when their delegation finishes.
     */
    private static final ThreadLocal<IdentityHashMap<DistributedCacheClient<?>, Integer>> ACTIVE_BRIDGES = ThreadLocal.withInitial(IdentityHashMap::new);

    private DistributedCacheClientCompatibilitySupport() {
        // Utility class.
    }

    static boolean isOverridden(final Class<?> type, final String methodName) {
        final int methodMask = switch (methodName) {
            case "put" -> PUT;
            case "set" -> SET;
            case "remove" -> REMOVE;
            case "delete" -> DELETE;
            default -> 0;
        };

        return methodMask != 0 && (OVERRIDE_MASK.get(type) & methodMask) != 0;
    }

    static boolean tryEnter(final DistributedCacheClient<?> client, final int pairMask) {
        final IdentityHashMap<DistributedCacheClient<?>, Integer> activeBridges = ACTIVE_BRIDGES.get();
        final int activeMask = activeBridges.getOrDefault(client, 0);

        if ((activeMask & pairMask) != 0) {
            return false;
        }

        activeBridges.put(client, activeMask | pairMask);
        return true;
    }

    static void exit(final DistributedCacheClient<?> client, final int pairMask) {
        final IdentityHashMap<DistributedCacheClient<?>, Integer> activeBridges = ACTIVE_BRIDGES.get();
        final Integer activeMask = activeBridges.get(client);

        if (activeMask == null) {
            return;
        }

        final int remainingMask = activeMask & ~pairMask;

        if (remainingMask == 0) {
            activeBridges.remove(client);
        } else {
            activeBridges.put(client, remainingMask);
        }
    }

    private static boolean isDeclaredOutsideInterface(final Class<?> type, final String methodName, final Class<?>... parameterTypes) {
        try {
            return type.getMethod(methodName, parameterTypes).getDeclaringClass() != DistributedCacheClient.class;
        } catch (final NoSuchMethodException | SecurityException e) {
            // All four methods are declared by the interface. Treat an unusual proxy/class-loader
            // lookup failure as missing so callers receive the documented bounded failure.
            return false;
        }
    }
}
