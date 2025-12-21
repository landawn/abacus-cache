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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.landawn.abacus.parser.KryoParser;
import com.landawn.abacus.parser.ParserFactory;
import com.landawn.abacus.util.AddrUtil;
import com.landawn.abacus.util.Charsets;
import com.landawn.abacus.util.N;

import redis.clients.jedis.BinaryShardedJedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;

/**
 * A Redis-based distributed cache client implementation using Jedis with sharding support.
 * This class provides a distributed caching solution that connects to multiple Redis instances
 * for horizontal scaling and data distribution. Objects are serialized using Kryo for efficient
 * storage and retrieval.
 *
 * <p><b>Key Features:</b></p>
 * <ul>
 *   <li>Automatic sharding across multiple Redis instances for horizontal scaling</li>
 *   <li>Efficient object serialization using Kryo parser</li>
 *   <li>Thread-safe operations for concurrent access</li>
 *   <li>Atomic operations for counters (incr/decr)</li>
 *   <li>TTL (time-to-live) support for automatic expiration</li>
 * </ul>
 *
 * <p><b>Redis-Specific Behaviors:</b></p>
 * <ul>
 *   <li>Keys are automatically distributed across shards using consistent hashing</li>
 *   <li>Increment/decrement operations auto-initialize non-existent keys to 0</li>
 *   <li>Decrement operations can result in negative values (unlike Memcached)</li>
 *   <li>All string keys are encoded using UTF-8</li>
 * </ul>
 *
 * <p><b>Thread Safety:</b> All public methods are thread-safe and can be called concurrently
 * from multiple threads. The underlying Jedis client handles connection pooling and thread safety.</p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Create cache client with multiple Redis instances
 * JRedis<User> cache = new JRedis<>("localhost:6379,localhost:6380");
 *
 * // Store and retrieve objects
 * User user = new User("John", "john@example.com");
 * cache.set("user:123", user, 3600000);   // Cache for 1 hour
 * User cached = cache.get("user:123");
 *
 * // Use atomic counters
 * long pageViews = cache.incr("page:views");
 *
 * // Clean up when done
 * cache.disconnect();
 * }</pre>
 *
 * @param <T> the type of objects to be cached
 * @see AbstractDistributedCacheClient
 * @see BinaryShardedJedis
 */
@SuppressWarnings("deprecation")
public class JRedis<T> extends AbstractDistributedCacheClient<T> {

    private static final KryoParser kryoParser = ParserFactory.createKryoParser();

    private final BinaryShardedJedis jedis;

    /**
     * Creates a new JRedis instance with the default timeout.
     * The server URL should contain comma-separated host:port pairs for multiple Redis instances.
     * Data is automatically sharded across all specified Redis instances using consistent hashing
     * for horizontal scaling and improved performance.
     *
     * <p>Each Redis instance becomes a shard in the distributed cache. When storing or retrieving
     * data, the appropriate shard is determined by hashing the key. This ensures even distribution
     * of data across all servers.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Single Redis instance
     * JRedis<User> cache = new JRedis<>("localhost:6379");
     *
     * // Multiple Redis instances for sharding
     * JRedis<User> cache = new JRedis<>("localhost:6379,localhost:6380,localhost:6381");
     *
     * // Remote Redis servers
     * JRedis<Data> cache = new JRedis<>("redis1.example.com:6379,redis2.example.com:6379");
     *
     * // Use the cache
     * User user = new User("Alice", "alice@example.com");
     * cache.set("user:123", user, 3600000);
     * }</pre>
     *
     * @param serverUrl the Redis server URL(s) in format "host1:port1,host2:port2,...", must not be {@code null} or empty
     * @throws IllegalArgumentException if {@code serverUrl} is {@code null}, empty, or contains no valid server addresses
     * @see #JRedis(String, long)
     */
    public JRedis(final String serverUrl) {
        this(serverUrl, DEFAULT_TIMEOUT);
    }

    /**
     * Creates a new JRedis instance with a specified timeout.
     * The server URL should contain comma-separated host:port pairs for multiple Redis instances.
     * The timeout applies to both connection establishment and socket read/write operations.
     * Data is automatically sharded across all specified Redis instances using consistent hashing.
     *
     * <p>The timeout value affects network operations with Redis servers. If a Redis operation
     * takes longer than the specified timeout, a timeout exception will be thrown. Choose an
     * appropriate timeout based on your network latency and expected operation duration.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Cache with 5 second timeout
     * JRedis<Data> cache = new JRedis<>("redis1:6379,redis2:6379", 5000);
     *
     * // High-latency network with longer timeout
     * JRedis<User> cache = new JRedis<>("remote-redis:6379", 10000);   // 10 seconds
     *
     * // Low-latency local network with short timeout
     * JRedis<Session> cache = new JRedis<>("localhost:6379", 2000);   // 2 seconds
     *
     * // Use the cache
     * Data data = new Data("value");
     * cache.set("key", data, 7200000);   // Cache for 2 hours
     * Data retrieved = cache.get("key");
     * }</pre>
     *
     * @param serverUrl the Redis server URL(s) in format "host1:port1,host2:port2,...", must not be {@code null} or empty
     * @param timeout the connection and socket timeout in milliseconds, must be positive
     * @throws IllegalArgumentException if {@code serverUrl} is {@code null}, empty, contains no valid server addresses, or {@code timeout} is not positive
     * @see #JRedis(String)
     */
    public JRedis(final String serverUrl, final long timeout) {
        super(serverUrl);

        final List<InetSocketAddress> addressList = AddrUtil.getAddressList(serverUrl);

        if (N.isEmpty(addressList)) {
            throw new IllegalArgumentException("No valid server addresses found in: " + serverUrl);
        }

        final List<JedisShardInfo> jedisClusterNodes = new ArrayList<>();

        for (final InetSocketAddress addr : addressList) {
            jedisClusterNodes.add(new JedisShardInfo(addr.getHostName(), addr.getPort(), (int) timeout));
        }

        jedis = new BinaryShardedJedis(jedisClusterNodes);
    }

    /**
     * Retrieves a value from the cache by its key.
     * The value is deserialized from its binary representation using Kryo parser.
     * The appropriate Redis shard is automatically determined based on the key's hash.
     *
     * <p><b>Redis-specific behavior:</b> This operation uses the Redis GET command. If the key
     * does not exist or has expired, {@code null} is returned. The operation is O(1) time complexity.</p>
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Simple get operation
     * User user = cache.get("user:123");
     * if (user != null) {
     *     System.out.println("Found user: " + user.getName());
     * } else {
     *     System.out.println("User not found in cache");
     * }
     *
     * // Get with fallback to database (cache-aside pattern)
     * User user = cache.get("user:123");
     * if (user == null) {
     *     user = database.findUser(123);
     *     if (user != null) {
     *         cache.set("user:123", user, 3600000);
     *     }
     * }
     *
     * // Get with type safety
     * String sessionKey = "session:" + sessionId;
     * Session session = cache.get(sessionKey);
     * if (session != null && !session.isExpired()) {
     *     processSession(session);
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @return the cached value, or {@code null} if not found, expired, or evicted
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error, timeout, or deserialization error occurs
     * @see #set(String, Object, long)
     * @see #delete(String)
     */
    @Override
    public T get(final String key) {
        return decode(jedis.get(getKeyBytes(key)));
    }

    /**
     * Stores a key-value pair in the cache with a specified time-to-live.
     * The value is serialized using Kryo parser before storage. If the key already exists,
     * its value and TTL will be replaced. The appropriate Redis shard is automatically
     * determined based on the key's hash.
     *
     * <p><b>Redis-specific behavior:</b> This operation uses the Redis SETEX command which atomically
     * sets both the value and expiration time. The operation is O(1) time complexity. The TTL is
     * converted from milliseconds to seconds (rounded down). If the key already exists, the previous
     * value and TTL are completely replaced.</p>
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.
     * When multiple clients set the same key concurrently, the last write wins (Redis guarantees
     * atomic execution of SETEX command).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Cache with 1 hour TTL
     * User user = new User("John", "john@example.com");
     * boolean success = cache.set("user:123", user, 3600000);
     * if (success) {
     *     System.out.println("User cached successfully");
     * }
     *
     * // Cache session data with 30 minute TTL
     * Session session = new Session("abc123", user);
     * cache.set("session:" + session.getId(), session, 1800000);
     *
     * // Cache with very long expiration (30 days)
     * Config config = loadConfig();
     * cache.set("app:config", config, 2592000000L);   // 30 days
     *
     * // Updating existing value (replaces both value and TTL)
     * Product product = cache.get("product:456");
     * if (product != null) {
     *     product.setPrice(99.99);
     *     cache.set("product:456", product, 7200000);   // 2 hour TTL
     * }
     *
     * // Cache null values
     * cache.set("empty:key", null, 3600000);   // Stores empty byte array
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @param obj the value to cache, may be {@code null} (stored as empty byte array)
     * @param liveTime the time-to-live in milliseconds, must be positive (0 or negative values may cause unexpected behavior)
     * @return {@code true} if the operation was successful (Redis responds with "OK"), {@code false} otherwise
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error, timeout, or serialization error occurs
     * @see #get(String)
     * @see #delete(String)
     */
    @Override
    public boolean set(final String key, final T obj, final long liveTime) {
        return "OK".equals(jedis.setex(getKeyBytes(key), toSeconds(liveTime), encode(obj)));
    }

    /**
     * Removes the mapping for a key from the cache if it is present.
     * The appropriate Redis shard is automatically determined based on the key's hash.
     *
     * <p><b>Redis-specific behavior:</b> This method uses the Redis DEL command to remove the key.
     * The operation is O(1) time complexity. The command succeeds regardless of whether the key exists.
     * This method always returns {@code true} to indicate the DEL command was sent successfully,
     * not to indicate whether the key actually existed.</p>
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients. If multiple
     * clients delete the same key concurrently, all will succeed (idempotent operation).</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Simple delete
     * boolean success = cache.delete("user:123");
     * System.out.println("Delete operation sent: " + success);
     *
     * // Delete after conditional check
     * User user = cache.get("user:456");
     * if (user != null && user.isInactive()) {
     *     cache.delete("user:456");
     * }
     *
     * // Delete multiple keys sequentially
     * String[] keysToDelete = {"session:1", "session:2", "session:3"};
     * Arrays.stream(keysToDelete).forEach(cache::delete);
     *
     * // Cache invalidation pattern (write-through)
     * void updateUser(User user) {
     *     database.save(user);
     *     cache.delete("user:" + user.getId());   // Invalidate cache
     *     logger.debug("Cache invalidated for user: {}", user.getId());
     * }
     *
     * // Delete with error handling
     * try {
     *     cache.delete("temp:key");
     * } catch (RuntimeException e) {
     *     logger.error("Failed to delete cache key", e);
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @return {@code true} if the delete operation was successfully sent to the server (always returns {@code true} unless an exception occurs)
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error or timeout occurs
     * @see #get(String)
     * @see #set(String, Object, long)
     */
    @Override
    public boolean delete(final String key) {
        jedis.del(getKeyBytes(key));

        return true;
    }

    /**
     * Atomically increments a numeric value by 1.
     * The appropriate Redis shard is automatically determined based on the key's hash.
     *
     * <p><b>Redis-specific behavior:</b> This operation uses the Redis INCR command. If the key doesn't exist,
     * it will be automatically created with value 1 (Redis initializes to 0, then increments by 1).
     * This differs from Memcached which returns -1 for non-existent keys. The operation is O(1) time complexity.
     * If the key contains a value that cannot be represented as an integer, an error will occur.</p>
     *
     * <p><b>Important:</b> The key should contain a string representation of an integer. If the key
     * was previously set with a non-numeric value (using {@link #set(String, Object, long)}), this operation
     * will fail. Increment operations are meant for counters, not for general objects.</p>
     *
     * <p><b>Thread Safety:</b> This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent increment operations are guaranteed to be serialized correctly by Redis,
     * ensuring no increments are lost. This makes it ideal for distributed counters and rate limiting.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Simple counter (auto-initializes to 1 if not exists)
     * long pageViews = cache.incr("page:views");
     * System.out.println("Page views: " + pageViews);
     *
     * // Request counter (auto-initializes)
     * long requestCount = cache.incr("api:requests");
     * System.out.println("Total API requests: " + requestCount);
     *
     * // Rate limiting with expiration
     * String key = "rate:limit:" + userId;
     * long attempts = cache.incr(key);
     * if (attempts > MAX_ATTEMPTS) {
     *     throw new RateLimitException("Too many requests");
     * }
     *
     * // Daily statistics counter
     * String dailyKey = "stats:daily:" + LocalDate.now();
     * long todayCount = cache.incr(dailyKey);
     * logger.info("Today's count: {}", todayCount);
     *
     * // Distributed ID generator (caution: not persistent across Redis restarts)
     * long uniqueId = cache.incr("id:generator");
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @return the value after increment (will be 1 if the key did not exist before)
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error, timeout occurs, or if the key contains a non-integer value
     * @see #incr(String, int)
     * @see #decr(String)
     * @see #decr(String, int)
     */
    @Override
    public long incr(final String key) {
        return jedis.incr(getKeyBytes(key));
    }

    /**
     * Atomically increments a numeric value by a specified amount.
     * The appropriate Redis shard is automatically determined based on the key's hash.
     *
     * <p><b>Redis-specific behavior:</b> This operation uses the Redis INCRBY command. If the key doesn't exist,
     * it will be automatically created with the delta value (Redis initializes to 0, then increments by delta).
     * This differs from Memcached which returns -1 for non-existent keys. The operation is O(1) time complexity.
     * If the key contains a value that cannot be represented as an integer, an error will occur.</p>
     *
     * <p><b>Important:</b> The key should contain a string representation of an integer. If the key
     * was previously set with a non-numeric value (using {@link #set(String, Object, long)}), this operation
     * will fail. Increment operations are meant for counters, not for general objects.</p>
     *
     * <p><b>Thread Safety:</b> This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent increment operations are guaranteed to be serialized correctly by Redis,
     * ensuring no increments are lost. This makes it ideal for distributed counters and batch operations.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Game score increment
     * long score = cache.incr("player:score:123", 10);
     * System.out.println("New score: " + score);
     *
     * // Batch processing counter (auto-initializes to 100 if not exists)
     * long processed = cache.incr("batch:processed", 100);
     * logger.info("Processed {} items so far", processed);
     *
     * // Dynamic points system
     * int points = calculatePoints(action);
     * long totalPoints = cache.incr("user:points:" + userId, points);
     * System.out.println("User earned " + points + " points, total: " + totalPoints);
     *
     * // Bandwidth tracking
     * int fileSize = uploadedFile.getSize();
     * long bytesTransferred = cache.incr("bandwidth:today", fileSize);
     * if (bytesTransferred > QUOTA) {
     *     logger.warn("Bandwidth quota exceeded: {}/{}", bytesTransferred, QUOTA);
     * }
     *
     * // Aggregating metrics from multiple sources
     * int errorCount = parseLogFile();
     * long totalErrors = cache.incr("errors:total", errorCount);
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @param delta the increment amount, can be positive or negative (negative delta effectively decrements)
     * @return the value after increment (will be equal to delta if the key did not exist before)
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error, timeout occurs, or if the key contains a non-integer value
     * @see #incr(String)
     * @see #decr(String)
     * @see #decr(String, int)
     */
    @Override
    public long incr(final String key, final int delta) {
        return jedis.incrBy(getKeyBytes(key), delta);
    }

    /**
     * Atomically decrements a numeric value by 1.
     * The appropriate Redis shard is automatically determined based on the key's hash.
     *
     * <p><b>Redis-specific behavior:</b> This operation uses the Redis DECR command. If the key doesn't exist,
     * it will be automatically created with value -1 (Redis initializes to 0, then decrements by 1).
     * Unlike Memcached where values cannot go below 0, Redis allows negative values. The operation is
     * O(1) time complexity. If the key contains a value that cannot be represented as an integer, an error will occur.</p>
     *
     * <p><b>Important:</b> The key should contain a string representation of an integer. If the key
     * was previously set with a non-numeric value (using {@link #set(String, Object, long)}), this operation
     * will fail. Decrement operations are meant for counters, not for general objects. Unlike Memcached,
     * Redis allows decrementing below zero, so you must implement your own boundary checks if needed.</p>
     *
     * <p><b>Thread Safety:</b> This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent decrement operations are guaranteed to be serialized correctly by Redis,
     * ensuring no decrements are lost. This makes it ideal for resource quotas and inventory management.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Token bucket rate limiting
     * long remainingTokens = cache.decr("api:tokens:" + userId);
     * if (remainingTokens < 0) {
     *     cache.incr("api:tokens:" + userId);   // Restore the token
     *     throw new RateLimitException("Rate limit exceeded");
     * }
     *
     * // Inventory management with rollback
     * long stock = cache.decr("product:stock:123");
     * if (stock < 0) {
     *     // Handle out of stock - rollback the decrement
     *     cache.incr("product:stock:123");
     *     throw new OutOfStockException("Product out of stock");
     * }
     * processOrder();
     *
     * // Download quota tracking
     * long remaining = cache.decr("downloads:remaining:" + userId);
     * if (remaining >= 0) {
     *     processDownload();
     * } else {
     *     cache.incr("downloads:remaining:" + userId);   // Revert
     *     throw new QuotaExceededException("Download quota exceeded");
     * }
     *
     * // Countdown timer
     * long countdown = cache.decr("event:countdown");
     * if (countdown == 0) {
     *     triggerEvent();
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @return the value after decrement (can be negative in Redis, will be -1 if the key did not exist before)
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error, timeout occurs, or if the key contains a non-integer value
     * @see #decr(String, int)
     * @see #incr(String)
     * @see #incr(String, int)
     */
    @Override
    public long decr(final String key) {
        return jedis.decr(getKeyBytes(key));
    }

    /**
     * Atomically decrements a numeric value by a specified amount.
     * The appropriate Redis shard is automatically determined based on the key's hash.
     *
     * <p><b>Redis-specific behavior:</b> This operation uses the Redis DECRBY command. If the key doesn't exist,
     * it will be automatically created with the negative delta value (Redis initializes to 0, then decrements by delta).
     * Unlike Memcached where values cannot go below 0, Redis allows negative values. The operation is
     * O(1) time complexity. If the key contains a value that cannot be represented as an integer, an error will occur.</p>
     *
     * <p><b>Important:</b> The key should contain a string representation of an integer. If the key
     * was previously set with a non-numeric value (using {@link #set(String, Object, long)}), this operation
     * will fail. Decrement operations are meant for counters, not for general objects. Unlike Memcached,
     * Redis allows decrementing below zero, so you must implement your own boundary checks if needed.</p>
     *
     * <p><b>Thread Safety:</b> This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent decrement operations are guaranteed to be serialized correctly by Redis,
     * ensuring no decrements are lost. This makes it ideal for bulk resource management and quota systems.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Bulk inventory decrement
     * int quantity = 5;
     * long inventory = cache.decr("product:stock:456", quantity);
     * System.out.println("Remaining inventory: " + inventory);
     * if (inventory < 0) {
     *     cache.incr("product:stock:456", quantity);   // Rollback
     *     throw new InsufficientStockException();
     * }
     *
     * // API quota management with variable costs
     * int requestCost = calculateCost(request);
     * long quotaRemaining = cache.decr("quota:" + apiKey, requestCost);
     * if (quotaRemaining < 0) {
     *     cache.incr("quota:" + apiKey, requestCost);   // Restore quota
     *     throw new QuotaExceededException("Insufficient quota");
     * }
     *
     * // Event reservation system
     * int numberOfTickets = 3;
     * long availableSeats = cache.decr("event:seats:789", numberOfTickets);
     * if (availableSeats < 0) {
     *     // Revert the decrement
     *     cache.incr("event:seats:789", numberOfTickets);
     *     throw new NotEnoughSeatsException("Only " + (availableSeats + numberOfTickets) + " seats available");
     * }
     * confirmReservation(numberOfTickets);
     *
     * // Resource pool management
     * long availableConnections = cache.decr("pool:connections", 1);
     * if (availableConnections >= 0) {
     *     return acquireConnection();
     * } else {
     *     cache.incr("pool:connections", 1);
     *     throw new NoAvailableConnectionException();
     * }
     *
     * // Batch withdrawal
     * int withdrawAmount = 50;
     * long balance = cache.decr("account:balance:" + accountId, withdrawAmount);
     * if (balance < 0) {
     *     cache.incr("account:balance:" + accountId, withdrawAmount);
     *     throw new InsufficientFundsException();
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @param delta the decrement amount, can be positive or negative (negative delta effectively increments)
     * @return the value after decrement (can be negative in Redis, will be equal to -delta if the key did not exist before)
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error, timeout occurs, or if the key contains a non-integer value
     * @see #decr(String)
     * @see #incr(String)
     * @see #incr(String, int)
     */
    @Override
    public long decr(final String key, final int delta) {
        return jedis.decrBy(getKeyBytes(key), delta);
    }

    /**
     * Removes all keys from all connected Redis instances.
     * This is a destructive operation that affects all data across all shards and all databases
     * within each Redis instance. Use with extreme caution in production environments.
     *
     * <p><b>Redis-specific behavior:</b> This operation uses the Redis FLUSHALL command on each shard.
     * It removes all keys from all databases (not just the currently selected database). The operation
     * is synchronous and blocks until all keys are removed from all shards. The time complexity is
     * O(N) where N is the total number of keys across all databases on all shards.</p>
     *
     * <p><b>Warning:</b> This operation affects ALL databases on each Redis instance (DB 0 through DB 15
     * by default), not just the one being used by this client. If other applications share the same
     * Redis instances, their data will also be deleted.</p>
     *
     * <p><b>Thread Safety:</b> This method is thread-safe but its effects are visible immediately to all clients.
     * Once executed, all cached data will be permanently lost. There is no way to recover
     * the data after this operation completes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // WARNING: This removes ALL data from ALL Redis instances!
     * cache.flushAll();
     * System.out.println("All cache data cleared");
     *
     * // Safe usage in testing environments
     * @After
     * public void cleanupCache() {
     *     if (isTestEnvironment()) {
     *         cache.flushAll();
     *     }
     * }
     *
     * // Integration test cleanup
     * @BeforeEach
     * public void setUp() {
     *     cache.flushAll();   // Clean slate for each test
     * }
     *
     * // Production usage with confirmation and audit
     * public void clearCache(String confirmationToken, User user) {
     *     if (!"CONFIRM_FLUSH_ALL".equals(confirmationToken)) {
     *         throw new IllegalArgumentException("Invalid confirmation");
     *     }
     *     logger.warn("Flushing all cache data, requested by: {}", user.getId());
     *     cache.flushAll();
     *     auditLog.record("CACHE_FLUSH_ALL", user);
     * }
     *
     * // Application reset sequence
     * public void resetApplication() {
     *     logger.info("Starting application reset");
     *     cache.flushAll();
     *     database.resetToDefaults();
     *     logger.info("Application reset complete");
     * }
     *
     * // Emergency cache clear
     * public void emergencyCacheClear() {
     *     logger.error("Emergency cache flush initiated");
     *     try {
     *         cache.flushAll();
     *         logger.info("Emergency flush completed");
     *     } catch (Exception e) {
     *         logger.error("Emergency flush failed", e);
     *     }
     * }
     * }</pre>
     *
     * @throws RuntimeException if a network error or timeout occurs while flushing any shard
     * @see #disconnect()
     */
    @Override
    public void flushAll() {
        final Collection<Jedis> allShards = jedis.getAllShards();

        if (allShards != null) {
            for (final Jedis j : allShards) {
                if (j != null) {
                    j.flushAll();
                }
            }
        }
    }

    /**
     * Disconnects from all Redis instances and releases resources.
     * After calling this method, the client cannot be used anymore and any subsequent
     * operations will fail or throw exceptions.
     *
     * <p><b>Redis-specific behavior:</b> This method closes all connections to all Redis shards.
     * It releases network connections, socket resources, and any internal buffers. The underlying
     * Jedis client resources are cleaned up. This operation does not remove any data from Redis;
     * it only closes the client-side connections.</p>
     *
     * <p><b>Important:</b> This method should be called when the client is no longer needed to ensure
     * proper cleanup of network connections and other resources. It is safe to call this method
     * multiple times; subsequent calls will have no effect. After calling disconnect(), attempting
     * to use the cache client will result in exceptions.</p>
     *
     * <p><b>Thread Safety:</b> This method is thread-safe, but once called, no other operations should be
     * attempted on this client instance from any thread.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Try-finally pattern (recommended)
     * JRedis<User> cache = new JRedis<>("localhost:6379");
     * try {
     *     cache.set("user:123", user, 3600000);
     *     User cached = cache.get("user:123");
     *     processUser(cached);
     * } finally {
     *     cache.disconnect();
     *     System.out.println("Cache client disconnected");
     * }
     *
     * // Try-with-resources pattern (requires wrapper)
     * try (AutoCloseable closeable = cache::disconnect) {
     *     cache.set("key", value, 3600000);
     *     // Client will be disconnected automatically
     * }
     *
     * // Application shutdown hook
     * Runtime.getRuntime().addShutdownHook(new Thread(() -> {
     *     logger.info("Shutting down cache client");
     *     cache.disconnect();
     *     logger.info("Cache client shutdown complete");
     * }));
     *
     * // Spring Bean destruction callback
     * @PreDestroy
     * public void cleanup() {
     *     if (cache != null) {
     *         logger.info("Cleaning up cache client");
     *         cache.disconnect();
     *     }
     * }
     *
     * // Graceful application shutdown
     * public void shutdownGracefully() {
     *     logger.info("Disconnecting from Redis cache servers");
     *     try {
     *         cache.disconnect();
     *         logger.info("Disconnection successful");
     *     } catch (Exception e) {
     *         logger.error("Error during disconnect", e);
     *     }
     * }
     *
     * // Reconnection pattern (not recommended - create new instance instead)
     * public void reconnect() {
     *     oldCache.disconnect();
     *     cache = new JRedis<>(serverUrl, timeout);   // Create new instance
     * }
     * }</pre>
     *
     * @see #flushAll()
     */
    @Override
    public void disconnect() {
        jedis.disconnect();
    }

    /**
     * Converts a string key to UTF-8 encoded bytes for Redis operations.
     * All Redis operations use binary-safe keys, so strings must be converted to bytes.
     * This method is used internally by all cache operations to ensure consistent
     * key encoding across the system.
     *
     * <p><b>Implementation Note:</b> Redis keys are binary-safe, meaning they can contain any
     * byte sequence. This method uses UTF-8 encoding which is the standard for most string-based
     * keys. UTF-8 ensures compatibility across different systems and handles international characters
     * correctly.</p>
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     * String.getBytes() creates a new byte array on each call, so there are no shared mutable state issues.</p>
     *
     * @param key the cache key to convert, must not be {@code null}
     * @return the UTF-8 encoded byte array representation of the key, never {@code null}
     * @see #encode(Object)
     * @see #decode(byte[])
     */
    protected byte[] getKeyBytes(final String key) {
        if (key == null) {
            throw new IllegalArgumentException("key cannot be null");
        }
        return key.getBytes(Charsets.UTF_8);
    }

    /**
     * Serializes an object to bytes using Kryo for storage in Redis.
     * Kryo provides efficient serialization with compact binary representation.
     * Null objects are encoded as empty byte arrays. This method is used internally
     * by the {@link #set(String, Object, long)} method.
     *
     * <p><b>Serialization Details:</b> Kryo is a fast and efficient binary object graph serialization
     * framework for Java. It provides better performance and smaller serialized size compared to
     * standard Java serialization. Kryo handles object graphs, circular references, and complex
     * data structures automatically.</p>
     *
     * <p><b>Important:</b> The objects being serialized should be compatible with Kryo serialization.
     * Most Java objects work out of the box, but objects with special serialization requirements may
     * need custom serializers. Classes should have proper constructors or be registered with Kryo.</p>
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     * The KryoParser instance is shared and designed to be thread-safe through proper synchronization.</p>
     *
     * @param obj the object to encode, may be {@code null}
     * @return the serialized byte array representation of the object, or empty array if obj is {@code null}, never {@code null}
     * @throws RuntimeException if serialization fails due to incompatible object types or serialization errors
     * @see #decode(byte[])
     * @see #getKeyBytes(String)
     */
    protected byte[] encode(final Object obj) {
        return obj == null ? N.EMPTY_BYTE_ARRAY : kryoParser.encode(obj);
    }

    /**
     * Deserializes bytes to an object using Kryo.
     * This method reverses the serialization performed by {@link #encode(Object)}.
     * Empty or null byte arrays are decoded as {@code null}. This method is used internally
     * by the {@link #get(String)} method.
     *
     * <p><b>Deserialization Details:</b> Kryo is a fast and efficient binary object graph serialization
     * framework for Java. It provides better performance compared to standard Java serialization.
     * The deserialized object is reconstructed with the same state as when it was serialized,
     * including all fields and nested objects.</p>
     *
     * <p><b>Important:</b> The class of the serialized object must be available on the classpath
     * at deserialization time. If the class structure has changed since serialization, deserialization
     * may fail or produce unexpected results. Ensure class compatibility when upgrading applications.</p>
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     * The KryoParser instance is shared and designed to be thread-safe through proper synchronization.</p>
     *
     * @param bytes the byte array to decode, may be {@code null} or empty
     * @return the deserialized object, or {@code null} if the byte array is {@code null} or empty
     * @throws RuntimeException if deserialization fails due to missing classes, incompatible class versions, or corrupted data
     * @see #encode(Object)
     * @see #getKeyBytes(String)
     */
    protected T decode(final byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        return kryoParser.decode(bytes);
    }
}