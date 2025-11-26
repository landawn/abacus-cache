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
 * <br><br>
 * Example usage:
 * <pre>{@code
 * JRedis<User> cache = new JRedis<>("localhost:6379,localhost:6380");
 * User user = new User();
 * cache.set("user:123", user, 3600000); // Cache for 1 hour
 * User cached = cache.get("user:123");
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
     * Data is automatically sharded across all specified Redis instances for horizontal scaling.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * JRedis<User> cache = new JRedis<>("localhost:6379,localhost:6380");
     * User user = new User();
     * cache.set("user:123", user, 3600000);
     * }</pre>
     *
     * @param serverUrl the Redis server URL(s) in format "host1:port1,host2:port2,..."
     * @throws IllegalArgumentException if no valid server addresses found in serverUrl
     * @see #JRedis(String, long)
     */
    public JRedis(final String serverUrl) {
        this(serverUrl, DEFAULT_TIMEOUT);
    }

    /**
     * Creates a new JRedis instance with a specified timeout.
     * The server URL should contain comma-separated host:port pairs for multiple Redis instances.
     * The timeout applies to both connection establishment and socket read/write operations.
     * Data is automatically sharded across all specified Redis instances.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * JRedis<Data> cache = new JRedis<>("redis1:6379,redis2:6379", 5000); // 5 second timeout
     * Data data = new Data();
     * cache.set("key", data, 7200000); // Cache for 2 hours
     * }</pre>
     *
     * @param serverUrl the Redis server URL(s) in format "host1:port1,host2:port2,..."
     * @param timeout the connection timeout in milliseconds
     * @throws IllegalArgumentException if no valid server addresses found in serverUrl
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
     * The value is deserialized from its binary representation using Kryo.
     *
     * <p>This method is thread-safe and can be called concurrently from multiple threads.
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
     * // Get with fallback to database
     * User user = cache.get("user:123");
     * if (user == null) {
     *     user = database.findUser(123);
     *     cache.set("user:123", user, 3600000);
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @return the cached value, or {@code null} if not found, expired, or evicted
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error or timeout occurs
     * @see #set(String, Object, long)
     * @see #delete(String)
     */
    @Override
    public T get(final String key) {
        return decode(jedis.get(getKeyBytes(key)));
    }

    /**
     * Stores a key-value pair in the cache with a specified time-to-live.
     * The value is serialized using Kryo before storage. If the key already exists,
     * its value will be replaced. The operation uses Redis SETEX command which atomically
     * sets the value and expiration time.
     *
     * <p>This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.
     * When multiple clients set the same key concurrently, the last write wins.</p>
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
     * // Cache with no expiration
     * Config config = loadConfig();
     * cache.set("app:config", config, 0); // No expiration
     *
     * // Updating existing value
     * Product product = cache.get("product:456");
     * product.setPrice(99.99);
     * cache.set("product:456", product, 7200000); // 2 hour TTL
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @param obj the value to cache, may be {@code null} (stored as empty byte array)
     * @param liveTime the time-to-live in milliseconds (0 means no expiration), must not be negative
     * @return {@code true} if the operation was successful (Redis responds with "OK"), {@code false} otherwise
     * @throws IllegalArgumentException if {@code key} is {@code null} or {@code liveTime} is negative
     * @throws RuntimeException if a network error or timeout occurs
     * @see #get(String)
     * @see #delete(String)
     */
    @Override
    public boolean set(final String key, final T obj, final long liveTime) {
        return "OK".equals(jedis.setex(getKeyBytes(key), toSeconds(liveTime), encode(obj)));
    }

    /**
     * Removes the mapping for a key from the cache if it is present.
     * This method uses the Redis DEL command to remove the key. The operation succeeds
     * regardless of whether the key exists or not. The return value always indicates
     * that the command was successfully executed (not whether the key existed).
     *
     * <p>This method is thread-safe and can be called concurrently from multiple threads.
     * The implementation handles concurrent access safely across distributed cache clients.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Simple delete
     * boolean success = cache.delete("user:123");
     * System.out.println("Delete operation sent: " + success);
     *
     * // Delete after update
     * User user = cache.get("user:456");
     * if (user != null && user.isInactive()) {
     *     cache.delete("user:456");
     * }
     *
     * // Delete multiple keys
     * String[] keysToDelete = {"session:1", "session:2", "session:3"};
     * Arrays.stream(keysToDelete).forEach(cache::delete);
     *
     * // Invalidate cache on entity update
     * void updateUser(User user) {
     *     database.save(user);
     *     cache.delete("user:" + user.getId()); // Invalidate cache
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @return {@code true} if the delete operation was successfully sent to the server, {@code false} otherwise
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
     *
     * <p><b>Redis-specific behavior:</b> If the key doesn't exist, it will be created with value 1
     * (initializes to 0, then increments). This differs from Memcached which returns -1 for non-existent keys.</p>
     *
     * <p>This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent increment operations are guaranteed to be serialized correctly,
     * ensuring no increments are lost.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Simple counter
     * long pageViews = cache.incr("page:views");
     * System.out.println("Page views: " + pageViews);
     *
     * // Request counter (auto-initializes)
     * long requestCount = cache.incr("api:requests");
     *
     * // Rate limiting
     * String key = "rate:limit:" + userId;
     * long attempts = cache.incr(key);
     * if (attempts > MAX_ATTEMPTS) {
     *     throw new RateLimitException("Too many requests");
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @return the value after increment
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error or timeout occurs
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
     *
     * <p><b>Redis-specific behavior:</b> If the key doesn't exist, it will be created with the delta value
     * (initializes to 0, then increments by delta). This differs from Memcached which returns -1 for non-existent keys.</p>
     *
     * <p>This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent increment operations are guaranteed to be serialized correctly,
     * ensuring no increments are lost.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Game score increment
     * long score = cache.incr("player:score", 10);
     * System.out.println("New score: " + score);
     *
     * // Batch processing counter
     * long processed = cache.incr("batch:processed", 100);
     *
     * // Points system
     * int points = calculatePoints(action);
     * long totalPoints = cache.incr("user:points:" + userId, points);
     *
     * // Bandwidth tracking
     * long bytesTransferred = cache.incr("bandwidth:today", fileSize);
     * if (bytesTransferred > QUOTA) {
     *     logger.warn("Bandwidth quota exceeded");
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @param delta the increment amount (positive value), must be non-negative
     * @return the value after increment
     * @throws IllegalArgumentException if {@code key} is {@code null} or {@code delta} is negative
     * @throws RuntimeException if a network error or timeout occurs
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
     *
     * <p><b>Redis-specific behavior:</b> If the key doesn't exist, it will be created with value -1
     * (initializes to 0, then decrements). Unlike Memcached where values cannot go below 0,
     * Redis allows negative values.</p>
     *
     * <p>This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent decrement operations are guaranteed to be serialized correctly,
     * ensuring no decrements are lost.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Token bucket rate limiting
     * long remainingTokens = cache.decr("api:tokens:" + userId);
     * if (remainingTokens <= 0) {
     *     throw new RateLimitException("Rate limit exceeded");
     * }
     *
     * // Inventory management
     * long stock = cache.decr("product:stock:123");
     * if (stock < 0) {
     *     // Handle out of stock
     *     cache.incr("product:stock:123"); // Revert
     *     throw new OutOfStockException();
     * }
     *
     * // Download counter
     * long remaining = cache.decr("downloads:remaining:" + userId);
     * if (remaining >= 0) {
     *     processDownload();
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @return the value after decrement (can be negative in Redis)
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error or timeout occurs
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
     *
     * <p><b>Redis-specific behavior:</b> If the key doesn't exist, it will be created with the negative delta value
     * (initializes to 0, then decrements by delta). Unlike Memcached where values cannot go below 0,
     * Redis allows negative values.</p>
     *
     * <p>This operation is atomic and thread-safe across all distributed cache clients.
     * Multiple concurrent decrement operations are guaranteed to be serialized correctly,
     * ensuring no decrements are lost.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Bulk inventory decrement
     * long inventory = cache.decr("product:stock:456", 5);
     * System.out.println("Remaining inventory: " + inventory);
     *
     * // API quota management
     * int requestCost = calculateCost(request);
     * long quotaRemaining = cache.decr("quota:" + apiKey, requestCost);
     * if (quotaRemaining < 0) {
     *     throw new QuotaExceededException();
     * }
     *
     * // Reservation system
     * long availableSeats = cache.decr("event:seats:789", numberOfTickets);
     * if (availableSeats < 0) {
     *     // Revert the decrement
     *     cache.incr("event:seats:789", numberOfTickets);
     *     throw new NotEnoughSeatsException();
     * }
     *
     * // Resource pool management
     * long availableConnections = cache.decr("pool:connections", 1);
     * if (availableConnections >= 0) {
     *     return acquireConnection();
     * }
     * }</pre>
     *
     * @param key the cache key, must not be {@code null}
     * @param delta the decrement amount (positive value), must be non-negative
     * @return the value after decrement (can be negative in Redis)
     * @throws IllegalArgumentException if {@code key} is {@code null} or {@code delta} is negative
     * @throws RuntimeException if a network error or timeout occurs
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
     * This is a destructive operation that affects all data across all shards and databases.
     * Use with extreme caution in production environments.
     *
     * <p>This method is thread-safe but its effects are visible immediately to all clients.
     * Once executed, all cached data will be permanently lost. There is no way to recover
     * the data after this operation completes.</p>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // WARNING: This removes ALL data from ALL Redis instances!
     * cache.flushAll();
     * System.out.println("All cache data cleared");
     *
     * // Safe usage in testing
     * @After
     * public void cleanupCache() {
     *     if (isTestEnvironment()) {
     *         cache.flushAll();
     *     }
     * }
     *
     * // Production usage with confirmation
     * public void clearCache(String confirmationToken) {
     *     if (!"CONFIRM_FLUSH_ALL".equals(confirmationToken)) {
     *         throw new IllegalArgumentException("Invalid confirmation");
     *     }
     *     logger.warn("Flushing all cache data");
     *     cache.flushAll();
     *     auditLog.record("CACHE_FLUSH_ALL", user);
     * }
     *
     * // Application reset
     * public void resetApplication() {
     *     cache.flushAll();
     *     database.resetToDefaults();
     *     logger.info("Application reset complete");
     * }
     * }</pre>
     *
     * @throws RuntimeException if a network error or timeout occurs
     */
    @Override
    public void flushAll() {
        final Collection<Jedis> allShards = jedis.getAllShards();

        for (final Jedis j : allShards) {
            j.flushAll();
        }
    }

    /**
     * Disconnects from all Redis instances and releases resources.
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
     * JRedis<User> cache = new JRedis<>("localhost:6379");
     * try {
     *     cache.set("user:123", user, 3600000);
     *     User cached = cache.get("user:123");
     * } finally {
     *     cache.disconnect();
     *     System.out.println("Cache client disconnected");
     * }
     *
     * // Try-with-resources pattern (if implementing AutoCloseable)
     * try (AutoCloseable closeable = cache::disconnect) {
     *     cache.set("key", value, 3600000);
     *     // Client will be disconnected automatically
     * }
     *
     * // Application shutdown hook
     * Runtime.getRuntime().addShutdownHook(new Thread(() -> {
     *     logger.info("Shutting down cache client");
     *     cache.disconnect();
     * }));
     *
     * // Spring Bean destruction
     * @PreDestroy
     * public void cleanup() {
     *     if (cache != null) {
     *         cache.disconnect();
     *     }
     * }
     *
     * // Graceful shutdown with timeout (implementation-specific)
     * public void shutdownGracefully() {
     *     logger.info("Disconnecting from cache servers");
     *     cache.disconnect();
     *     logger.info("Disconnection complete");
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
     * <p>This method is thread-safe and can be called concurrently from multiple threads.</p>
     *
     * @param key the cache key to convert, must not be {@code null}
     * @return the UTF-8 encoded byte array representation of the key, never {@code null}
     * @throws NullPointerException if {@code key} is {@code null}
     * @see #encode(Object)
     * @see #decode(byte[])
     */
    protected byte[] getKeyBytes(final String key) {
        return key.getBytes(Charsets.UTF_8);
    }

    /**
     * Serializes an object to bytes using Kryo for storage in Redis.
     * Kryo provides efficient serialization with compact binary representation.
     * Null objects are encoded as empty byte arrays. This method is used internally
     * by the {@link #set(String, Object, long)} method.
     *
     * <p>Kryo is a fast and efficient binary object graph serialization framework for Java.
     * It provides better performance and smaller serialized size compared to standard Java serialization.</p>
     *
     * <p>This method is thread-safe and can be called concurrently from multiple threads.</p>
     *
     * @param obj the object to encode, may be {@code null}
     * @return the serialized byte array representation of the object, or empty array if obj is {@code null}, never {@code null}
     * @see #decode(byte[])
     * @see #getKeyBytes(String)
     */
    protected byte[] encode(final Object obj) {
        return obj == null ? N.EMPTY_BYTE_ARRAY : kryoParser.encode(obj);
    }

    /**
     * Deserializes bytes to an object using Kryo.
     * This method reverses the serialization performed by {@link #encode(Object)}.
     * Empty byte arrays are decoded as {@code null}. This method is used internally
     * by the {@link #get(String)} method.
     *
     * <p>Kryo is a fast and efficient binary object graph serialization framework for Java.
     * It provides better performance compared to standard Java serialization.</p>
     *
     * <p>This method is thread-safe and can be called concurrently from multiple threads.</p>
     *
     * @param bytes the byte array to decode, may be {@code null} or empty
     * @return the deserialized object, or {@code null} if the byte array is {@code null} or empty
     * @see #encode(Object)
     * @see #getKeyBytes(String)
     */
    protected T decode(final byte[] bytes) {
        if (bytes == null || N.isEmpty(bytes)) {
            return null;
        }
        return kryoParser.decode(bytes);
    }
}