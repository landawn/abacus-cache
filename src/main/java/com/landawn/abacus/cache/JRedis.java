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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = cache.get("user:123");
     * if (user != null) {
     *     System.out.println("Found user: " + user.getName());
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be returned
     * @return the value associated with the specified key, or {@code null} if not found or expired
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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * User user = new User("John", "john@example.com");
     * boolean success = cache.set("user:123", user, 3600000); // 1 hour TTL
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated
     * @param obj the cache value to be associated with the specified key
     * @param liveTime the time-to-live in milliseconds (converted to seconds, rounded up if not exact)
     * @return {@code true} if the Redis server responds with "OK", {@code false} otherwise
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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * boolean success = cache.delete("user:123");
     * System.out.println("Delete operation sent: " + success);
     * }</pre>
     *
     * @param key the cache key whose mapping is to be removed from the cache
     * @return always {@code true} indicating the delete command was successfully executed
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
     * If the key doesn't exist, it will be created with value 1.
     * This operation is atomic and thread-safe across all clients.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long pageViews = cache.incr("page:views");
     * System.out.println("Page views: " + pageViews);
     * }</pre>
     *
     * @param key the cache key whose associated value is to be incremented
     * @return the value after increment
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
     * If the key doesn't exist, it will be created with the delta value.
     * This operation is atomic and thread-safe across all clients.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long score = cache.incr("player:score", 10);
     * System.out.println("New score: " + score);
     * }</pre>
     *
     * @param key the cache key whose associated value is to be incremented
     * @param delta the amount by which to increment the value (positive value)
     * @return the value after increment
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
     * If the key doesn't exist, it will be created with value -1.
     * This operation is atomic and thread-safe across all clients.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long remainingTokens = cache.decr("api:tokens");
     * if (remainingTokens <= 0) {
     *     System.out.println("Rate limit exceeded");
     * }
     * }</pre>
     *
     * @param key the cache key whose associated value is to be decremented
     * @return the value after decrement
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
     * If the key doesn't exist, it will be created with the negative delta value.
     * This operation is atomic and thread-safe across all clients.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * long inventory = cache.decr("product:stock", 5);
     * System.out.println("Remaining inventory: " + inventory);
     * }</pre>
     *
     * @param key the cache key whose associated value is to be decremented
     * @param delta the amount by which to decrement the value (positive value)
     * @return the value after decrement
     * @see #decr(String)
     * @see #incr(String)
     * @see #incr(String, int)
     */
    @Override
    public long decr(final String key, final int delta) {
        return jedis.decrBy(getKeyBytes(key), delta);
    }

    /**
     * Flushes all data from all connected Redis instances.
     * This operation removes all keys from all databases on all shards.
     * Use with extreme caution in production environments as this is a destructive operation.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Warning: This removes ALL data from all Redis instances!
     * cache.flushAll();
     * System.out.println("All cache data cleared");
     * }</pre>
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
     * After calling this method, the cache client cannot be used anymore.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * cache.disconnect();
     * System.out.println("Cache client disconnected");
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
     *
     * @param key the cache key to convert
     * @return the UTF-8 encoded byte array representation of the key
     * @see #encode(Object)
     * @see #decode(byte[])
     */
    protected byte[] getKeyBytes(final String key) {
        return key.getBytes(Charsets.UTF_8);
    }

    /**
     * Serializes an object to bytes using Kryo for storage in Redis.
     * Kryo provides efficient serialization with compact binary representation.
     * Null objects are encoded as empty byte arrays.
     *
     * @param obj the object to encode
     * @return the serialized byte array representation of the object, or empty array if obj is {@code null}
     * @see #decode(byte[])
     * @see #getKeyBytes(String)
     */
    protected byte[] encode(final Object obj) {
        return obj == null ? N.EMPTY_BYTE_ARRAY : kryoParser.encode(obj);
    }

    /**
     * Deserializes bytes to an object using Kryo.
     * This method reverses the serialization performed by {@link #encode(Object)}.
     * Empty byte arrays are decoded as {@code null}.
     *
     * @param bytes the byte array to decode
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