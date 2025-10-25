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
     *
     * @param serverUrl the Redis server URL(s) in format "host1:port1,host2:port2,..."
     */
    public JRedis(final String serverUrl) {
        this(serverUrl, DEFAULT_TIMEOUT);
    }

    /**
     * Creates a new JRedis instance with a specified timeout.
     * The server URL should contain comma-separated host:port pairs for multiple Redis instances.
     * The timeout applies to connection and socket operations.
     *
     * @param serverUrl the Redis server URL(s) in format "host1:port1,host2:port2,..."
     * @param timeout the connection timeout in milliseconds
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
     * Retrieves an object from the cache by its key.
     * The object is deserialized from its binary representation using Kryo.
     *
     * @param key the cache key
     * @return the cached object, or null if not found or expired
     */
    @Override
    public T get(final String key) {
        return decode(jedis.get(getKeyBytes(key)));
    }

    /**
     * Stores an object in the cache with a specified time-to-live.
     * The object is serialized using Kryo before storage.
     *
     * @param key the cache key
     * @param obj the object to cache
     * @param liveTime the time-to-live in milliseconds
     * @return true if the operation was successful
     */
    @Override
    public boolean set(final String key, final T obj, final long liveTime) {
        return "OK".equals(jedis.setex(getKeyBytes(key), toSeconds(liveTime), encode(obj)));
    }

    /**
     * Removes an object from the cache.
     *
     * @param key the cache key to delete
     * @return true if the operation was successful
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
     * @param key the cache key
     * @return the value after increment
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
     * @param key the cache key
     * @param delta the increment amount
     * @return the value after increment
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
     * @param key the cache key
     * @return the value after decrement
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
     * @param key the cache key
     * @param delta the decrement amount
     * @return the value after decrement
     */
    @Override
    public long decr(final String key, final int delta) {
        return jedis.decrBy(getKeyBytes(key), delta);
    }

    /**
     * Flushes all data from all connected Redis instances.
     * This operation removes all keys from all databases on all shards.
     * Use with caution as this is a destructive operation.
     */
    @Override
    public void flushAll() {
        final Collection<Jedis> allShards = jedis.getAllShards();

        for (final Jedis j : allShards) {
            j.flushAll();
        }
    }

    /**
     * Disconnects from all Redis instances.
     * After calling this method, the cache client cannot be used anymore.
     */
    @Override
    public void disconnect() {
        jedis.disconnect();
    }

    /**
     * Converts a string key to UTF-8 encoded bytes for Redis operations.
     *
     * @param key the string key
     * @return UTF-8 encoded byte array
     */
    protected byte[] getKeyBytes(final String key) {
        return key.getBytes(Charsets.UTF_8);
    }

    /**
     * Serializes an object to bytes using Kryo.
     * Null objects are encoded as empty byte arrays.
     *
     * @param obj the object to encode
     * @return serialized byte array
     */
    protected byte[] encode(final Object obj) {
        return obj == null ? N.EMPTY_BYTE_ARRAY : kryoParser.encode(obj);
    }

    /**
     * Deserializes bytes to an object using Kryo.
     * Empty byte arrays are decoded as null.
     *
     * @param bytes the byte array to decode
     * @return the deserialized object
     */
    protected T decode(final byte[] bytes) {
        if (bytes == null || N.isEmpty(bytes)) {
            return null;
        }
        return kryoParser.decode(bytes);
    }
}