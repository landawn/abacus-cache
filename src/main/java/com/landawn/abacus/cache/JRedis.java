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
import java.util.List;
import java.util.zip.CRC32;

import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.util.AddrUtil;
import com.landawn.abacus.util.N;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.RedisClient;

/**
 * A Redis-based distributed cache client implementation using Jedis with client-side sharding.
 * This class provides a distributed caching solution that connects to one or more <b>standalone</b>
 * Redis instances for horizontal scaling and data distribution. Objects are serialized using Kryo for
 * efficient storage and retrieval (see {@link AbstractJedisCacheClient}). For a <b>Redis Cluster</b>
 * deployment (where the servers cooperate and shard by hash slot), use {@link JRedisCluster} instead.
 *
 * <p><b>Key Features:</b>
 * <ul>
 *   <li>Client-side sharding across multiple standalone Redis instances for horizontal scaling</li>
 *   <li>A thread-safe, internally pooled connection ({@link RedisClient}) per shard</li>
 *   <li>Efficient object serialization using Kryo parser</li>
 *   <li>Atomic operations for counters (incr/decr)</li>
 *   <li>TTL (time-to-live) support for automatic expiration</li>
 * </ul>
 *
 * <p><b>Sharding:</b> Each configured {@code host:port} becomes an independent shard backed by its
 * own {@link RedisClient} client. A key is routed to a shard by hashing its UTF-8 bytes with CRC-32
 * modulo the number of shards, so a given key always maps to the same shard within a fixed topology.
 * The mapping is purely client-side: the standalone Redis servers do not coordinate with each other.
 * Changing the number of servers changes the mapping for many keys — acceptable for a cache, where a
 * miss simply triggers a reload. With a single server there is exactly one shard and no hashing is
 * performed.
 *
 * <p><b>Thread Safety:</b> This client is thread-safe. Each shard is backed by a {@link RedisClient}
 * client, which maintains its own internal connection pool and transparently borrows and returns a
 * connection for each command, so the client may be freely shared across threads.
 *
 * <p><b>Connection pooling:</b> Each shard's {@link RedisClient} uses the default connection pool
 * configuration (Apache Commons Pool). The pool is created lazily — constructing a {@code JRedis}
 * does not open a socket; connections are established on first use. If a Redis server is unreachable,
 * the failure surfaces on the first operation routed to that shard, not at construction time.
 *
 * <p><b>Usage Examples:</b>
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
 * @see AbstractJedisCacheClient
 * @see JRedisCluster
 * @see RedisClient
 */
public class JRedis<T> extends AbstractJedisCacheClient<T> {

    private static final Logger logger = LoggerFactory.getLogger(JRedis.class);

    /** One internally-pooled client per shard, in the order the addresses were supplied. Never empty. */
    private final List<RedisClient> clients;

    /**
     * Creates a new JRedis instance with the default timeout.
     * The server URL should contain comma-separated host:port pairs for one or more Redis instances.
     * Data is distributed across all specified Redis instances using client-side sharding for
     * horizontal scaling.
     *
     * <p>Each Redis instance becomes a shard backed by its own connection pool. When storing or
     * retrieving data, the owning shard is determined by hashing the key (see the class
     * documentation), ensuring a stable distribution of keys across all servers.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Single Redis instance
     * JRedis<User> cache = new JRedis<>("localhost:6379");           // uses DEFAULT_TIMEOUT
     * String url = cache.serverUrl();                                // returns "localhost:6379" (preserved verbatim)
     *
     * // Multiple Redis instances for sharding
     * JRedis<User> sharded = new JRedis<>("localhost:6379,localhost:6380,localhost:6381");
     *
     * // Use the cache
     * User user = new User("Alice", "alice@example.com");
     * cache.set("user:123", user, 3600000);                         // returns true on success
     *
     * // Negative: null serverUrl is rejected up-front
     * JRedis<User> bad = new JRedis<>((String) null);               // throws IllegalArgumentException
     *
     * // Negative: blank serverUrl is rejected (checkArgNotBlank)
     * JRedis<User> blank = new JRedis<>("   ");                     // throws IllegalArgumentException
     * }</pre>
     *
     * @param serverUrl the Redis server URL(s) in format "host1:port1,host2:port2,...". Must not be {@code null}, empty, or blank.
     * @throws IllegalArgumentException if {@code serverUrl} is {@code null}, empty, blank, or contains no valid server addresses
     * @see #JRedis(String, long)
     */
    public JRedis(final String serverUrl) {
        this(serverUrl, DEFAULT_TIMEOUT);
    }

    /**
     * Creates a new JRedis instance with a specified timeout.
     * The server URL should contain comma-separated host:port pairs for one or more Redis instances.
     * The timeout applies to both connection establishment and socket read/write operations on every
     * shard. Data is distributed across all specified Redis instances using client-side sharding.
     *
     * <p>The timeout value affects network operations with Redis servers. If a Redis operation
     * takes longer than the specified timeout, a timeout exception will be thrown. Choose an
     * appropriate timeout based on your network latency and expected operation duration.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Cache with 5 second timeout
     * JRedis<Data> cache = new JRedis<>("redis1:6379,redis2:6379", 5000);   // timeout in milliseconds
     * String url = cache.serverUrl();                                       // returns "redis1:6379,redis2:6379"
     *
     * // Use the cache
     * Data data = new Data("value");
     * cache.set("key", data, 7200000);                                      // Cache for 2 hours; returns true
     * Data retrieved = cache.get("key");                                    // the cached Data, or null
     *
     * // Negative: a non-positive timeout is rejected (checkArgPositive)
     * JRedis<Data> zero = new JRedis<>("localhost:6379", 0);              // throws IllegalArgumentException
     *
     * // Negative: a timeout above Integer.MAX_VALUE ms cannot fit the int Jedis API
     * JRedis<Data> tooBig = new JRedis<>("localhost:6379", Integer.MAX_VALUE + 1L);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param serverUrl the Redis server URL(s) in format "host1:port1,host2:port2,...". Must not be {@code null}, empty, or blank.
     * @param timeout the connection and socket timeout in milliseconds. Must be positive and must not exceed {@link Integer#MAX_VALUE} (since the underlying Jedis API accepts an {@code int} timeout).
     * @throws IllegalArgumentException if {@code serverUrl} is {@code null}, empty, blank, or contains no valid server addresses,
     *         or if {@code timeout} is not positive or exceeds {@link Integer#MAX_VALUE}
     * @see #JRedis(String)
     */
    public JRedis(final String serverUrl, final long timeout) {
        super(serverUrl);

        N.checkArgPositive(timeout, "timeout");

        if (timeout > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("timeout exceeds maximum value: " + timeout + " (max: " + Integer.MAX_VALUE + ")");
        }

        final List<InetSocketAddress> addressList = AddrUtil.getAddressList(serverUrl);

        if (N.isEmpty(addressList)) {
            throw new IllegalArgumentException("No valid server addresses found in: " + serverUrl);
        }

        final int timeoutMillis = (int) timeout;

        final JedisClientConfig clientConfig = DefaultJedisClientConfig.builder()
                .connectionTimeoutMillis(timeoutMillis)
                .socketTimeoutMillis(timeoutMillis)
                .build();

        final List<RedisClient> shardClients = new ArrayList<>(addressList.size());

        try {
            for (final InetSocketAddress addr : addressList) {
                // Use getHostString() (returns the literal host or IP) instead of getHostName(), which
                // performs a reverse DNS lookup. Reverse DNS can block startup, fail if no PTR record
                // exists, and produce shard hash keys that differ between processes whose resolvers
                // disagree — silently breaking sharding consistency.
                shardClients.add(RedisClient.builder().hostAndPort(new HostAndPort(addr.getHostString(), addr.getPort())).clientConfig(clientConfig).build());
            }
        } catch (final RuntimeException | Error e) {
            // If a later shard's construction fails, close the already-built ones: each owns a
            // connection pool with a scheduled evictor task that would otherwise survive until JVM
            // exit with no reference left to close it.
            for (final RedisClient built : shardClients) {
                try {
                    built.close();
                } catch (final RuntimeException suppressed) {
                    e.addSuppressed(suppressed);
                }
            }

            throw e;
        }

        clients = shardClients;
    }

    /**
     * Selects the pooled client for the shard that owns the given key.
     * With a single shard this is always the sole client (no hashing). With multiple shards the key's
     * UTF-8 bytes are hashed with CRC-32 modulo the shard count, giving a deterministic, process-stable
     * mapping (CRC-32 does not depend on JVM identity hashing or locale).
     *
     * @param keyBytes the UTF-8 encoded key bytes; must not be {@code null}
     * @return the {@link RedisClient} for the shard that owns the key, never {@code null}
     */
    @Override
    protected RedisClient clientFor(final byte[] keyBytes) {
        final int shardCount = clients.size();

        if (shardCount == 1) {
            return clients.get(0);
        }

        final CRC32 crc = new CRC32();
        crc.update(keyBytes);

        return clients.get(Math.floorMod(crc.getValue(), shardCount));
    }

    /**
     * Removes all keys from all connected standalone Redis instances.
     * This is a destructive operation that affects all data across all shards and all databases
     * within each Redis instance. Use with extreme caution in production environments.
     *
     * <p><b>Redis-specific behavior:</b> This operation issues the Redis FLUSHALL command on each
     * shard. It removes all keys from all databases (not just the currently selected database). If a
     * FLUSHALL on one shard fails, the remaining shards are still attempted (each failure is logged at
     * WARN level) and the <em>first</em> encountered exception is rethrown after all shards have been
     * processed.
     *
     * <p><b>Warning:</b> This operation affects ALL databases on each Redis instance, not just the one
     * being used by this client. If other applications share the same Redis instances, their data will
     * also be deleted.
     *
     * @throws RuntimeException the first exception encountered while flushing any shard (all remaining
     *         shards are still attempted before the exception is rethrown)
     * @see #disconnect()
     */
    @Override
    public void flushAll() {
        RuntimeException firstException = null;

        for (final RedisClient jedis : clients) {
            try {
                jedis.flushAll();
            } catch (final RuntimeException e) {
                // Continue flushing the remaining shards; the first failure is rethrown
                // after the loop. Log every shard failure so errors on later shards
                // (which are otherwise swallowed) remain visible.
                if (logger.isWarnEnabled()) {
                    logger.warn("Failed to flush a Redis shard during flushAll(); continuing with remaining shards", e);
                }

                if (firstException == null) {
                    firstException = e;
                }
            }
        }

        if (firstException != null) {
            throw firstException;
        }
    }

    /**
     * Closes every shard's {@link RedisClient}, shutting down its connection pool. Best-effort: a
     * failure closing one shard is logged at WARN level and does not prevent the remaining shards from
     * being closed. Invoked once by the idempotent {@link #disconnect()} template.
     */
    @Override
    protected void closeClients() {
        for (final RedisClient jedis : clients) {
            try {
                jedis.close();
            } catch (final RuntimeException e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Failed to close a Redis shard client during disconnect(); continuing with remaining shards", e);
                }
            }
        }
    }
}
