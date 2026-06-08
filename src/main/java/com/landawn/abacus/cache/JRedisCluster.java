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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.util.AddrUtil;
import com.landawn.abacus.util.N;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.RedisClusterClient;

/**
 * A Redis-based distributed cache client implementation backed by a <b>Redis Cluster</b>.
 * Unlike {@link JRedis} — which shards client-side across independent standalone servers — this class
 * targets a Redis Cluster, where the servers cooperate and shard data themselves by hash slot. A
 * single {@link RedisClusterClient} discovers the cluster topology from the supplied seed nodes and
 * routes every command to the slot-owning node server-side (following {@code MOVED}/{@code ASK}
 * redirects). Objects are serialized using Kryo (see {@link AbstractJedisCacheClient}).
 *
 * <p><b>When to use this vs {@link JRedis}:</b>
 * <ul>
 *   <li>{@link JRedis} — several <em>standalone</em> Redis servers that do not know about each other;
 *       this client distributes keys across them with client-side CRC-32 hashing.</li>
 *   <li>{@link JRedisCluster} — a single logical <em>Redis Cluster</em> (servers started in cluster
 *       mode); the cluster shards by hash slot and this client follows the cluster protocol. It will
 *       <b>not</b> work against standalone servers that are not in cluster mode.</li>
 * </ul>
 *
 * <p><b>Seed nodes:</b> the server URL is a comma-separated list of {@code host:port} cluster nodes
 * used only to bootstrap topology discovery; you need not list every node. The client maintains the
 * full slot-to-node map internally and refreshes it as the cluster changes.
 *
 * <p><b>Thread Safety:</b> This client is thread-safe. The single {@link RedisClusterClient} maintains
 * an internal connection pool per node and transparently borrows and returns connections per command,
 * so it may be freely shared across threads.
 *
 * <p><b>flushAll:</b> {@link #flushAll()} broadcasts {@code FLUSHALL} to every primary (master) node
 * in the cluster. If any master fails, Jedis raises a broadcast exception after attempting them all.
 *
 * <p><b>Usage Examples:</b>
 * <pre>{@code
 * // Connect to a Redis Cluster via a few seed nodes
 * JRedisCluster<User> cache = new JRedisCluster<>("10.0.0.1:7000,10.0.0.2:7000,10.0.0.3:7000");
 *
 * // Store and retrieve objects (routing is handled server-side by the cluster)
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
 * @see JRedis
 * @see RedisClusterClient
 */
public class JRedisCluster<T> extends AbstractJedisCacheClient<T> {

    private static final Logger logger = LoggerFactory.getLogger(JRedisCluster.class);

    private final RedisClusterClient cluster;

    /**
     * Creates a new JRedisCluster instance with the default timeout.
     * The server URL should contain comma-separated host:port pairs for one or more Redis Cluster
     * seed nodes. The cluster topology is discovered automatically; you need not enumerate every node.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // A single seed node is enough to discover the whole cluster
     * JRedisCluster<User> cache = new JRedisCluster<>("10.0.0.1:7000");        // uses DEFAULT_TIMEOUT
     * String url = cache.serverUrl();                                          // returns "10.0.0.1:7000" (verbatim)
     *
     * // Several seed nodes for resilient bootstrapping
     * JRedisCluster<User> ha = new JRedisCluster<>("10.0.0.1:7000,10.0.0.2:7000,10.0.0.3:7000");
     *
     * // Negative: null serverUrl is rejected up-front
     * JRedisCluster<User> bad = new JRedisCluster<>((String) null);           // throws IllegalArgumentException
     *
     * // Negative: blank serverUrl is rejected (checkArgNotBlank)
     * JRedisCluster<User> blank = new JRedisCluster<>("   ");                  // throws IllegalArgumentException
     * }</pre>
     *
     * @param serverUrl the Redis Cluster seed node(s) in format "host1:port1,host2:port2,...". Must not be {@code null}, empty, or blank.
     * @throws IllegalArgumentException if {@code serverUrl} is {@code null}, empty, blank, or contains no valid server addresses
     * @see #JRedisCluster(String, long)
     */
    public JRedisCluster(final String serverUrl) {
        this(serverUrl, DEFAULT_TIMEOUT);
    }

    /**
     * Creates a new JRedisCluster instance with a specified timeout.
     * The server URL should contain comma-separated host:port pairs for one or more Redis Cluster
     * seed nodes. The timeout applies to both connection establishment and socket read/write
     * operations against every cluster node.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Cluster cache with a 5 second timeout
     * JRedisCluster<Data> cache = new JRedisCluster<>("10.0.0.1:7000,10.0.0.2:7000", 5000);
     * String url = cache.serverUrl();                                          // returns the verbatim seed list
     *
     * // Use the cache
     * Data data = new Data("value");
     * cache.set("key", data, 7200000);                                        // Cache for 2 hours; returns true
     * Data retrieved = cache.get("key");                                      // the cached Data, or null
     *
     * // Negative: a non-positive timeout is rejected (checkArgPositive)
     * JRedisCluster<Data> zero = new JRedisCluster<>("10.0.0.1:7000", 0);     // throws IllegalArgumentException
     *
     * // Negative: a timeout above Integer.MAX_VALUE ms cannot fit the int Jedis API
     * JRedisCluster<Data> tooBig = new JRedisCluster<>("10.0.0.1:7000", Integer.MAX_VALUE + 1L);   // throws IllegalArgumentException
     * }</pre>
     *
     * @param serverUrl the Redis Cluster seed node(s) in format "host1:port1,host2:port2,...". Must not be {@code null}, empty, or blank.
     * @param timeout the connection and socket timeout in milliseconds. Must be positive and must not exceed {@link Integer#MAX_VALUE} (since the underlying Jedis API accepts an {@code int} timeout).
     * @throws IllegalArgumentException if {@code serverUrl} is {@code null}, empty, blank, or contains no valid server addresses,
     *         or if {@code timeout} is not positive or exceeds {@link Integer#MAX_VALUE}
     * @see #JRedisCluster(String)
     */
    public JRedisCluster(final String serverUrl, final long timeout) {
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

        final Set<HostAndPort> nodes = new LinkedHashSet<>(addressList.size());

        for (final InetSocketAddress addr : addressList) {
            // Use getHostString() (the literal host or IP) rather than getHostName(), which performs a
            // blocking reverse-DNS lookup that can fail or hang during bootstrap.
            nodes.add(new HostAndPort(addr.getHostString(), addr.getPort()));
        }

        cluster = RedisClusterClient.builder().nodes(nodes).clientConfig(clientConfig).build();
    }

    /**
     * Creates a JRedisCluster around an already-built {@link RedisClusterClient}.
     * The public constructors build the cluster client themselves (which eagerly discovers the cluster
     * topology); this constructor takes a ready client instead, so it does not open any connection on
     * its own. It is used to inject a client in tests, and could back a "bring your own client"
     * overload. This instance takes ownership of {@code cluster} and closes it on {@link #disconnect()}.
     *
     * @param serverUrl the seed node URL(s) to report via {@link #serverUrl()}; must not be {@code null}, empty, or blank
     * @param cluster the pre-built cluster client to use; must not be {@code null}
     * @throws IllegalArgumentException if {@code serverUrl} is {@code null}/empty/blank or {@code cluster} is {@code null}
     */
    JRedisCluster(final String serverUrl, final RedisClusterClient cluster) {
        super(serverUrl);

        this.cluster = N.checkArgNotNull(cluster, "cluster");
    }

    /**
     * Returns the single cluster client for every key. A {@link RedisClusterClient} routes commands to
     * the slot-owning node server-side, so the key argument is intentionally ignored here (no
     * client-side shard selection is performed).
     *
     * @param keyBytes the UTF-8 encoded key bytes (ignored; the cluster routes by hash slot)
     * @return the {@link RedisClusterClient}, never {@code null}
     */
    @Override
    protected RedisClusterClient clientFor(final byte[] keyBytes) {
        return cluster;
    }

    /**
     * Removes all keys from the entire Redis Cluster.
     * This is a destructive operation that affects all data across all nodes. Use with extreme caution
     * in production environments.
     *
     * <p><b>Redis-specific behavior:</b> {@code FLUSHALL} is broadcast to every primary (master) node
     * in the cluster (replicas are not targeted, so no {@code READONLY} errors occur). If one or more
     * masters fail, Jedis raises a broadcast exception <em>after</em> attempting every master, so a
     * single failing node does not prevent the others from being flushed.
     *
     * <p><b>Warning:</b> This affects ALL databases on every cluster node. If other applications share
     * the cluster, their data will also be deleted.
     *
     * @throws RuntimeException if flushing one or more cluster nodes fails (typically a
     *         {@code JedisBroadcastException} reporting the per-node outcomes)
     * @see #disconnect()
     */
    @Override
    public void flushAll() {
        // RedisClusterClient.flushAll() is a broadcast command: Jedis sends FLUSHALL to every primary
        // node and aggregates the per-node results, so no manual node iteration is required here.
        cluster.flushAll();
    }

    /**
     * Closes the underlying {@link RedisClusterClient}, shutting down the connection pools for every
     * cluster node. Best-effort: a failure is logged at WARN level. Invoked once by the idempotent
     * {@link #disconnect()} template.
     */
    @Override
    protected void closeClients() {
        try {
            cluster.close();
        } catch (final RuntimeException e) {
            if (logger.isWarnEnabled()) {
                logger.warn("Failed to close the Redis Cluster client during disconnect()", e);
            }
        }
    }
}
