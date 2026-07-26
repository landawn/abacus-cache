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
import com.landawn.abacus.util.N;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.RedisClusterClient;

/**
 * A Redis-based distributed cache client implementation backed by a <b>Redis Cluster</b>.
 * Unlike {@link JRedis} — which shards client-side across independent standalone servers — this class
 * targets a Redis Cluster, where the servers cooperate and shard data themselves by hash slot. A
 * single {@link RedisClusterClient} discovers the cluster topology from the supplied seed nodes and
 * uses it to send every command directly to the slot-owning node (following {@code MOVED}/{@code ASK}
 * redirects when its cached topology is stale). Objects are serialized using Kryo (see
 * {@link AbstractJedisCacheClient}).
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
 * Instances are intended to be long-lived and application-scoped; optionally call
 * {@link #disconnect()} once during application shutdown rather than around each cache use.
 *
 * <p><b>flushAll:</b> {@link #flushAll()} broadcasts {@code FLUSHALL} to every primary (master) node
 * in the cluster. If any master fails, Jedis raises a broadcast exception after attempting them all.
 *
 * <p><b>Usage Examples:</b>
 * <pre>{@code
 * // Connect to a Redis Cluster via a few seed nodes
 * JRedisCluster<User> cache = new JRedisCluster<>("10.0.0.1:7000,10.0.0.2:7000,10.0.0.3:7000");
 *
 * // Store and retrieve objects (each key is routed to its slot-owning node by hash slot)
 * User user = new User("John", "john@example.com");
 * cache.put("user:123", user, 3600000);   // Cache for 1 hour
 * User cached = cache.get("user:123");
 *
 * // Use atomic counters
 * long pageViews = cache.incr("page:views");
 *
 * // Retain and share the client; optionally disconnect it during application shutdown.
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
     * // Retain and share cache/ha; optionally disconnect them during application shutdown.
     *
     * // Negative: null serverUrl is rejected up-front
     * JRedisCluster<User> bad = new JRedisCluster<>((String) null);           // throws IllegalArgumentException
     *
     * // Negative: blank serverUrl is rejected (checkArgNotBlank)
     * JRedisCluster<User> blank = new JRedisCluster<>("   ");                  // throws IllegalArgumentException
     * }</pre>
     *
     * <p><b>Implementation note:</b> if topology discovery fails partway (after Jedis has created
     * per-node connection pools internally but before the client is fully constructed), the
     * builder throws and this wrapper receives no handle it could close, so any pools created
     * during the failed discovery are stranded inside Jedis until they are garbage-collected.
     * This is internal to Jedis and cannot be remediated from this wrapper; avoid tight
     * construction-retry loops against a partially unavailable cluster.
     *
     * @param serverUrl the Redis Cluster seed node(s) in format "host1:port1,host2:port2,...". Must not be {@code null}, empty, or blank.
     * @throws IllegalArgumentException if {@code serverUrl} is {@code null}, empty, blank, or contains no valid server addresses
     * @throws RuntimeException if seed resolution, initial topology discovery, or client construction fails
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
     * cache.put("key", data, 7200000);                                        // Cache for 2 hours; returns true
     * Data retrieved = cache.get("key");                                      // the cached Data, or null
     *
     * // Negative: a non-positive timeout is rejected (checkArgPositive)
     * JRedisCluster<Data> zero = new JRedisCluster<>("10.0.0.1:7000", 0);     // throws IllegalArgumentException
     *
     * // Negative: a timeout above Integer.MAX_VALUE ms cannot fit the int Jedis API
     * JRedisCluster<Data> tooBig = new JRedisCluster<>("10.0.0.1:7000", Integer.MAX_VALUE + 1L);   // throws IllegalArgumentException
     * }</pre>
     *
     * <p><b>Implementation note:</b> a partway topology-discovery failure can strand
     * Jedis-internal connection pools; see {@link #JRedisCluster(String)} for details.
     *
     * <p><b>Cluster retry amplification:</b> the timeout is per attempt. The underlying cluster
     * client retries an operation across redirects and failures (5 attempts by default, with a
     * total retry budget of about 5&times; the socket timeout), so a single operation against a
     * failing cluster can block for several multiples of the configured timeout — unlike
     * {@link JRedis}, which makes exactly one attempt.
     *
     * @param serverUrl the Redis Cluster seed node(s) in format "host1:port1,host2:port2,...". Must not be {@code null}, empty, or blank.
     * @param timeout the connection and socket timeout in milliseconds. Must be positive and must not exceed {@link Integer#MAX_VALUE} (since the underlying Jedis API accepts an {@code int} timeout).
     * @throws IllegalArgumentException if {@code serverUrl} is {@code null}, empty, blank, or contains no valid server addresses,
     *         or if {@code timeout} is not positive or exceeds {@link Integer#MAX_VALUE}
     * @throws RuntimeException if seed resolution, initial topology discovery, or client construction fails
     * @see #JRedisCluster(String)
     */
    public JRedisCluster(final String serverUrl, final long timeout) {
        super(serverUrl);

        final JedisClientConfig clientConfig = buildClientConfig(timeout);
        final List<InetSocketAddress> addressList = resolveServerAddresses(serverUrl);

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
     * the slot-owning node by hash slot, so the key argument is intentionally ignored here (this class
     * performs no client-side shard selection of its own).
     *
     * @param keyBytes the UTF-8 encoded key bytes (ignored; the cluster routes by hash slot)
     * @return the {@link RedisClusterClient}, never {@code null}
     */
    @Override
    protected RedisClusterClient clientFor(final byte[] keyBytes) {
        return cluster;
    }

    /**
     * Requests removal of all keys from every primary in the Redis Cluster.
     * A successful broadcast removes all data across the cluster. Use with extreme caution in
     * production environments because a partially failed broadcast may still flush some primaries.
     *
     * <p><b>Redis-specific behavior:</b> {@code FLUSHALL} is broadcast to every primary (master) node
     * in the cluster (replicas are not targeted, so no {@code READONLY} errors occur). If one or more
     * masters fail, Jedis raises a broadcast exception <em>after</em> attempting every master, so a
     * single failing node does not prevent the others from being flushed.
     *
     * <p><b>&#9888;&#65039; Destructive operation:</b> This removes all keys from database 0 on every primary
     * node. If other applications share the cluster, their data is also deleted.
     *
     * @throws RuntimeException if flushing one or more cluster nodes fails (typically a
     *         {@code JedisBroadcastException} reporting the per-node outcomes)
     * @throws IllegalStateException if this client has been disconnected or is being disconnected
     * @see #disconnect()
     */
    @Override
    public void flushAll() {
        assertNotShutdown();

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
