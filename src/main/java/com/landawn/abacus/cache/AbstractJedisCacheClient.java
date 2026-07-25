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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.landawn.abacus.parser.KryoParser;
import com.landawn.abacus.parser.ParserFactory;
import com.landawn.abacus.util.AddrUtil;
import com.landawn.abacus.util.Charsets;
import com.landawn.abacus.util.N;

import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.SetParams;

/**
 * Abstract base class for Jedis-backed distributed cache clients that serialize values with Kryo.
 * It implements all the per-key cache operations once, against the {@link UnifiedJedis} command
 * surface, and delegates the single backend-specific decision — <em>which</em> client should run a
 * command for a given key — to the abstract {@link #clientFor(byte[])} hook. This lets standalone
 * client-side sharding ({@link JRedis}) and Redis Cluster ({@link JRedisCluster}) share identical
 * command, serialization, and key-encoding logic.
 *
 * <p>In Jedis 7.x both the standalone client ({@code RedisClient}) and the cluster client
 * ({@code RedisClusterClient}) extend {@link UnifiedJedis}, so the GET/SET/DEL/INCR/DECR command
 * bodies are the same regardless of topology. The concrete subclass decides routing:
 * <ul>
 *   <li>{@link JRedis} hashes the key client-side and returns the owning shard's client;</li>
 *   <li>{@link JRedisCluster} returns its single cluster client, which routes by hash slot
 *       server-side.</li>
 * </ul>
 *
 * <p><b>Serialization:</b> values are encoded with a shared {@link KryoParser}. {@code null} values
 * are stored as an empty byte array and decode back to {@code null}. Keys are encoded as UTF-8.
 *
 * <p><b>Redis-Specific Behaviors</b> (common to both subclasses):
 * <ul>
 *   <li>Increment/decrement operations auto-initialize non-existent keys to 0</li>
 *   <li>Decrement operations can result in negative values (unlike Memcached)</li>
 *   <li>All string keys are encoded using UTF-8</li>
 *   <li>Counter keys and object keys are disjoint worlds: {@code incr}/{@code decr} store raw
 *       ASCII digits that {@code get} cannot deserialize (Kryo decode fails), and {@code incr}/
 *       {@code decr} on a {@code put}-stored value always fails because its Kryo bytes are not a
 *       Redis integer — even when the logical value is a number. Read counters back with
 *       {@code incr(key, 0)} or {@code decr(key, 0)}, never with {@code get}. Caveat: reading an
 *       absent or expired counter this way re-creates it as a persistent key with value 0
 *       (Redis {@code INCRBY} auto-initializes missing keys and sets no TTL)</li>
 * </ul>
 *
 * <p><b>Thread Safety:</b> Thread-safe. Each command is dispatched through a {@link UnifiedJedis}
 * client that maintains its own internal connection pool and transparently borrows and returns a
 * connection per command, so instances may be freely shared across threads. {@link #disconnect()}
 * publishes the shutdown state before closing those pools, so operations started after shutdown
 * begins fail with {@link IllegalStateException}. An operation that already passed its lifecycle
 * check may still race with shutdown and surface the underlying Jedis close/connection exception;
 * callers should quiesce users before disconnecting when that distinction matters.
 *
 * @param <T> the type of objects to be cached
 * @see JRedis
 * @see JRedisCluster
 * @see AbstractDistributedCacheClient
 */
abstract class AbstractJedisCacheClient<T> extends AbstractDistributedCacheClient<T> {

    /**
     * The Kryo wire format is this client family's documented serialization contract, so unlike
     * the backends that fall back to another format, a missing Kryo dependency must fail — but
     * with an actionable message instead of a bare {@code NoClassDefFoundError} at class load.
     */
    private static final KryoParser KRYO_PARSER = createRequiredKryoParser();

    private static KryoParser createRequiredKryoParser() {
        if (!ParserFactory.isKryoParserAvailable()) {
            throw new IllegalStateException("Kryo is required by the Jedis cache clients (JRedis/JRedisCluster) but is not on the classpath;"
                    + " add the optional com.esotericsoftware:kryo dependency");
        }

        return ParserFactory.createKryoParser();
    }

    private volatile boolean isShutdown = false;

    /**
     * Constructs the base client with the specified server URL.
     *
     * @param serverUrl the server URL(s); must not be {@code null}, empty, or blank
     * @throws IllegalArgumentException if {@code serverUrl} is {@code null}, empty, or blank
     */
    protected AbstractJedisCacheClient(final String serverUrl) {
        super(serverUrl);
    }

    /**
     * Validates and converts a constructor timeout, shared by both concrete constructors so their
     * validation stays identical.
     *
     * @param timeout the connection and socket timeout in milliseconds
     * @return a client config with both the connection and socket timeout set to {@code timeout}
     * @throws IllegalArgumentException if {@code timeout} is not positive or exceeds
     *         {@link Integer#MAX_VALUE} (the underlying Jedis API accepts an {@code int} timeout)
     */
    protected static JedisClientConfig buildClientConfig(final long timeout) {
        N.checkArgPositive(timeout, "timeout");

        if (timeout > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("timeout exceeds maximum value: " + timeout + " (max: " + Integer.MAX_VALUE + ")");
        }

        final int timeoutMillis = (int) timeout;

        return DefaultJedisClientConfig.builder().connectionTimeoutMillis(timeoutMillis).socketTimeoutMillis(timeoutMillis).build();
    }

    /**
     * Parses a constructor server URL into socket addresses, shared by both concrete constructors
     * so their validation stays identical.
     *
     * @param serverUrl the server URL(s) in {@code host1:port1,host2:port2,...} format
     * @return the parsed addresses; never empty
     * @throws IllegalArgumentException if {@code serverUrl} is malformed or yields no addresses
     */
    protected static List<InetSocketAddress> resolveServerAddresses(final String serverUrl) {
        final List<InetSocketAddress> addressList = AddrUtil.getAddressList(serverUrl);

        // Currently unreachable (getAddressList throws rather than returning an empty list); kept
        // deliberately as a guard against that contract changing in a future abacus-common release.
        if (N.isEmpty(addressList)) {
            throw new IllegalArgumentException("No valid server addresses found in: " + serverUrl);
        }

        return addressList;
    }

    /**
     * Returns the Jedis client that should execute a command for the given key.
     * This is the single point of backend variation: {@link JRedis} maps the key to one of several
     * standalone shards via client-side hashing, while {@link JRedisCluster} returns its single
     * cluster client (which performs server-side, hash-slot routing and therefore ignores the
     * client-side mapping).
     *
     * @param keyBytes the UTF-8 encoded key bytes; never {@code null}
     * @return the {@link UnifiedJedis} client that owns the key, never {@code null}
     */
    protected abstract UnifiedJedis clientFor(byte[] keyBytes);

    /**
     * Retrieves a value from the cache by its key.
     * The value is deserialized from its binary representation using Kryo parser.
     *
     * <p><b>Redis-specific behavior:</b> This operation uses the Redis GET command. If the key
     * does not exist or has expired, {@code null} is returned. The operation is O(1) time complexity.
     *
     * <p><b>&#9888;&#65039; Counter keys cannot be read with this method:</b> keys created by {@code incr}/{@code decr}
     * hold raw ASCII digits, not Kryo-serialized bytes, so deserialization fails. Read counters back
     * with {@code incr(key, 0)} (or {@code decr(key, 0)}) instead — but note that reading an absent
     * or expired counter this way re-creates it as a persistent key with value 0.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * User user = cache.get("user:123");                            // the cached User, or null if absent
     *
     * // Edge: a missing or expired key returns null (not an exception)
     * User missing = cache.get("does:not:exist");                   // returns null
     *
     * // Negative: a null key is rejected
     * User bad = cache.get(null);                                   // throws IllegalArgumentException
     *
     * // Negative: a counter key created by incr()/decr() cannot be deserialized by get()
     * cache.incr("page:views");
     * cache.get("page:views");                                      // throws RuntimeException (Kryo decode)
     * }</pre>
     *
     * @param key the cache key whose associated value is to be retrieved. Must not be {@code null}.
     * @return the cached value, or {@code null} if not found, expired, or evicted
     * @throws IllegalStateException if this client has been disconnected or is being disconnected
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error, timeout, or deserialization error occurs
     * @see #put(String, Object, long)
     * @see #remove(String)
     */
    @Override
    public T get(final String key) {
        assertNotShutdown();

        final byte[] keyBytes = getKeyBytes(key);

        return decode(clientFor(keyBytes).get(keyBytes));
    }

    /**
     * Retrieves multiple values from the cache, returning a map of only the keys that were found.
     * Keys that are absent, expired, or decode to {@code null} are omitted from the result.
     *
     * <p><b>Behavior:</b> This method issues one {@code GET} per <em>distinct</em> key (each routed to
     * its owning client) and assembles the results. Duplicate input keys therefore produce one map
     * entry and one backend lookup, rather than observing the same key repeatedly at different
     * instants. The keys are validated up-front, before any network call is made.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // "user:1" exists, "user:2" is missing
     * Map<String, User> found = cache.getBulk("user:1", "user:2");   // size == 1; contains only "user:1"
     *
     * // Edge: no arguments -> empty (never null) map; no GET is issued
     * Map<String, User> none = cache.getBulk();                      // returns an empty map
     *
     * // Negative: a null element throws; no GET is issued for any key
     * cache.getBulk("user:1", null, "user:3");                      // throws IllegalArgumentException
     * }</pre>
     *
     * @param keys the cache keys to retrieve; must not be {@code null} or contain {@code null} elements
     * @return a map of the found key-value pairs, never {@code null} (empty if no keys are found)
     * @throws IllegalStateException if this client has been disconnected or is being disconnected
     * @throws IllegalArgumentException if {@code keys} is {@code null} or contains a {@code null} element
     * @throws RuntimeException if a network error, timeout, or deserialization error occurs
     * @see #get(String)
     * @see #getBulk(Collection)
     */
    @Override
    public Map<String, T> getBulk(final String... keys) {
        assertNotShutdown();

        // Work from a private snapshot. The caller owns the array and may reuse or mutate it as soon
        // as this method starts; validating one view and later fetching from the caller's live array
        // could otherwise issue commands for keys that were never validated.
        N.checkArgNotNull(keys, "keys");
        final String[] keySnapshot = keys.clone();
        checkBulkKeys(keySnapshot);

        return fetchBulk(Arrays.asList(keySnapshot));
    }

    /**
     * Retrieves multiple values from the cache, returning a map of only the keys that were found.
     * Collection-based counterpart of {@link #getBulk(String...)}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * List<String> ids = List.of("user:1", "user:2", "user:3");
     * Map<String, User> users = cache.getBulk(ids);                  // contains only the keys that exist
     *
     * // Edge: empty collection -> empty (never null) map; no GET is issued
     * Map<String, User> none = cache.getBulk(List.of());            // returns an empty map
     *
     * // Negative: a null collection throws (validated before any network call)
     * cache.getBulk((Collection<String>) null);                     // throws IllegalArgumentException
     * }</pre>
     *
     * @param keys the collection of cache keys to retrieve; must not be {@code null} or contain {@code null} elements
     * @return a map of the found key-value pairs, never {@code null} (empty if no keys are found)
     * @throws IllegalStateException if this client has been disconnected or is being disconnected
     * @throws IllegalArgumentException if {@code keys} is {@code null} or contains a {@code null} element
     * @throws RuntimeException if a network error, timeout, or deserialization error occurs
     * @see #get(String)
     * @see #getBulk(String...)
     */
    @Override
    public Map<String, T> getBulk(final Collection<String> keys) {
        assertNotShutdown();

        return fetchBulk(snapshotBulkKeys(keys));
    }

    /**
     * Copies and validates a collection in one pass. Besides insulating the operation from later
     * caller mutations, the single pass avoids the validate-then-iterate time-of-check/time-of-use
     * gap that would otherwise be observable for concurrent or custom collections.
     */
    private List<String> snapshotBulkKeys(final Collection<String> keys) {
        N.checkArgNotNull(keys, "keys");

        // Do not trust keys.size() for preallocation: custom/concurrent collections may report a
        // stale or adversarially large size. ArrayList will grow according to elements actually seen.
        final List<String> keySnapshot = new ArrayList<>();
        int index = 0;

        for (final String key : keys) {
            if (key == null) {
                throw new IllegalArgumentException("'keys' cannot contain a null element at index: " + index);
            }

            keySnapshot.add(key);
            index++;
        }

        return keySnapshot;
    }

    private Map<String, T> fetchBulk(final Collection<String> keys) {
        // A map cannot represent duplicate requested keys. Fetch each logical key once as well:
        // besides avoiding redundant round trips, this prevents an internally inconsistent result
        // when the first GET finds a duplicate key and a later GET observes it as expired/missing.
        final Set<String> fetchedKeys = new HashSet<>();
        final Map<String, T> result = new HashMap<>();

        for (final String key : keys) {
            if (!fetchedKeys.add(key)) {
                continue;
            }

            final byte[] keyBytes = getKeyBytes(key);

            final T value = decode(clientFor(keyBytes).get(keyBytes));

            if (value != null) {
                result.put(key, value);
            }
        }

        return result;
    }

    /**
     * Stores a key-value pair in the cache with a specified time-to-live.
     * The value is serialized using Kryo parser before storage. If the key already exists,
     * its value and TTL will be replaced.
     *
     * <p><b>Redis-specific behavior:</b> When {@code liveTime} is positive, this operation uses the
     * Redis SET command with the {@code PX} option, which atomically sets both the value and a
     * millisecond-precision expiration time — the requested {@code liveTime} is passed unchanged,
     * with no client-side rounding to seconds. Redis may reject an extreme value that cannot be
     * represented as an absolute server-side expiration. When {@code liveTime} is 0 or negative, a
     * plain SET command is used without expiration, meaning the key will persist until explicitly
     * deleted. If the key already exists, the previous value and TTL are completely replaced.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Cache with 1 hour TTL (3600000 ms -> SET ... PX 3600000)
     * boolean success = cache.put("user:123", user, 3600000);       // returns true when Redis replies "OK"
     *
     * // Edge: liveTime <= 0 means NO expiration -> plain SET (no PX)
     * cache.put("forever", user, 0);                                // uses SET; key never auto-expires
     *
     * // Edge: a null value is allowed; it is stored as an empty byte array
     * cache.put("empty:key", (User) null, 3600000);                // returns true; get(...) later returns null
     *
     * // Negative: a null key is rejected
     * cache.put(null, user, 60000);                                // throws IllegalArgumentException
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated. Must not be {@code null}.
     * @param value the value to cache. May be {@code null} (stored as empty byte array).
     * @param liveTime the time-to-live in milliseconds. Positive values are passed unchanged to
     *        SET ... PX (subject to Redis's representable expiration range). 0 or negative means no
     *        expiration (plain SET).
     * @return {@code true} when Redis acknowledges the write with "OK" (the normal success reply). An
     *         unconditional {@code SET} either replies "OK" or fails with a {@code JedisException}, so in
     *         practice this returns {@code true} on success and throws on failure rather than returning {@code false}
     * @throws IllegalStateException if this client has been disconnected or is being disconnected
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error, timeout, or serialization error occurs
     * @see #get(String)
     * @see #remove(String)
     */
    @Override
    public boolean put(final String key, final T value, final long liveTime) {
        assertNotShutdown();

        final byte[] keyBytes = getKeyBytes(key);
        final byte[] valueBytes = encode(value);
        final UnifiedJedis jedis = clientFor(keyBytes);

        if (liveTime <= 0) {
            // liveTime <= 0 means no expiration, use a plain SET.
            return "OK".equals(jedis.set(keyBytes, valueBytes));
        }

        // SET ... PX atomically sets the value and a millisecond-precision expiration, honoring the
        // requested liveTime exactly. The previous SET ... EX (seconds) silently rounded sub-second
        // TTLs up to a full second - 10x or more longer than requested for short-lived entries.
        return "OK".equals(jedis.set(keyBytes, valueBytes, SetParams.setParams().px(liveTime)));
    }

    /**
     * Removes the mapping for a key from the cache if it is present.
     *
     * <p><b>Redis-specific behavior:</b> This method uses the Redis DEL command to remove the key.
     * The operation is O(1) time complexity. The return value reflects Redis's DEL semantics (which
     * returns the number of keys removed): {@code true} when the key existed and was removed,
     * {@code false} when the key did not exist at the time the command was issued.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * cache.put("user:123", user, 60000);                          // key now present
     * boolean removed = cache.remove("user:123");                  // returns true (DEL count == 1)
     *
     * // Edge: deleting a key that does not exist returns false (DEL count == 0)
     * boolean wasThere = cache.remove("never:set");                // returns false
     *
     * // Negative: a null key is rejected
     * cache.remove(null);                                          // throws IllegalArgumentException
     * }</pre>
     *
     * @param key the cache key whose associated value is to be removed. Must not be {@code null}.
     * @return {@code true} if Redis reported at least one key was actually removed; {@code false}
     *         if the key did not exist at the time the {@code DEL} command was issued
     * @throws IllegalStateException if this client has been disconnected or is being disconnected
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error or timeout occurs
     * @see #get(String)
     * @see #put(String, Object, long)
     */
    @Override
    public boolean remove(final String key) {
        assertNotShutdown();

        final byte[] keyBytes = getKeyBytes(key);

        return clientFor(keyBytes).del(keyBytes) > 0L;
    }

    /**
     * Atomically increments a numeric value by 1.
     *
     * <p><b>Redis-specific behavior:</b> This operation uses the Redis INCR command. If the key doesn't exist,
     * it will be automatically created with value 1 (Redis initializes to 0, then increments by 1).
     * This differs from Memcached which returns -1 for non-existent keys. The operation is O(1) time complexity.
     * If the key is not an integer or the result exceeds Redis's signed 64-bit range, an error occurs.
     *
     * <p><b>Thread Safety:</b> The Redis INCR command is atomic on the server side, so concurrent
     * increments — whether from separate client instances or multiple threads sharing this pooled
     * client — are serialized correctly by Redis and no increments are lost.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * long pageViews = cache.incr("page:views");                   // returns 1 the first time, 2 the second, ...
     *
     * // Negative: a null key is rejected
     * cache.incr(null);                                           // throws IllegalArgumentException
     *
     * // Negative: incrementing a non-integer value fails on the server
     * cache.put("name", "Alice", 0);
     * cache.incr("name");                                        // throws RuntimeException
     * }</pre>
     *
     * @param key the cache key whose associated value is to be incremented. Must not be {@code null}.
     * @return the value after increment (will be 1 if the key did not exist before)
     * @throws IllegalStateException if this client has been disconnected or is being disconnected
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error or timeout occurs, the key contains a non-integer
     *         value, or the result exceeds Redis's signed 64-bit integer range
     * @see #incr(String, long)
     * @see #decr(String)
     */
    @Override
    public long incr(final String key) {
        assertNotShutdown();

        final byte[] keyBytes = getKeyBytes(key);

        return clientFor(keyBytes).incr(keyBytes);
    }

    /**
     * Atomically increments a numeric value by a specified amount.
     *
     * <p><b>Redis-specific behavior:</b> This operation uses the Redis INCRBY command. If the key doesn't exist,
     * it will be automatically created with the delta value. The operation is O(1) time complexity.
     * If the key is not an integer or the result exceeds Redis's signed 64-bit range, an error occurs.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * long score = cache.incr("player:score:123", 10);            // returns 10 the first time
     *
     * // Edge: a delta of 0 returns the current value unchanged
     * long current = cache.incr("player:score:123", 0);
     *
     * // Negative: a negative delta is rejected by this wrapper (for cross-backend portability)
     * cache.incr("counter", -1);                                 // throws IllegalArgumentException
     * }</pre>
     *
     * @param key the cache key whose associated value is to be incremented. Must not be {@code null}.
     * @param delta the increment amount; must be non-negative. Although the Redis INCRBY command itself
     *              supports negative deltas, this implementation rejects them with an
     *              {@link IllegalArgumentException} for portability across cache backends (e.g.
     *              SpyMemcached, which also rejects negative deltas).
     * @return the value after increment (will be equal to delta if the key did not exist before)
     * @throws IllegalStateException if this client has been disconnected or is being disconnected
     * @throws IllegalArgumentException if {@code key} is {@code null} or {@code delta} is negative
     * @throws RuntimeException if a network error or timeout occurs, the key contains a non-integer
     *         value, or the result exceeds Redis's signed 64-bit integer range
     * @see #incr(String)
     * @see #decr(String, long)
     */
    @Override
    public long incr(final String key, final long delta) {
        assertNotShutdown();

        N.checkArgNotNull(key, "key");
        N.checkArgNotNegative(delta, "delta");

        final byte[] keyBytes = getKeyBytes(key);

        return clientFor(keyBytes).incrBy(keyBytes, delta);
    }

    /**
     * Atomically decrements a numeric value by 1.
     *
     * <p><b>Redis-specific behavior:</b> This operation uses the Redis DECR command. If the key doesn't exist,
     * it will be automatically created with value -1. Unlike Memcached where values cannot go below 0,
     * Redis allows negative values. The operation is O(1) time complexity. If the key is not an
     * integer or the result exceeds Redis's signed 64-bit range, an error occurs.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * long stock = cache.decr("product:stock:123");                // value after decrement
     *
     * // Edge: a fresh key auto-initializes to 0 then -1 (Redis allows negatives)
     * long first = cache.decr("brand:new:counter");               // returns -1 the first time
     *
     * // Negative: a null key is rejected
     * cache.decr(null);                                          // throws IllegalArgumentException
     * }</pre>
     *
     * @param key the cache key whose associated value is to be decremented. Must not be {@code null}.
     * @return the value after decrement (can be negative in Redis, will be -1 if the key did not exist before)
     * @throws IllegalStateException if this client has been disconnected or is being disconnected
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error or timeout occurs, the key contains a non-integer
     *         value, or the result exceeds Redis's signed 64-bit integer range
     * @see #decr(String, long)
     * @see #incr(String)
     */
    @Override
    public long decr(final String key) {
        assertNotShutdown();

        final byte[] keyBytes = getKeyBytes(key);

        return clientFor(keyBytes).decr(keyBytes);
    }

    /**
     * Atomically decrements a numeric value by a specified amount.
     *
     * <p><b>Redis-specific behavior:</b> This operation uses the Redis DECRBY command. If the key doesn't exist,
     * it will be automatically created with the negative delta value. Unlike Memcached where values
     * cannot go below 0, Redis allows negative values. The operation is O(1) time complexity. If the
     * key is not an integer or the result exceeds Redis's signed 64-bit range, an error occurs.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * long inventory = cache.decr("product:stock:456", 5);        // value after subtracting 5
     *
     * // Edge: a delta of 0 returns the current value unchanged
     * long current = cache.decr("quota:abc", 0);
     *
     * // Negative: a negative delta is rejected by this wrapper
     * cache.decr("counter", -1);                                 // throws IllegalArgumentException
     * }</pre>
     *
     * @param key the cache key whose associated value is to be decremented. Must not be {@code null}.
     * @param delta the decrement amount; must be non-negative. Although the Redis DECRBY command itself
     *              supports negative deltas, this implementation rejects them with an
     *              {@link IllegalArgumentException} for portability across cache backends (e.g.
     *              SpyMemcached, which also rejects negative deltas).
     * @return the value after decrement (can be negative in Redis, will be equal to {@code -delta}
     *         if the key did not exist before)
     * @throws IllegalStateException if this client has been disconnected or is being disconnected
     * @throws IllegalArgumentException if {@code key} is {@code null} or {@code delta} is negative
     * @throws RuntimeException if a network error or timeout occurs, the key contains a non-integer
     *         value, or the result exceeds Redis's signed 64-bit integer range
     * @see #decr(String)
     * @see #incr(String, long)
     */
    @Override
    public long decr(final String key, final long delta) {
        assertNotShutdown();

        N.checkArgNotNull(key, "key");
        N.checkArgNotNegative(delta, "delta");

        final byte[] keyBytes = getKeyBytes(key);

        return clientFor(keyBytes).decrBy(keyBytes, delta);
    }

    /**
     * Removes all keys from all connected Redis instances. Concrete subclasses implement this
     * according to their topology (every standalone shard for {@link JRedis}; every cluster master
     * node for {@link JRedisCluster}).
     *
     * @throws IllegalStateException if this client has been disconnected or is being disconnected
     * @throws RuntimeException if flushing a backend fails
     * @see #disconnect()
     */
    @Override
    public abstract void flushAll();

    /**
     * Disconnects from all Redis instances and releases resources.
     * After calling this method, the client cannot be used anymore and any subsequent
     * operations will fail or throw exceptions.
     *
     * <p>This template method is idempotent: the first call marks the instance shut down and then
     * closes the underlying client(s) (via {@code closeClients()}); subsequent calls are no-ops.
     * Publishing shutdown first prevents new operations from starting while pool closure is in
     * progress. It does not remove any data from Redis; it only closes client-side connections.
     * Redis clients are intended to be long-lived and shared; this terminal operation is appropriate
     * for optional component or application shutdown, not routine per-request cleanup.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe, but once called, no other operations should be
     * attempted on this client instance from any thread.
     *
     * @see #flushAll()
     */
    @Override
    public final synchronized void disconnect() {
        // Guard with isShutdown so repeated calls are safe.
        if (isShutdown) {
            return;
        }

        // Publish shutdown before closing potentially slow pools. Operations already in flight are
        // not forcibly cancelled, but no operation beginning after this write may enter a client.
        isShutdown = true;
        closeClients();
    }

    /**
     * Closes the underlying Jedis client(s). Invoked at most once, by the idempotent
     * {@link #disconnect()} template. Implementations should make a best-effort attempt to close every
     * client even if one fails (logging per-client failures), rather than aborting on the first error.
     */
    protected abstract void closeClients();

    /**
     * Rejects an operation once {@link #disconnect()} has begun. Concrete topology-specific
     * operations such as {@code flushAll()} call this hook as well as the common per-key methods.
     *
     * @throws IllegalStateException if this client has been disconnected or is being disconnected
     */
    protected final void assertNotShutdown() {
        if (isShutdown) {
            throw new IllegalStateException("This Redis cache client has been disconnected");
        }
    }

    /**
     * Converts a string key to UTF-8 encoded bytes for Redis operations.
     * All Redis operations use binary-safe keys, so strings must be converted to bytes.
     *
     * @param key the cache key to convert. Must not be {@code null}.
     * @return the UTF-8 encoded byte array representation of the key, never {@code null}
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @see #encode(Object)
     * @see #decode(byte[])
     */
    protected byte[] getKeyBytes(final String key) {
        N.checkArgNotNull(key, "key");

        return key.getBytes(Charsets.UTF_8);
    }

    /**
     * Serializes an object to bytes using Kryo for storage in Redis.
     * Null objects are encoded as empty byte arrays.
     *
     * <p><b>Thread Safety:</b> Thread-safe; the {@link KryoParser} instance is shared and safe for concurrent use.
     *
     * @param value the value to encode. May be {@code null}.
     * @return the serialized byte array, or an empty array if {@code value} is {@code null}. Never {@code null}.
     * @throws RuntimeException if serialization fails
     * @see #decode(byte[])
     */
    protected byte[] encode(final Object value) {
        return value == null ? N.EMPTY_BYTE_ARRAY : KRYO_PARSER.encode(value);
    }

    /**
     * Deserializes bytes to an object using Kryo.
     * Empty or {@code null} byte arrays are decoded as {@code null}.
     *
     * <p><b>Thread Safety:</b> Thread-safe; the {@link KryoParser} instance is shared and safe for concurrent use.
     *
     * @param bytes the byte array to decode. May be {@code null} or empty.
     * @return the deserialized object, or {@code null} if the byte array is {@code null} or empty
     * @throws RuntimeException if deserialization fails
     * @see #encode(Object)
     */
    protected T decode(final byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        return KRYO_PARSER.decode(bytes);
    }
}
