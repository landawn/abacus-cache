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
import java.util.List;
import java.util.Map;

import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
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
 * <p><b>Key Features:</b>
 * <ul>
 *   <li>Automatic sharding across multiple Redis instances for horizontal scaling</li>
 *   <li>Efficient object serialization using Kryo parser</li>
 *   <li>Atomic operations for counters (incr/decr)</li>
 *   <li>TTL (time-to-live) support for automatic expiration</li>
 * </ul>
 *
 * <p><b>Redis-Specific Behaviors:</b>
 * <ul>
 *   <li>Keys are automatically distributed across shards using consistent hashing</li>
 *   <li>Increment/decrement operations auto-initialize non-existent keys to 0</li>
 *   <li>Decrement operations can result in negative values (unlike Memcached)</li>
 *   <li>All string keys are encoded using UTF-8</li>
 * </ul>
 *
 * <p><b>Thread Safety (IMPORTANT):</b> This class wraps a single {@link BinaryShardedJedis} instance,
 * and each underlying {@link Jedis} shard is itself <b>not</b> thread-safe. Concurrent calls into the
 * same {@code JRedis} instance can interleave on the wire and corrupt protocol framing, returning
 * wrong values or causing "unexpected reply" / "ERR Protocol error" disconnects. There is no
 * connection pooling here. Callers must either:
 * <ul>
 *   <li>serialize all access externally (e.g., a single dedicated thread),</li>
 *   <li>or wrap each method call in their own synchronization, or</li>
 *   <li>or replace this implementation with one backed by a {@code ShardedJedisPool}.</li>
 * </ul>
 * The atomicity advertised for counter operations is the <em>server-side</em> atomicity of Redis
 * commands, not concurrency safety of this client wrapper.
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
 * @see AbstractDistributedCacheClient
 * @see BinaryShardedJedis
 */
@SuppressWarnings("deprecation")
public class JRedis<T> extends AbstractDistributedCacheClient<T> {

    private static final Logger logger = LoggerFactory.getLogger(JRedis.class);

    private static final KryoParser kryoParser = ParserFactory.createKryoParser();

    private final BinaryShardedJedis jedis;

    private volatile boolean isShutdown = false;

    /**
     * Creates a new JRedis instance with the default timeout.
     * The server URL should contain comma-separated host:port pairs for multiple Redis instances.
     * Data is automatically sharded across all specified Redis instances using consistent hashing
     * for horizontal scaling and improved performance.
     *
     * <p>Each Redis instance becomes a shard in the distributed cache. When storing or retrieving
     * data, the appropriate shard is determined by hashing the key. This ensures even distribution
     * of data across all servers.
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
     * // Remote Redis servers
     * JRedis<Data> remote = new JRedis<>("redis1.example.com:6379,redis2.example.com:6379");
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
     *
     * // Negative: a string with no valid host:port yields no addresses -> rejected
     * JRedis<User> empty = new JRedis<>("");                        // throws IllegalArgumentException
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
     * The server URL should contain comma-separated host:port pairs for multiple Redis instances.
     * The timeout applies to both connection establishment and socket read/write operations.
     * Data is automatically sharded across all specified Redis instances using consistent hashing.
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
     * // High-latency network with longer timeout
     * JRedis<User> remote = new JRedis<>("remote-redis:6379", 10000);       // 10 seconds
     *
     * // Low-latency local network with short timeout
     * JRedis<Session> local = new JRedis<>("localhost:6379", 2000);         // 2 seconds
     *
     * // Use the cache
     * Data data = new Data("value");
     * cache.set("key", data, 7200000);                                      // Cache for 2 hours; returns true
     * Data retrieved = cache.get("key");                                    // the cached Data, or null
     *
     * // Negative: a non-positive timeout is rejected (checkArgPositive)
     * JRedis<Data> zero = new JRedis<>("localhost:6379", 0);              // throws IllegalArgumentException
     * JRedis<Data> neg = new JRedis<>("localhost:6379", -1);              // throws IllegalArgumentException
     *
     * // Negative: a timeout above Integer.MAX_VALUE ms cannot fit the int Jedis API
     * JRedis<Data> tooBig = new JRedis<>("localhost:6379", Integer.MAX_VALUE + 1L);   // throws IllegalArgumentException
     *
     * // Negative: a blank serverUrl is rejected (super(serverUrl) runs checkArgNotBlank first)
     * JRedis<Data> blank = new JRedis<>("", 5000);                         // throws IllegalArgumentException
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

        final List<JedisShardInfo> jedisClusterNodes = new ArrayList<>();

        for (final InetSocketAddress addr : addressList) {
            // Use getHostString() (returns the literal host or IP) instead of getHostName(), which
            // performs a reverse DNS lookup. Reverse DNS can block startup, fail if no PTR record
            // exists, and produce shard hash keys that differ between processes whose resolvers
            // disagree — silently breaking sharding consistency.
            jedisClusterNodes.add(new JedisShardInfo(addr.getHostString(), addr.getPort(), (int) timeout));
        }

        jedis = new BinaryShardedJedis(jedisClusterNodes);
    }

    /**
     * Retrieves a value from the cache by its key.
     * The value is deserialized from its binary representation using Kryo parser.
     * The appropriate Redis shard is automatically determined based on the key's hash.
     *
     * <p><b>Redis-specific behavior:</b> This operation uses the Redis GET command. If the key
     * does not exist or has expired, {@code null} is returned. The operation is O(1) time complexity.
     *
     * <p><b>Thread Safety:</b> This wrapper is <b>not</b> thread-safe — see the class-level Thread Safety
     * note. Concurrent invocations against the same {@code JRedis} instance can corrupt protocol
     * framing on the underlying {@link Jedis} shards. Serialize access externally if needed.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple get operation
     * User user = cache.get("user:123");                            // the cached User, or null if absent
     * if (user != null) {
     *     System.out.println("Found user: " + user.getName());     // cache hit branch
     * } else {
     *     System.out.println("User not found in cache");           // cache miss branch
     * }
     *
     * // Get with fallback to database (cache-aside pattern)
     * User u = cache.get("user:123");                               // null on a cache miss
     * if (u == null) {
     *     u = database.findUser(123);
     *     if (u != null) {
     *         cache.set("user:123", u, 3600000);                    // returns true on success
     *     }
     * }
     *
     * // Edge: a missing or expired key returns null (not an exception)
     * User missing = cache.get("does:not:exist");                   // returns null
     *
     * // Edge: a key whose stored value was null decodes back to null
     * cache.set("blank", (User) null, 60000);                       // stored as an empty byte array
     * User blank = cache.get("blank");                              // returns null
     *
     * // Negative: a null key is rejected
     * User bad = cache.get(null);                                   // throws IllegalArgumentException
     * }</pre>
     *
     * @param key the cache key whose associated value is to be retrieved. Must not be {@code null}.
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
     * Retrieves multiple values from the cache, returning a map of only the keys that were found.
     * Keys that are absent, expired, or decode to {@code null} are omitted from the result.
     *
     * <p><b>Redis-specific behavior:</b> Because this client shards keys across multiple Redis
     * instances using consistent hashing, a single cross-shard {@code MGET} is not possible.
     * This method therefore issues one {@code GET} per key (each routed to its owning shard) and
     * assembles the results. The keys are validated up-front, before any network call is made.
     *
     * <p><b>Thread Safety:</b> This wrapper is <b>not</b> thread-safe — see the class-level Thread
     * Safety note. Serialize access externally if needed.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Typical: fetch several keys at once; only the keys present in Redis appear in the result.
     * // Here "user:1" exists, "user:2" is missing.
     * Map<String, User> found = cache.getBulk("user:1", "user:2");   // size == 1; contains only "user:1"
     * User u1 = found.get("user:1");                                 // the cached User
     * boolean hasU2 = found.containsKey("user:2");                   // false (missing keys are omitted)
     *
     * // Typical: iterate over the returned entries
     * Map<String, Session> sessions = cache.getBulk("s:a", "s:b", "s:c");
     * sessions.forEach((k, v) -> process(k, v));                     // only found sessions are visited
     *
     * // Edge: no arguments -> empty (never null) map; no GET is issued
     * Map<String, User> none = cache.getBulk();                      // returns an empty map
     * boolean empty = none.isEmpty();                                // true
     *
     * // Edge: a key stored as null decodes to null and is omitted from the result
     * cache.set("blank", (User) null, 60000);                        // stored as empty byte array
     * Map<String, User> r = cache.getBulk("blank");                  // returns an empty map (blank omitted)
     *
     * // Negative: a null array throws (validated before any network call)
     * cache.getBulk((String[]) null);                               // throws IllegalArgumentException
     *
     * // Negative: a null element throws; no GET is issued for any key
     * cache.getBulk("user:1", null, "user:3");                      // throws IllegalArgumentException
     * }</pre>
     *
     * @param keys the cache keys to retrieve; must not be {@code null} or contain {@code null} elements
     * @return a map of the found key-value pairs, never {@code null} (empty if no keys are found)
     * @throws IllegalArgumentException if {@code keys} is {@code null} or contains a {@code null} element
     * @throws RuntimeException if a network error, timeout, or deserialization error occurs
     * @see #get(String)
     * @see #getBulk(Collection)
     */
    @Override
    public Map<String, T> getBulk(final String... keys) {
        checkBulkKeys(keys);

        return fetchBulk(Arrays.asList(keys));
    }

    /**
     * Retrieves multiple values from the cache, returning a map of only the keys that were found.
     * Collection-based counterpart of {@link #getBulk(String...)}; see that method for the
     * Redis-specific sharding behavior.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Typical: fetch a dynamic list of keys; only present keys appear in the result
     * List<String> ids = List.of("user:1", "user:2", "user:3");
     * Map<String, User> users = cache.getBulk(ids);                  // contains only the keys that exist
     * for (Map.Entry<String, User> e : users.entrySet()) {
     *     handle(e.getKey(), e.getValue());                         // missing keys are simply absent
     * }
     *
     * // Typical: a Set of keys works too (any Collection is accepted)
     * Set<String> keys = Set.of("a", "b");
     * Map<String, User> found = cache.getBulk(keys);                 // never null; empty if none found
     *
     * // Edge: empty collection -> empty (never null) map; no GET is issued
     * Map<String, User> none = cache.getBulk(List.of());            // returns an empty map
     * boolean empty = none.isEmpty();                                // true
     *
     * // Edge: keys whose stored value is null/empty decode to null and are omitted
     * Map<String, User> r = cache.getBulk(List.of("blank"));        // "blank" omitted if it decodes to null
     *
     * // Negative: a null collection throws (validated before any network call)
     * cache.getBulk((Collection<String>) null);                     // throws IllegalArgumentException
     *
     * // Negative: a null element throws; no GET is issued for any key
     * cache.getBulk(Arrays.asList("a", null));                      // throws IllegalArgumentException
     * }</pre>
     *
     * @param keys the collection of cache keys to retrieve; must not be {@code null} or contain {@code null} elements
     * @return a map of the found key-value pairs, never {@code null} (empty if no keys are found)
     * @throws IllegalArgumentException if {@code keys} is {@code null} or contains a {@code null} element
     * @throws RuntimeException if a network error, timeout, or deserialization error occurs
     * @see #get(String)
     * @see #getBulk(String...)
     */
    @Override
    public Map<String, T> getBulk(final Collection<String> keys) {
        checkBulkKeys(keys);

        return fetchBulk(keys);
    }

    private Map<String, T> fetchBulk(final Collection<String> keys) {
        final Map<String, T> result = new HashMap<>(Math.max(16, keys.size() * 2));

        for (final String key : keys) {
            final T value = decode(jedis.get(getKeyBytes(key)));

            if (value != null) {
                result.put(key, value);
            }
        }

        return result;
    }

    /**
     * Stores a key-value pair in the cache with a specified time-to-live.
     * The value is serialized using Kryo parser before storage. If the key already exists,
     * its value and TTL will be replaced. The appropriate Redis shard is automatically
     * determined based on the key's hash.
     *
     * <p><b>Redis-specific behavior:</b> When {@code liveTime} is positive, this operation uses the
     * Redis SETEX command which atomically sets both the value and expiration time. When
     * {@code liveTime} is 0 or negative, the Redis SET command is used without expiration, meaning
     * the key will persist until explicitly deleted. The operation is O(1) time complexity. If the
     * key already exists, the previous value and TTL are completely replaced.
     *
     * <p><b>Thread Safety:</b> This wrapper is <b>not</b> thread-safe — see the class-level Thread Safety
     * note. The Redis SET/SETEX commands themselves are atomic on the server side, and when multiple
     * <em>separate</em> clients set the same key concurrently the last write wins. However, calls into
     * the same {@code JRedis} instance from multiple threads can corrupt protocol framing on the
     * underlying {@link Jedis} shards; serialize access externally if needed.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Cache with 1 hour TTL (3600000 ms -> SETEX 3600 seconds)
     * User user = new User("John", "john@example.com");
     * boolean success = cache.set("user:123", user, 3600000);       // returns true when Redis replies "OK"
     * if (success) {
     *     System.out.println("User cached successfully");          // reached only when set returned true
     * }
     *
     * // Cache session data with 30 minute TTL (1800000 ms -> SETEX 1800 seconds)
     * Session session = new Session("abc123", user);
     * cache.set("session:" + session.getId(), session, 1800000);   // returns true on success
     *
     * // Edge: sub-second / fractional TTL rounds UP to whole seconds
     * cache.set("k1", user, 1);                                     // 1 ms -> SETEX 1 second
     * cache.set("k2", user, 1500);                                  // 1500 ms -> SETEX 2 seconds (rounds up)
     *
     * // Edge: liveTime <= 0 means NO expiration -> plain SET (not SETEX 0)
     * cache.set("forever", user, 0);                                // uses SET; key never auto-expires
     * cache.set("forever2", user, -1);                              // also SET; negative TTL treated as 0
     *
     * // Edge: a null value is allowed; it is stored as an empty byte array
     * cache.set("empty:key", (User) null, 3600000);                // SETEX with an empty byte[] payload; returns true
     * User back = cache.get("empty:key");                          // returns null (empty bytes decode to null)
     *
     * // Edge: if Redis does not reply "OK", set returns false
     * boolean ok = cache.set("k", user, 60000);                    // false if the server reply was not "OK"
     *
     * // Negative: a null key is rejected
     * cache.set(null, user, 60000);                                // throws IllegalArgumentException
     * }</pre>
     *
     * @param key the cache key with which the specified value is to be associated. Must not be {@code null}.
     * @param value the value to cache. May be {@code null} (stored as empty byte array).
     * @param liveTime the time-to-live in milliseconds. Positive values set expiration via SETEX (converted to seconds). 0 or negative means no expiration (uses SET without TTL).
     * @return {@code true} if the operation was successful (Redis responds with "OK"), {@code false} otherwise
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error, timeout, or serialization error occurs
     * @see #get(String)
     * @see #delete(String)
     */
    @Override
    public boolean set(final String key, final T value, final long liveTime) {
        final byte[] keyBytes = getKeyBytes(key);
        final byte[] valueBytes = encode(value);

        if (liveTime <= 0) {
            // liveTime <= 0 means no expiration, use SET instead of SETEX
            return "OK".equals(jedis.set(keyBytes, valueBytes));
        }

        return "OK".equals(jedis.setex(keyBytes, toSeconds(liveTime), valueBytes));
    }

    /**
     * Removes the mapping for a key from the cache if it is present.
     * The appropriate Redis shard is automatically determined based on the key's hash.
     *
     * <p><b>Redis-specific behavior:</b> This method uses the Redis DEL command to remove the key.
     * The operation is O(1) time complexity. The return value reflects Redis's DEL semantics (which
     * returns the number of keys removed): {@code true} when the key existed and was removed,
     * {@code false} when the key did not exist at the time the command was issued.
     *
     * <p><b>Thread Safety:</b> This wrapper is <b>not</b> thread-safe — see the class-level Thread Safety
     * note. The Redis DEL command itself is idempotent and atomic on the server side, so when multiple
     * <em>separate</em> clients delete the same key concurrently all will succeed. Concurrent calls into
     * the same {@code JRedis} instance from multiple threads, however, can corrupt protocol framing on
     * the underlying {@link Jedis} shards; serialize access externally if needed.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Typical: delete an existing key
     * cache.set("user:123", user, 60000);                          // key now present
     * boolean removed = cache.delete("user:123");                  // returns true (DEL count == 1)
     *
     * // Typical: cache invalidation after a write (write-through)
     * database.save(user);                                         // persist the new state first
     * cache.delete("user:" + user.getId());                        // returns true if the entry existed
     *
     * // Edge: deleting a key that does not exist returns false (DEL count == 0)
     * boolean wasThere = cache.delete("never:set");                // returns false
     *
     * // Edge: delete is effectively idempotent; a second delete just reports false
     * cache.delete("user:123");                                    // first call -> true
     * boolean again = cache.delete("user:123");                    // second call -> false (already gone)
     *
     * // Negative: a null key is rejected
     * cache.delete(null);                                          // throws IllegalArgumentException
     * }</pre>
     *
     * @param key the cache key whose associated value is to be removed. Must not be {@code null}.
     * @return {@code true} if Redis reported at least one key was actually removed; {@code false}
     *         if the key did not exist at the time the {@code DEL} command was issued. This matches
     *         Redis's DEL semantics (returns the number of keys removed) and gives callers a way to
     *         distinguish a real delete from an idempotent no-op.
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error or timeout occurs
     * @see #get(String)
     * @see #set(String, Object, long)
     */
    @Override
    public boolean delete(final String key) {
        final Long removed = jedis.del(getKeyBytes(key));

        return removed != null && removed > 0L;
    }

    /**
     * Atomically increments a numeric value by 1.
     * The appropriate Redis shard is automatically determined based on the key's hash.
     *
     * <p><b>Redis-specific behavior:</b> This operation uses the Redis INCR command. If the key doesn't exist,
     * it will be automatically created with value 1 (Redis initializes to 0, then increments by 1).
     * This differs from Memcached which returns -1 for non-existent keys. The operation is O(1) time complexity.
     * If the key contains a value that cannot be represented as an integer, an error will occur.
     *
     * <p><b>Important:</b> The key should contain a string representation of an integer. If the key
     * was previously set with a non-numeric value (using {@link #set(String, Object, long)}), this operation
     * will fail. Increment operations are meant for counters, not for general objects.
     *
     * <p><b>Thread Safety:</b> The Redis INCR command is atomic on the server side, so concurrent
     * increments issued by <em>separate</em> client instances are guaranteed to be serialized correctly
     * by Redis and no increments are lost — making it suitable for distributed counters and rate
     * limiting. <b>However, this wrapper itself is not thread-safe</b> — see the class-level Thread
     * Safety note. Concurrent calls into the same {@code JRedis} instance can corrupt protocol framing
     * on the underlying {@link Jedis} shards.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Simple counter on a fresh key (auto-initializes to 0, then +1)
     * long pageViews = cache.incr("page:views");                   // returns 1 the first time
     * long again = cache.incr("page:views");                       // returns 2 the second time
     *
     * // Rate limiting
     * String key = "rate:limit:" + userId;
     * long attempts = cache.incr(key);                             // value after increment
     * if (attempts > MAX_ATTEMPTS) {
     *     throw new RateLimitException("Too many requests");
     * }
     *
     * // Distributed ID generator (caution: not persistent across Redis restarts)
     * long uniqueId = cache.incr("id:generator");                  // a monotonically increasing value
     *
     * // Edge: a nil reply from the shard is coalesced to 0 (no NullPointerException)
     * long safe = cache.incr("transient:counter");                 // returns 0 if the shard replied nil
     *
     * // Negative: a null key is rejected
     * cache.incr(null);                                           // throws IllegalArgumentException
     *
     * // Negative: incrementing a key whose value is not an integer fails on the server
     * cache.set("name", "Alice", 0);                              // stored as a non-numeric value
     * cache.incr("name");                                        // throws RuntimeException (Redis "not an integer" error)
     * }</pre>
     *
     * @param key the cache key whose associated value is to be incremented. Must not be {@code null}.
     * @return the value after increment (will be 1 if the key did not exist before). Returns {@code 0}
     *         if the Redis shard returns a {@code nil} reply (e.g., on transient errors), rather than
     *         throwing a {@link NullPointerException}.
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error, timeout occurs, or if the key contains a non-integer value
     * @see #incr(String, int)
     * @see #decr(String)
     * @see #decr(String, int)
     */
    @Override
    public long incr(final String key) {
        // BinaryShardedJedis.incr returns a boxed Long; auto-unboxing a null reply (which can
        // occur on a transient shard error returning nil) would NPE rather than surfacing a
        // meaningful exception. Treat null as "no value" / 0.
        final Long v = jedis.incr(getKeyBytes(key));
        return v == null ? 0L : v.longValue();
    }

    /**
     * Atomically increments a numeric value by a specified amount.
     * The appropriate Redis shard is automatically determined based on the key's hash.
     *
     * <p><b>Redis-specific behavior:</b> This operation uses the Redis INCRBY command. If the key doesn't exist,
     * it will be automatically created with the delta value (Redis initializes to 0, then increments by delta).
     * This differs from Memcached which returns -1 for non-existent keys. The operation is O(1) time complexity.
     * If the key contains a value that cannot be represented as an integer, an error will occur.
     *
     * <p><b>Important:</b> The key should contain a string representation of an integer. If the key
     * was previously set with a non-numeric value (using {@link #set(String, Object, long)}), this operation
     * will fail. Increment operations are meant for counters, not for general objects.
     *
     * <p><b>Thread Safety:</b> The Redis INCRBY command is atomic on the server side, so concurrent
     * increments issued by <em>separate</em> client instances are guaranteed to be serialized correctly
     * by Redis and no increments are lost — making it suitable for distributed counters and batch
     * operations. <b>However, this wrapper itself is not thread-safe</b> — see the class-level Thread
     * Safety note. Concurrent calls into the same {@code JRedis} instance can corrupt protocol framing
     * on the underlying {@link Jedis} shards.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Game score increment on a fresh key (auto-initializes to 0, then +delta)
     * long score = cache.incr("player:score:123", 10);            // returns 10 the first time
     *
     * // Batch processing counter
     * long processed = cache.incr("batch:processed", 100);        // returns the running total after +100
     *
     * // Dynamic points system
     * int points = calculatePoints(action);
     * long totalPoints = cache.incr("user:points:" + userId, points);   // value after adding 'points'
     *
     * // Edge: a delta of 0 is allowed and returns the current value unchanged
     * long current = cache.incr("user:points:" + userId, 0);      // returns the current counter value
     *
     * // Edge: a nil reply from the shard is coalesced to 0 (no NullPointerException)
     * long safe = cache.incr("transient:counter", 5);             // returns 0 if the shard replied nil
     *
     * // Negative: a negative delta is rejected by this wrapper (for cross-backend portability)
     * cache.incr("counter", -1);                                 // throws IllegalArgumentException
     *
     * // Negative: a null key is rejected
     * cache.incr(null, 1);                                       // throws IllegalArgumentException
     * }</pre>
     *
     * @param key the cache key whose associated value is to be incremented. Must not be {@code null}.
     * @param delta the increment amount; must be non-negative. Although the Redis INCRBY command itself
     *              supports negative deltas, this implementation rejects them with an
     *              {@link IllegalArgumentException} for portability across cache backends (e.g.
     *              SpyMemcached, which also rejects negative deltas).
     * @return the value after increment (will be equal to delta if the key did not exist before).
     *         Returns {@code 0} if the Redis shard returns a {@code nil} reply, rather than throwing
     *         a {@link NullPointerException}.
     * @throws IllegalArgumentException if {@code key} is {@code null} or {@code delta} is negative
     * @throws RuntimeException if a network error, timeout occurs, or if the key contains a non-integer value
     * @see #incr(String)
     * @see #decr(String)
     * @see #decr(String, int)
     */
    @Override
    public long incr(final String key, final int delta) {
        N.checkArgNotNegative(delta, "delta");

        final Long v = jedis.incrBy(getKeyBytes(key), delta);
        return v == null ? 0L : v.longValue();
    }

    /**
     * Atomically decrements a numeric value by 1.
     * The appropriate Redis shard is automatically determined based on the key's hash.
     *
     * <p><b>Redis-specific behavior:</b> This operation uses the Redis DECR command. If the key doesn't exist,
     * it will be automatically created with value -1 (Redis initializes to 0, then decrements by 1).
     * Unlike Memcached where values cannot go below 0, Redis allows negative values. The operation is
     * O(1) time complexity. If the key contains a value that cannot be represented as an integer, an error will occur.
     *
     * <p><b>Important:</b> The key should contain a string representation of an integer. If the key
     * was previously set with a non-numeric value (using {@link #set(String, Object, long)}), this operation
     * will fail. Decrement operations are meant for counters, not for general objects. Unlike Memcached,
     * Redis allows decrementing below zero, so you must implement your own boundary checks if needed.
     *
     * <p><b>Thread Safety:</b> The Redis DECR command is atomic on the server side, so concurrent
     * decrements issued by <em>separate</em> client instances are guaranteed to be serialized correctly
     * by Redis and no decrements are lost — making it suitable for resource quotas and inventory
     * management. <b>However, this wrapper itself is not thread-safe</b> — see the class-level Thread
     * Safety note. Concurrent calls into the same {@code JRedis} instance can corrupt protocol framing
     * on the underlying {@link Jedis} shards.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Inventory management with rollback
     * long stock = cache.decr("product:stock:123");                // value after decrement
     * if (stock < 0) {
     *     cache.incr("product:stock:123");                         // rollback the decrement
     *     throw new OutOfStockException("Product out of stock");
     * }
     * processOrder();                                             // reached only when stock stayed >= 0
     *
     * // Countdown timer
     * long countdown = cache.decr("event:countdown");              // value after decrement
     * if (countdown == 0) {
     *     triggerEvent();                                          // fires when the counter reaches 0
     * }
     *
     * // Edge: a fresh key auto-initializes to 0 then -1 (unlike Memcached, Redis allows negatives)
     * long first = cache.decr("brand:new:counter");               // returns -1 the first time
     *
     * // Edge: a nil reply from the shard is coalesced to 0 (no NullPointerException)
     * long safe = cache.decr("transient:counter");                // returns 0 if the shard replied nil
     *
     * // Negative: a null key is rejected
     * cache.decr(null);                                          // throws IllegalArgumentException
     *
     * // Negative: decrementing a non-integer value fails on the server
     * cache.set("name", "Alice", 0);                             // stored as a non-numeric value
     * cache.decr("name");                                       // throws RuntimeException (Redis "not an integer" error)
     * }</pre>
     *
     * @param key the cache key whose associated value is to be decremented. Must not be {@code null}.
     * @return the value after decrement (can be negative in Redis, will be -1 if the key did not exist
     *         before). Returns {@code 0} if the Redis shard returns a {@code nil} reply, rather than
     *         throwing a {@link NullPointerException}.
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if a network error, timeout occurs, or if the key contains a non-integer value
     * @see #decr(String, int)
     * @see #incr(String)
     * @see #incr(String, int)
     */
    @Override
    public long decr(final String key) {
        final Long v = jedis.decr(getKeyBytes(key));
        return v == null ? 0L : v.longValue();
    }

    /**
     * Atomically decrements a numeric value by a specified amount.
     * The appropriate Redis shard is automatically determined based on the key's hash.
     *
     * <p><b>Redis-specific behavior:</b> This operation uses the Redis DECRBY command. If the key doesn't exist,
     * it will be automatically created with the negative delta value (Redis initializes to 0, then decrements by delta).
     * Unlike Memcached where values cannot go below 0, Redis allows negative values. The operation is
     * O(1) time complexity. If the key contains a value that cannot be represented as an integer, an error will occur.
     *
     * <p><b>Important:</b> The key should contain a string representation of an integer. If the key
     * was previously set with a non-numeric value (using {@link #set(String, Object, long)}), this operation
     * will fail. Decrement operations are meant for counters, not for general objects. Unlike Memcached,
     * Redis allows decrementing below zero, so you must implement your own boundary checks if needed.
     *
     * <p><b>Thread Safety:</b> The Redis DECRBY command is atomic on the server side, so concurrent
     * decrements issued by <em>separate</em> client instances are guaranteed to be serialized correctly
     * by Redis and no decrements are lost — making it suitable for bulk resource management and quota
     * systems. <b>However, this wrapper itself is not thread-safe</b> — see the class-level Thread
     * Safety note. Concurrent calls into the same {@code JRedis} instance can corrupt protocol framing
     * on the underlying {@link Jedis} shards.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Bulk inventory decrement
     * int quantity = 5;
     * long inventory = cache.decr("product:stock:456", quantity);  // value after subtracting 'quantity'
     * if (inventory < 0) {
     *     cache.incr("product:stock:456", quantity);               // rollback
     *     throw new InsufficientStockException();
     * }
     *
     * // API quota management with variable costs
     * int requestCost = calculateCost(request);
     * long quotaRemaining = cache.decr("quota:" + apiKey, requestCost);   // value after the deduction
     * if (quotaRemaining < 0) {
     *     cache.incr("quota:" + apiKey, requestCost);              // restore quota
     *     throw new QuotaExceededException("Insufficient quota");
     * }
     *
     * // Edge: a fresh key auto-initializes to 0 then subtracts delta (negatives allowed)
     * long first = cache.decr("brand:new:counter", 5);            // returns -5 the first time
     *
     * // Edge: a delta of 0 returns the current value unchanged
     * long current = cache.decr("quota:" + apiKey, 0);            // returns the current counter value
     *
     * // Edge: a nil reply from the shard is coalesced to 0 (no NullPointerException)
     * long safe = cache.decr("transient:counter", 3);             // returns 0 if the shard replied nil
     *
     * // Negative: a negative delta is rejected by this wrapper
     * cache.decr("counter", -1);                                 // throws IllegalArgumentException
     *
     * // Negative: a null key is rejected
     * cache.decr(null, 1);                                       // throws IllegalArgumentException
     * }</pre>
     *
     * @param key the cache key whose associated value is to be decremented. Must not be {@code null}.
     * @param delta the decrement amount; must be non-negative
     * @return the value after decrement (can be negative in Redis, will be equal to {@code -delta}
     *         if the key did not exist before). Returns {@code 0} if the Redis shard returns a
     *         {@code nil} reply, rather than throwing a {@link NullPointerException}.
     * @throws IllegalArgumentException if {@code key} is {@code null} or {@code delta} is negative
     * @throws RuntimeException if a network error, timeout occurs, or if the key contains a non-integer value
     * @see #decr(String)
     * @see #incr(String)
     * @see #incr(String, int)
     */
    @Override
    public long decr(final String key, final int delta) {
        N.checkArgNotNegative(delta, "delta");

        final Long v = jedis.decrBy(getKeyBytes(key), delta);
        return v == null ? 0L : v.longValue();
    }

    /**
     * Removes all keys from all connected Redis instances.
     * This is a destructive operation that affects all data across all shards and all databases
     * within each Redis instance. Use with extreme caution in production environments.
     *
     * <p><b>Redis-specific behavior:</b> This operation issues the Redis FLUSHALL command on each shard.
     * It removes all keys from all databases (not just the currently selected database). The operation
     * is synchronous and blocks until all keys are removed from all shards. The time complexity is
     * O(N) where N is the total number of keys across all databases on all shards. If a FLUSHALL on
     * one shard fails, the remaining shards are still attempted (each failure is logged at WARN level)
     * and the <em>first</em> encountered exception is rethrown after all shards have been processed.
     *
     * <p><b>Warning:</b> This operation affects ALL databases on each Redis instance (DB 0 through DB 15
     * by default), not just the one being used by this client. If other applications share the same
     * Redis instances, their data will also be deleted.
     *
     * <p><b>Thread Safety:</b> This wrapper is <b>not</b> thread-safe — see the class-level Thread Safety
     * note. The Redis FLUSHALL command itself is atomic on each server, but concurrent invocations
     * against the same {@code JRedis} instance can corrupt protocol framing on the underlying
     * {@link Jedis} shards. Once executed, all cached data on the affected servers is permanently
     * lost and visible immediately to all other clients connected to those servers.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // WARNING: This removes ALL data from ALL Redis instances!
     * cache.flushAll();                                           // issues FLUSHALL on every shard; returns void
     * User gone = cache.get("user:123");                          // returns null afterwards (all keys cleared)
     *
     * // Integration test cleanup
     * @BeforeEach
     * public void setUp() {
     *     cache.flushAll();                                        // clean slate for each test
     * }
     *
     * // Edge: with a single empty/null shard set there is simply nothing to flush (no exception)
     * cache.flushAll();                                           // no-op safe when there are no shards
     *
     * // Edge/Negative: if one shard fails, the others are still flushed and the FIRST error is rethrown
     * try {
     *     cache.flushAll();                                       // attempts every shard even if one throws
     * } catch (RuntimeException e) {
     *     logger.error("Flush failed on a shard (remaining shards were still flushed)", e);   // rethrows the first failure
     * }
     * }</pre>
     *
     * @throws RuntimeException the first exception encountered while flushing any shard (all remaining
     *         shards are still attempted before the exception is rethrown). Typically a network error
     *         or timeout from the underlying Jedis client.
     * @see #disconnect()
     */
    @Override
    public void flushAll() {
        final Collection<Jedis> allShards = jedis.getAllShards();

        if (allShards != null) {
            RuntimeException firstException = null;

            for (final Jedis j : allShards) {
                if (j != null) {
                    try {
                        j.flushAll();
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
            }

            if (firstException != null) {
                throw firstException;
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
     * it only closes the client-side connections.
     *
     * <p><b>Important:</b> This method should be called when the client is no longer needed to ensure
     * proper cleanup of network connections and other resources. It is safe to call this method
     * multiple times; subsequent calls will have no effect. After calling disconnect(), attempting
     * to use the cache client will result in exceptions.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe, but once called, no other operations should be
     * attempted on this client instance from any thread.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Try-finally pattern (recommended)
     * JRedis<User> cache = new JRedis<>("localhost:6379");
     * try {
     *     cache.set("user:123", user, 3600000);                    // returns true on success
     *     User cached = cache.get("user:123");                     // the cached User, or null
     *     processUser(cached);                                     // application logic on the cached value
     * } finally {
     *     cache.disconnect();                                      // closes all shard connections
     * }
     *
     * // Try-with-resources pattern (requires wrapper)
     * try (AutoCloseable closeable = cache::disconnect) {
     *     cache.set("key", value, 3600000);                        // returns true on success
     *     // disconnect() is invoked automatically when the block exits
     * }
     *
     * // Edge: disconnect() is idempotent and safe to call more than once
     * cache.disconnect();                                          // closes the underlying client
     * cache.disconnect();                                          // no-op; the second call does nothing
     *
     * // Spring Bean destruction callback
     * @PreDestroy
     * public void cleanup() {
     *     if (cache != null) {
     *         cache.disconnect();                                  // releases network/socket resources
     *     }
     * }
     * }</pre>
     *
     * @see #flushAll()
     */
    @Override
    public synchronized void disconnect() {
        // Guard with isShutdown so repeated calls are safe (the Javadoc above promises
        // "It is safe to call this method multiple times"). Without the guard, calling
        // jedis.disconnect() after the underlying sockets have already been closed can
        // throw obscure low-level exceptions on some shards.
        if (isShutdown) {
            return;
        }

        try {
            jedis.disconnect();
        } finally {
            isShutdown = true;
        }
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
     * correctly.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     * String.getBytes() creates a new byte array on each call, so there are no shared mutable state issues.
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
     * Kryo provides efficient serialization with compact binary representation.
     * Null objects are encoded as empty byte arrays. This method is used internally
     * by the {@link #set(String, Object, long)} method.
     *
     * <p><b>Serialization Details:</b> Kryo is a fast and efficient binary object graph serialization
     * framework for Java. It provides better performance and smaller serialized size compared to
     * standard Java serialization. Kryo handles object graphs, circular references, and complex
     * data structures automatically.
     *
     * <p><b>Important:</b> The objects being serialized should be compatible with Kryo serialization.
     * Most Java objects work out of the box, but objects with special serialization requirements may
     * need custom serializers. Classes should have proper constructors or be registered with Kryo.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     * The KryoParser instance is shared and designed to be thread-safe through proper synchronization.
     *
     * @param value the value to encode. May be {@code null}.
     * @return the serialized byte array representation of the value, or empty array if value is {@code null}. Never {@code null}.
     * @throws RuntimeException if serialization fails due to incompatible object types or serialization errors
     * @see #decode(byte[])
     * @see #getKeyBytes(String)
     */
    protected byte[] encode(final Object value) {
        return value == null ? N.EMPTY_BYTE_ARRAY : kryoParser.encode(value);
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
     * including all fields and nested objects.
     *
     * <p><b>Important:</b> The class of the serialized object must be available on the classpath
     * at deserialization time. If the class structure has changed since serialization, deserialization
     * may fail or produce unexpected results. Ensure class compatibility when upgrading applications.
     *
     * <p><b>Thread Safety:</b> This method is thread-safe and can be called concurrently from multiple threads.
     * The KryoParser instance is shared and designed to be thread-safe through proper synchronization.
     *
     * @param bytes the byte array to decode. May be {@code null} or empty.
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
