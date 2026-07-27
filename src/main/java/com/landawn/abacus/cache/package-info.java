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

/**
 * The Abacus caching framework: one {@link com.landawn.abacus.cache.Cache Cache} interface over
 * in-memory, off-heap, third-party, and distributed cache backends.
 *
 * <p>Every implementation in this package speaks the same contract — {@code Optional}-based reads,
 * per-entry time-to-live and idle timeout where the backend supports it, asynchronous variants
 * returning {@link com.landawn.abacus.util.ContinuableFuture ContinuableFuture}, a property bag for
 * custom configuration, and an explicit {@link com.landawn.abacus.cache.Cache#close() close()}
 * operation for retiring a cache early. Swapping a local cache for Memcached or Redis is therefore a
 * construction-site change, not a call-site one.
 *
 * <h2>Contents</h2>
 *
 * <p><b>Core contract</b>
 * <ul>
 * <li>{@link com.landawn.abacus.cache.Cache} — the interface every cache implements.</li>
 * <li>{@link com.landawn.abacus.cache.AbstractCache} — base class supplying the asynchronous
 *     operations, default TTL/idle handling, and property management.</li>
 * <li>{@link com.landawn.abacus.cache.CacheFactory} — creates caches programmatically or from a
 *     configuration string such as {@code "Memcached(localhost:11211)"}.</li>
 * </ul>
 *
 * <p><b>In-memory</b>
 * <ul>
 * <li>{@link com.landawn.abacus.cache.LocalCache} — thread-safe on-heap cache backed by a keyed
 *     object pool, with capacity-based eviction plus per-entry TTL and idle timeout.</li>
 * <li>{@link com.landawn.abacus.cache.ChronicleMap} — name-compatibility adapter that delegates to
 *     {@code LocalCache}; it is not a Chronicle-Map integration.</li>
 * </ul>
 *
 * <p><b>Off-heap</b>
 * <ul>
 * <li>{@link com.landawn.abacus.cache.AbstractOffHeapCache} — shared machinery for caches that
 *     serialize values into native memory divided into 1 MB segments of equally sized slots,
 *     keeping large data sets out of reach of the garbage collector.</li>
 * <li>{@link com.landawn.abacus.cache.OffHeapCache} — {@code sun.misc.Unsafe}-based implementation.</li>
 * <li>{@link com.landawn.abacus.cache.ForeignMemoryOffHeapCache} — same design on the Foreign
 *     Function &amp; Memory API ({@code java.lang.foreign}), the preferred choice on modern JDKs.</li>
 * <li>{@link com.landawn.abacus.cache.OffHeapStore} — optional disk-backed spill-over for values
 *     that no longer fit in native memory.</li>
 * </ul>
 *
 * <p><b>Third-party wrappers</b>
 * <ul>
 * <li>{@link com.landawn.abacus.cache.CaffeineCache} and {@link com.landawn.abacus.cache.Ehcache} —
 *     adapt a pre-configured Caffeine or Ehcache 3.x instance. Both configure expiration at the
 *     cache level, so the per-entry {@code liveTime}/{@code maxIdleTime} arguments are ignored.</li>
 * </ul>
 *
 * <p><b>Distributed</b>
 * <ul>
 * <li>{@link com.landawn.abacus.cache.DistributedCache} — adapts any client to {@code Cache},
 *     adding key prefixing, Base64 key encoding, and a circuit breaker on reads.</li>
 * <li>{@link com.landawn.abacus.cache.DistributedCacheClient} and
 *     {@link com.landawn.abacus.cache.AbstractDistributedCacheClient} — the client-side contract and
 *     its base class.</li>
 * <li>{@link com.landawn.abacus.cache.SpyMemcached} — Memcached client, with
 *     {@link com.landawn.abacus.cache.KryoTranscoder} for compact Kryo payloads.</li>
 * <li>{@link com.landawn.abacus.cache.JRedis} (client-side sharding across standalone servers) and
 *     {@link com.landawn.abacus.cache.JRedisCluster} (Redis Cluster), sharing command and
 *     serialization logic through {@link com.landawn.abacus.cache.AbstractJedisCacheClient}.</li>
 * </ul>
 *
 * <p><b>Statistics</b>
 * <ul>
 * <li>{@link com.landawn.abacus.cache.CacheStats} — immutable snapshot of hits, misses, evictions,
 *     and memory use.</li>
 * <li>{@link com.landawn.abacus.cache.OffHeapCacheStats} — adds off-heap specifics such as segment
 *     allocation and disk I/O timings.</li>
 * </ul>
 *
 * <h2>Example</h2>
 *
 * <p>A cache normally lives as long as the component that owns it:
 *
 * <pre>{@code
 * class UserService {
 *     // Capacity 1000, evict every 60s, 1h default TTL, 30min default idle timeout.
 *     // Swapping in a distributed backend changes only this line:
 *     //     CacheFactory.createCache("Redis(localhost:6379,myapp:cache:,5000)")
 *     private final Cache<String, User> cache =
 *             CacheFactory.createLocalCache(1000, 60_000, 3_600_000, 1_800_000);
 *
 *     User load(String id) {
 *         Optional<User> cached = cache.get("user:" + id);
 *
 *         if (cached.isPresent()) {
 *             return cached.get();
 *         }
 *
 *         User user = readFromDatabase(id);
 *         cache.put("user:" + id, user);            // the cache's default TTL and idle timeout
 *         return user;
 *     }
 *
 *     void preload(String id, User user) {
 *         cache.put("temp:" + id, user, 5000, 2000);   // per-entry: 5s TTL, 2s idle timeout
 *     }
 *
 *     // Cache is not AutoCloseable by design: close() belongs in the owner's shutdown
 *     // path, not in a try-with-resources block around a few operations.
 *     void shutdown() {
 *         cache.close();
 *     }
 * }
 * }</pre>
 *
 * <h2>Dependencies</h2>
 *
 * <p>The backing libraries — Kryo, SpyMemcached, Jedis, Caffeine, and Ehcache — are declared with
 * {@code provided} scope. Only {@code LocalCache} and the off-heap caches work out of the box; add
 * the library for whichever backend you use to your own runtime classpath. Kryo is optional
 * everywhere it appears: the off-heap and Memcached caches fall back to JSON serialization when it
 * is absent, while the Redis clients require it.
 *
 * <h2>Lifecycle and thread safety</h2>
 *
 * <p>All cache implementations here are safe for concurrent use.
 *
 * <p>{@code Cache} deliberately does <em>not</em> extend {@link java.lang.AutoCloseable}: a cache is
 * a long-lived, owner-managed resource rather than a block-scoped one, and
 * {@link com.landawn.abacus.cache.Cache#close() close()} exists to retire an instance early — when
 * the owning component stops, or when a dynamically configured cache is replaced. The call is
 * idempotent, and a cache cannot be reopened once closed; the behavior of subsequent operations is
 * implementation-defined — they typically throw {@link java.lang.IllegalStateException} or behave as
 * if the cache were empty.
 *
 * @see com.landawn.abacus.cache.Cache
 * @see com.landawn.abacus.cache.CacheFactory
 */
package com.landawn.abacus.cache;
