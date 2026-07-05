# abacus-cache

[![Maven Central](https://img.shields.io/maven-central/v/com.landawn.abacus/abacus-cache.svg)](https://central.sonatype.com/artifact/com.landawn.abacus/abacus-cache/2.8.4)
[![Javadocs](https://img.shields.io/badge/javadoc-2.8.4-brightgreen.svg)](https://www.javadoc.io/doc/com.landawn.abacus/abacus-cache/2.8.4/index.html)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE.txt)

A small, unified caching library for Java. One `Cache<K, V>` interface — synchronous *and* asynchronous — backs a range of implementations: on-heap, off-heap (native memory), and distributed (Memcached / Redis). Swap the backend without touching your call sites, or build one from a configuration string.

```java
Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);

cache.put("user:123", user);
User user = cache.getOrNull("user:123");          // null on miss
Optional<User> opt = cache.get("user:123");       // Optional on miss
```

## Contents

- [Why abacus-cache](#why-abacus-cache)
- [Install](#install)
- [Quick start](#quick-start)
- [Cache implementations](#cache-implementations)
- [The `Cache` interface](#the-cache-interface)
- [Distributed caches](#distributed-caches)
- [Building caches from a string (DSL)](#building-caches-from-a-string-dsl)
- [Off-heap caching](#off-heap-caching)
- [Distributed locking](#distributed-locking)
- [Requirements & optional dependencies](#requirements--optional-dependencies)
- [Links](#links)

## Why abacus-cache

- **One interface, many backends.** `Cache<K, V>` is the single contract. Start with an in-memory cache, move to Redis in production, and your application code stays the same.
- **Sync and async out of the box.** Every operation has an asynchronous counterpart (`asyncGet`, `asyncPut`, …) returning a `ContinuableFuture`.
- **Null-safe by choice.** Use `get` for an `Optional<V>` or `getOrNull` when you'd rather avoid the wrapper allocation.
- **Off-heap without the GC bill.** Store large values in native memory (via `sun.misc.Unsafe` or the modern Foreign Function & Memory API) with optional disk spillover.
- **Resilient distributed access.** The distributed wrapper adds key prefixing, Base64 key encoding, and a circuit breaker that fails fast when the backend is down.
- **Bring your own backend.** Caffeine, Ehcache, Memcached, and Redis dependencies are all declared `provided` — add only the one you use.

## Install

Requires **JDK 25+**.

**Maven**

```xml
<dependency>
    <groupId>com.landawn.abacus</groupId>
    <artifactId>abacus-cache</artifactId>
    <version>2.8.4</version>
</dependency>
```

**Gradle**

```gradle
implementation 'com.landawn.abacus:abacus-cache:2.8.4'
```

Then add whichever backend you need (see [Requirements & optional dependencies](#requirements--optional-dependencies)) — e.g. `spymemcached` for Memcached, `jedis` for Redis, `caffeine` or `ehcache` for those wrappers.

## Quick start

```java
import com.landawn.abacus.cache.Cache;
import com.landawn.abacus.cache.CacheFactory;
import com.landawn.abacus.util.u.Optional;

// In-memory cache: 1000 entries max, scan for expired entries every 60s
Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);

// Store with the cache's default expiration
cache.put("user:123", user);

// Store with explicit TTL and idle timeout (in milliseconds)
cache.put("session:abc", session, 3_600_000, 1_800_000);   // 1h TTL, 30m idle

// Read
User u = cache.getOrNull("user:123");                      // null if absent/expired
Optional<User> maybe = cache.get("user:123");              // Optional.empty() if absent/expired
String name = cache.get("user:123").map(User::getName).orElse("Unknown");

// Async
cache.asyncPut("user:456", other)
     .thenAcceptAsync(ok -> log("cached: " + ok));

cache.remove("user:123");
cache.close();   // releases resources; also usable in try-with-resources
```

## Cache implementations

| Implementation | Storage | Per-entry TTL / idle | Stats | Notes |
| --- | --- | --- | --- | --- |
| `LocalCache` | On-heap (`KeyedObjectPool`) | ✅ / ✅ | `stats()` → `CacheStats` | Thread-safe, automatic eviction |
| `OffHeapCache` | Native memory (`Unsafe`) | ✅ / ✅ | `stats()` → `OffHeapCacheStats` | Optional disk spillover; needs JVM flags |
| `ForeignMemoryOffHeapCache` | Native memory (FFM API) | ✅ / ✅ | `stats()` → `OffHeapCacheStats` | Safer `Unsafe` alternative (Java 22+) |
| `CaffeineCache` | Wraps a Caffeine cache | ⚠️ configure on Caffeine | `stats()` + `caffeineStats()` | TTL params ignored — set on Caffeine |
| `Ehcache` | Wraps an Ehcache 3.x cache | ⚠️ configure on Ehcache | — (use Ehcache's `StatisticsService`) | TTL params ignored — set on Ehcache |
| `DistributedCache` | Wraps a `DistributedCacheClient` | TTL only (idle ignored) | — | Key prefix + Base64 + circuit breaker |
| `ChronicleMap` | On-heap (`LocalCache`) | ✅ / ✅ | `stats()` | **Deprecated** compat shim — use `LocalCache` |

Every implementation is created through `CacheFactory`:

```java
// On-heap, fully configured: capacity, evict delay, default TTL, default idle time
LocalCache<String, User> local =
    CacheFactory.createLocalCache(1000, 60000, 3_600_000, 1_800_000);

// Off-heap: 256 MB of native memory
OffHeapCache<String, byte[]> offHeap = CacheFactory.createOffHeapCache(256);

// Wrap a pre-configured Caffeine cache
com.github.benmanes.caffeine.cache.Cache<String, User> caffeine =
    Caffeine.newBuilder().maximumSize(10_000).recordStats().build();
CaffeineCache<String, User> wrapped = CacheFactory.createCaffeineCache(caffeine);
```

## The `Cache` interface

The full contract (each read/write also has an `async*` form returning a `ContinuableFuture`):

| Method | Purpose |
| --- | --- |
| `Optional<V> get(K)` | Read, wrapped in `Optional` |
| `V getOrNull(K)` | Read, `null` on miss (no wrapper allocation) |
| `boolean put(K, V)` | Store with default expiration |
| `boolean put(K, V, liveTime, maxIdleTime)` | Store with explicit TTL + idle timeout |
| `void remove(K)` | Remove (idempotent) |
| `boolean containsKey(K)` | Membership test for a live entry |
| `Set<K> keySet()` / `int size()` | Enumerate / count (may be unsupported on some backends) |
| `void clear()` | Remove all entries |
| `void close()` / `boolean isClosed()` | Release resources / check state |
| `getProperties()` / `getProperty` / `setProperty` / `removeProperty` | Per-cache metadata bag |

**Expiration semantics.** `liveTime` (TTL) is measured from insertion; `maxIdleTime` from last access. When both are set the entry expires on whichever fires first. `maxIdleTime` is **not** honored by distributed backends (Memcached/Redis are TTL-only) and is ignored by the Caffeine/Ehcache wrappers (configure expiration on those instances directly). Defaults, when you don't pass them: `Cache.DEFAULT_LIVE_TIME` = 3 hours, `Cache.DEFAULT_MAX_IDLE_TIME` = 30 minutes.

## Distributed caches

Distributed backends are wrapped in a `DistributedCache`, which adds namespace key prefixing, Base64 key encoding, and a read-path circuit breaker (fail fast after N consecutive failures, then retry after a delay).

```java
// Memcached
SpyMemcached<User> memcached = new SpyMemcached<>("localhost:11211", 5000);
DistributedCache<String, User> cache =
    CacheFactory.createDistributedCache(memcached, "myapp:", 100, 1000);

cache.put("user:123", user, 3_600_000, 0);   // 1h TTL (idle timeout ignored)
User u = cache.getOrNull("user:123");
cache.close();
```

Three clients ship in the box, all implementing `DistributedCacheClient`:

- **`SpyMemcached`** — Memcached over the SpyMemcached library. Uses a `KryoTranscoder` when Kryo is on the classpath, otherwise SpyMemcached's default transcoder. Supports bulk `getBulk` and atomic `incr`/`decr`.
- **`JRedis`** — one or more **standalone** Redis servers with **client-side** CRC-32 sharding. Kryo serialization. Use this when your Redis servers don't know about each other.
- **`JRedisCluster`** — a **Redis Cluster** (servers in cluster mode) that shards **server-side** by hash slot; the client follows `MOVED`/`ASK` redirects. Seed it with a few cluster nodes.

> Standalone vs cluster: pick `JRedis` for independent standalone servers, `JRedisCluster` for a real Redis Cluster. `JRedisCluster` will not work against non-cluster-mode servers.

## Building caches from a string (DSL)

`CacheFactory.createCache(String)` instantiates a distributed cache from a configuration string — handy for properties files or environment-driven setup. Provider names are case-insensitive.

```java
// Memcached, default 1000ms timeout
Cache<String, User> c1 = CacheFactory.createCache("Memcached(localhost:11211)");

// Multiple Memcached servers are SPACE-separated (a comma is parsed as the key-prefix arg)
Cache<String, Object> c2 =
    CacheFactory.createCache("Memcached(host1:11211 host2:11211,myprefix:,3000)");

// Redis with key prefix and 5s timeout
Cache<String, Session> c3 = CacheFactory.createCache("Redis(localhost:6379,app:cache:,5000)");

// Redis Cluster from comma-separated seed nodes
Cache<String, Object> c4 =
    CacheFactory.createCache("RedisCluster(10.0.0.1:7000,10.0.0.2:7000,10.0.0.3:7000)");

// A custom Cache implementation by fully-qualified class name
Cache<String, Object> c5 = CacheFactory.createCache("com.example.MyCache(param1,param2)");
```

Supported forms: `Provider(serverUrl)`, `Provider(serverUrl, keyPrefix)`, and `Provider(serverUrl, keyPrefix, timeoutMillis)` for `Memcached`, `Redis`, and `RedisCluster`; or any fully-qualified class name implementing `Cache`.

## Off-heap caching

Off-heap caches keep values in native memory to sidestep GC pressure and grow far beyond the heap. Use the builder for full control:

```java
OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]>builder()
    .capacityInMB(256)
    .evictDelay(60_000)
    .defaultLiveTime(3_600_000)
    .defaultMaxIdleTime(1_800_000)
    .build();

cache.put("key1", largeByteArray);
byte[] value = cache.getOrNull("key1");

OffHeapCacheStats stats = cache.stats();
double utilization = (double) stats.occupiedMemory() / stats.allocatedMemory();
```

Good to know:

- **Not for tiny objects** (< ~128 bytes serialized) — the per-entry overhead isn't worth it.
- **Values are copied** in and out (serialized), so mutating a retrieved object doesn't touch the cached copy. Serialization defaults to Kryo when available, otherwise JSON.
- **Two backends:** `OffHeapCache` uses `sun.misc.Unsafe`; `ForeignMemoryOffHeapCache` uses the Foreign Function & Memory API (Java 22+) as a safer, future-proof alternative.
- **Optional disk spillover** via the `OffHeapStore` interface when memory fills up (e.g. a RocksDB-backed store).
- The `Unsafe` backend needs JVM flags to open the required modules. The project's own test run uses, for example:
  ```
  --add-opens java.base/java.nio=ALL-UNNAMED
  --add-exports java.base/jdk.internal.ref=ALL-UNNAMED
  --add-exports java.base/sun.nio.ch=ALL-UNNAMED
  --add-exports jdk.unsupported/sun.misc=ALL-UNNAMED
  ```

## Distributed locking

`MemcachedLock` provides a simple, non-reentrant distributed mutex built on Memcached's atomic `add`:

```java
MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211");

if (lock.lock("resource1", 30_000)) {   // 30s lock TTL guards against a crashed holder
    try {
        performExclusiveOperation();
    } finally {
        lock.unlock("resource1");
    }
} else {
    // lock held elsewhere — acquisition is a single, immediate attempt (no retry/queue)
}
```

## Requirements & optional dependencies

- **Java:** JDK 25 or above.
- **Core:** [abacus-common](https://github.com/landawn/abacus-common).
- **Optional backends** (declared `provided` — add the ones you use):

  | Feature | Dependency |
  | --- | --- |
  | Off-heap / Redis / Memcached serialization | [Kryo](https://github.com/EsotericSoftware/kryo) |
  | Memcached client | [SpyMemcached](https://github.com/couchbase/spymemcached) |
  | Redis client | [Jedis](https://github.com/redis/jedis) |
  | Caffeine wrapper | [Caffeine](https://github.com/ben-manes/caffeine) |
  | Ehcache wrapper | [Ehcache 3.x](https://www.ehcache.org/) |

## Links

- **User guide:** [Wiki](https://github.com/landawn/abacus-cache/wiki)
- **API reference:** [Javadoc](https://www.javadoc.io/doc/com.landawn.abacus/abacus-cache/2.8.4/index.html)
- **Changelog:** [CHANGES.md](CHANGES.md)
- **HTML API views:** [SpyMemcached](https://htmlpreview.github.io/?https://github.com/landawn/abacus-common/master/docs/SpyMemcached_view.html), [JRedis](https://htmlpreview.github.io/?https://github.com/landawn/abacus-common/master/docs/JRedis_view.html), [MemcachedLock](https://htmlpreview.github.io/?https://github.com/landawn/abacus-common/master/docs/MemcachedLock_view.html)
- **Related projects:** [abacus-common](https://github.com/landawn/abacus-common), [abacus-jdbc](https://github.com/landawn/abacus-jdbc)

## License

Licensed under the [Apache License 2.0](LICENSE.txt).
