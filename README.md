# abacus-cache

[![Maven Central](https://img.shields.io/maven-central/v/com.landawn.abacus/abacus-cache.svg)](https://central.sonatype.com/artifact/com.landawn.abacus/abacus-cache)
[![Javadocs](https://img.shields.io/badge/javadoc-reference-brightgreen.svg)](https://www.javadoc.io/doc/com.landawn.abacus/abacus-cache)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

abacus-cache is a Java caching library with a common `Cache<K, V>` API for on-heap, native-memory, and distributed storage. It includes local caches, two off-heap implementations, Caffeine and Ehcache adapters, Memcached and Redis clients, and a Memcached lease utility.

This guide describes the source in this checkout, whose [pom.xml](pom.xml) declares version **2.9.1** and **Java 25**. Dependency versions below match that POM; they do not claim to be the latest published releases. Select a published artifact version when consuming the library from a repository, or build this checkout locally.

## Contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [Choosing an implementation](#choosing-an-implementation)
- [Common Cache API](#common-cache-api)
- [Expiration and eviction](#expiration-and-eviction)
- [Asynchronous operations](#asynchronous-operations)
- [LocalCache](#localcache)
- [Caffeine and Ehcache adapters](#caffeine-and-ehcache-adapters)
- [Off-heap caches](#off-heap-caches)
- [Distributed caches](#distributed-caches)
- [Distributed client reference](#distributed-client-reference)
- [Configuration strings](#configuration-strings)
- [CacheFactory reference](#cachefactory-reference)
- [Statistics](#statistics)
- [Memcached locking](#memcached-locking)
- [Lifecycle and concurrency](#lifecycle-and-concurrency)
- [Troubleshooting](#troubleshooting)
- [Build and test](#build-and-test)
- [Further reference](#further-reference)
- [License](#license)

## Installation

Use JDK 25 or later. The artifact is compiled for Java 25, including when using only the local cache or the foreign-memory implementation.

The POM declares **all runtime libraries, including `abacus-common`, with `provided` scope**. Consumers must declare `abacus-common` and the dependencies required by their chosen backend; they are not automatically supplied as runtime dependencies by abacus-cache.

### Maven

The minimum dependencies for `LocalCache` are:

```xml
<dependencies>
    <dependency>
        <groupId>com.landawn.abacus</groupId>
        <artifactId>abacus-cache</artifactId>
        <version>2.9.1</version>
    </dependency>
    <dependency>
        <groupId>com.landawn.abacus</groupId>
        <artifactId>abacus-common</artifactId>
        <version>8.3.1</version>
    </dependency>
</dependencies>
```

### Gradle

```groovy
dependencies {
    implementation 'com.landawn.abacus:abacus-cache:2.9.1'
    implementation 'com.landawn.abacus:abacus-common:8.3.1'
}
```

Add the relevant backend libraries using normal application dependency scope (`compile` in Maven, `implementation` in Gradle):

| Feature | Additional coordinates in this checkout | Notes |
| --- | --- | --- |
| `LocalCache` | None beyond `abacus-common` | Values remain Java objects on the heap. |
| Off-heap caches | Optionally `com.esotericsoftware:kryo:5.6.2` | Object serialization uses Kryo when available, otherwise the abacus-common JSON parser; byte arrays and byte buffers use raw-byte paths. |
| `CaffeineCache` | `com.github.ben-manes.caffeine:caffeine:3.3.0` | Configure the Caffeine instance before wrapping it. |
| `Ehcache` | `org.ehcache:ehcache:3.12.0` | The project excludes `org.glassfish.jaxb:jaxb-runtime` for programmatic configuration. XML configuration requires a suitable JAXB setup. |
| `SpyMemcached`, `MemcachedLock` | `net.spy:spymemcached:2.12.3` | Kryo is optional; without it, SpyMemcached's default transcoder is used. |
| `JRedis`, `JRedisCluster` | `redis.clients:jedis:8.0.1` and `com.esotericsoftware:kryo:5.6.2` | These clients require Kryo to initialize successfully. |
| Custom disk spillover | Your `OffHeapStore<K>` implementation and its dependencies | RocksDB is a test dependency; no production RocksDB adapter is bundled. |

Lombok is used to build the fluent builders. Applications using the compiled artifact do not need Lombok annotation processing to call them. JVM options for native-memory and serializer access are discussed under [off-heap caches](#off-heap-caches).

## Quick start

This complete example requires no external cache server:

```java
import com.landawn.abacus.cache.CacheFactory;
import com.landawn.abacus.cache.LocalCache;
import com.landawn.abacus.util.u.Optional;

public class CacheQuickStart {
    public static void main(String[] args) {
        // 1,000 entries; scan for expired entries every 60 seconds.
        LocalCache<String, String> cache =
                CacheFactory.createLocalCache(1_000, 60_000);
        try {
            boolean stored = cache.put("user:123", "Ada");
            if (!stored) {
                throw new IllegalStateException("Cache rejected the entry");
            }

            String name = cache.getOrNull("user:123");
            Optional<String> missing = cache.get("user:404");
            System.out.println(name);                       // Ada
            System.out.println(missing.orElse("Unknown")); // Unknown

            // Explicit TTL of 10 minutes and idle timeout of 2 minutes.
            cache.put("session:abc", "active", 600_000, 120_000);
            cache.remove("session:abc");
            System.out.println(cache.containsKey("session:abc")); // false
        } finally {
            cache.close();
        }
    }
}
```

`Optional` is **`com.landawn.abacus.util.u.Optional`**, not `java.util.Optional`. `Cache` does **not** extend `AutoCloseable`; use `try/finally` for the built-in cache implementations. Keep application caches alive across requests and close them when their owner shuts down. The short lifetime above is for a standalone example.

### Cache-aside loading

The common API has no loader or atomic `computeIfAbsent`. A cache-aside helper can load on a miss:

```java
import java.util.function.Supplier;
import com.landawn.abacus.cache.Cache;

public class CacheAside {
    public static <K, V> V getOrLoad(Cache<K, V> cache, K key,
            Supplier<? extends V> loader) {
        V value = cache.getOrNull(key);
        if (value != null) {
            return value;
        }

        value = loader.get();
        if (value != null) {
            // A false return means the loaded value was not retained.
            // The caller can still use the value from the source of truth.
            cache.put(key, value);
        }
        return value;
    }
}
```

Concurrent misses can run the loader more than once. Add application-level coordination if loading is expensive. After updating the source of truth, remove or replace the cached entry according to the consistency needs of the application. A cache hit is not a transactional guarantee about the underlying data.

## Choosing an implementation

Backend differences remain visible even though the read/write API is shared:

| Implementation | Storage and value behavior | Expiration through `put` | Enumeration and size | Statistics |
| --- | --- | --- | --- | --- |
| `LocalCache` | On-heap object references in a `KeyedObjectPool` | TTL and idle timeout | Key snapshot; size may include expired entries awaiting removal | `CacheStats` |
| `OffHeapCache` | Serialized/raw bytes in native memory using `sun.misc.Unsafe` | TTL and idle timeout | Key snapshot; size may include expired entries | `OffHeapCacheStats` |
| `ForeignMemoryOffHeapCache` | Serialized/raw bytes in a shared foreign-memory arena | TTL and idle timeout | Same as `OffHeapCache` | `OffHeapCacheStats` |
| `CaffeineCache` | Wraps a configured Caffeine cache | Both arguments ignored; configure Caffeine | `keySet()` unsupported; estimated `size()` | `CacheStats`, `caffeineStats()` |
| `Ehcache` | Wraps a configured Ehcache 3 cache | Both arguments ignored; configure Ehcache | `keySet()` and `size()` unsupported | Use Ehcache's statistics facilities |
| `DistributedCache` | Wraps a Memcached, Redis, or custom client | TTL only; idle argument ignored | `keySet()` and `size()` unsupported | No common statistics method |
| `ChronicleMap` | Deprecated `LocalCache` compatibility subclass | Same as `LocalCache` | Same as `LocalCache` | `CacheStats` |

`com.landawn.abacus.cache.ChronicleMap` is not an OpenHFT Chronicle Map integration and supplies no native-memory or persistent-map behavior. Use `LocalCache` directly for that compatibility behavior.

## Common Cache API

The interface is [Cache.java](src/main/java/com/landawn/abacus/cache/Cache.java). All time arguments in abacus-cache are in **milliseconds** unless explicitly stated otherwise.

| Method | Result and contract |
| --- | --- |
| `get(K key)` | `Optional<V>`; empty if the read produces `null`. |
| `getOrNull(K key)` | Value, or `null` when absent or expired. Distributed reads can also return `null` on backend failure. |
| `put(K key, V value)` | `boolean`; uses the instance's default expiration. Check the result when admission matters. |
| `put(K key, V value, long liveTime, long maxIdleTime)` | `boolean`; uses explicit expiration where supported. |
| `remove(K key)` | `void`; removing an absent key is allowed. Operational failures can still throw. |
| `containsKey(K key)` | `boolean`; not an atomic promise that a later read will succeed. |
| `keySet()` | `Set<K>` where supported. Local/off-heap sets are snapshots, not writable cache views. |
| `size()` | `int` where supported; not necessarily a count of currently readable entries. |
| `clear()` | Removes entries; **for a distributed wrapper this calls backend-wide `flushAll()`**. |
| `close()` | Retires the instance and releases resources according to its ownership rules. |
| `isClosed()` | Reports wrapper/cache state; not backend health. |
| `getProperties()` | Live `com.landawn.abacus.util.Properties<String, Object>` metadata bag. |
| `getProperty(String)` | Typed metadata lookup; `null` if absent. |
| `setProperty(String, Object)` | Sets metadata and returns the previous value. |
| `removeProperty(String)` | Removes metadata and returns the previous value. |

`stats()` is implementation-specific, not a member of `Cache`.

### Keys, values, and failures

Use non-null keys and values for portable behavior. Local, off-heap, Caffeine, and Ehcache implementations reject null keys and values with `IllegalArgumentException`. Distributed null-value handling depends on the client/transcoder; a null read is indistinguishable from a miss, so avoid caching nulls. Use an explicit application value to represent a negative lookup if needed.

Local caches retain object references: mutating a stored or retrieved mutable object changes the same object. Off-heap caches and distributed clients encode values and reconstruct them on reads. Choose keys with stable equality/hash behavior locally and stable, unique string representations for distributed storage.

An ordinary unsuccessful `put` can return `false`, while validation, serialization, lifecycle, and backend errors can throw. The distributed wrapper has special read-failure handling described below. Unsupported methods throw `UnsupportedOperationException`; do not assume every interface method works on every implementation.

### Metadata

Properties are local to the cache object. They are neither cached entries nor a way to change TTL, capacity, or server configuration. For example, `setProperty("region", "west")` stores application metadata only. Individual operations are synchronized by the built-in base class. Use `getProperties().copy()` for a detached snapshot when inspecting metadata. Typed accessors rely on the caller requesting the correct type.

## Expiration and eviction

`liveTime` is the maximum lifetime measured from insertion or replacement. `maxIdleTime` is the maximum time since the last qualifying access. When both are positive, whichever deadline arrives first makes the entry expired. A successful local/off-heap read refreshes idle time but does not restart the TTL.

| Setting | Local and off-heap caches | Distributed caches | Caffeine/Ehcache adapters |
| --- | --- | --- | --- |
| Positive `liveTime` | TTL in milliseconds | Redis uses millisecond TTL; Memcached rounds up to seconds | Ignored |
| `liveTime <= 0` | Disables TTL expiration | No expiration | Ignored |
| Positive `maxIdleTime` | Idle timeout in milliseconds | Ignored | Ignored |
| `maxIdleTime <= 0` | Disables idle expiration | Ignored | Ignored |
| `evictDelay` | Interval between maintenance passes; not an entry TTL | Not applicable | Configure the delegate |

The common constants are `Cache.DEFAULT_LIVE_TIME = 10_800_000` (3 hours) and `Cache.DEFAULT_MAX_IDLE_TIME = 1_800_000` (30 minutes). Local convenience constructors and off-heap factory methods use them; the distributed wrapper uses the 3-hour TTL and ignores idle time. **Off-heap builders instead default both times to zero.**

For local/off-heap entries, `put(key, value, 0, 0)` disables both expiration limits. It does not prevent capacity eviction, explicit removal, or shutdown. With `evictDelay = 0`, background expiry scans are disabled, but reads still check expiration. `LocalCache` rejects a negative eviction delay; off-heap builders treat a non-positive delay as disabling the scheduler.

`containsKey` does not refresh local/off-heap idle timers. Local probing removes an expired entry it encounters; off-heap probing leaves it for a read or maintenance pass. Consequently, `size()` and `keySet()` may include expired entries awaiting reclamation. Enumeration is not a transactional snapshot of readable values.

## Asynchronous operations

The common async methods return `com.landawn.abacus.util.ContinuableFuture`:

| Method | Future value |
| --- | --- |
| `asyncGet(key)` | `Optional<V>` |
| `asyncGetOrNull(key)` | `V` or `null` |
| `asyncPut(key, value)` | `Boolean` |
| `asyncPut(key, value, liveTime, maxIdleTime)` | `Boolean` |
| `asyncRemove(key)` | `Void` |
| `asyncContainsKey(key)` | `Boolean` |

There are no common `asyncClear`, `asyncSize`, `asyncKeySet`, or `asyncClose` methods. The built-in base class submits synchronous operations to a shared executor; this is not a separate nonblocking storage implementation.

The following method-body example waits for the write before issuing the read. Here and below, fragments with imports followed by statements belong inside a method, with the imports at the top of its source file.

```java
import java.util.concurrent.TimeUnit;
import com.landawn.abacus.cache.Cache;
import com.landawn.abacus.cache.CacheFactory;
import com.landawn.abacus.util.ContinuableFuture;

Cache<String, String> cache = CacheFactory.createLocalCache(100, 60_000);
try {
    ContinuableFuture<Boolean> pending = cache.asyncPut("greeting", "Hello");
    if (!pending.get(5, TimeUnit.SECONDS)) {
        throw new IllegalStateException("Cache rejected the entry");
    }
    String value = cache.asyncGetOrNull("greeting").get(5, TimeUnit.SECONDS);
    System.out.println(value); // Hello
} finally {
    cache.close();
}
```

Handle or declare `InterruptedException`, `ExecutionException`, and `TimeoutException` in the enclosing method. Independently submitted calls can execute out of order. Validation and backend failures inside the task normally surface through the future (`ExecutionException` from `get()`); submission itself can fail with `RejectedExecutionException`. A timeout while waiting does not prove a write was never applied. Bound outstanding work and finish it before closing a shared cache.

`SpyMemcached` also exposes client-specific async methods that wrap the underlying client's futures. Those validate arguments at submission time and differ from `Cache`'s executor-based wrappers.

## LocalCache

`LocalCache` uses an abacus-common `KeyedObjectPool<K, PoolableAdapter<V>>`. Capacity is a **positive entry count**; `evictDelay` is a non-negative number of milliseconds.

```java
import com.landawn.abacus.cache.CacheFactory;
import com.landawn.abacus.cache.LocalCache;

LocalCache<String, String> cache = CacheFactory.createLocalCache(
        10_000,   // maximum entries
        30_000,   // maintenance interval
        600_000,  // default TTL
        120_000); // default idle timeout
try {
    cache.put("configuration", "enabled");
    System.out.println(cache.stats().capacity()); // 10000
    System.out.println(cache.get("configuration").orElse("disabled"));
} finally {
    cache.close();
}
```

The pool manages admission, expiration, and capacity eviction. Do not assume every `put` must succeed. For a custom pool, use `CacheFactory.createLocalCache(pool, defaultLiveTime, defaultMaxIdleTime)`. The cache closes the supplied pool when it closes, so give the cache ownership of that pool.

## Caffeine and Ehcache adapters

Both adapters ignore `put`'s expiration arguments, including the default times supplied by `AbstractCache`. Set expiration on the underlying cache.

### Caffeine

```java
import java.time.Duration;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.landawn.abacus.cache.CacheFactory;
import com.landawn.abacus.cache.CaffeineCache;

com.github.benmanes.caffeine.cache.Cache<String, String> delegate =
        Caffeine.newBuilder()
                .maximumSize(10_000)
                .expireAfterWrite(Duration.ofMinutes(10))
                .expireAfterAccess(Duration.ofMinutes(2))
                .recordStats()
                .build();
CaffeineCache<String, String> cache = CacheFactory.createCaffeineCache(delegate);
try {
    cache.put("user:123", "Ada");
    cache.getOrNull("user:123");
    System.out.println(cache.stats().hitRate());
    System.out.println(cache.caffeineStats());
} finally {
    cache.close();
}
```

Enable `recordStats()` to collect delegate hit/miss/eviction metrics. `size()` is an estimate capped at `Integer.MAX_VALUE`; `keySet()` is unsupported. `close()` marks the wrapper closed and invalidates **all entries in the delegate**, including directly written entries. The wrapper reads with `getIfPresent`; it does not invoke a Caffeine loading-cache loader on a miss.

### Ehcache

```java
import java.time.Duration;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import com.landawn.abacus.cache.CacheFactory;
import com.landawn.abacus.cache.Ehcache;

try (CacheManager manager = CacheManagerBuilder.newCacheManagerBuilder().build(true)) {
    org.ehcache.Cache<String, String> delegate = manager.createCache("users",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(
                    String.class, String.class, ResourcePoolsBuilder.heap(1_000))
                    .withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(
                            Duration.ofMinutes(10))));
    Ehcache<String, String> cache = CacheFactory.createEhcache(delegate);
    try {
        cache.put("user:123", "Ada");
        System.out.println(cache.getOrNull("user:123"));
    } finally {
        cache.close();
    }
}
```

The wrapper also exposes `putIfAbsent`, `getAll(Set)`, `putAll(Map)`, and `removeAll(Set)`, outside the common interface. `putIfAbsent` returns the existing value, or `null` when it inserts. Delegate loader/writer behavior and exceptions still apply.

`keySet()` and `size()` are unsupported. `close()` closes only wrapper state: it does not clear the delegate or close its manager. The application must close the `CacheManager`, as above.

## Off-heap caches

`OffHeapCache` and `ForeignMemoryOffHeapCache` share storage, expiration, serialization, and spillover behavior. Their allocation mechanism differs: Unsafe native memory versus `Arena.ofShared()` and `MemorySegment`.

Values are copied into native memory and reconstructed on retrieval. Keys, entry metadata, and allocation bookkeeping remain on the heap; serialization/read buffers also use heap memory. `capacityInMB` reserves `capacityInMB * 1_048_576` bytes at construction. It is neither an entry limit nor a total process-memory limit. Account for both heap and native memory when sizing the process.

### Creating a cache

```java
import com.landawn.abacus.cache.ForeignMemoryOffHeapCache;
import com.landawn.abacus.cache.OffHeapCacheStats;

ForeignMemoryOffHeapCache<String, byte[]> cache =
        ForeignMemoryOffHeapCache.<String, byte[]>builder()
                .capacityInMB(64)
                .evictDelay(30_000)
                .defaultLiveTime(600_000)
                .defaultMaxIdleTime(120_000)
                .build();
try {
    byte[] original = new byte[] { 1, 2, 3 };
    if (!cache.put("payload", original)) {
        throw new IllegalStateException("Cache rejected the payload");
    }
    original[0] = 9;
    System.out.println(cache.getOrNull("payload")[0]); // 1: stored copy

    OffHeapCacheStats stats = cache.stats();
    double utilization = (double) stats.occupiedMemory() / stats.allocatedMemory();
    System.out.println(utilization);
} finally {
    cache.close();
}
```

For the Unsafe implementation, use `OffHeapCache.<K, V>builder()` with the same settings. `CacheFactory.createOffHeapCache(64)` creates an **Unsafe** cache with a 3,000 ms maintenance interval, 3-hour TTL, and 30-minute idle timeout. There is no `CacheFactory` method for the foreign-memory implementation; use its builder.

### Builder reference

Both builders expose these fluent settings:

| Setting | Builder default | Meaning |
| --- | --- | --- |
| `capacityInMB(int)` | `0`, must be supplied | Positive size of the native allocation in MiB. |
| `maxBlockSizeInBytes(int)` | `8192` | Maximum single-block size. `0` selects 8192; otherwise 1,024 through 1,048,576, rounded up to a multiple of 64. Larger values span multiple blocks. |
| `evictDelay(long)` | `0` | Expiry scan and empty-segment reclamation interval; non-positive disables scheduled maintenance. |
| `defaultLiveTime(long)` | `0` | Default TTL; non-positive disables it. |
| `defaultMaxIdleTime(long)` | `0` | Default idle timeout; non-positive disables it. |
| `vacatingFactor(float)` | `0.2f` | Fraction of memory-resident entries targeted for eviction under allocation pressure, least recently accessed first; `0` selects the default. Valid range is 0 through 1. |
| `serializer(BiConsumer)` | Built-in serializer | Writes a value to an abacus-common `ByteArrayOutputStream`. |
| `deserializer(BiFunction)` | Built-in deserializer | Receives `byte[]` and `Type<V>`, returns a value. |
| `offHeapStore(OffHeapStore<K>)` | `null` | Optional backing store for spillover. |
| `statsTimeOnDisk(boolean)` | `false` | Records store read/write timing in milliseconds. |
| `storeSelector(TriFunction)` | `null` | Selects storage per put using `(key, value, serializedSize)`. |
| `testerForLoadingItemFromDiskToMemory(TriPredicate)` | `null` | Decides whether a disk read should attempt promotion using `(activityPrint, size, readMillis)`. |

The allocator uses 1 MiB segments and 64-byte minimum slots. Slot rounding, segments dedicated to different slot sizes, and fragmentation mean serialized data size can be below configured capacity while allocation fails. An unsuccessful memory write may schedule eviction and return `false`; the triggering write is not automatically retried. Check admission results. `maxBlockSizeInBytes` is not a maximum value size.

### Serialization

The default object serializer uses Kryo if its parser is available and JSON otherwise. `byte[]` and `ByteBuffer` values bypass custom serializers/deserializers and use raw-byte handling. For other values, supply matching, thread-safe callbacks when a specific format is needed:

```java
import java.nio.charset.StandardCharsets;
import com.landawn.abacus.cache.ForeignMemoryOffHeapCache;

ForeignMemoryOffHeapCache<String, String> cache =
        ForeignMemoryOffHeapCache.<String, String>builder()
                .capacityInMB(8)
                .evictDelay(30_000)
                .defaultLiveTime(600_000)
                .defaultMaxIdleTime(120_000)
                .serializer((value, out) -> {
                    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                    out.write(bytes, 0, bytes.length);
                })
                .deserializer((bytes, type) -> new String(bytes, StandardCharsets.UTF_8))
                .build();
try {
    cache.put("greeting", "Hello");
    System.out.println(cache.getOrNull("greeting")); // Hello
} finally {
    cache.close();
}
```

Do not retain pooled serializer output streams or call `close()` on the owning cache from a cache callback. Reentrant off-heap close is rejected. Serialization failures propagate instead of being reported as ordinary admission failure.

### Spillover and promotion

Implement [OffHeapStore<K>](src/main/java/com/landawn/abacus/cache/OffHeapStore.java) to add disk or other byte storage:

| Method | Required behavior |
| --- | --- |
| `byte[] get(K key)` | Return the exact bytes originally stored; `null` means confirmed absence. Throw an unchecked exception for an operational read failure. |
| `boolean put(K key, byte[] bytes)` | Return `true` on success. A false result or exception must leave the previous value unchanged. |
| `boolean remove(K key)` | Return whether an entry was removed; `false` means confirmed absence. Operational failures must throw. |
| `close()` | Release store resources; default implementation is a no-op. The owning cache calls it during shutdown. |

Stores must support concurrent access and exact byte round trips. Compression/encryption must be reversed before returning bytes. A stored-length mismatch fails the read. Give each cache its own store or isolated namespace; direct writes under a cache-owned key can invalidate its metadata.

`storeSelector` returns `0` for memory first with spillover, `1` for memory only, or `2` for disk only. Disk-only puts without a store return `false`. Other results, including `null`, throw `IllegalArgumentException`. Default routing spills incoming values when they cannot be placed in memory; it does not automatically move every evicted memory entry to disk.

Disk reads remain on disk unless a promotion predicate is supplied. Returning `true` attempts promotion if memory is available, preserving expiration and access metadata. Treat the callback's `ActivityPrint` as read-only. Promotion may be skipped when space is unavailable.

**A persistent store does not make the cache restart-persistent.** The key index, value types, and expiration state live in JVM memory. Store bytes alone cannot restore entries, and normal cache shutdown removes tracked entries before closing the store. See [OffHeapCacheRocksDbIT](src/test/java/com/landawn/abacus/cache/OffHeapCacheRocksDbIT.java) for a test adapter and round-trip examples.

### JVM options

The repository's tests configure these native-memory and reflective-access options. They reproduce the tested environment; a local-only application does not need the whole list, and serializer access needs depend on the values and dependencies used.

```text
--sun-misc-unsafe-memory-access=allow
--add-opens java.base/java.lang=ALL-UNNAMED
--add-opens java.base/java.lang.reflect=ALL-UNNAMED
--add-opens java.base/java.io=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
--add-opens java.base/java.nio=ALL-UNNAMED
--add-exports java.base/jdk.internal.ref=ALL-UNNAMED
--add-exports java.base/sun.nio.ch=ALL-UNNAMED
--add-exports jdk.unsupported/sun.misc=ALL-UNNAMED
```

The foreign-memory backend does not use Unsafe for allocation, but its serializer can still require reflective access. Always close native caches when retiring them; do not rely on garbage collection to release their allocation.

## Distributed caches

`DistributedCache<K, V>` wraps a `DistributedCacheClient<V>` and adds key conversion, a namespace prefix, and read-failure handling. The public factory accepts an already-created client:

```java
import com.landawn.abacus.cache.CacheFactory;
import com.landawn.abacus.cache.DistributedCache;
import com.landawn.abacus.cache.JRedis;

JRedis<String> client = new JRedis<>("localhost:6379", 5_000);
DistributedCache<String, String> cache =
        CacheFactory.createDistributedCache(client, "example:v1:", 5, 2_000);
try {
    // 10-minute TTL; the idle argument is ignored by distributed storage.
    if (!cache.put("user:123", "Ada", 600_000, 0)) {
        throw new IllegalStateException("Backend rejected the entry");
    }
    System.out.println(cache.getOrNull("user:123"));
    cache.remove("user:123");
} finally {
    cache.close(); // disconnects the supplied client
}
```

This example needs a running standalone Redis server plus Jedis and Kryo. Substitute `new SpyMemcached<String>("localhost:11211", 5_000)` for Memcached, or `new JRedisCluster<String>("node1:7000 node2:7000", 5_000)` for an actual Redis Cluster; these classes are also in `com.landawn.abacus.cache`.

### Keys and namespaces

The backend key is `prefix + Base64(UTF-8(keyString))`. String keys are used directly as `keyString`; other keys use their string representation. The empty string is encoded as the marker `-`. Keys with unpaired UTF-16 surrogates are rejected.

Prefixes are kept literally and must contain only printable ASCII characters without spaces or control characters. Logical key `user:123` with prefix `app:` becomes `app:dXNlcjoxMjM=`. Direct client methods do not perform this encoding or prefixing, so `client.get("user:123")` will not read that wrapper entry.

Use stable, unique key strings, preferably with an explicit type/version namespace. Objects with the same string representation share a distributed key. Base64 increases key length; the Memcached limit applies to the final encoded key plus prefix.

**`clear()` is backend-wide.** It delegates to `flushAll()` and does not restrict removal to `keyPrefix`. With bundled clients this flushes the configured Memcached servers, all databases on configured standalone Redis shards, or Redis Cluster primaries. For namespace-level invalidation, remove known logical keys individually or switch to a new application-managed prefix and let old entries expire.

### Read circuit breaker

`createDistributedCache(client, prefix, maxFailuresBeforeCircuitOpen, retryDelay)` configures the read path. Defaults are **100 consecutive failures** and a **1,000 ms retry delay**.

1. Reads call the client while the circuit permits access.
2. Ordinary client exceptions become misses and count toward the threshold. `IllegalArgumentException` propagates and does not count; `Error` is not swallowed.
3. After the threshold, reads return `null` without contacting the client until the retry delay has elapsed since the most recent failure.
4. A subsequent successful client read, including a legitimate miss, resets the failure sequence. A failed retry starts another delay.

`get`, `getOrNull`, and `containsKey` use this behavior. A miss cannot distinguish absence, expiration, decoding failure, an unavailable backend, or an open circuit. Use source-of-truth fallback and backend/log monitoring accordingly. Opening is logged at warning level; individual read failures at debug level.

The breaker does **not** protect `put`, `remove`, or `clear`; those still reach the client and may throw. It does not queue writes or coordinate a single retry probe. A threshold of `0` opens after the first failure; a retry delay of `0` disables the waiting interval. Neither means all reads are permanently disabled.

## Distributed client reference

Use a client directly for bulk retrieval, counters, or Memcached-specific operations. Direct reads propagate backend errors and use raw string keys. The shared interface is [DistributedCacheClient.java](src/main/java/com/landawn/abacus/cache/DistributedCacheClient.java).

| API | Behavior |
| --- | --- |
| `serverUrl()` | Returns the configured address string. |
| `get(String)` | Returns a value or `null`. |
| `getBulk(String...)`, `getBulk(Collection<String>)` | Map of found values; bundled clients support both forms. Custom clients may reject them. |
| `put(String, V, long liveTime)` | Writes with TTL in milliseconds; returns success. |
| `remove(String)` | Returns whether an entry was removed. |
| `incr(key)`, `incr(key, delta)` | Atomic backend increment; explicit deltas must be non-negative. |
| `decr(key)`, `decr(key, delta)` | Atomic backend decrement; explicit deltas must be non-negative. |
| `flushAll()` | Backend-wide flush, potentially unsupported by a custom client. |
| `disconnect()` | Releases resources; idempotent in bundled clients. |

`set` and `delete` are deprecated aliases for `put` and `remove`. New implementations should implement the current names. Default bridges support legacy implementations but throw when neither side has a usable implementation.

### Memcached: SpyMemcached

`SpyMemcached<T>` accepts one or more `host:port` addresses separated by whitespace or commas in its constructor. Timeout defaults to 1,000 ms and must be positive. Synchronous operations wait for the underlying future using that operation timeout; timeouts and interruptions propagate as runtime failures.

Raw keys must satisfy Memcached's key rules, including the 250-byte limit and no whitespace/control characters. `SpyMemcached.checkKey(String)` exposes validation. Positive TTLs round up to seconds; values over 30 days convert to absolute expiry timestamps. Non-positive TTL means no expiration; times beyond the supported integer timestamp range are rejected.

Additional methods include `add` (store only if absent), `replace` (store only if present), and async counterparts for get, bulk get, put, add, replace, remove, and flush. `flushAll(delay)` accepts milliseconds. `disconnect(timeout)` allows a bounded shutdown wait in milliseconds.

With Kryo available, the client uses `KryoTranscoder`; otherwise it uses SpyMemcached's default transcoder. Keep reader/writer configurations compatible across processes. `KryoTranscoder` also accepts a maximum encoded size, a custom `KryoParser`, or both. Size limits must be positive; oversized encoded payloads fail rather than being truncated.

Counters use the backend's numeric format. Basic `incr`/`decr` returns `-1` for a missing Memcached counter; decrement floors at zero. Overloads accepting `(key, delta, defaultValue[, liveTime])` seed an absent counter with `defaultValue`, returning that initial value without applying the delta on creation. Initialization uses ASCII counter encoding even when ordinary values use Kryo. Do not mix counter keys with serialized object keys or decode counter bytes with a Kryo `get`.

### Standalone Redis: JRedis

`JRedis<T>` accepts standalone endpoints and routes keys by **CRC-32 of the UTF-8 key bytes modulo the number of configured servers**. Server order is significant. This is client-side sharding across independent servers, not Redis Cluster routing or consistent hashing; changing the list or order remaps keys without migrating existing data.

Positive TTLs use `SET ... PX` with millisecond precision. Non-positive TTL uses plain `SET`. Kryo is required for object encoding/decoding. Timeout must be positive and at most `Integer.MAX_VALUE` milliseconds; it applies to connection and socket timeouts.

Bulk reads deduplicate keys and issue individual `GET` commands, so they are not one atomic snapshot or a pipelined batch. Counters use native Redis integer commands: a missing counter starts from zero, and decrement can produce negative results. Counter bytes are not Kryo payloads; keep these keys separate from `put`/`get` object values.

### Redis Cluster: JRedisCluster

`JRedisCluster<T>` requires servers in Redis Cluster mode. Supply one or more seed nodes; Jedis handles slot routing and redirections. It is not a substitute for `JRedis` against independent standalone servers. TTL, serialization, timeout validation, bulk-read strategy, and counter behavior match the shared Redis implementation.

The bundled Redis constructors accept address lists and a timeout, not a full application-specific Jedis configuration for credentials, TLS, or database selection. For features beyond these constructors, implement `DistributedCacheClient` around a suitably configured client and wrap it with `CacheFactory.createDistributedCache`.

## Configuration strings

`CacheFactory.createCache(String)` creates a cache from a provider specification. Built-in names `Memcached`, `Redis`, and `RedisCluster` are case-insensitive. Creating a provider allocates client resources and may initiate connections; it is not just parsing.

For Memcached and standalone Redis:

```text
Provider(serverUrl)
Provider(serverUrl,keyPrefix)
Provider(serverUrl,timeoutMillis)
Provider(serverUrl,keyPrefix,timeoutMillis)
```

Timeout defaults to 1,000 ms. A numeric second parameter is a timeout, not a prefix. To use a numeric prefix, supply the third parameter explicitly. Timeouts must be positive decimal integers (an optional sign is parsed); Redis clients also enforce the integer-millisecond maximum.

Use **space-separated servers in the first argument** for multiple Memcached or standalone Redis endpoints. Commas delimit factory parameters even though direct constructors accept comma-separated address lists.

```java
import com.landawn.abacus.cache.Cache;
import com.landawn.abacus.cache.CacheFactory;

Cache<String, String> cache = CacheFactory.createCache(
        "Memcached(localhost:11211 localhost:11212,example:v1:,3000)");
try {
    cache.put("greeting", "Hello");
} finally {
    cache.close();
}
```

Examples of accepted specifications:

| Specification | Meaning |
| --- | --- |
| `Memcached(localhost:11211)` | One server, no prefix, default timeout. |
| `Redis(localhost:6379,5000)` | One server, no prefix, 5-second timeout. |
| `Redis(host1:6379 host2:6379,app:,5000)` | Two standalone shards, prefix `app:`, 5-second timeout. |
| `Redis(localhost:6379,123,5000)` | Literal prefix `123`, 5-second timeout. |
| `RedisCluster(node1:7000 node2:7000,app:,3000)` | Space-separated seeds, prefix, explicit timeout. |
| `RedisCluster(node1:7000,node2:7000)` | Comma-separated seeds, no prefix, default timeout. |
| `RedisCluster(node1:7000,node2:7000,app:,3000)` | Comma-separated seeds, distinct prefix, timeout. |
| `RedisCluster(node1:7000 node2:7000,3000)` | Multiple seeds grouped in one argument, timeout only. |

For Redis Cluster, comma-separated tokens shaped like `host:port` can be seed nodes. Endpoint-shaped prefixes are therefore ambiguous. For example, `RedisCluster(node1:7000,node2:7000,3000)` is rejected because the penultimate token could be a seed or prefix. Group seeds with spaces, use a prefix ending in `:` (such as `tenant:1:`), or create the client programmatically.

A fully qualified name can instantiate a custom concrete `Cache` through `TypeAttrParser`, for example `com.example.MyCache(option1,option2)` or a no-argument provider. Its constructor/factory must match the parser's supported construction rules. The class must implement `Cache`; abstract and non-cache classes are rejected. Built-in short names for `LocalCache`, Caffeine, or off-heap builders are not provided; use their factories/builders.

## CacheFactory reference

All methods are static and infer `<K, V>` from the assignment or explicit type arguments:

| Factory | Parameters and behavior |
| --- | --- |
| `createLocalCache(capacity, evictDelay)` | Positive entry capacity and non-negative maintenance interval; common TTL/idle defaults. |
| `createLocalCache(capacity, evictDelay, defaultLiveTime, defaultMaxIdleTime)` | Local cache with explicit default expiration. |
| `createLocalCache(pool, defaultLiveTime, defaultMaxIdleTime)` | Wraps and owns a `KeyedObjectPool<K, PoolableAdapter<V>>`. |
| `createOffHeapCache(capacityInMB)` | Unsafe cache; 3,000 ms maintenance, common TTL/idle defaults. |
| `createOffHeapCache(capacityInMB, evictDelay)` | Unsafe cache with explicit maintenance interval. |
| `createOffHeapCache(capacityInMB, evictDelay, defaultLiveTime, defaultMaxIdleTime)` | Unsafe cache with explicit expiration defaults. |
| `createCaffeineCache(delegate)` | Wraps a non-null Caffeine cache. |
| `createEhcache(delegate)` | Wraps a non-null Ehcache 3 cache. |
| `createDistributedCache(client)` | Wraps a client without a prefix; default breaker settings. |
| `createDistributedCache(client, keyPrefix)` | Adds a literal namespace prefix; default breaker settings. |
| `createDistributedCache(client, keyPrefix, maxFailuresBeforeCircuitOpen, retryDelay)` | Explicit read breaker settings; threshold and delay must be non-negative. |
| `createCache(provider)` | Parses the provider DSL described above. |

Use concrete return types such as `LocalCache<K, V>` for implementation-specific methods like `stats()`. `ForeignMemoryOffHeapCache` is constructed through its builder.

## Statistics

Statistics are monitoring snapshots. Concurrent updates mean they should not be treated as a transactionally consistent audit log. Neither `clear()` nor normal eviction resets the lifetime counters.

### CacheStats

`LocalCache.stats()` and `CaffeineCache.stats()` return [CacheStats](src/main/java/com/landawn/abacus/cache/CacheStats.java):

| Accessors | Meaning |
| --- | --- |
| `capacity()`, `size()` | Configured/reported capacity and current/reported size. |
| `putCount()` | Successful admissions reported by the local pool, or puts through the Caffeine wrapper. |
| `getCount()`, `hitCount()`, `missCount()` | Read request and result counters. |
| `evictionCount()` | Implementation-reported evictions. |
| `maxMemory()`, `dataSize()` | Memory accounting where supplied; `-1` means not tracked. |
| `hitRate()`, `missRate()` | Ratios from hits plus misses; both return `0.0` with no requests. |

Caffeine's read/eviction counters require `recordStats()`. Its wrapper put count excludes direct delegate writes. Capacity comes from the delegate's eviction maximum (which can be a weight limit), or zero when absent; memory fields are `-1`.

### OffHeapCacheStats

[OffHeapCacheStats](src/main/java/com/landawn/abacus/cache/OffHeapCacheStats.java) is a separate record, not a subclass of `CacheStats`. Besides common count/rate fields, it provides:

| Accessors | Meaning |
| --- | --- |
| `allocatedMemory()` | Total reserved native bytes. |
| `occupiedMemory()` | Native bytes occupied by slots, including rounding. |
| `dataSize()` | Serialized payload bytes across memory and store. |
| `sizeOnDisk()`, `dataSizeOnDisk()` | Count and payload bytes tracked in the store. |
| `putCountToDisk()`, `hitCountFromDisk()`, `evictionCountFromDisk()` | Store-specific operation counts. |
| `writeToDiskTimeStats()`, `readFromDiskTimeStats()` | `MinMaxAvg` records in milliseconds; enable `statsTimeOnDisk` to collect timing. |
| `segmentSize()` | Allocator segment size in bytes. |
| `occupiedSlots()` | Immutable nested map: slot size to segment index to occupied-slot count. |

Off-heap `capacity()` is the theoretical number of minimum-size slots, capped at `Integer.MAX_VALUE`; it is not a promised entry count. Use `occupiedMemory() / allocatedMemory()` for native utilization, and compare `dataSize() - dataSizeOnDisk()` with occupied memory to understand slot overhead. Small payloads can spend proportionally more memory on slots and heap metadata.

## Memcached locking

`com.landawn.abacus.util.MemcachedLock<K, V>` provides a **best-effort expiring lease** using Memcached's atomic `add`. Acquisition makes one attempt and returns after that server operation completes; it does not retry, queue contenders, or provide reentrancy/fairness.

```java
import com.landawn.abacus.util.MemcachedLock;

try (MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211")) {
    if (lock.tryLock("example:refresh:catalog", "worker-1", 30_000)) {
        try {
            // Do a bounded cache-refresh operation while the lease is valid.
            System.out.println("Lease metadata: " + lock.get("example:refresh:catalog"));
        } finally {
            lock.unlockQuietly("example:refresh:catalog");
        }
    } else {
        System.out.println("Another worker holds the lease");
    }
}
```

`MemcachedLock` implements `AutoCloseable`, so try-with-resources is supported here.

| Method | Behavior |
| --- | --- |
| `tryLock(target, liveTime)` | Acquires without user metadata; TTL must be positive. |
| `tryLock(target, value, liveTime)` | Acquires with optional diagnostic metadata. |
| `isLocked(target)` | Tests current presence; not ownership. |
| `get(target)` | Reads metadata; no-value and absent leases both return `null`. |
| `tryUnlock(target)` | Deletes the lease key; returns whether it was removed. |
| `unlockQuietly(target)` | Logs operational release failures and returns `false`; invalid targets/keys still throw. |
| `client()` | Exposes the underlying `SpyMemcached`. |
| `close()` | Disconnects the client; does not explicitly release every outstanding lease. |

Lock keys use the target's string representation and raw Memcached validation, not `DistributedCache` Base64 encoding. Add your own namespace. `lock`, `unlock`, and `tryUnlockQuietly` are deprecated aliases; prefer the methods above.

**Lease limitations matter for correctness:** release deletes without checking ownership. If a holder pauses beyond expiry and another acquires the key, the first holder can delete the newer lease. Eviction, restart, or a global flush can also allow overlapping holders. Metadata is diagnostic, not an ownership token checked during release. There is no ownership-safe renewal API. Use coordination with fencing and atomic conditional release when overlapping execution would violate correctness. An acquisition exception can leave the acquisition state indeterminate; it differs from a definite `false` result.

## Lifecycle and concurrency

Create and share cache instances at an appropriate application scope. Built-in implementations support concurrent operations through their pool, delegate, client, or internal synchronization; custom callbacks, stores, and implementations must meet their concurrency contracts. Compound sequences such as `containsKey` followed by `get`, or `get` followed by `put`, are not atomic.

| Owner | Effect of `close()` |
| --- | --- |
| `LocalCache` | Closes its pool, including a caller-supplied pool. |
| Either off-heap cache | Stops maintenance, removes tracked entries, frees native memory, and closes its store. |
| `CaffeineCache` | Marks the wrapper closed and invalidates the delegate's contents. |
| `Ehcache` | Marks only the wrapper closed; the application owns manager shutdown. |
| `DistributedCache` | Marks the wrapper closed and disconnects its client; does not flush server data. |
| Direct distributed client | Use `disconnect()`; the interface is not `AutoCloseable`. |
| `MemcachedLock` | Disconnects its owned client. |

Closing built-in instances is idempotent, and closed instances cannot reopen. Ordinary data operations after closure generally throw `IllegalStateException`; unsupported methods remain unsupported, and some metadata/statistics accessors have different post-close behavior. Do not use `isClosed()` as synchronization: another thread can close immediately after the check. Stop new work and finish outstanding work before shutdown.

Closing a distributed wrapper disconnects the supplied client. Avoid sharing it with owners that expect it to remain active. Off-heap close can propagate cleanup failures after attempting remaining cleanup steps; distributed close logs ordinary disconnect failures while leaving the wrapper closed.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| `NoClassDefFoundError` for an abacus or backend class | Add `abacus-common` and selected backend dependencies explicitly; the POM uses `provided` scope. |
| Redis reports Kryo unavailable | Add Kryo and ensure its parser initializes with needed module access. Redis has no JSON fallback. |
| `InaccessibleObjectException` during serialization | Compare runtime JVM options with the tested options above and inspect the package named in the exception. |
| Entry expires despite repeated `containsKey` calls | Local/off-heap membership probes do not refresh idle time. |
| Off-heap builder entries never expire | Builder TTL/idle defaults are zero; set them explicitly. |
| Expired entries appear in `size()` or `keySet()` | Reclamation can lag expiration; enable maintenance or inspect values with reads. |
| Off-heap `put` returns `false` below nominal capacity | Check serialized size, slot fragmentation, allocation pressure, store admission, and `storeSelector`. |
| Distributed reads all look like misses | Check reachability, read-failure logs, circuit opening, prefix/encoding, and serializer compatibility. |
| Redis `get` fails to decode a counter | Native counter bytes are not Kryo payloads; separate counter and object keys. |
| Memcached rejects a seemingly short key | Count the final Base64 key plus prefix; direct clients validate raw keys. |
| `keySet()` or `size()` throws | Consult the implementation matrix; several adapters deliberately do not support them. |
| Per-entry TTL has no effect with Caffeine/Ehcache | Configure expiration on the delegate. |
| RedisCluster configuration fails | Check DSL ambiguity and ensure endpoints are cluster-mode servers. |
| Other prefixes vanished after `clear()` | Distributed `clear()` flushes the backend; prefixes do not scope it. |
| Async read races ahead of a write | Await or chain the write's future before the dependent read. |
| Spillover bytes do not restore after restart | Cache metadata is in memory and is not reconstructed from store bytes. |

## Build and test

Use JDK 25 and Maven from the repository root:

```shell
# Isolated unit tests; no running Redis or Memcached service required.
mvn test

# Package the library and its attached artifacts.
mvn package

# Install this checkout's version locally without signing artifacts.
mvn -Dgpg.skip=true install

# Generate a coverage report after the unit tests.
mvn test jacoco:report
```

Unit tests use `*Test.java` names. Integration tests (`*IT.java`) and performance workloads (`*Performance.java`) are opt-in. The optional IDE launcher is `com.landawn.abacus.AbacusCacheTestSuite`.

For integration tests, explicitly configure a **dedicated disposable Memcached instance**: these tests flush its data.

```shell
mvn -Pintegration-tests "-Dcache.memcached.serverUrl=127.0.0.1:11212" test-compile failsafe:integration-test failsafe:verify
```

The current RocksDB test dependency uses a `win64` classifier, so its integration cases require Windows with this POM. A small performance run can be selected separately:

```shell
mvn -Pperformance "-Dtest=OffHeapCachePerformance" -Dcache.performance.iterations=10 -Dcache.performance.threads=2 test
```

See [docs/TESTING.md](docs/TESTING.md) for service setup, IDE VM options, full verification, memory settings, and test conventions. See [docs/COVERAGE.md](docs/COVERAGE.md) for coverage notes. Surefire/Failsafe reports are under `target/surefire-reports` and `target/failsafe-reports`; the JaCoCo HTML report is `target/site/jacoco/index.html`.

## Further reference

- [Published API documentation](https://www.javadoc.io/doc/com.landawn.abacus/abacus-cache) — select the version matching your dependency.
- [CacheFactory source](src/main/java/com/landawn/abacus/cache/CacheFactory.java) — factory overloads and provider parsing.
- [Cache package documentation](src/main/java/com/landawn/abacus/cache/package-info.java) — implementation overview.
- [Test examples](src/test/java/com/landawn/abacus/cache) — executable contracts and backend-specific cases.
- [Changelog](CHANGES.md) and [project wiki](https://github.com/landawn/abacus-cache/wiki).
- Related projects: [abacus-common](https://github.com/landawn/abacus-common) and [abacus-jdbc](https://github.com/landawn/abacus-jdbc).

## License

Licensed under the [Apache License 2.0](LICENSE).
