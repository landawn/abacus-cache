# Production source review — 2026-09-20

Four reviewers covered every line of all 25 Java source files under `src/main/java`, including nested classes, compatibility helpers, and package documentation. The review checked implementation, callers, existing tests, and relevant locally installed dependency sources. Existing staged POM changes were preserved.

Seven defects were fixed, with 22 new tests added to seven existing test classes and one existing test strengthened. No new permanent test classes were introduced.

| Defect | Resulting behavior | Regression coverage |
| --- | --- | --- |
| Unsynchronized property formatting | `AbstractCache` locks the property map while formatting, avoiding `ConcurrentModificationException` during concurrent writes and preserving self-reference formatting. | `AbstractCacheTest`: concurrent formatting/write and self-reference tests |
| Promotion restarts expiration/access metadata | Off-heap promotion preserves original TTL and idle deadlines and access history; expiration during copying prevents installation. | `AbstractOffHeapCacheTest`: TTL/idle expiration during copying and strengthened promotion metadata test |
| Stale idle-expiration candidate | Maintenance rechecks expiration under the entry monitor after an in-flight reader can refresh idle time. | `AbstractOffHeapCacheTest`: maintenance waiting on a refreshed entry |
| Repeated promotion-slot release | Failed reclamation cannot trigger a second release of already-released slots; cleanup preserves the primary failure. | `AbstractOffHeapCacheTest`: injected reclamation failure and copy-failure rollback |
| Malformed Unicode key collisions | Distributed, Redis, Memcached, and lock operations reject unpaired UTF-16 surrogates before UTF-8 replacement can alias a different key. Valid supplementary characters retain their existing encoding. | `JRedisTest`, `JRedisClusterTest`, `DistributedCacheCircuitBreakerUnitTest`, `SpyMemcachedFutureUnitTest`, `MemcachedLockValidationUnitTest` |
| Cancellation masks a wait failure | Memcached timeout/interruption remains the primary failure when cancellation throws; cancellation failure is suppressed. | `SpyMemcachedFutureUnitTest`: synchronous timeout, interruption, and bulk timeout |
| Async failure interrupts the waiting caller | An `InterruptedException` carried by `ExecutionException` no longer sets the caller's interrupt flag. Direct interruption still restores it. | `SpyMemcachedFutureUnitTest`: worker interruption plus runtime/error compatibility checks |

Javadoc changes clarify expiration policies, namespace separation, Unicode validation, timeout scope, Redis routing, serializer compatibility, store ownership and restart limitations, statistics, and backend-specific behavior. Broken constant and exception references were corrected.

## Coverage

Paths below are relative to `src/main/java/com/landawn/abacus`.

| Source | Review result |
| --- | --- |
| `cache/AbstractCache.java` | Property-formatting fix and synchronization documentation |
| `cache/AbstractDistributedCacheClient.java` | Shared UTF-8 key validation and TTL documentation |
| `cache/AbstractJedisCacheClient.java` | Key/bulk validation and routing/operation documentation |
| `cache/AbstractOffHeapCache.java` | Promotion, expiration, cleanup fixes and lifecycle documentation |
| `cache/Cache.java` | Backend-specific success/failure and retention contracts |
| `cache/CacheFactory.java` | Exception references, namespace, routing, decimal-timeout documentation |
| `cache/CacheStats.java` | Metric interpretation and example corrections |
| `cache/CaffeineCache.java` | Reviewed unchanged; no additional confirmed defect |
| `cache/ChronicleMap.java` | Reviewed unchanged; no additional confirmed defect |
| `cache/cs.java` | Reviewed unchanged; no additional confirmed defect |
| `cache/DistributedCache.java` | Collision prevention and breaker/key documentation |
| `cache/DistributedCacheClient.java` | Key contracts; compatibility-support implementation reviewed |
| `cache/Ehcache.java` | Expiry policy, loader, retention, enumeration documentation |
| `cache/ForeignMemoryOffHeapCache.java` | Allocator/store documentation and exception references; inherits shared fixes |
| `cache/JRedis.java` | Sharding/topology and timeout documentation; inherits key validation |
| `cache/JRedisCluster.java` | Slot-routing documentation; inherits key validation |
| `cache/KryoTranscoder.java` | Wire compatibility, registration, instantiation, decoding documentation |
| `cache/LocalCache.java` | Reviewed unchanged; no additional confirmed defect |
| `cache/OffHeapCache.java` | Allocator/store documentation and exception references; inherits shared fixes |
| `cache/OffHeapCacheStats.java` | Timing, allocation, and eviction metric documentation |
| `cache/OffHeapStore.java` | Ownership and restart-recovery limitations |
| `cache/SpyMemcached.java` | Key validation, future failure handling, timeout/serialization documentation |
| `cache/package-info.java` | Backend differences, dependency/fallback and thread-safety documentation |
| `util/MemcachedLock.java` | Derived-key validation, including quiet release and subclass overrides |
| `util/package-info.java` | Lock key identity and namespace guidance |

## Verification

- All production and test sources compile with Java 25 and the dependencies declared in the current POM.
- **355 distinct tests passed** across 16 existing test classes. The initial selected run passed 352 tests; after the final fixes, both affected classes were rerun, passing 54 tests. The combined latest reports contain 355 tests with zero failures/errors.
- A separate compilation of the original `HEAD` production sources ran against the final regression tests: **17 failures and 6 passing compatibility/rollback controls**. The same 23 checks against the fixed sources **all passed**.
- Both actual native backends passed additional disk-to-memory promotion, byte round-trip, cleanup, and close checks using **1 MB per cache**.
- Javadoc generation with `doclint=all` passed.
- `git diff --check` passed.

The full configured suite could not be verified in this environment: Memcached was unavailable at `localhost:11211`, and an existing large-cache fixture failed to allocate 4 GB of native memory. The selected regression tests do not require a running cache server. Small native-backend checks supplement the shared off-heap unit tests; the large-memory and server-dependent integration tests remain unverified.

The selected classes can be rerun with:

```powershell
mvn -o '-Dmaven.repo.local=C:\Users\haiyangl\.m2\repository' '-Dmaven.test.failure.ignore=false' '-Dtest=AbstractCacheTest,LocalCacheTest,CacheStatsTest,CacheFactoryTest,CaffeineCacheTest,EhcacheTest,ChronicleMapTest,AbstractDistributedCacheClientTest,DistributedCacheCircuitBreakerUnitTest,JRedisTest,JRedisClusterTest,SpyMemcachedFutureUnitTest,MemcachedLockValidationUnitTest,KryoTranscoderTest,AbstractOffHeapCacheTest,OffHeapCacheStatsTest' test
mvn -o '-Dmaven.repo.local=C:\Users\haiyangl\.m2\repository' -DskipTests javadoc:javadoc
```

Verification logs and temporary before/after runners are under `target/review-*`; they are not part of the source changes.
