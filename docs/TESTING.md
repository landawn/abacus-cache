# Testing abacus-cache

Use JDK 25 and Maven. Run commands from the repository root.

## Test categories

| Category | Naming and location | Command | Requirements |
| --- | --- | --- | --- |
| Unit | `*Test.java`, matching the production package | `mvn test` | No running services |
| Integration | `*IT.java` | `integration-tests` profile | Dedicated Memcached server; RocksDB native library |
| Performance | `*Performance.java` | `performance` profile | Explicit opt-in; configurable workloads |

Surefire discovers unit tests by filename. No suite registration or base class is needed. Test failures and empty test selections fail the build. Unit tests use fresh, small caches and mocked network clients; they do not need Memcached, Redis, or a RocksDB database. Redis unit tests cover the shared client contract and implementation-specific routing with mocked Jedis clients.

## Unit tests and coverage

See [the coverage results](COVERAGE.md) for the measured improvement, remaining gaps, and the optional assertions-disabled allocator check.

```shell
mvn test
mvn "-Dtest=LocalCacheTest" test
mvn test jacoco:report
```

Surefire reports are in `target/surefire-reports`. The JaCoCo HTML report is `target/site/jacoco/index.html`; XML and CSV reports are alongside it. The forked JVM retains the JaCoCo agent together with the native-memory module options. Coverage data accumulates across invocations; use `mvn clean test jacoco:report` for a fresh report.

Tests default to a 1 GB heap and a 256 MB direct-memory limit. Override them only when needed with `-Dtest.heap=2g` or `-Dtest.directMemory=512m`. Native off-heap allocations are separate from those JVM limits; each fixture must close its cache.

To check independence with a reproducible random order (single quotes also preserve `$` in PowerShell):

```shell
mvn test '-Djunit.jupiter.testmethod.order.default=org.junit.jupiter.api.MethodOrderer$Random' '-Djunit.jupiter.testclass.order.default=org.junit.jupiter.api.ClassOrderer$Random' -Djunit.jupiter.execution.order.random.seed=20261014 -Dsurefire.runOrder=random -Dsurefire.runOrder.random.seed=20261014
```

## IDE suite launcher

After refreshing the Maven project, run `com.landawn.abacus.AbacusCacheTestSuite` as a JUnit test in your IDE. It discovers Jupiter classes ending in `Test` under `com.landawn.abacus`, including their nested and parameterized cases. Discovery does not depend on tags or `TestBase`. Integration and performance tests remain opt-in.

You can also launch the same suite from Maven, with its JVM options applied automatically:

```shell
mvn "-Dtest=AbacusCacheTestSuite" test
```

Normal `mvn test` continues to discover unit classes directly. Its filename pattern excludes the launcher, so adding the suite does not run tests twice.

For a direct IDE JUnit run, use JDK 25 and add these VM options to the launch configuration (Maven's `argLine` is not applied by a direct JUnit runner):

```text
-Dfile.encoding=UTF-8
-Xmx1g
-XX:MaxDirectMemorySize=256m
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

## Integration tests

Memcached integration tests flush server data. Always provide a dedicated disposable instance explicitly through `cache.memcached.serverUrl`. Missing configuration fails immediately instead of silently skipping tests.

For example, start a test container, run the integration tests, then stop that container:

```shell
docker run --detach --rm --name abacus-cache-memcached-test --publish 127.0.0.1:11212:11211 memcached:1.6
mvn -Pintegration-tests "-Dcache.memcached.serverUrl=127.0.0.1:11212" test-compile failsafe:integration-test failsafe:verify
docker stop abacus-cache-memcached-test
```

Always stop the container after the run, including when tests fail. Failsafe reports are in `target/failsafe-reports`. To run unit and integration tests through the full lifecycle without signing artifacts:

```shell
mvn -Pintegration-tests "-Dcache.memcached.serverUrl=127.0.0.1:11212" -Dgpg.skip=true verify
```

To run only the RocksDB integration cases, with no Memcached server:

```shell
mvn -Pintegration-tests "-Dit.test=RocksDbIT,OffHeapCacheRocksDbIT" test-compile failsafe:integration-test failsafe:verify
```

RocksDB tests own temporary databases and close all native handles before cleanup. The existing dependency uses the `win64` classifier, so these cases currently require Windows. The off-heap integration cases run against both Unsafe and foreign-memory backends.

## Performance workloads

```shell
mvn -Pperformance "-Dtest=OffHeapCachePerformance" -Dcache.performance.iterations=1000 -Dcache.performance.threads=8 test
```

Use `-Dcache.performance.capacityMb=64` to set each cache allocation. These values are the defaults. A quick smoke run can use 10 iterations and 2 threads. The profile also includes unit tests when no `-Dtest` selection is supplied. Workloads check results and worker failures while printing profiler output; use a controlled benchmark environment before drawing performance conclusions.

## Conventions for adding or changing tests

- Match production packages. Use package-private test classes and methods, `*Test` for isolated unit tests, and lowerCamelCase method names describing behavior and conditions.
- Keep fixture fields and lifecycle methods first, then named behavior groups in API order: construction/configuration, reads, writes/counters, removals, views/statistics, expiration/allocation, lifecycle/concurrency, compatibility, and validation, omitting groups that do not apply. Alphabetize test methods within each group and put helpers last. Use `@Nested` when a large suite needs named groups. Source ordering is for navigation; tests must not depend on execution order.
- Keep assertions close to the operation they verify. Use `assertThrows`, exact outcomes and counters, and meaningful mock interaction checks. Avoid print-only checks and assertions that merely restate the setup.
- Parameterize shared behavior and boundary values, with a separate reported invocation per case. Consolidate only genuinely equivalent scenarios; preserve backend-specific behavior and regression cases.
- Give each case fresh mutable state. Close native caches, client mocks, managers, stores, and executors even after failure. Use `@TempDir` for disk state.
- Prefer explicit latches and bounded futures for concurrency. Use `TestSupport.awaitCondition` for asynchronous state changes, with a condition that does not refresh expiry. Do not use sleeps to guess whether a competing operation has started.
- Keep external service calls in integration tests and heavy workloads in performance tests. Never add a default dependency on a developer's local server.
- When consolidating, map each removed scenario to its surviving test and compare coverage. A smaller method count alone does not demonstrate equivalent coverage.

See [the implemented cleanup plan](TEST_CLEANUP_PLAN.md) for validation results and the map from removed or moved cases to their surviving owners.

## Consolidated ownership

| Behavior | Owner |
| --- | --- |
| Common off-heap behavior and allocation boundaries | `OffHeapCacheContractTest`, parameterized over both backends |
| Shared off-heap serialization, disk-store, promotion, expiry, and lifecycle regressions | Behavior groups in `AbstractOffHeapCacheTest` |
| Optional-dependency startup and background-task submission failures | `OffHeapCacheInitializationTest`, with isolated static state |
| Native backend details and allocator regressions | `OffHeapCacheTest`, `ForeignMemoryOffHeapCacheTest`, `OffHeapAllocatorTest` |
| Shared Redis operations and bulk-memory regressions | `AbstractJedisCacheClientTest`; routing and construction stay in `JRedisTest` and `JRedisClusterTest` |
| Distributed cache operations and argument validation | `DistributedCacheTest`; breaker transitions and recovery races in `DistributedCacheCircuitBreakerTest` |
| Provider-independent factory validation | `CacheFactoryTest.ProviderValidation`, with shared invalid-timeout and parameter-count matrices |
| Memcached protocol versus future handling | `SpyMemcachedTest` and `SpyMemcachedFutureTest`; live server cases in `SpyMemcachedIT` |
| Lock key validation, Unicode, and lifecycle | `com.landawn.abacus.util.MemcachedLockTest`; live leases in `MemcachedLockIT` |
| Kryo encoding, decoding, limits, and failures | Behavior groups in `KryoTranscoderTest` |
| Real disk persistence and promotion | `RocksDbIT` and `OffHeapCacheRocksDbIT` |

The optional suite launcher selects unit classes by name, so tests do not need to extend `TestBase`. Duplicate `*UnitTest` classes and the print-only RocksDB example have been replaced by the consolidated suites and explicit integration tests above.
