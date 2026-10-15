# Unit-test coverage

Measured on 2026-10-15 with JDK 25 and JaCoCo 0.8.15. Production code and coverage exclusions are unchanged.

| Metric | Before | Expanded unit suite | With allocator assertions-disabled check |
| --- | ---: | ---: | ---: |
| Lines | 89.36% (1,638/1,833) | 99.73% (1,828/1,833) | 99.84% (1,830/1,833) |
| Branches | 83.24% (720/865) | 98.61% (853/865) | 98.61% (853/865) |
| Methods | 90.89% (369/406) | 100% (406/406) | 100% (406/406) |
| Classes | 100% (41/41) | 100% (41/41) | 100% (41/41) |

The retained `AbacusCacheTestSuite` passed **730 tests**, with no failures, errors, or skips, in randomized order using seed `20261015`. The baseline passed 614 tests. The additional allocator invocation also passed with assertions disabled, exercising the runtime fallback behind its development-time assertion.

## Reproduce

For a fresh unit-only report:

```shell
mvn clean test jacoco:report
```

To include the allocator's assertions-disabled fallback in the same report, run this afterward without cleaning:

```shell
mvn "-Dtest=OffHeapAllocatorTest#staleSlotReleaseLeavesAllocatorConsistent" -DenableAssertions=false test jacoco:report
```

Open `target/site/jacoco/index.html`. JaCoCo appends the second invocation to the first execution data file. This is intentional: the two runs exercise different assertion modes of the same production bytecode.

The verified run used the suite launcher and explicit data file `target/coverage-verified.exec`. Its full-suite log is `target/coverage-verified.log`; the supplementary log is `target/coverage-assertions-disabled.log`. The report combines only those two successful runs. Generated files are local build artifacts.

## Added coverage

Tests now cover property-map operations, factory parsing and context-classloader providers, optional serializers, Redis shard cleanup, Memcached futures and counters, lock aliases, logging levels, breaker recovery races, native-allocation cleanup, disk promotion, statistics overflow, and rejected background tasks.

`OffHeapCacheInitializationTest` uses fresh implementation classes for startup and scheduler scenarios, preserving their production code source for coverage instrumentation. Its controlled executors avoid timing-dependent task submission tests. `LogCapture` restores the original logger configuration; tests using it share a JUnit resource lock.

## Remaining gaps

The combined report still contains **three uncovered lines and twelve uncovered branch outcomes**:

- `SpyMemcached` constructor cleanup: `tempMc.shutdown()` is explicitly documented in the source as unreachable with the current try-block body. It protects future changes after client creation.
- `SpyMemcached.checkValue`: the stock-transcoder null rejection is tested across synchronous, asynchronous, and legacy write APIs. JaCoCo leaves the throwing `N.checkArgNotNull` call uncovered because its implicit exception prevents the subsequent probe from executing, consistent with [JaCoCo's exception-coverage limitation](https://www.jacoco.org/jacoco/trunk/doc/faq.html).
- `AbstractOffHeapCache.reclaimEmptySegmentsLocked`: the final fall-through return would require the tracked empty-segment count to disagree with the queues; normal completion returns earlier when that count reaches zero.

The remaining branch outcomes include the above guards, already-freed entry checks, repeated hook removal, promotion rollback after publication, reused exception identities in factory cleanup/class loading, and the non-IOException cause guard in `KryoTranscoder`.

The report therefore does **not** establish 100% line or branch coverage. Tests retain the production invariants and defensive checks; no exclusions were added to reach a percentage.
