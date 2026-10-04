# Iterative production-source review — 2026-10-03

This ledger is the sole handoff between review cycles. Scope: every line of all 25 Java files under `src/main/java`, including nested classes and package documentation. Starting commit: `2548d62`; worktree initially clean. The older `source-review-2026-09-20.md` is historical evidence, not coverage for this review.

**Follow-up correction:** the user identified a direct-buffer compatibility regression missed by the three-cycle review. The original OH-1 subtype-preserving implementation has now been reverted at the user's direction. All buffer inputs again reconstruct as writable, array-backed heap buffers; `V = MappedByteBuffer` is explicitly unsupported. The follow-up section at the end supersedes the earlier OH-1 assessment and test totals. The original cycle history is retained for accuracy.

## Procedure and stop conditions

Each cycle uses fresh reviewers with no prior conversation context; they read this ledger and current files. Review implementation, contracts, callers, tests, and local dependency evidence. Confirm substantive defects, add regression tests, comment complex fixes, align Javadoc, and inspect every changed line. Rerun the suite for each fix. Stop after a cycle with no new blocker/major/minor findings, two consecutive cycles containing only WONTFIX findings, or three cycles. Report cycle count, severity/state totals, net test-count delta, and unresolved limitations.

The primary coordinator cannot erase its own conversation context via an available tool; fresh agents are used for independent cycle review, and this ledger carries all durable review evidence.

## Baseline and verification

- Source inventory: 25 files (23 cache files, `util/MemcachedLock.java`, `util/package-info.java`).
- Baseline: 617 source test annotations, of which 616 are in suite-discoverable test classes (one is in the unselected `RocksDBExample`). Verified against `HEAD` using `git grep`, including 14 fully qualified `@org.junit.jupiter.api.Test` annotations. The initial simple-name-only count was 603 and was corrected during the audit. Initially there were no source/test changes.
- Suite command: `mvn -o -Dmaven.repo.local=C:\Users\haiyangl\.m2\repository -Dmaven.test.failure.ignore=false test`.
- Every suite run must inspect actual Surefire XML results; the POM otherwise ignores test failures.
- Baseline configured suite stopped at `ForeignMemoryOffHeapCacheTest` when its static fixture could not allocate 4 GB. Its preceding 324 tests had two missing nested-class errors caused by an earlier sandbox-interrupted test compilation, not source defects. Fresh compilation of all 25 production and 27 test source files succeeded outside the sandbox (`target/review-compile.log`).
- The sandbox blocked javac's dependency JAR real-path lookup. Subsequent compilation/tests use elevated local Maven access. Concurrent work in other repositories also causes host memory pressure; those unrelated processes were left alone.
- Reproducible reduced-memory runner: temporary `target/review-pom.xml` copies the real POM, points at the real source/test directories, writes to `target/review-build`, sets Maven and test heaps to 512 MB (initial 32 MB), and explicitly sets `testFailureIgnore=false` (the real POM's literal `true` overrides its usual command-line property). Invoke with `MAVEN_OPTS=-Xms32m -Xmx512m`, `mvn -o -f target/review-pom.xml -Dmaven.repo.local=C:\Users\haiyangl\.m2\repository '-Dtest=*Test,!OffHeapCacheTest,!ForeignMemoryOffHeapCacheTest' test`. The initial 256 MB direct-memory cap worked for this selection but prevented the existing 4 GB Ehcache benchmark from starting in the full suite, so it has now been restored to the original 5 GB. Full suite retry uses `-Dtest=*Test -DreuseForks=false` to isolate each class in a new JVM; no source tests are altered or disabled.
- Red run of 563 tests (560 existing plus three OH-1 tests): exactly the two mapped-buffer regressions failed with `ClassCastException`; the heap-buffer compatibility control and all other tests passed. Evidence: `target/review-cycle1-small.log`, XML copied to `target/review-cycle1-red-reports`. This first temporary runner still inherited ignored test failures; the XML, not its misleading BUILD SUCCESS, establishes the red result.

## Cycle 1 — complete

### Coverage assignments

- Off-heap reviewer: `AbstractOffHeapCache`, `OffHeapCache`, `ForeignMemoryOffHeapCache`, `OffHeapCacheStats`, `OffHeapStore`.
- Distributed reviewer: `AbstractDistributedCacheClient`, `AbstractJedisCacheClient`, `DistributedCache`, `DistributedCacheClient`, `JRedis`, `JRedisCluster`.
- Memcached reviewer: `SpyMemcached`, `KryoTranscoder`, `util/MemcachedLock`, `util/package-info`.
- Coordinator: `AbstractCache`, `Cache`, `CacheFactory`, `CacheStats`, `CaffeineCache`, `ChronicleMap`, `cs`, `Ehcache`, `LocalCache`, `cache/package-info`.

### Findings

| ID | Severity | State | Defect and action | Regression coverage |
| --- | --- | --- | --- | --- |
| OH-1 | Minor | Fixed | Both native backends accept a public `MappedByteBuffer` value but reconstruct `HeapByteBuffer`, causing the caller's generic result cast to fail. Reconstruct that public family as an independent writable direct buffer; retain heap behavior for heap inputs. Comment explains subtype preservation; class Javadoc explains content-copy semantics and loss of original mapping. | `AbstractOffHeapCacheTest`: typed mapped memory read; typed disk read/promotion; heap compatibility control. Real read-only file maps, both native backends, single/multiple slots, source-arena closure, and independent returned content. |
| MC-1 | Minor | Fixed | `SpyMemcached.disconnect(long)` describes a maximum shutdown deadline, but spymemcached synchronously admits broadcast operations before starting its response latch timeout, and the wrapper may first wait for its monitor. Corrected summary, cross-reference, body, example and parameter documentation; zero skips graceful waiting but does not promise immediate return. | `SpyMemcachedFutureUnitTest.gracefulDisconnectTimeoutDoesNotBoundQueueAdmission`: real delegate shutdown with mock connection and latch-controlled queue admission, no network dependency. |

### Review evidence

All assigned production files were read line by line, including nested types, builders, records, Javadoc, and examples. The coordinator covered its ten files and relevant tests; the three reviewers covered the remaining fifteen.

- Off-heap: allocation/chunk arithmetic, slot rollback/reclamation, lifecycle and entry/bin lock order, same-key disk replacement, mutable array ownership, expiration/promotion metadata, snapshots, close cleanup, both native copy hooks, builders, and store examples. Exact abacus-common 8.1.0 `ActivityPrint` and `ByteBufferType` sources checked. Only OH-1 confirmed.
- Distributed: all six files (3,980 lines) and five test files; TTL boundaries, key validation, bulk snapshots, compatibility aliases/recursion guards, circuit breaker races, pooled/cluster routing, flush aggregation, rollback, serialization, and close. Exact Jedis 8.0.1 and abacus-common 8.1.0 sources plus 20,101 numeric payload decode probes checked. Zero findings. Details: `target/review-cycle1-distributed.md`.
- Memcached: all four files (4,077 lines); futures, interruption/cancellation, TTL, bulk snapshots, counters, key encoding, shutdown, transcoding, lock token ownership and quiet release. Exact spymemcached 2.12.3 sources checked. Only MC-1 confirmed. Details: `target/review-cycle1-memcached.md`.
- Coordinator: synchronized property map and async delegation; all factory provider layouts, reflective initialization/classloader/nested-name rules, and cleanup; record validation/rate overflow; Caffeine stats and concurrent close/late-write cleanup; Ehcache ownership/lifecycle/bulk/writer contracts; LocalCache pool delegation, TTL normalization and snapshots; Chronicle compatibility, constants, core and package contracts. Exact installed `Properties`, `GenericKeyedObjectPool`, and `TypeAttrParser` sources checked. Zero findings.
- Rejected hypotheses include off-heap overflow/use-after-free/stale promotions, distributed TTL overflow/key collisions/alias recursion/partial flush, property-map compound-operation races, Caffeine loss of newer different-instance writes, Ehcache wrapper close improperly owning the manager, and LocalCache negative TTL reaching `ActivityPrint`. Existing code or documented contracts already address them. No WONTFIX finding was manufactured from a documented limitation.

### Self-review and tests

Coordinator and fix authors read every changed source/test line, checking real typed casts, byte ownership, both backends, heap compatibility, comment/Javadoc agreement, shutdown dependency semantics, and test cleanup. No production change lacks regression coverage. `git diff --check` passed.

- After OH-1: 564 tests passed, zero failures/errors/skips, including all four new tests and live Memcached integration. Log `target/review-cycle1-oh1.log`; archived XML `target/review-cycle1-oh1-reports`.
- Full suite after OH-1 was also attempted: the reduced direct-memory cap prevented one existing Ehcache benchmark; later the fork exhausted host native memory during `OffHeapCacheTest`. 530 tests reported, zero assertion failures, one resource error before the crash. Logs `target/review-cycle1-oh1-full.log`, `target/hs_err_pid8820.log`; archived XML `target/review-cycle1-oh1-full-reports`. These resource failures are not counted as production findings.
- After MC-1: full suite in separate class JVMs finished with the original 5 GB direct-memory limit and smaller heaps. `ForeignMemoryOffHeapCacheTest` still crashed because the host could not commit memory. **599 tests passed**, zero failures/errors/skips in fresh XML, including all 35 `OffHeapCacheTest` tests. Log `target/review-cycle1-mc1-full.log`; archived XML `target/review-cycle1-mc1-full-reports`.
- FFM retry: the full class exhausted its 512 MB Java heap in its existing Ehcache benchmark. Running its 20 other tests unchanged passed, zero failures/errors/skips (`target/review-ffm-functional.log`, `target/review-ffm-functional.xml`). Combined distinct current-source results: **619 passed**. The sole unverified test is `ForeignMemoryOffHeapCacheTest.test_perf_vs_ehcache`: it eagerly holds the class's 4 GB native fixture plus a 2 GB Ehcache off-heap tier and up to one million heap entries; it benchmarks Ehcache directly rather than either modified method. Current host commit headroom was approximately 7.3 GB after the functional run, insufficient for that combination and a larger test heap. No fixture/source logic was reduced to manufacture a pass. The unchanged `RocksDBExample` is outside configured suite discovery.
- Javadoc generation with `doclint=all` passed (`target/review-javadoc.log`). Final production hashes matched all 25 files reviewed/tested before cycle 2; no files changed during that independent pass up to the CF-1 discovery.
- Current source test count: 621; suite-discoverable test count: 620; net +4 by either measure (three OH-1 tests, one MC-1 test). No parameterized/dynamic multiplicity is involved; the extra source annotation is the unchanged unselected example.

## Cycle 2 — complete

Start from this ledger and current files only. New agents have no prior conversation context. Do not count either fixed finding again unless a concrete defect remains. Review every assigned production line and relevant tests/dependencies. Send confirmed blocker/major/minor findings to the coordinator before editing; the coordinator serializes fixes and test runs. Do not run Maven concurrently. Record coverage, rejected hypotheses, and evidence. Nits are not findings.

Assignments (paths relative to `src/main/java/com/landawn/abacus`):

- `cycle2_native`: `cache/AbstractOffHeapCache.java`, `cache/OffHeapCache.java`, `cache/ForeignMemoryOffHeapCache.java`, `cache/OffHeapCacheStats.java`, `cache/OffHeapStore.java`, `cache/KryoTranscoder.java`.
- `cycle2_network`: `cache/AbstractDistributedCacheClient.java`, `cache/AbstractJedisCacheClient.java`, `cache/DistributedCache.java`, `cache/DistributedCacheClient.java`, `cache/JRedis.java`, `cache/JRedisCluster.java`, `cache/SpyMemcached.java`, `util/MemcachedLock.java`, `util/package-info.java`.
- `cycle2_core`: `cache/AbstractCache.java`, `cache/Cache.java`, `cache/CacheFactory.java`, `cache/CacheStats.java`, `cache/CaffeineCache.java`, `cache/ChronicleMap.java`, `cache/cs.java`, `cache/Ehcache.java`, `cache/LocalCache.java`, `cache/package-info.java`.

Cycle-2 native/transcoder and network reviews are complete with zero new findings; evidence is in `target/review-cycle2-native.md` and `target/review-cycle2-network.md`. The core reviewer completed its production reads and discovered CF-1 below. Cycle 3 is therefore required after cycle-2 fixes and self-review.

| ID | Severity | State | Evidence / next action |
| --- | --- | --- | --- |
| CF-1 | Major | Fixed | A real Caffeine 3.3.0 cache with `maximumSize(1)`, `executor(Runnable::run)`, and a removal listener that calls wrapper `close()` deadlocks when a concurrent external `close()` or `clear()` holds the wrapper's destructive-operation lock and waits for Caffeine's eviction lock. Listener reentry waits for the wrapper lock while holding the eviction lock. `target/CaffeineCloseProbe.java` reproduced a `ThreadMXBean`-detected deadlock. Two deterministic tests against the original lifecycle code fail; all 25 prior Caffeine tests pass (`target/review-cycle2-cf1-red.log`, `target/review-cycle2-cf1-red.xml`). |

CF-1 design: remove blocking wrapper locks spanning delegate calls; elect a single close state transition, then invalidate outside its synchronization. Retain validation order, one-time close cleanup, and quiet value-conditional cleanup of late puts. A closed-flag fast path alone fails the concurrent-clear case. Locking wrapper hot paths or tracking wrapper threads does not cover callbacks originating from direct writes to the caller-retained delegate; tryLock/timeouts invent new contention failures. The necessary compatibility change is explicit: close publishes terminal state but is no longer a quiescence barrier for already-admitted remove/clear calls or a concurrent repeated close. Owners must stop wrapper users before reusing the delegate. Javadoc and the two existing tests that assumed a waiting barrier now match this contract; direct-delegate callback coverage was added. Reviewer evidence: `target/review-cycle2-core.md`.

### Implementation and self-review

CF-1 uses `AtomicBoolean.compareAndSet` to elect a single close; all delegate calls execute without a wrapper lock. Both lifecycle-ordering tests now describe the admitted-operation contract. Three new tests cover listener close against concurrent close/clear, plus callbacks from direct retained-delegate writes that call close, clear, or remove. The existing paused-close test also verifies that repeated close does not wait or invalidate twice. The author and coordinator independently read every changed production/test line, including validation order, visibility, one-time cleanup, quiet late-put cleanup, callback lock order, test latch ordering/failure capture, bounded regression termination, and Javadoc. The coordinator re-read all cycle-1 diffs as part of this audit. No extra production/test edits were introduced.

- Caffeine green run: **28 tests passed**, zero failures/errors/skips (`target/review-cycle2-cf1-green.log`, `target/review-cycle2-cf1-green.xml`).
- Source annotations: **624**, suite-discoverable methods: **623**, net **+7** from baseline (+3 OH-1, +1 MC-1, +3 CF-1). Two existing Caffeine tests were revised, not added or removed.
- Full-suite retry used isolated class JVMs and a **1 GB test heap** (Maven heap stays 512 MB), with the original 5 GB direct-memory cap. The existing FFM/Ehcache benchmark again exhausted Java heap. The other 20 classes produced fresh XML with **602 passed**, zero failures/errors/skips (`target/review-cycle2-cf1-full.log`, `target/review-cycle2-cf1-full-reports`). The 20 unchanged FFM functional tests then passed separately (`target/review-cycle2-ffm-functional.log`, `target/review-cycle2-ffm-functional.xml`). Combined current-source coverage: **622 of 623 suite tests passed**; only `ForeignMemoryOffHeapCacheTest.test_perf_vs_ehcache` remains unverified because of memory limits. No source/test workload was reduced.
- Javadoc generation with `doclint=all` passed again. The first invocation reused stale generated output; the coordinator detected the old timestamp/text and forced regeneration with a fresh `staleDataPath`. The generated Caffeine HTML now contains the updated close contract and has the current timestamp (`target/review-cycle2-javadoc-forced.log`). `git diff --check` passed.
- Current production/test source hash snapshot: `target/review-cycle2-source-test-hashes.json`.

## Cycle 3 — complete, zero new findings

This is the third and final cycle. Each newly created reviewer receives only this ledger path, with no inherited conversation context. Read current files rather than trusting prior findings or evidence. Review every assigned production line, nested type, comment, contract and example, and inspect relevant callers/tests/exact installed dependency sources. Independently assess the existing fixes as well. Confirm substantive blocker/major/minor defects; do not count nits, speculative risks or already documented limitations. Send confirmed findings to the coordinator before any edit. Do not run Maven concurrently; the coordinator owns test execution. Record coverage and rejected hypotheses in `target/review-cycle3-<role>.md`, and report zero explicitly if no new finding survives investigation.

Assignments (paths relative to `src/main/java/com/landawn/abacus`; use the role in your agent task name):

- `cycle3_native`: `cache/AbstractOffHeapCache.java`, `cache/OffHeapCache.java`, `cache/ForeignMemoryOffHeapCache.java`, `cache/OffHeapCacheStats.java`, `cache/OffHeapStore.java`, `cache/KryoTranscoder.java`.
- `cycle3_network`: `cache/AbstractDistributedCacheClient.java`, `cache/AbstractJedisCacheClient.java`, `cache/DistributedCache.java`, `cache/DistributedCacheClient.java`, `cache/JRedis.java`, `cache/JRedisCluster.java`, `cache/SpyMemcached.java`, `util/MemcachedLock.java`, `util/package-info.java`.
- `cycle3_core`: `cache/AbstractCache.java`, `cache/Cache.java`, `cache/CacheFactory.java`, `cache/CacheStats.java`, `cache/CaffeineCache.java`, `cache/ChronicleMap.java`, `cache/cs.java`, `cache/Ehcache.java`, `cache/LocalCache.java`, `cache/package-info.java`.

All three fresh reviewers completed their assignments with **zero new blocker, major, minor, or WONTFIX findings**. The assignment inventory exactly matches all 25 current production files, with no duplicates or omissions: **17,452 lines** reviewed in this final cycle.

- Native/transcoder: six files, 4,704 lines. Rechecked native/store lifecycle and ownership, lock order, allocation/index arithmetic, rollback/reclamation, failure-atomic disk replacement, private byte arrays, stale promotion/expiration, statistics, parser/transcoder limits and OH-1's real typed tests. Read exact ActivityPrint, ByteBufferType/TypeFactory, KryoParser/ParserFactory, Kryo and spymemcached transcoder sources. OH-1 remains sound. Evidence: `target/review-cycle3-native.md`.
- Network/lock: nine files, 7,618 lines. Rechecked compatibility bridges/recursion guards, immutable breaker publication, key/TTL boundaries, bulk snapshots, Redis pool/topology ownership and error aggregation, Memcached counters/future timing/interruption/shutdown, and marker/quiet-release lock semantics. Read exact Jedis, spymemcached and abacus-common sources. MC-1 remains sound. Evidence: `target/review-cycle3-network.md`.
- Core: ten files, 5,130 lines. Rechecked property-map synchronization, factory parsing/reflection/cleanup, pool expiration/replacement/ownership, Ehcache callbacks/bulk/lifecycle, statistics and API/package contracts. Independently checked CF-1 against exact Caffeine maintenance/removal callback paths and the current real-delegate regressions. Read exact abacus-common and Ehcache sources. CF-1 remains sound. Evidence: `target/review-cycle3-core.md`.

All reviewed production hashes match the cycle-2 verification snapshot. No production/test edits occurred during cycle 3, so no additional suite rerun was warranted. The coordinator read all three final evidence reports and checked coverage against the actual source inventory.

## Original three-cycle stop and completion audit

**Stopped after three cycles.** Cycle 3 has zero new substantive findings, and the three-cycle maximum has also been reached. The two-consecutive-WONTFIX condition was not needed.

| Severity | Fixed | WONTFIX | Open |
| --- | ---: | ---: | ---: |
| Blocker | 0 | 0 | 0 |
| Major | 1 (CF-1) | 0 | 0 |
| Minor | 2 (OH-1, MC-1) | 0 | 0 |

- Every production file received line-by-line review in each cycle, using multiple reviewers. Cycles 2 and 3 used newly created agents with no inherited conversation history and only this ledger path as their initial handoff. The coordinator's context-reset limitation is disclosed above.
- Every fix has relevant regression/characterization coverage, comments for unusual implementation choices, aligned Javadoc, author/coordinator line-by-line diff review, and a subsequent full-suite attempt. The final independent cycle also rechecked all three fixes.
- Net tests: **+7** (617 to 624 source annotations; 616 to 623 suite-discoverable methods). No existing tests were removed; two lifecycle tests were revised for CF-1's explicit contract change.
- Final verification: **622 distinct current-source tests passed**, zero failures/errors/skips among those reported cases. Audit IDs are recorded in `target/review-cycle2-test-audit.json`. Forced Javadoc generation with `doclint=all` passed; generated output was checked for the updated Caffeine text. `git diff --check` passed.
- **Open verification limit:** the unchanged `ForeignMemoryOffHeapCacheTest.test_perf_vs_ehcache` could not complete within available memory. Its full-suite runs exhausted native host commit and/or the 512 MB and 1 GB Java heaps. All 20 other FFM tests passed separately. This benchmark eagerly combines the class's 4 GB native fixture with 2 GB Ehcache off-heap storage and up to one million heap entries. No production finding is left open, and no fixture/workload was weakened to claim a pass.
- **Compatibility note:** Caffeine `close()` publishes a terminal state and performs invalidation once. Already-admitted operations can finish afterward, and repeated close need not wait for the first invalidation. Owners must stop wrapper users before reusing the retained delegate. This documented change removes the confirmed callback lock inversion.
- Intended deliverables: five changed production files, three changed test files, and this ledger. The project POM is unchanged; temporary runners, logs, dependency extracts, probes, generated Javadoc, and XML archives remain under ignored `target/`.

## Follow-up — restore heap reconstruction for all buffer inputs

The user reported **OH-2 (Medium):** OH-1's `MappedByteBuffer.class.isAssignableFrom(type.javaType())` branch also matched ordinary `ByteBuffer.allocateDirect()` values and their read-only views on JDK 25. It therefore changed their results from heap to direct buffers, removed array access, and added native allocation/zeroing/copying on every read while the lifecycle read lock was held. The coordinator confirmed the installed JDK class hierarchy and read the exact OpenJDK 25 allocation/reservation code. Earlier review claims that OH-1 was sound missed this compatibility and resource-cost change.

The user selected restoration of the original heap-buffer behavior. **OH-2 is fixed:** both the default deserializer and the actual memory/disk read path use `ByteBufferType.valueOf(bytes)` again; the subtype-dependent helper and production `MappedByteBuffer` import are removed. A comment explains why that runtime superclass must not drive reconstruction. Class Javadoc for the shared implementation and both public backends documents writable, array-backed heap results for heap/direct/read-only/mapped inputs and the supported `ByteBuffer` value type.

**OH-1's original runtime fix is withdrawn.** Mapped inputs remain supported as content through `Cache<K, ByteBuffer>`; `Cache<K, MappedByteBuffer>` is an explicitly unsupported declaration because the subtype and original mapping are not preserved. This is the user's accepted resolution, not a remaining promise of subtype preservation.

Regression coverage now includes ordinary direct buffers and read-only direct views across both backends, single/multiple slots, memory, disk without promotion, and disk with promotion. Tests check heap allocation, writable array access, content/position/limit, preservation of input state/mark, and independence from mutations to both the input and returned array. The two prior mapped tests now read through `ByteBuffer` and assert heap content copies after the source mapping's arena closes. The default-deserializer test also covers direct runtime classes and the abstract mapped type.

- Red: six selected buffer tests ran against the direct-allocation implementation; the five direct/mapped/default-deserializer tests failed on `isDirect`, while the heap control passed (`target/review-direct-buffer-red.log`, `target/review-direct-buffer-red.xml`).
- Green: all **62 `AbstractOffHeapCacheTest` tests passed**, zero failures/errors/skips (`target/review-direct-buffer-green.log`, `target/review-direct-buffer-green.xml`).
- Broader verification used isolated 512 MB class JVMs, with only the previously memory-exhausting `ForeignMemoryOffHeapCacheTest.test_perf_vs_ehcache` excluded. The FFM class could not start because its 4 GB native allocation failed. Twenty other classes reported 604 tests, with zero assertion failures and two Mockito self-attachment errors (`target/review-direct-buffer-suite.log`, `target/review-direct-buffer-suite-reports`). Loading Mockito's agent at JVM startup in the temporary runner resolved both setup failures: all 54 tests in `LocalCacheTest` and `MemcachedLockTest` passed on retry (`target/review-direct-buffer-mockito-retry.log`). No project POM or test workload/fixture was changed.
- Final combined current-source verification: **604 distinct tests passed**, zero failures/errors/skips in the final reports (`target/review-direct-buffer-final-reports`, IDs in `target/review-direct-buffer-test-audit.json`). This includes all 62 shared off-heap tests exercising both backends and all 35 `OffHeapCacheTest` methods. The 21-method FFM class remains unverified in this follow-up: one benchmark was deliberately excluded for its known memory demand and its 20 other tests were blocked by static-fixture allocation. Host commit headroom remained approximately 5.4 GB; no further memory-heavy retry was attempted.
- Forced Javadoc regeneration with `doclint=all` passed (`target/review-direct-buffer-javadoc.log`). The generated public backend pages contain the updated heap-buffer/unsupported-subtype contract and current timestamps. `git diff --check` passed.
- Two new test methods were added in this follow-up; the two mapped tests and existing default-deserializer test were revised. Confirmed totals: **626 source annotations, 625 suite-discoverable methods, net +9** from the original baseline.
- The coordinator read every changed source/test line, confirmed both buffer deserialization paths preserve prior runtime behavior, and checked test cleanup and public documentation. No unrelated production/test changes were made in this follow-up.
- The final 52-file source/test hash snapshot is `target/review-direct-buffer-source-test-hashes.json`. Compared with the prior verification snapshot, only `AbstractOffHeapCache`, the two public native backends, and `AbstractOffHeapCacheTest` changed. The earlier Caffeine and Memcached changes remain intact.
