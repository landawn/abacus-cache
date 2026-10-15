# Unit-test cleanup plan

Implement this plan directly, preserving the retained `AbacusCacheTestSuite` launcher and the existing `TestBase` helper.

1. Record a fresh normal-discovery baseline and its JaCoCo data. Inventory unit-test ownership, repeated scenarios, fixture lifecycle, and timing dependencies.
2. Consolidate equivalent scenarios under one owner. Shared Redis operations belong to `AbstractJedisCacheClientTest`; shard routing and cluster lifecycle remain implementation-specific. Parameterize repeated validation inputs and retain the strongest existing assertions.
3. Apply consistent behavior groups and source order: construction/configuration, reads, writes/counters, removals, views/statistics, expiration/allocation, lifecycle/concurrency, compatibility, and validation. Keep fixtures first and helpers last; source order must not determine execution order.
4. Improve reliability and diagnostics. Replace bounded sleep loops with the shared condition waiter where applicable, close owned resources, use descriptive names, and separate independently reportable inputs.
5. Run focused checks, normal Maven discovery, randomized suite-launcher discovery, and the allocator check with assertions disabled. Compare coverage against the fresh baseline, review each removed case's surviving owner, and update testing documentation.

Production behavior, integration-test opt-in, and performance opt-in remain outside this cleanup.

## Completion record

Completed on 2026-10-15. All 24 unit-test classes now use named behavior groups with alphabetical method order inside each group. Fixture setup stays before tests and helpers stay after them. Existing nested groups remain where they help navigation; execution order is independent.

- Consolidated inherited Redis operations in `AbstractJedisCacheClientTest`, retaining routing, construction, and lifecycle cases in the concrete client suites.
- Parameterized provider validation, missing inputs, cache-stat fields, property callbacks, bulk operations, Unicode keys, and TTL boundaries. Serialization checks compare complete values, and invalid bulk writes verify that no partial write occurs.
- Moved breaker-specific cases to `DistributedCacheCircuitBreakerTest` and argument validation to `DistributedCacheTest`.
- Replaced timing guesses with observed lock/thread state and bounded condition waits. The remaining 15 ms sleep intentionally simulates store latency for timing statistics. Replacement-TTL checks now inspect when the installed expiration clock starts.
- Standardized imports, names, fixture visibility, and owned-resource cleanup.
- Retained `AbacusCacheTestSuite` and `TestBase` unchanged. Production code, Maven configuration, integration tests, and performance workloads are unchanged.

| Measure | Before cleanup | After cleanup |
| --- | ---: | ---: |
| Unit-test classes | 24 | 24 |
| Annotated test methods | 615 | 562 |
| Reported unit-test cases | 730 | 746 |
| Line coverage, normal assertion mode | 99.73% (1,828/1,833) | 99.73% (1,828/1,833) |
| Branch coverage | 98.61% (853/865) | 98.61% (853/865) |
| Method coverage | 100% (406/406) | 100% (406/406) |

Parameterized inputs account for the increased case count despite 53 fewer annotated methods. Comparing the fresh before/after JaCoCo XML reports found no lost covered instructions or branch outcomes on any source line.

## Validation

- Focused contract and validation run: 249 cases passed.
- Focused reliability run: 197 cases passed.
- Full normal Maven discovery: 746 cases passed, with no failures, errors, or skips.
- Retained suite launcher with randomized class/method order, seed `20261015`: the same 746 cases passed, with no failures, errors, or skips.
- Assertions-disabled allocator fallback: one additional invocation passed. Combining it with the successful launcher run preserves 99.84% line coverage (1,830/1,833), 98.61% branch coverage, and 100% method coverage.
- `git diff --check` passed with the repository's CRLF Java line endings respected.

Logs and JaCoCo data are local build artifacts: `target/cleanup-before.*`, `target/cleanup-after.*`, `target/cleanup-suite.*`, and `target/cleanup-assertions-disabled.log`. The XML snapshots are `target/cleanup-before.xml`, `target/cleanup-after-default.xml`, and `target/cleanup-suite-default.xml`. The final combined HTML report is `target/site/jacoco/index.html`.

See [TESTING.md](TESTING.md) for commands and conventions, and [COVERAGE.md](COVERAGE.md) for coverage limits.

## Scenario ownership and renames

Every removed, moved, or renamed test method has a surviving owner below. This records 93 method identifiers; moves and renames do not reduce the method count. Methods moved between nested groups within the same class retain their identifiers.

| Previous test method | Surviving owner |
| --- | --- |
| `AbstractCacheTest.getReturnsPresentValue` | `AbstractCacheTest.getWrapsGetOrNull` |
| `AbstractCacheTest.propertiesFunctionalArgumentsNullRejected` | `AbstractCacheTest.propertyCallbacksRejectNullWithoutMutatingValues` |
| `AbstractCacheTest.propertyMutatorsAfterCloseThrowISEWhileReadsStayUsable` | `AbstractCacheTest.propertyMutatorsAfterCloseThrowIllegalStateExceptionWhileReadsStayUsable` |
| `AbstractCacheTest.putTwoArgUsesDefaults` | `AbstractCacheTest.putTwoArgDelegatesDefaultLiveAndIdleTimesNoSwap`; `LocalCacheTest.defaultPutAndRemove` |
| `AbstractDistributedCacheClientTest.constructorBlankServerUrl` | `AbstractDistributedCacheClientTest.constructorRejectsMissingServerUrls` |
| `AbstractDistributedCacheClientTest.constructorEmptyServerUrl` | `AbstractDistributedCacheClientTest.constructorRejectsMissingServerUrls` |
| `AbstractDistributedCacheClientTest.constructorNullServerUrl` | `AbstractDistributedCacheClientTest.constructorRejectsMissingServerUrls` |
| `AbstractJedisCacheClientTest.deleteCallsDelAndReturnsTrue` | `AbstractJedisCacheClientTest.removeReportsWhetherRedisDeletedAnyKeys` |
| `AbstractJedisCacheClientTest.getReturnsNullForEmptyByteArray` | `AbstractJedisCacheClientTest.getTreatsMissingAndEmptyPayloadsAsAbsent` |
| `AbstractJedisCacheClientTest.getReturnsNullWhenKeyMissing` | `AbstractJedisCacheClientTest.getTreatsMissingAndEmptyPayloadsAsAbsent` |
| `AbstractJedisCacheClientTest.setLargeTtlIsNotNarrowedOrConverted` | `AbstractJedisCacheClientTest.putPassesTheExactTtlInMilliseconds` |
| `AbstractJedisCacheClientTest.setNegativeTtlUsesPlainSet` | `AbstractJedisCacheClientTest.putWithoutExpirationUsesPlainSet` |
| `AbstractJedisCacheClientTest.setNullValueEncodesAsEmptyByteArray` | `AbstractJedisCacheClientTest.putNullValueEncodesAsEmptyByteArray` |
| `AbstractJedisCacheClientTest.setReturnsFalseWhenServerDoesNotSayOK` | `AbstractJedisCacheClientTest.putReturnsFalseWhenServerDoesNotSayOK` |
| `AbstractJedisCacheClientTest.setSubSecondTtlHonoredWithMillisecondPrecision` | `AbstractJedisCacheClientTest.putPassesTheExactTtlInMilliseconds` |
| `CacheFactoryTest.createCacheAbstractOrInterfaceCustomClassRejectedWithIae` | `CacheFactoryTest.createCacheAbstractOrInterfaceCustomClassRejectedWithIllegalArgumentException` |
| `CacheFactoryTest.createCacheClassNotFound` | `CacheFactoryTest.createCacheClassNotFoundHasDescriptiveMessage` |
| `CacheFactoryTest.createCacheEmptyProvider` | `CacheFactoryTest.blankProviderIsRejected` |
| `CacheFactoryTest.createCacheMemcachedBlankTimeoutRejected` | `CacheFactoryTest.invalidTimeoutIsRejectedBeforeClientConstruction` |
| `CacheFactoryTest.createCacheMemcachedNonNumericTimeout` | `CacheFactoryTest.invalidTimeoutIsRejectedBeforeClientConstruction` |
| `CacheFactoryTest.createCacheMemcachedNonPositiveTimeout` | `CacheFactoryTest.invalidTimeoutIsRejectedBeforeClientConstruction` |
| `CacheFactoryTest.createCacheMemcachedOverflowTimeout` | `CacheFactoryTest.invalidTimeoutIsRejectedBeforeClientConstruction` |
| `CacheFactoryTest.createCacheMemcachedTooManyParameters` | `CacheFactoryTest.extraParametersAreRejectedBeforeClientConstruction` |
| `CacheFactoryTest.createCacheNonDecimalTimeoutRejected` | `CacheFactoryTest.invalidTimeoutIsRejectedBeforeClientConstruction` |
| `CacheFactoryTest.createCacheNullProvider` | `CacheFactoryTest.blankProviderIsRejected` |
| `CacheFactoryTest.createCacheRedisClusterNonNumericTimeout` | `CacheFactoryTest.invalidTimeoutIsRejectedBeforeClientConstruction` |
| `CacheFactoryTest.createCacheRedisClusterNonPositiveTimeout` | `CacheFactoryTest.invalidTimeoutIsRejectedBeforeClientConstruction` |
| `CacheFactoryTest.createCacheRedisClusterOverflowTimeout` | `CacheFactoryTest.invalidTimeoutIsRejectedBeforeClientConstruction` |
| `CacheFactoryTest.createCacheRedisClusterTooManyParameters` | `CacheFactoryTest.extraParametersAreRejectedBeforeClientConstruction` |
| `CacheFactoryTest.createCacheRedisNonNumericTimeout` | `CacheFactoryTest.invalidTimeoutIsRejectedBeforeClientConstruction` |
| `CacheFactoryTest.createCacheRedisNonPositiveTimeout` | `CacheFactoryTest.invalidTimeoutIsRejectedBeforeClientConstruction` |
| `CacheFactoryTest.createCacheRedisTooManyParameters` | `CacheFactoryTest.extraParametersAreRejectedBeforeClientConstruction` |
| `CacheFactoryTest.rejectsTimeoutContainingAPunctuationCharacter` | `CacheFactoryTest.invalidTimeoutIsRejectedBeforeClientConstruction` |
| `CacheStatsTest.recordNegativeCapacityRejected` | `CacheStatsTest.recordRejectsInvalidNumericComponents` |
| `CacheStatsTest.recordNegativeDataSizeBelowSentinelRejected` | `CacheStatsTest.recordRejectsInvalidNumericComponents` |
| `CacheStatsTest.recordNegativeEvictionCountRejected` | `CacheStatsTest.recordRejectsInvalidNumericComponents` |
| `CacheStatsTest.recordNegativeGetCountRejected` | `CacheStatsTest.recordRejectsInvalidNumericComponents` |
| `CacheStatsTest.recordNegativeHitCountRejected` | `CacheStatsTest.recordRejectsInvalidNumericComponents` |
| `CacheStatsTest.recordNegativeMaxMemoryBelowSentinelRejected` | `CacheStatsTest.recordRejectsInvalidNumericComponents` |
| `CacheStatsTest.recordNegativeMissCountRejected` | `CacheStatsTest.recordRejectsInvalidNumericComponents` |
| `CacheStatsTest.recordNegativePutCountRejected` | `CacheStatsTest.recordRejectsInvalidNumericComponents` |
| `CacheStatsTest.recordNegativeSizeRejected` | `CacheStatsTest.recordRejectsInvalidNumericComponents` |
| `CaffeineCacheTest.statsReturnsNonNull` | `CaffeineCacheTest.statsContentReflectsTraffic` |
| `DistributedCacheCircuitBreakerTest.malformedUnicodeKeysCannotAddressAnotherEntryOrChangeBreakerState` | `DistributedCacheTest.malformedUnicodeKeysCannotAddressAnotherEntryOrChangeBreakerState` |
| `DistributedCacheCircuitBreakerTest.nullArgumentsAreRejectedWithIaeWithoutTouchingClientOrBreaker` | `DistributedCacheTest.nullArgumentsAreRejectedWithIllegalArgumentExceptionWithoutTouchingClientOrBreaker` |
| `DistributedCacheCircuitBreakerTest.nullKeyPrefixMeansNoPrefixAndNullValueIsForwardedToClient` | `DistributedCacheTest.nullKeyPrefixMeansNoPrefixAndNullValueIsForwardedToClient` |
| `DistributedCacheTest.backendValidationFailureDoesNotChangeBreakerState` | `DistributedCacheCircuitBreakerTest.backendValidationFailureDoesNotChangeBreakerState` |
| `DistributedCacheTest.circuitBreakerCountAndTimestampUseOneAtomicState` | `DistributedCacheCircuitBreakerTest.circuitBreakerCountAndTimestampUseOneAtomicState` |
| `DistributedCacheTest.constructorNullClient` | `DistributedCacheTest.nullArgumentsAreRejectedWithIllegalArgumentExceptionWithoutTouchingClientOrBreaker` |
| `DistributedCacheTest.generateKeyNullKey` | `DistributedCacheTest.nullKeysAreRejectedRegardlessOfCircuitState` |
| `DistributedCacheTest.generateKeyNullStringRepresentation` | `DistributedCacheTest.nullStringRepresentationsAreRejectedRegardlessOfCircuitState` |
| `DistributedCacheTest.generateKeyNullStringRepresentationThrowsEvenWhenCircuitOpen` | `DistributedCacheTest.nullStringRepresentationsAreRejectedRegardlessOfCircuitState` |
| `DistributedCacheTest.getOrNullNullKeyThrowsEvenWhenCircuitOpen` | `DistributedCacheTest.nullKeysAreRejectedRegardlessOfCircuitState` |
| `DistributedCacheTest.immediateRetryLogsOnlyTheFirstCircuitOpening` | `DistributedCacheCircuitBreakerTest.immediateRetryLogsOnlyTheFirstCircuitOpening` |
| `DistributedCacheTest.putAndRemoveNullKey` | `DistributedCacheTest.nullArgumentsAreRejectedWithIllegalArgumentExceptionWithoutTouchingClientOrBreaker` |
| `EhcacheTest.closeDoesNotClearUnderlyingCache` | `EhcacheTest.closeIsIdempotentAndNonDestructive` |
| `EhcacheTest.getAllNullElementThrowsIAE` | `EhcacheTest.getAllNullElementThrowsIllegalArgumentException` |
| `EhcacheTest.putAllNullKeyOrValueMessageIsDescriptive` | `EhcacheTest.putAllRejectsInvalidEntriesBeforeWritingAnything` |
| `EhcacheTest.putAllNullKeyOrValueThrowsIAE` | `EhcacheTest.putAllRejectsInvalidEntriesBeforeWritingAnything` |
| `EhcacheTest.removeAllNullElementThrowsIAE` | `EhcacheTest.removeAllNullElementThrowsIllegalArgumentException` |
| `JRedisClusterTest.deleteReturnsFalseWhenKeyDidNotExist` | `AbstractJedisCacheClientTest.removeReportsWhetherRedisDeletedAnyKeys` |
| `JRedisClusterTest.disconnectSwallowsClusterCloseFailure` | `JRedisClusterTest.disconnectRemainsTerminalWhenTheClusterCloseFails` |
| `JRedisClusterTest.getBulkCollectionAndEmpty` | `AbstractJedisCacheClientTest.getBulkReturnsOnlyFoundKeys`; `AbstractJedisCacheClientTest.getBulkEmptyRequestsDoNotContactTheBackend` |
| `JRedisClusterTest.getBulkReturnsOnlyFoundKeys` | `AbstractJedisCacheClientTest.getBulkReturnsOnlyFoundKeys` |
| `JRedisClusterTest.getReturnsDecodedValue` | `AbstractJedisCacheClientTest.getDecodesStringsAndObjects` |
| `JRedisClusterTest.incrReturnsServerValue` | `AbstractJedisCacheClientTest.incrReturnsTheServerValue` |
| `JRedisClusterTest.setRoundTripUsesKryoEncoding` | `AbstractJedisCacheClientTest.putEncodesStringsAndObjects` |
| `JRedisClusterTest.setWithoutTtlUsesPlainSet` | `AbstractJedisCacheClientTest.putWithoutExpirationUsesPlainSet` |
| `JRedisClusterTest.setWithTtlUsesSetWithExpiry` | `AbstractJedisCacheClientTest.putPassesTheExactTtlInMilliseconds` |
| `JRedisClusterTest.validSupplementaryUnicodeKeyPreservesUtf8Bytes` | `AbstractJedisCacheClientTest.validSupplementaryUnicodeKeysPreserveUtf8Bytes` |
| `JRedisTest.constructorWithDefaultTimeoutDoesNotThrow` | `JRedisTest.setUp`; `JRedisTest.serverUrlReturnsConstructorValue` |
| `JRedisTest.deleteReturnsFalseWhenKeyDidNotExist` | `AbstractJedisCacheClientTest.removeReportsWhetherRedisDeletedAnyKeys` |
| `JRedisTest.deleteReturnsTrueWhenMultipleRemoved` | `AbstractJedisCacheClientTest.removeReportsWhetherRedisDeletedAnyKeys` |
| `JRedisTest.getBulkCollectionReturnsOnlyFoundKeys` | `AbstractJedisCacheClientTest.getBulkReturnsOnlyFoundKeys` |
| `JRedisTest.getBulkEmptyReturnsEmptyMap` | `AbstractJedisCacheClientTest.getBulkEmptyRequestsDoNotContactTheBackend` |
| `JRedisTest.getBulkVarargsReturnsOnlyFoundKeys` | `AbstractJedisCacheClientTest.getBulkReturnsOnlyFoundKeys` |
| `JRedisTest.getKeyBytesNullKey` | `AbstractJedisCacheClientTest.nullKeyIsRejectedWithIllegalArgumentExceptionBeforeAnyCommand` |
| `JRedisTest.getReturnsDecodedValue` | `AbstractJedisCacheClientTest.getDecodesStringsAndObjects` |
| `JRedisTest.incrReturnsServerValue` | `AbstractJedisCacheClientTest.incrReturnsTheServerValue` |
| `JRedisTest.setRoundTripUsesKryoEncoding` | `AbstractJedisCacheClientTest.putEncodesStringsAndObjects` |
| `JRedisTest.setWithoutTtlUsesPlainSet` | `AbstractJedisCacheClientTest.putWithoutExpirationUsesPlainSet` |
| `JRedisTest.setWithTtlUsesSetWithExpiry` | `AbstractJedisCacheClientTest.putPassesTheExactTtlInMilliseconds` |
| `JRedisTest.validSupplementaryUnicodeKeyPreservesUtf8Bytes` | `AbstractJedisCacheClientTest.validSupplementaryUnicodeKeysPreserveUtf8Bytes` |
| `LocalCacheTest.containsKeyNullKeyThrowsIAE` | `LocalCacheTest.containsKeyNullKeyThrowsIllegalArgumentException` |
| `LocalCacheTest.getOrNullNullKeyThrowsIAE` | `LocalCacheTest.getOrNullNullKeyThrowsIllegalArgumentException` |
| `LocalCacheTest.removeNullKeyThrowsIAE` | `LocalCacheTest.removeNullKeyThrowsIllegalArgumentException` |
| `MemcachedLockTest.nullOrBlankServerUrlIsRejectedWithIae` | `MemcachedLockTest.nullOrBlankServerUrlIsRejectedWithIllegalArgumentException` |
| `MemcachedLockTest.nullTargetIsRejectedWithIaeBeforeAnyNetworkOperation` | `MemcachedLockTest.nullTargetIsRejectedWithIllegalArgumentExceptionBeforeAnyNetworkOperation` |
| `OffHeapCacheStatsTest.recordNullComponentsThrowNPE` | `OffHeapCacheStatsTest.recordNullComponentsThrowNullPointerException` |
| `OffHeapCacheTest.statsUnderConcurrentDiskWritesConsistent` | `OffHeapCacheTest.statisticsRemainValidDuringConcurrentDiskWrites` |
| `OffHeapCacheTest.totalDataSizeDoubleCountForDiskEntries` | `OffHeapCacheTest.diskEntriesAreCountedOnceInDataSize` |
| `SpyMemcachedFutureTest.resultOfRejectsNullFutureWithIae` | `SpyMemcachedFutureTest.resultOfRejectsNullFutureWithIllegalArgumentException` |
| `SpyMemcachedTest.createSpyMemcachedClientRejectsNullConnectionFactoryWithIae` | `SpyMemcachedTest.createSpyMemcachedClientRejectsNullConnectionFactoryWithIllegalArgumentException` |
