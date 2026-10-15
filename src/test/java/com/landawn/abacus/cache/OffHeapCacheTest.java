/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.Strings;

class OffHeapCacheTest {

    // Construction

    @Test
    void builderFunctionalArgumentsNullRejected() {
        final OffHeapCache.Builder<String, Account> builder = OffHeapCache.builder();

        assertThrows(IllegalArgumentException.class, () -> builder.serializer(null));
        assertThrows(IllegalArgumentException.class, () -> builder.deserializer(null));
        assertThrows(IllegalArgumentException.class, () -> builder.testerForLoadingItemFromDiskToMemory(null));
        assertThrows(IllegalArgumentException.class, () -> builder.storeSelector(null));
    }

    /** {@code OffHeapCache(capacityInMB, evictDelay)} delegates to the default-TTL constructor. */
    @Test
    void constructorCapacityAndEvictDelay() {
        final OffHeapCache<String, byte[]> cache = new OffHeapCache<>(1, 0L);

        try {
            final byte[] value = { 5, 6, 7 };
            assertTrue(cache.put("k", value));
            assertArrayEquals(value, cache.getOrNull("k"));
            assertNotNull(cache.stats());
        } finally {
            cache.close();
        }
    }

    /** {@code OffHeapCache(capacityInMB, evictDelay, defaultLiveTime, defaultMaxIdleTime)} full basic form. */
    @Test
    void constructorCapacityEvictDelayLiveTimeIdleTime() {
        final OffHeapCache<String, byte[]> cache = new OffHeapCache<>(1, 0L, 60_000L, 60_000L);

        try {
            final byte[] value = { 8, 9 };
            assertTrue(cache.put("k", value));
            assertArrayEquals(value, cache.getOrNull("k"));
        } finally {
            cache.close();
        }
    }

    /** {@code OffHeapCache(capacityInMB)} delegates to the (capacity, evictDelay=3000) constructor. */
    @Test
    void constructorCapacityOnly() {
        final OffHeapCache<String, byte[]> cache = new OffHeapCache<>(1);

        try {
            assertFalse(cache.isClosed());
            final byte[] value = { 1, 2, 3, 4 };
            assertTrue(cache.put("k", value));
            assertArrayEquals(value, cache.getOrNull("k"));
        } finally {
            cache.close();
        }
    }

    // Writes

    /**
     * Regression coverage for the "return inside finally" exception-swallowing bug in
     * {@code AbstractOffHeapCache.put()}. The put body's {@code finally} block ended with a bare
     * {@code return false} whenever no wrapper was produced. A {@code return} executed in a
     * {@code finally} silently discards any exception propagating out of the {@code try}, so an
     * {@code offHeapStore.put()} that threw (or any copy/allocation failure inside the try) was
     * converted into a silent {@code put == false} instead of surfacing the real error — the value
     * was dropped with no signal that the disk store was broken.
     *
     * <p>After the fix the "could not store" handling (vacate + {@code return false}) was moved out
     * of the {@code finally}, so a genuine exception now propagates while the {@code finally} still
     * releases any allocated slots. This test forces the disk-only path with a store that throws for
     * "BOOM" keys and succeeds for "ok" keys: the throw must propagate out of {@code put()}, and the
     * cache must stay fully usable (every interleaved normal put/get round-trips).
     */
    @Test
    void putOffHeapStoreThrowsPropagatesAndCacheStaysUsable() {
        final java.util.Map<String, byte[]> memStore = new java.util.concurrent.ConcurrentHashMap<>();

        final OffHeapStore<String> throwingStore = new OffHeapStore<>() {
            @Override
            public boolean put(final String key, final byte[] value) {
                if (key.startsWith("BOOM")) {
                    throw new IllegalStateException("disk store failed for: " + key);
                }
                memStore.put(key, value);
                return true;
            }

            @Override
            public byte[] get(final String key) {
                return memStore.get(key);
            }

            @Override
            public boolean remove(final String key) {
                return memStore.remove(key) != null;
            }
        };

        final OffHeapCache<String, byte[]> c = OffHeapCache.<String, byte[]> builder()
                .capacityInMB(8)
                .evictDelay(0)
                .defaultLiveTime(1000_000)
                .defaultMaxIdleTime(1000_000)
                .offHeapStore(throwingStore)
                .storeSelector((k, v, size) -> 2) // disk only — route every put through offHeapStore.put
                .build();

        try {
            for (int i = 0; i < 200; i++) {
                final int n = i;
                final byte[] boom = new byte[128];
                // A throwing offHeapStore.put must propagate out of put() (it used to be silently
                // swallowed as `return false` by the return-in-finally).
                assertThrows(IllegalStateException.class, () -> c.put("BOOM" + n, boom));
                // ...and the cache must stay usable: a normal disk-only put/get round-trips.
                final byte[] ok = ("value" + n).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                assertTrue(c.put("ok" + n, ok));
                assertArrayEquals(ok, c.get("ok" + n).orElse(null));
            }
        } finally {
            c.close();
        }
    }

    /**
     * Regression coverage for the pooled-buffer handling on the serialization-failure path in
     * {@code AbstractOffHeapCache.put()}. The pooled {@code ByteArrayOutputStream} is obtained
     * before the main try/finally that recycles it, so a serializer that throws used to skip
     * {@code Objectory.recycle(os)} and leak the pooled buffer. The fix recycles the buffer on the
     * failure path and nulls the reference so the outer finally cannot double-recycle it.
     *
     * <p>This test drives many serialization failures interleaved with successful puts/gets: the
     * serializer exception must propagate cleanly, and—critically—the shared Objectory buffer pool
     * must not be corrupted (a double-recycle could hand a still-referenced buffer to a later
     * successful put), so every interleaved normal put/get must round-trip correctly.
     */
    @Test
    void putSerializerThrowsPropagatesAndCacheStaysUsable() {
        final OffHeapCache<String, String> c = OffHeapCache.<String, String> builder().capacityInMB(8).evictDelay(0).serializer((v, os) -> {
            if (v.startsWith("BOOM")) {
                throw new IllegalStateException("serialization failed for: " + v);
            }
            final byte[] b = v.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            os.write(b, 0, b.length);
        }).deserializer((bytes, type) -> new String(bytes, java.nio.charset.StandardCharsets.UTF_8)).build();

        try {
            for (int i = 0; i < 300; i++) {
                final int n = i;
                // A throwing serializer must propagate its exception out of put()...
                assertThrows(IllegalStateException.class, () -> c.put("BOOM" + n, "BOOM" + n));
                // ...without corrupting the cache or the shared buffer pool: a normal put/get
                // must still round-trip on every iteration.
                assertTrue(c.put("ok" + n, "value" + n));
                assertEquals("value" + n, c.get("ok" + n).orElse(null));
            }
        } finally {
            c.close();
        }
    }

    @Test
    void putToDiskDiskToDiskReplacePreservesNewValue() {
        final java.util.Map<String, byte[]> memStore = new java.util.concurrent.ConcurrentHashMap<>();

        final OffHeapStore<String> inMemoryStore = new OffHeapStore<>() {
            @Override
            public boolean put(final String key, final byte[] value) {
                memStore.put(key, value);
                return true;
            }

            @Override
            public byte[] get(final String key) {
                return memStore.get(key);
            }

            @Override
            public boolean remove(final String key) {
                return memStore.remove(key) != null;
            }
        };

        final OffHeapCache<String, Account> diskCache = OffHeapCache.<String, Account> builder()
                .capacityInMB(8)
                .evictDelay(0)
                .defaultLiveTime(1000_000)
                .defaultMaxIdleTime(1000_000)
                .offHeapStore(inMemoryStore)
                .storeSelector((k, v, size) -> 2) // disk only
                .build();

        try {
            final String key = "shared-key";

            final Account first = new Account();
            first.setFirstName(Strings.repeat("a", 5000));
            assertTrue(diskCache.put(key, first));
            assertEquals(first, diskCache.get(key).orElse(null));

            // Replace under the same key with a different on-disk value.
            final Account second = new Account();
            second.setFirstName(Strings.repeat("b", 5000));
            assertTrue(diskCache.put(key, second));

            // Critical assertion: the value MUST be the new one and MUST NOT be null.
            final Account fetched = diskCache.get(key).orElse(null);
            assertEquals(second, fetched);
            // And the disk store must hold exactly one entry for this key.
            assertTrue(memStore.containsKey(key));
            assertEquals(1, memStore.size());

            // Sanity: stats reflect exactly one on-disk entry.
            final var stats = diskCache.stats();
            assertEquals(1L, stats.sizeOnDisk());
            assertEquals(stats.dataSizeOnDisk(), stats.dataSize());
        } finally {
            diskCache.close();
        }
    }

    /**
     * Regression test for the array-retention hazard on the disk-spill path.
     *
     * <p>When the serialized size exactly matched the buffer length (always true for {@code byte[]}
     * values), {@code putToDisk()} handed the CALLER'S OWN array to {@code OffHeapStore.put}. The
     * OffHeapStore contract leaves defensive copying implementation-specific, so a store that
     * retains the array (like this in-memory one) saw its cached bytes silently change when the
     * caller later mutated the array. The fix always passes a private copy.
     */
    @Test
    void putToDiskHandsStoreAPrivateCopy() {
        final java.util.Map<String, byte[]> memStore = new java.util.concurrent.ConcurrentHashMap<>();
        final OffHeapStore<String> retainingStore = new OffHeapStore<>() {
            @Override
            public boolean put(final String key, final byte[] value) {
                memStore.put(key, value); // retains the array reference - allowed by the contract
                return true;
            }

            @Override
            public byte[] get(final String key) {
                return memStore.get(key);
            }

            @Override
            public boolean remove(final String key) {
                memStore.remove(key);
                return true;
            }
        };

        final OffHeapCache<String, byte[]> c = OffHeapCache.<String, byte[]> builder()
                .capacityInMB(1)
                .evictDelay(0)
                .offHeapStore(retainingStore)
                .storeSelector((k, v, size) -> 2) // disk only
                .build();
        try {
            final byte[] value = { 10, 20, 30, 40 };
            assertTrue(c.put("k", value));

            // The caller mutates its own array after the put...
            value[0] = 99;

            // ...and the cached entry must be unaffected.
            assertArrayEquals(new byte[] { 10, 20, 30, 40 }, c.getOrNull("k"));
        } finally {
            c.close();
        }
    }

    /**
     * Regression test for disk-byte ownership across same-key replacements.
     *
     * <p>The offHeapStore is keyed by the cache key alone, so when a put replaces an existing
     * disk-spilled entry, the replaced wrapper must not delete the store bytes that now belong to
     * the replacing wrapper. Ownership is now tracked per key: replacement transfers it, reads of
     * the current entry succeed, and removing the entry deletes the store bytes exactly once.
     */
    @Test
    void putToDiskReplacementKeepsCurrentBytesAndRemoveDeletesThem() {
        final java.util.Map<String, byte[]> memStore = new java.util.concurrent.ConcurrentHashMap<>();
        final OffHeapStore<String> inMemoryStore = new OffHeapStore<>() {
            @Override
            public boolean put(final String key, final byte[] value) {
                memStore.put(key, value);
                return true;
            }

            @Override
            public byte[] get(final String key) {
                return memStore.get(key);
            }

            @Override
            public boolean remove(final String key) {
                memStore.remove(key);
                return true;
            }
        };

        final OffHeapCache<String, byte[]> c = OffHeapCache.<String, byte[]> builder()
                .capacityInMB(1)
                .evictDelay(0)
                .offHeapStore(inMemoryStore)
                .storeSelector((k, v, size) -> 2) // disk only
                .build();
        try {
            assertTrue(c.put("k", new byte[] { 1, 1, 1 }));
            assertTrue(c.put("k", new byte[] { 2, 2, 2 })); // replaces the disk entry under the same key
            assertEquals(1, memStore.size());
            assertArrayEquals(new byte[] { 2, 2, 2 }, c.getOrNull("k"));

            c.remove("k");
            assertNull(c.getOrNull("k"));
            assertTrue(memStore.isEmpty());

            final OffHeapCacheStats stats = c.stats();
            assertEquals(0L, stats.sizeOnDisk());
            assertEquals(0L, stats.dataSizeOnDisk());
        } finally {
            c.close();
        }
    }

    /**
     * Regression test for the "no expiration" contract bug in AbstractOffHeapCache.put().
     *
     * <p>The OffHeapCache Builder javadoc documents {@code defaultLiveTime}/{@code defaultMaxIdleTime}
     * of {@code 0} (the Builder default) as "no TTL expiration" / "no idle timeout". But
     * {@code put(key, value)} forwards those defaults to {@code put(key, value, 0, 0)}, which built a
     * Wrapper whose underlying {@code ActivityPrint} throws {@code IllegalArgumentException} for any
     * value {@code <= 0}.
     *
     * <p>Before the fix: a default-built cache threw {@code IllegalArgumentException} on the simple
     * {@code put(key, value)} call. After the fix: non-positive live/idle times are treated as
     * "no expiration" and the entry is stored and retrievable.
     */
    @Test
    void putWithDefaultZeroExpirationDoesNotThrow() {
        final OffHeapCache<String, Account> noExpiryCache = OffHeapCache.<String, Account> builder()
                .capacityInMB(8)
                .evictDelay(0)
                // defaultLiveTime / defaultMaxIdleTime intentionally left at the Builder default (0)
                .build();

        try {
            final Account account = new Account();
            account.setEmailAddress("account@example.test");
            account.setFirstName("Ada");
            final String key = account.getEmailAddress();

            // Documented "no expiration" usage - must NOT throw and must store the value.
            assertTrue(noExpiryCache.put(key, account));
            assertEquals(account, noExpiryCache.get(key).orElse(null));

            // Explicit non-positive values must behave the same way.
            assertTrue(noExpiryCache.put("k2", account, 0, 0));
            assertTrue(noExpiryCache.put("k3", account, -1, -1));
            assertEquals(account, noExpiryCache.get("k2").orElse(null));
            assertEquals(account, noExpiryCache.get("k3").orElse(null));
        } finally {
            noExpiryCache.close();
        }
    }

    // Removals

    @Test
    void clearReclaimsEmptySegmentsForDifferentSlotSizes() {
        final OffHeapCache<String, byte[]> c = OffHeapCache.<String, byte[]> builder().capacityInMB(1).maxBlockSizeInBytes(1024 * 1024).evictDelay(0).build();

        try {
            assertTrue(c.put("small", new byte[64]));
            c.clear();

            assertTrue(c.put("large", new byte[256 * 1024]));
        } finally {
            c.close();
        }
    }

    // Statistics

    /**
     * Regression test for the LongSummaryStatistics race in
     * {@link com.landawn.abacus.cache.AbstractOffHeapCache#stats}.
     *
     * <p>Before the fix, {@code stats()} read {@code totalWriteToDiskTimeStats} /
     * {@code totalReadFromDiskTimeStats} without synchronization while concurrent puts/gets
     * were calling {@code accept(...)} under the same monitor. Because
     * {@code LongSummaryStatistics} is not thread-safe, the reader could observe a torn
     * snapshot — e.g. {@code count > 0} but {@code min == Long.MAX_VALUE} (so the produced
     * {@code MinMaxAvg.min} would be a wildly wrong, huge double). After the fix the reader
     * snapshots under the same monitor, so {@code min}, {@code max}, and {@code average} are
     * all consistent with a non-zero count.
     */
    @Test
    void statsUnderConcurrentDiskWritesConsistent() throws Exception {
        final java.util.Map<String, byte[]> memStore = new java.util.concurrent.ConcurrentHashMap<>();

        final OffHeapStore<String> inMemoryStore = new OffHeapStore<>() {
            @Override
            public boolean put(final String key, final byte[] value) {
                memStore.put(key, value);
                return true;
            }

            @Override
            public byte[] get(final String key) {
                return memStore.get(key);
            }

            @Override
            public boolean remove(final String key) {
                return memStore.remove(key) != null;
            }
        };

        final OffHeapCache<String, Account> diskCache = OffHeapCache.<String, Account> builder()
                .capacityInMB(8)
                .evictDelay(0)
                .defaultLiveTime(1000_000)
                .defaultMaxIdleTime(1000_000)
                .offHeapStore(inMemoryStore)
                .storeSelector((k, v, size) -> 2) // disk only — exercises totalWriteToDiskTimeStats
                .statsTimeOnDisk(true)
                .build();

        final java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(2);
        final java.util.concurrent.CountDownLatch firstWrite = new java.util.concurrent.CountDownLatch(1);
        final java.util.concurrent.CountDownLatch firstSample = new java.util.concurrent.CountDownLatch(1);
        try {
            final java.util.concurrent.Future<?> writer = executor.submit(() -> {
                for (int i = 0; i < 1_000; i++) {
                    final Account account = new Account();
                    account.setId(i);
                    account.setFirstName("x".repeat(2_048));
                    assertTrue(diskCache.put("k" + i % 64, account));
                    if (i == 0) {
                        firstWrite.countDown();
                        assertTrue(firstSample.await(5, java.util.concurrent.TimeUnit.SECONDS));
                    }
                }
                return null;
            });
            final java.util.concurrent.Future<?> reader = executor.submit(() -> {
                assertTrue(firstWrite.await(5, java.util.concurrent.TimeUnit.SECONDS));
                for (int i = 0; i < 1_000; i++) {
                    final var snapshot = diskCache.stats();
                    final var timing = snapshot.writeToDiskTimeStats();
                    assertTrue(snapshot.putCountToDisk() > 0);
                    assertTrue(Double.isFinite(timing.min()) && Double.isFinite(timing.max()) && Double.isFinite(timing.avg()));
                    assertTrue(timing.min() >= 0 && timing.min() <= timing.avg() && timing.avg() <= timing.max());
                    firstSample.countDown();
                }
                return null;
            });
            writer.get(15, java.util.concurrent.TimeUnit.SECONDS);
            reader.get(15, java.util.concurrent.TimeUnit.SECONDS);
            assertEquals(1_000, diskCache.stats().putCountToDisk());
        } finally {
            firstSample.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS));
            diskCache.close();
        }
    }

    /**
     * Regression test for the totalDataSize double-counting bug in AbstractOffHeapCache.put().
     *
     * <p>StoreWrapper's constructor already accounts for totalDataSize, but put() also added it
     * unconditionally for every wrapper type, while StoreWrapper.destroy() only subtracts it once.
     * As a result, for every disk-stored entry the reported dataSize was double the real value and
     * never reconciled (it leaked permanently).
     *
     * <p>Before the fix: after a single disk-only put, stats().dataSize() == 2 * dataSizeOnDisk(),
     * and after removing the entry dataSize() stays non-zero. After the fix dataSize() ==
     * dataSizeOnDisk() and returns to 0 once the entry is removed.
     */
    @Test
    void totalDataSizeDoubleCountForDiskEntries() {
        final java.util.Map<String, byte[]> memStore = new java.util.concurrent.ConcurrentHashMap<>();

        final OffHeapStore<String> inMemoryStore = new OffHeapStore<>() {
            @Override
            public boolean put(final String key, final byte[] value) {
                memStore.put(key, value);
                return true;
            }

            @Override
            public byte[] get(final String key) {
                return memStore.get(key);
            }

            @Override
            public boolean remove(final String key) {
                memStore.remove(key);
                return true;
            }
        };

        final OffHeapCache<String, Account> diskCache = OffHeapCache.<String, Account> builder()
                .capacityInMB(8)
                .evictDelay(0)
                .defaultLiveTime(1000_000)
                .defaultMaxIdleTime(1000_000)
                .offHeapStore(inMemoryStore)
                .storeSelector((k, v, size) -> 2) // disk only
                .build();

        try {
            final Account account = new Account();
            account.setEmailAddress("account@example.test");
            account.setFirstName("Ada");
            account.setFirstName(Strings.repeat("x", 5000));
            final String key = account.getEmailAddress();

            assertTrue(diskCache.put(key, account));

            final var statsAfterPut = diskCache.stats();
            // The single on-disk entry must be counted exactly once in dataSize.
            assertEquals(statsAfterPut.dataSizeOnDisk(), statsAfterPut.dataSize());
            assertEquals(account, diskCache.get(key).orElse(null));

            diskCache.remove(key);

            final var statsAfterRemove = diskCache.stats();
            assertEquals(0L, statsAfterRemove.dataSizeOnDisk());
            assertEquals(0L, statsAfterRemove.dataSize());
        } finally {
            diskCache.close();
        }
    }

    // Validation And Failures

    @Test
    void diskDeserializerReturningNullIsReportedAsConfigurationFailure() {
        final java.util.Map<String, byte[]> memStore = new java.util.concurrent.ConcurrentHashMap<>();
        final OffHeapStore<String> inMemoryStore = new OffHeapStore<>() {
            @Override
            public byte[] get(final String key) {
                return memStore.get(key);
            }

            @Override
            public boolean put(final String key, final byte[] value) {
                memStore.put(key, value);
                return true;
            }

            @Override
            public boolean remove(final String key) {
                return memStore.remove(key) != null;
            }
        };

        final OffHeapCache<String, String> diskCache = OffHeapCache.<String, String> builder().capacityInMB(1).evictDelay(0).serializer((value, output) -> {
            final byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            output.write(bytes, 0, bytes.length);
        }).deserializer((bytes, type) -> null).offHeapStore(inMemoryStore).storeSelector((key, value, size) -> 2).build();

        try {
            assertTrue(diskCache.put("key", "value"));

            final IllegalStateException failure = assertThrows(IllegalStateException.class, () -> diskCache.getOrNull("key"));
            assertTrue(failure.getMessage().contains("deserializer returned null"));
            assertTrue(diskCache.containsKey("key"), "valid stored bytes must not be discarded as though they were missing");
            assertEquals(0L, diskCache.stats().hitCountFromDisk(), "a value that was not reconstructed is not a successful disk hit");
        } finally {
            diskCache.close();
        }
    }

    /**
     * Documented full-constructor contract: unlike {@code Builder.build()}, the constructor performs no
     * {@code 0}-to-default substitution for {@code maxBlockSize}, so {@code 0} is rejected. The
     * invalid argument is detected before the native allocation, so the supplied store stays open
     * and owned by the caller.
     */
    @Test
    void fullConstructorZeroMaxBlockSizeRejectedWithoutClosingStore() {
        final AtomicBoolean storeClosed = new AtomicBoolean();
        final OffHeapStore<String> store = new OffHeapStore<>() {
            @Override
            public byte[] get(final String key) {
                return null;
            }

            @Override
            public boolean put(final String key, final byte[] value) {
                return false;
            }

            @Override
            public boolean remove(final String key) {
                return false;
            }

            @Override
            public void close() {
                storeClosed.set(true);
            }
        };

        assertThrows(IllegalArgumentException.class, () -> new OffHeapCache<String, byte[]>(1, 0, 0, 0, 0, 0f, null, null, store, false, null, null));
        assertFalse(storeClosed.get());
    }

    @Test
    void memoryDeserializerReturningNullFailsForSingleAndMultiSlotValues() {
        final OffHeapCache<String, String> memoryCache = OffHeapCache.<String, String> builder()
                .capacityInMB(2)
                .maxBlockSizeInBytes(1024)
                .evictDelay(0)
                .serializer((value, output) -> {
                    final byte[] bytes = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    output.write(bytes, 0, bytes.length);
                })
                .deserializer((bytes, type) -> null)
                .build();

        try {
            assertTrue(memoryCache.put("single", "small"));
            assertTrue(memoryCache.put("multi", Strings.repeat("x", 2048)));

            final IllegalStateException singleFailure = assertThrows(IllegalStateException.class, () -> memoryCache.getOrNull("single"));
            final IllegalStateException multiFailure = assertThrows(IllegalStateException.class, () -> memoryCache.getOrNull("multi"));
            assertTrue(singleFailure.getMessage().contains("deserializer returned null"));
            assertTrue(multiFailure.getMessage().contains("deserializer returned null"));
        } finally {
            memoryCache.close();
        }
    }

    // Other Behavior

    /**
     * Documented {@code allocate} contract: {@code Unsafe.allocateMemory} rejects a negative size
     * with {@link IllegalArgumentException}, and the rejected call leaves the live cache untouched.
     */
    @Test
    void allocateNegativeSizeThrowsIllegalArgumentException() {
        final OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]> builder().capacityInMB(1).build();

        try {
            final byte[] value = { 1, 2, 3 };
            assertTrue(cache.put("k", value));
            assertThrows(IllegalArgumentException.class, () -> cache.allocate(-1L));
            assertArrayEquals(value, cache.getOrNull("k"));
        } finally {
            cache.close();
        }
    }

    /**
     * An entry's expiration clock starts when the entry is installed, i.e. AFTER a slow disk
     * write completes — not when the put was issued. A short-TTL replacement written through a
     * slow store therefore still succeeds, is readable for its full TTL afterwards, and expires
     * (with clean disk state) once the TTL elapses. This holds for both a memory-backed and a
     * disk-backed prior entry.
     */
    @Test
    void diskReplacementTtlStartsAfterStoreWrite() throws Exception {
        final java.util.Map<String, byte[]> memStore = new java.util.concurrent.ConcurrentHashMap<>();

        final OffHeapCache<String, byte[]> c = OffHeapCache.<String, byte[]> builder()
                .capacityInMB(1)
                .evictDelay(0)
                .offHeapStore(newSlowInMemoryStore(memStore, 60))
                .storeSelector((k, v, size) -> size < 1000 ? 1 : 2) // small values in memory, large on disk
                .build();

        try {
            // Memory-backed prior replaced by a disk-routed value with a TTL shorter than the
            // 60 ms store write: the put still succeeds and the value is readable afterwards,
            // because the TTL starts when the write completes.
            assertTrue(c.put("k", new byte[] { 1, 2, 3, 4 })); // memory-backed, no expiration
            final byte[] replacement = new byte[2000];
            assertTrue(c.put("k", replacement, 500, 0));
            assertArrayEquals(replacement, c.getOrNull("k"), "the replacement must be readable for its full TTL after the write");
            assertEquals(1L, c.stats().sizeOnDisk());

            // Disk-backed prior replaced disk-to-disk: same rule.
            final byte[] second = new byte[3000];
            assertTrue(c.put("k", second, 500, 0));
            assertArrayEquals(second, c.getOrNull("k"));
            assertEquals(1, memStore.size(), "the store holds exactly the current bytes for the key");

            // After the TTL elapses the entry expires: the key reads as absent, the stale
            // mapping is dropped, and the disk statistics return to zero.
            Thread.sleep(600);
            assertNull(c.getOrNull("k"));
            assertFalse(c.containsKey("k"));

            final var stats = c.stats();
            assertEquals(0L, stats.sizeOnDisk());
            assertEquals(0L, stats.dataSizeOnDisk());
        } finally {
            c.close();
        }
    }

    @Test
    void failedDiskReplacementPreservesExistingEntry() {
        final java.util.Map<String, byte[]> memStore = new java.util.concurrent.ConcurrentHashMap<>();
        final AtomicBoolean failWrites = new AtomicBoolean(false);

        final OffHeapStore<String> inMemoryStore = new OffHeapStore<>() {
            @Override
            public boolean put(final String key, final byte[] value) {
                if (failWrites.get()) {
                    return false;
                }

                memStore.put(key, value);
                return true;
            }

            @Override
            public byte[] get(final String key) {
                return memStore.get(key);
            }

            @Override
            public boolean remove(final String key) {
                return memStore.remove(key) != null;
            }
        };

        final OffHeapCache<String, Account> diskCache = OffHeapCache.<String, Account> builder()
                .capacityInMB(8)
                .evictDelay(0)
                .defaultLiveTime(1000_000)
                .defaultMaxIdleTime(1000_000)
                .offHeapStore(inMemoryStore)
                .storeSelector((k, v, size) -> 2)
                .build();

        try {
            final String key = "shared-key";
            final Account first = new Account();
            first.setFirstName(Strings.repeat("a", 5000));
            assertTrue(diskCache.put(key, first));

            final Account second = new Account();
            second.setFirstName(Strings.repeat("b", 5000));
            failWrites.set(true);

            assertFalse(diskCache.put(key, second));
            assertEquals(first, diskCache.get(key).orElse(null));
            assertTrue(memStore.containsKey(key));
            assertEquals(1, memStore.size());

            final var stats = diskCache.stats();
            assertEquals(1L, stats.sizeOnDisk());
            assertEquals(stats.dataSizeOnDisk(), stats.dataSize());
        } finally {
            diskCache.close();
        }
    }

    /**
     * A replacement put whose value is larger than the ENTIRE off-heap region is doomed: no amount
     * of vacating can make it fit. Such a put must fail side-effect-free — in particular it must
     * NOT schedule the memory-pressure vacate, which runs asynchronously and destroys ~20% of the
     * pool (here: the key's own prior mapping, turning a failed replacement into data loss that
     * won the race against this test's get() only by timing). A failed put of a value that COULD
     * fit still legitimately schedules a vacate, and the prior mapping may then be evicted.
     */
    @Test
    void failedMemoryReplacementPreservesExistingEntry() {
        final OffHeapCache<String, byte[]> c = OffHeapCache.<String, byte[]> builder().capacityInMB(1).evictDelay(0).build();

        try {
            final byte[] oldValue = new byte[] { 1, 2, 3, 4 };

            assertTrue(c.put("k", oldValue));
            assertFalse(c.put("k", new byte[2 * 1024 * 1024]));
            assertArrayEquals(oldValue, c.get("k").orElse(null));
            assertEquals(0, c.stats().evictionCount(), "a doomed oversized put must not schedule a vacate");
        } finally {
            c.close();
        }
    }

    @Test
    void missingDiskBytesNotCountedAsDiskHitAndStaleWrapperRemoved() {
        final java.util.Map<String, byte[]> memStore = new java.util.concurrent.ConcurrentHashMap<>();

        final OffHeapStore<String> inMemoryStore = new OffHeapStore<>() {
            @Override
            public boolean put(final String key, final byte[] value) {
                memStore.put(key, value);
                return true;
            }

            @Override
            public byte[] get(final String key) {
                return memStore.get(key);
            }

            @Override
            public boolean remove(final String key) {
                return memStore.remove(key) != null;
            }
        };

        final OffHeapCache<String, Account> diskCache = OffHeapCache.<String, Account> builder()
                .capacityInMB(8)
                .evictDelay(0)
                .defaultLiveTime(1000_000)
                .defaultMaxIdleTime(1000_000)
                .offHeapStore(inMemoryStore)
                .storeSelector((k, v, size) -> 2)
                .build();

        try {
            final Account account = new Account();
            account.setEmailAddress("account@example.test");
            account.setFirstName("Ada");
            account.setFirstName(Strings.repeat("x", 5000));
            final String key = account.getEmailAddress();

            assertTrue(diskCache.put(key, account));
            memStore.clear();

            assertNull(diskCache.get(key).orElse(null));

            final var stats = diskCache.stats();
            assertFalse(diskCache.containsKey(key), "the stale pool wrapper must be removed after the store reports missing bytes");
            assertEquals(1L, stats.getCount());
            assertEquals(0L, stats.hitCount(), "a get that returned no value must not remain classified as a pool-level hit");
            assertEquals(1L, stats.missCount(), "a confirmed backing-store miss must be reflected in cache miss statistics");
            assertEquals(0L, stats.hitCountFromDisk());
            assertEquals(0L, stats.sizeOnDisk());
            assertEquals(0L, stats.dataSizeOnDisk());
            assertEquals(0L, stats.dataSize());
        } finally {
            diskCache.close();
        }
    }

    @Test
    void throwingDiskReplacementPreservesExistingEntry() {
        final java.util.Map<String, byte[]> memStore = new java.util.concurrent.ConcurrentHashMap<>();
        final AtomicBoolean failWrites = new AtomicBoolean(false);

        final OffHeapStore<String> throwingStore = new OffHeapStore<>() {
            @Override
            public boolean put(final String key, final byte[] value) {
                if (failWrites.get()) {
                    throw new IllegalStateException("disk store failed for: " + key);
                }

                memStore.put(key, value);
                return true;
            }

            @Override
            public byte[] get(final String key) {
                return memStore.get(key);
            }

            @Override
            public boolean remove(final String key) {
                return memStore.remove(key) != null;
            }
        };

        final OffHeapCache<String, Account> diskCache = OffHeapCache.<String, Account> builder()
                .capacityInMB(8)
                .evictDelay(0)
                .defaultLiveTime(1000_000)
                .defaultMaxIdleTime(1000_000)
                .offHeapStore(throwingStore)
                .storeSelector((k, v, size) -> 2)
                .build();

        try {
            final String key = "shared-key-throwing";
            final Account first = new Account();
            first.setFirstName(Strings.repeat("a", 5000));
            assertTrue(diskCache.put(key, first));

            final Account second = new Account();
            second.setFirstName(Strings.repeat("b", 5000));
            failWrites.set(true);

            assertThrows(IllegalStateException.class, () -> diskCache.put(key, second));
            assertEquals(first, diskCache.get(key).orElse(null));
            assertTrue(memStore.containsKey(key));
            assertEquals(1, memStore.size());

            final var stats = diskCache.stats();
            assertEquals(1L, stats.sizeOnDisk());
            assertEquals(stats.dataSizeOnDisk(), stats.dataSize());
        } finally {
            diskCache.close();
        }
    }

    /** Store whose successful writes take {@code writeDelayMillis}, so a short-TTL replacement can expire mid-write. */
    private static OffHeapStore<String> newSlowInMemoryStore(final java.util.Map<String, byte[]> backing, final long writeDelayMillis) {
        return new OffHeapStore<>() {
            @Override
            public boolean put(final String key, final byte[] value) {
                try {
                    Thread.sleep(writeDelayMillis);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }

                backing.put(key, value);
                return true;
            }

            @Override
            public byte[] get(final String key) {
                return backing.get(key);
            }

            @Override
            public boolean remove(final String key) {
                return backing.remove(key) != null;
            }
        };
    }
}
