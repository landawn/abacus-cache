/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.ContinuableFuture;

class SpyMemcachedIT {

    private static final String SERVER_URL = com.landawn.abacus.cache.MemcachedTestSupport.serverUrl();

    private static SpyMemcached<Object> cache;

    @BeforeAll
    static void connect() {
        cache = new SpyMemcached<>(SERVER_URL);
    }

    @AfterAll
    static void disconnect() {
        if (cache != null) {
            cache.disconnect();
        }
    }

    @BeforeEach
    void flush() {
        // Isolate each test on the shared server.
        cache.flushAll();
    }

    // Construction

    /**
     * Regression test for huge-timeout overflow.
     *
     * <p>A huge configured operation timeout (e.g. {@code Long.MAX_VALUE} as a "no timeout"
     * sentinel) previously made every synchronous operation fail instantly: spymemcached's
     * internal millisecond-to-nanosecond conversion overflowed so every in-flight operation
     * appeared timed out. The timeout is now clamped to a ~146-year safety cap (which also bounds
     * {@code resultOf()}'s synchronous wait), so operations work.
     */
    @Test
    void constructorWithHugeTimeoutOperationsStillWork() {
        final SpyMemcached<Object> local = new SpyMemcached<>(SERVER_URL, Long.MAX_VALUE);
        try {
            assertTrue(local.put("huge-timeout-key", "v", 60_000));
            assertEquals("v", local.get("huge-timeout-key"));
        } finally {
            local.disconnect();
        }
    }

    // Reads

    @Test
    void asyncGetBulkCollectionForwards() throws Exception {
        cache.put("a", 1, 60_000);

        final Map<String, Object> got = cache.asyncGetBulk(Arrays.asList("a")).get();

        assertEquals(1, got.size());
        assertEquals(1, got.get("a"));
    }

    @Test
    void asyncGetBulkVarargsForwards() throws Exception {
        cache.put("a", 1, 60_000);
        cache.put("b", 2, 60_000);

        final Map<String, Object> got = cache.asyncGetBulk("a", "b").get();

        assertEquals(2, got.size());
        assertEquals(1, got.get("a"));
    }

    @Test
    void getReturnsNullForMissingKey() {
        assertNull(cache.get("missing"));
    }

    @Test
    void getReturnsValue() {
        cache.put("user:1", "hello", 60_000);
        assertEquals("hello", cache.get("user:1"));
    }

    // Bulk Reads

    /**
     * Validation and request dispatch must consume the caller's collection only once. Iterating the
     * live collection a second time creates a time-of-check/time-of-use gap in which a custom or
     * concurrently changing collection can supply keys that were never validated.
     */
    @Test
    void getBulkCollectionIsValidatedAndSnapshottedInOnePass() {
        cache.put("snapshot-key", "value", 60_000);
        final AtomicInteger iteratorCalls = new AtomicInteger();
        final Collection<String> oneShotView = new AbstractCollection<>() {
            @Override
            public Iterator<String> iterator() {
                if (iteratorCalls.incrementAndGet() > 1) {
                    throw new AssertionError("the caller's collection was iterated more than once");
                }

                return List.of("snapshot-key").iterator();
            }

            @Override
            public int size() {
                return 1;
            }
        };

        assertEquals("value", cache.getBulk(oneShotView).get("snapshot-key"));
        assertEquals(1, iteratorCalls.get());
    }

    @Test
    void getBulkCollectionReturnsOnlyFoundKeys() {
        cache.put("a", 1, 60_000);

        final List<String> keys = Arrays.asList("a", "b");
        final Map<String, Object> got = cache.getBulk(keys);

        assertEquals(1, got.size());
        assertEquals(1, got.get("a"));
    }

    @Test
    void getBulkNoneFoundReturnsEmptyMap() {
        assertTrue(cache.getBulk("zzz1", "zzz2").isEmpty());
    }

    @Test
    void getBulkVarargsReturnsOnlyFoundKeys() {
        cache.put("a", 1, 60_000);
        cache.put("b", 2, 60_000);

        final Map<String, Object> got = cache.getBulk("a", "b", "missing");

        assertEquals(2, got.size());
        assertEquals(1, got.get("a"));
        assertEquals(2, got.get("b"));
        assertFalse(got.containsKey("missing"));
    }

    // Writes

    @Test
    void addSucceedsWhenAbsentAndFailsWhenPresent() {
        assertTrue(cache.add("k", "v1", 60_000));
        assertFalse(cache.add("k", "v2", 60_000));
        // The original value is preserved (the second add was rejected).
        assertEquals("v1", cache.get("k"));
    }

    @Test
    void asyncSetThenAsyncGet() throws Exception {
        assertTrue(cache.asyncPut("k", "v", 60_000).get());
        assertEquals("v", cache.asyncGet("k").get());
    }

    @Test
    void decrMissingKeyReturnsMinusOne() {
        assertEquals(-1L, cache.decr("no-such-counter"));
    }

    /**
     * Decr counterpart of the Kryo counter-poisoning regression: the seeded counter must keep
     * decrementing on subsequent calls (previously the second call returned {@code -1} because the
     * Kryo-encoded seed was not a memcached-mutable ASCII number).
     */
    @Test
    void decrWithDefaultValueAdvancesOnSubsequentCalls() {
        assertEquals(100L, cache.decr("adv-down-counter", 10, 100L)); // absent: seeded with 100
        assertEquals(90L, cache.decr("adv-down-counter", 10, 100L)); // existing: 100 - 10
        assertEquals(89L, cache.decr("adv-down-counter", 1)); // plain decr also works on the seeded key
    }

    @Test
    void decrWithDefaultValueAndLiveTimeSeedsAbsentKey() {
        assertEquals(100L, cache.decr("counter", 1, 100L, 60_000L));
    }

    @Test
    void decrWithDefaultValueSeedsAbsentKey() {
        assertEquals(100L, cache.decr("counter", 1, 100L));
    }

    /**
     * Regression test for the negative-defaultValue counter-poisoning hole: memcached counters are
     * unsigned 64-bit decimals, so seeding an absent key with e.g. "-5" appears to succeed but makes
     * every subsequent incr/decr fail with CLIENT_ERROR (and tear down the connection). A negative
     * seed is now rejected up front in all four seeding overloads.
     */
    @Test
    void incrDecrWithNegativeDefaultValueRejected() {
        assertThrows(IllegalArgumentException.class, () -> cache.incr("k", 1, -5L));
        assertThrows(IllegalArgumentException.class, () -> cache.incr("k", 1, -5L, 1000L));
        assertThrows(IllegalArgumentException.class, () -> cache.decr("k", 1, -5L));
        assertThrows(IllegalArgumentException.class, () -> cache.decr("k", 1, -5L, 1000L));

        // The key was never seeded: a plain incr still reports it absent.
        assertEquals(-1L, cache.incr("k"));
    }

    @Test
    void incrMissingKeyReturnsMinusOne() {
        // Memcached returns -1 for a non-existent counter (no auto-initialization).
        assertEquals(-1L, cache.incr("no-such-counter"));
    }

    /**
     * Regression test for the Kryo-transcoder counter-poisoning bug.
     *
     * <p>spymemcached's incr-with-default ({@code mutateWithDefault}) seeds an absent key through the
     * client's DEFAULT transcoder. With {@link KryoTranscoder} installed (the default here), the seed
     * was stored as a Kryo-encoded blob that memcached's native incr cannot mutate: the first call
     * returned the default, but every subsequent call failed with {@code CLIENT_ERROR ... non-numeric
     * value}, returned {@code -1}, and tore down the connection. The seed is now written as raw ASCII
     * decimal via an atomic {@code add}, so the counter genuinely advances across calls.
     */
    @Test
    void incrWithDefaultValueAdvancesOnSubsequentCalls() {
        assertEquals(10L, cache.incr("adv-counter", 5, 10L)); // absent: seeded with 10, delta not applied
        assertEquals(15L, cache.incr("adv-counter", 5, 10L)); // existing: 10 + 5
        assertEquals(16L, cache.incr("adv-counter", 1)); // plain incr also works on the seeded key

        // The connection must remain healthy after the counter operations.
        assertTrue(cache.put("health-check", "ok", 60_000));
        assertEquals("ok", cache.get("health-check"));
    }

    @Test
    void incrWithDefaultValueAndLiveTimeAdvancesOnSubsequentCalls() {
        assertEquals(7L, cache.incr("adv-ttl-counter", 5, 7L, 60_000L));
        assertEquals(12L, cache.incr("adv-ttl-counter", 5, 7L, 60_000L));
    }

    @Test
    void incrWithDefaultValueAndLiveTimeSeedsAbsentKey() {
        assertEquals(7L, cache.incr("counter", 5, 7L, 60_000L));
    }

    @Test
    void incrWithDefaultValueSeedsAbsentKey() {
        // On a missing key the default is stored verbatim and returned (delta is NOT applied on insert).
        assertEquals(0L, cache.incr("counter", 1, 0L));
    }

    @Test
    void replaceFailsWhenAbsentAndSucceedsWhenPresent() {
        assertFalse(cache.replace("missing", "v", 60_000));

        cache.put("k", "v1", 60_000);
        assertTrue(cache.replace("k", "v2", 60_000));
        assertEquals("v2", cache.get("k"));
    }

    @Test
    void setLongTtlIsStoredAndRetrievable() {
        // A TTL beyond 30 days is converted to an absolute Unix expiration timestamp. The value must be
        // immediately retrievable (i.e. NOT stored already-expired, which is what a botched conversion
        // would produce).
        final long liveTime = 31L * 24 * 60 * 60 * 1000; // 31 days
        assertTrue(cache.put("k", "value", liveTime));
        assertEquals("value", cache.get("k"));
    }

    @Test
    void setNullValueIsStoredAndReadsBackNull() {
        // A null value is accepted (memcached stores the empty Kryo payload); get maps it back to null.
        assertTrue(cache.put("maybe-null", null, 60_000));
        assertNull(cache.get("maybe-null"));
    }

    @Test
    void setOverwritesExistingValue() {
        cache.put("k", "v1", 60_000);
        assertTrue(cache.put("k", "v2", 60_000));
        assertEquals("v2", cache.get("k"));
    }

    @Test
    void setShortTtlExpires() throws Exception {
        // 1_000 ms -> 1 s. The value is present immediately and gone shortly after the TTL elapses,
        // confirming the ms->s conversion is honored end-to-end by the real server.
        cache.put("ttl", "v", 1_000);
        assertEquals("v", cache.get("ttl"));

        Thread.sleep(2_500);
        assertNull(cache.get("ttl"));
    }

    @Test
    void setStoresAndReturnsTrue() {
        assertTrue(cache.put("k", "value", 60_000));
        assertEquals("value", cache.get("k"));
    }

    // Removals

    @Test
    void asyncDeleteForwards() throws Exception {
        cache.put("k", "v", 60_000);
        assertTrue(cache.asyncRemove("k").get());
        assertNull(cache.get("k"));
    }

    @Test
    void deleteExistingReturnsTrueAndRemoves() {
        cache.put("k", "v", 60_000);
        assertTrue(cache.remove("k"));
        assertNull(cache.get("k"));
    }

    @Test
    void deleteMissingReturnsFalse() {
        assertFalse(cache.remove("never-existed"));
    }

    @Test
    void flushAllClearsAllKeys() {
        cache.put("a", 1, 60_000);
        cache.put("b", 2, 60_000);

        cache.flushAll();

        assertNull(cache.get("a"));
        assertNull(cache.get("b"));
    }

    @Test
    void flushAllLargeDelayIsScheduledNotImmediate() {
        // memcached's `flush_all <delay>` argument goes through the server's realtime() conversion
        // just like storage expirations: raw values above 30 days are read as ABSOLUTE epoch
        // timestamps. A raw 40-day delay (3456000s) would be an epoch time in Feb 1970 - not a
        // deferred flush. The client therefore converts >30-day delays to absolute now+delay
        // timestamps (same rule as set/add/replace), so the flush is genuinely scheduled 40 days
        // out and the value is still present right after the call.
        cache.put("a", 1, 60_000);

        assertTrue(cache.flushAll(3_456_000_000L)); // 40 days

        assertEquals(1, cache.get("a"));
    }

    @Test
    void flushAllWithImmediateDelayClears() {
        cache.put("a", 1, 60_000);

        assertTrue(cache.flushAll(0));

        assertNull(cache.get("a"));
    }

    // Lifecycle

    @Test
    void disconnectWithTimeoutRejectsNegative() {
        assertThrows(IllegalArgumentException.class, () -> cache.disconnect(-1));
        // The shared client must remain usable (the negative timeout was rejected before shutdown).
        // serverUrl() alone would not prove that: it remains available after disconnect.
        assertNotNull(cache.serverUrl());
        assertTrue(cache.put("after-negative-disconnect", "v", 60_000));
        assertEquals("v", cache.get("after-negative-disconnect"));
    }

    // Compatibility

    @Test
    @SuppressWarnings("deprecation")
    void deprecatedAsyncDeleteAliasDelegatesToAsyncRemove() throws Exception {
        cache.put("legacy-delete", "v", 60_000);
        assertTrue(cache.asyncDelete("legacy-delete").get());
        assertNull(cache.get("legacy-delete"));
        assertEquals(Future.class, SpyMemcached.class.getDeclaredMethod("asyncDelete", String.class).getReturnType());
    }

    @Test
    @SuppressWarnings("deprecation")
    void deprecatedAsyncSetAliasDelegatesToAsyncPut() throws Exception {
        assertTrue(cache.asyncSet("legacy-set", "v", 60_000).get());
        assertEquals("v", cache.get("legacy-set"));
        assertEquals(Future.class, SpyMemcached.class.getDeclaredMethod("asyncSet", String.class, Object.class, long.class).getReturnType());
    }

    // Other Behavior

    @Test
    void asyncAddForwards() throws Exception {
        assertTrue(cache.asyncAdd("k", "v", 60_000).get());
        assertFalse(cache.asyncAdd("k", "v2", 60_000).get());
    }

    @Test
    void asyncFlushAllForwards() throws Exception {
        cache.put("a", 1, 60_000);

        assertTrue(cache.asyncFlushAll().get());

        assertNull(cache.get("a"));
    }

    @Test
    void asyncFlushAllWithDelayForwards() throws Exception {
        assertTrue(cache.asyncFlushAll(0).get());
    }

    @Test
    void asyncMethodsReturnContinuableFuture() throws Exception {
        // The async* methods wrap the spymemcached Future in an abacus ContinuableFuture; the static
        // types below only compile because of that, and the ContinuableFuture-specific map(...) chain
        // (absent from java.util.concurrent.Future) confirms the concrete type at runtime.
        final ContinuableFuture<Boolean> putFuture = cache.asyncPut("k", "v", 60_000);
        assertTrue(putFuture.get());

        final ContinuableFuture<Object> getFuture = cache.asyncGet("k");
        assertEquals("v", getFuture.get());

        assertEquals("v", cache.asyncGet("k").map(v -> v).get());

        final ContinuableFuture<Map<String, Object>> bulkFuture = cache.asyncGetBulk("k");
        assertEquals("v", bulkFuture.get().get("k"));
    }

    @Test
    void asyncReplaceForwards() throws Exception {
        cache.put("k", "v1", 60_000);
        assertTrue(cache.asyncReplace("k", "v2", 60_000).get());
        assertEquals("v2", cache.get("k"));
    }

    /**
     * The delegate's enqueue path waits interruptibly, so an operation started with the interrupt
     * status already set is rejected with {@link IllegalStateException} before anything is sent,
     * the status is preserved, and the client stays usable once the status is cleared.
     */
    @Test
    void operationStartedWhileInterruptedFailsBeforeEnqueueAndKeepsStatus() {
        Thread.currentThread().interrupt();

        try {
            assertThrows(IllegalStateException.class, () -> cache.put("interrupted-put", "v", 60_000));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted(); // Do not leak the intentional interrupt into the JUnit worker.
        }

        assertNull(cache.get("interrupted-put"));
        assertTrue(cache.put("interrupted-put", "v", 60_000));
        assertEquals("v", cache.get("interrupted-put"));
    }
}
