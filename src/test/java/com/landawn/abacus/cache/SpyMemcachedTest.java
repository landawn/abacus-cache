/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.landawn.abacus.util.ContinuableFuture;

import net.spy.memcached.MemcachedClient;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link SpyMemcached} that run against a real Memcached server reachable at
 * {@code localhost:11211} (e.g. {@code docker run --name memcached -p 11211:11211 -d memcached:latest}).
 *
 * <p>These tests deliberately use no mock client and no in-memory fake: every operation is exercised
 * end-to-end against the live server. A single client is shared across the class and the server is
 * flushed before each test for isolation.
 *
 * <p><b>Memcached + Kryo note:</b> this client serializes values with a Kryo transcoder, so a stored
 * value is NOT an ASCII-decimal string. Memcached's native {@code incr}/{@code decr} therefore only
 * operate correctly on counters created through the increment-with-default seeding path, which writes
 * a raw ASCII decimal. Those counters remain mutable on subsequent calls, but they cannot be decoded by
 * the Kryo-backed {@code get} method. Attempting to increment an ordinary Kryo-encoded value yields a
 * server-side "cannot increment or decrement non-numeric value" error.
 */
@Tag("2025")
public class SpyMemcachedTest {

    private static final String SERVER_URL = "localhost:11211";

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

    // --- get -----------------------------------------------------------------------------------

    @Test
    public void test_get_returns_value() {
        cache.put("user:1", "hello", 60_000);
        assertEquals("hello", cache.get("user:1"));
    }

    @Test
    public void test_get_returns_null_for_missing_key() {
        assertNull(cache.get("missing"));
    }

    @Test
    public void test_get_rejects_null_key() {
        assertThrows(IllegalArgumentException.class, () -> cache.get(null));
    }

    // --- set -----------------------------------------------------------------------------------

    @Test
    public void test_set_stores_and_returns_true() {
        assertTrue(cache.put("k", "value", 60_000));
        assertEquals("value", cache.get("k"));
    }

    @Test
    public void test_set_overwrites_existing_value() {
        cache.put("k", "v1", 60_000);
        assertTrue(cache.put("k", "v2", 60_000));
        assertEquals("v2", cache.get("k"));
    }

    @Test
    public void test_set_null_value_is_stored_and_reads_back_null() {
        // A null value is accepted (memcached stores the empty Kryo payload); get maps it back to null.
        assertTrue(cache.put("maybe-null", null, 60_000));
        assertNull(cache.get("maybe-null"));
    }

    @Test
    public void test_set_rejects_null_key() {
        assertThrows(IllegalArgumentException.class, () -> cache.put(null, "v", 60_000));
    }

    @Test
    public void test_set_long_ttl_is_stored_and_retrievable() {
        // A TTL beyond 30 days is converted to an absolute Unix expiration timestamp. The value must be
        // immediately retrievable (i.e. NOT stored already-expired, which is what a botched conversion
        // would produce).
        final long liveTime = 31L * 24 * 60 * 60 * 1000; // 31 days
        assertTrue(cache.put("k", "value", liveTime));
        assertEquals("value", cache.get("k"));
    }

    @Test
    public void test_set_short_ttl_expires() throws Exception {
        // 1_000 ms -> 1 s. The value is present immediately and gone shortly after the TTL elapses,
        // confirming the ms->s conversion is honored end-to-end by the real server.
        cache.put("ttl", "v", 1_000);
        assertEquals("v", cache.get("ttl"));

        Thread.sleep(2_500);
        assertNull(cache.get("ttl"));
    }

    /**
     * A liveTime so large that the derived absolute Unix expiration second overflows {@code int}
     * (while the relative seconds still fit) is rejected before any network call.
     */
    @Test
    public void test_set_expiration_overflow_throws() {
        assertThrows(IllegalArgumentException.class, () -> cache.put("k", "v", 2_000_000_000_000L));
    }

    @Test
    public void test_asyncSet_then_asyncGet() throws Exception {
        assertTrue(cache.asyncPut("k", "v", 60_000).get());
        assertEquals("v", cache.asyncGet("k").get());
    }

    @Test
    public void test_asyncSet_rejects_null_key() {
        assertThrows(IllegalArgumentException.class, () -> cache.asyncPut(null, "v", 60_000));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void test_deprecated_asyncSet_alias_delegates_to_asyncPut() throws Exception {
        assertTrue(cache.asyncSet("legacy-set", "v", 60_000).get());
        assertEquals("v", cache.get("legacy-set"));
        assertEquals(Future.class, SpyMemcached.class.getDeclaredMethod("asyncSet", String.class, Object.class, long.class).getReturnType());
    }

    @Test
    public void test_asyncGet_rejects_null_key() {
        assertThrows(IllegalArgumentException.class, () -> cache.asyncGet(null));
    }

    @Test
    public void test_async_methods_return_ContinuableFuture() throws Exception {
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

    /**
     * An in-place return-type refinement changes a JVM method descriptor even when the new return
     * type is covariant. These bridge checks protect already-compiled callers that still link to
     * the original {@link Future}-returning descriptors.
     */
    @Test
    public void test_existing_async_methods_retain_legacy_Future_bridges() {
        assertTrue(hasFutureBridge("asyncGet", String.class));
        assertTrue(hasFutureBridge("asyncGetBulk", String[].class));
        assertTrue(hasFutureBridge("asyncGetBulk", Collection.class));
        assertTrue(hasFutureBridge("asyncAdd", String.class, Object.class, long.class));
        assertTrue(hasFutureBridge("asyncReplace", String.class, Object.class, long.class));
        assertTrue(hasFutureBridge("asyncFlushAll"));
        assertTrue(hasFutureBridge("asyncFlushAll", long.class));
    }

    // --- add -----------------------------------------------------------------------------------

    @Test
    public void test_add_succeeds_when_absent_and_fails_when_present() {
        assertTrue(cache.add("k", "v1", 60_000));
        assertFalse(cache.add("k", "v2", 60_000));
        // The original value is preserved (the second add was rejected).
        assertEquals("v1", cache.get("k"));
    }

    @Test
    public void test_add_rejects_null_key() {
        assertThrows(IllegalArgumentException.class, () -> cache.add(null, "v", 60_000));
    }

    @Test
    public void test_asyncAdd_forwards() throws Exception {
        assertTrue(cache.asyncAdd("k", "v", 60_000).get());
        assertFalse(cache.asyncAdd("k", "v2", 60_000).get());
    }

    @Test
    public void test_asyncAdd_rejects_null_key() {
        assertThrows(IllegalArgumentException.class, () -> cache.asyncAdd(null, "v", 60_000));
    }

    // --- replace -------------------------------------------------------------------------------

    @Test
    public void test_replace_fails_when_absent_and_succeeds_when_present() {
        assertFalse(cache.replace("missing", "v", 60_000));

        cache.put("k", "v1", 60_000);
        assertTrue(cache.replace("k", "v2", 60_000));
        assertEquals("v2", cache.get("k"));
    }

    @Test
    public void test_replace_rejects_null_key() {
        assertThrows(IllegalArgumentException.class, () -> cache.replace(null, "v", 60_000));
    }

    @Test
    public void test_asyncReplace_forwards() throws Exception {
        cache.put("k", "v1", 60_000);
        assertTrue(cache.asyncReplace("k", "v2", 60_000).get());
        assertEquals("v2", cache.get("k"));
    }

    @Test
    public void test_asyncReplace_rejects_null_key() {
        assertThrows(IllegalArgumentException.class, () -> cache.asyncReplace(null, "v", 60_000));
    }

    // --- delete --------------------------------------------------------------------------------

    @Test
    public void test_delete_existing_returns_true_and_removes() {
        cache.put("k", "v", 60_000);
        assertTrue(cache.remove("k"));
        assertNull(cache.get("k"));
    }

    @Test
    public void test_delete_missing_returns_false() {
        assertFalse(cache.remove("never-existed"));
    }

    @Test
    public void test_delete_rejects_null_key() {
        assertThrows(IllegalArgumentException.class, () -> cache.remove(null));
    }

    @Test
    public void test_asyncDelete_forwards() throws Exception {
        cache.put("k", "v", 60_000);
        assertTrue(cache.asyncRemove("k").get());
        assertNull(cache.get("k"));
    }

    @Test
    public void test_asyncDelete_rejects_null_key() {
        assertThrows(IllegalArgumentException.class, () -> cache.asyncRemove(null));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void test_deprecated_asyncDelete_alias_delegates_to_asyncRemove() throws Exception {
        cache.put("legacy-delete", "v", 60_000);
        assertTrue(cache.asyncDelete("legacy-delete").get());
        assertNull(cache.get("legacy-delete"));
        assertEquals(Future.class, SpyMemcached.class.getDeclaredMethod("asyncDelete", String.class).getReturnType());
    }

    // --- incr ----------------------------------------------------------------------------------

    @Test
    public void test_incr_missing_key_returns_minus_one() {
        // Memcached returns -1 for a non-existent counter (no auto-initialization).
        assertEquals(-1L, cache.incr("no-such-counter"));
    }

    @Test
    public void test_incr_rejects_null_key() {
        assertThrows(IllegalArgumentException.class, () -> cache.incr(null));
        assertThrows(IllegalArgumentException.class, () -> cache.incr(null, 1));
    }

    @Test
    public void test_incr_rejects_negative_delta() {
        assertThrows(IllegalArgumentException.class, () -> cache.incr("counter", -1));
    }

    @Test
    public void test_incr_with_default_value_seeds_absent_key() {
        // On a missing key the default is stored verbatim and returned (delta is NOT applied on insert).
        assertEquals(0L, cache.incr("counter", 1, 0L));
    }

    @Test
    public void test_incr_with_default_value_and_liveTime_seeds_absent_key() {
        assertEquals(7L, cache.incr("counter", 5, 7L, 60_000L));
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
    public void test_incr_with_default_value_advances_on_subsequent_calls() {
        assertEquals(10L, cache.incr("adv-counter", 5, 10L)); // absent: seeded with 10, delta not applied
        assertEquals(15L, cache.incr("adv-counter", 5, 10L)); // existing: 10 + 5
        assertEquals(16L, cache.incr("adv-counter", 1)); // plain incr also works on the seeded key

        // The connection must remain healthy after the counter operations.
        assertTrue(cache.put("health-check", "ok", 60_000));
        assertEquals("ok", cache.get("health-check"));
    }

    @Test
    public void test_incr_with_default_value_and_liveTime_advances_on_subsequent_calls() {
        assertEquals(7L, cache.incr("adv-ttl-counter", 5, 7L, 60_000L));
        assertEquals(12L, cache.incr("adv-ttl-counter", 5, 7L, 60_000L));
    }

    @Test
    public void test_incr_with_default_value_validation() {
        assertThrows(IllegalArgumentException.class, () -> cache.incr(null, 1, 0L));
        assertThrows(IllegalArgumentException.class, () -> cache.incr("k", -1, 0L));
        assertThrows(IllegalArgumentException.class, () -> cache.incr(null, 1, 0L, 1000L));
        assertThrows(IllegalArgumentException.class, () -> cache.incr("k", -1, 0L, 1000L));
    }

    /**
     * Regression test for the negative-defaultValue counter-poisoning hole: memcached counters are
     * unsigned 64-bit decimals, so seeding an absent key with e.g. "-5" appears to succeed but makes
     * every subsequent incr/decr fail with CLIENT_ERROR (and tear down the connection). A negative
     * seed is now rejected up front in all four seeding overloads.
     */
    @Test
    public void test_incr_decr_with_negative_default_value_rejected() {
        assertThrows(IllegalArgumentException.class, () -> cache.incr("k", 1, -5L));
        assertThrows(IllegalArgumentException.class, () -> cache.incr("k", 1, -5L, 1000L));
        assertThrows(IllegalArgumentException.class, () -> cache.decr("k", 1, -5L));
        assertThrows(IllegalArgumentException.class, () -> cache.decr("k", 1, -5L, 1000L));

        // The key was never seeded: a plain incr still reports it absent.
        assertEquals(-1L, cache.incr("k"));
    }

    // --- decr ----------------------------------------------------------------------------------

    @Test
    public void test_decr_missing_key_returns_minus_one() {
        assertEquals(-1L, cache.decr("no-such-counter"));
    }

    @Test
    public void test_decr_rejects_null_key() {
        assertThrows(IllegalArgumentException.class, () -> cache.decr(null));
        assertThrows(IllegalArgumentException.class, () -> cache.decr(null, 1));
    }

    @Test
    public void test_decr_rejects_negative_delta() {
        assertThrows(IllegalArgumentException.class, () -> cache.decr("counter", -1));
    }

    @Test
    public void test_decr_with_default_value_seeds_absent_key() {
        assertEquals(100L, cache.decr("counter", 1, 100L));
    }

    @Test
    public void test_decr_with_default_value_and_liveTime_seeds_absent_key() {
        assertEquals(100L, cache.decr("counter", 1, 100L, 60_000L));
    }

    /**
     * Decr counterpart of the Kryo counter-poisoning regression: the seeded counter must keep
     * decrementing on subsequent calls (previously the second call returned {@code -1} because the
     * Kryo-encoded seed was not a memcached-mutable ASCII number).
     */
    @Test
    public void test_decr_with_default_value_advances_on_subsequent_calls() {
        assertEquals(100L, cache.decr("adv-down-counter", 10, 100L)); // absent: seeded with 100
        assertEquals(90L, cache.decr("adv-down-counter", 10, 100L)); // existing: 100 - 10
        assertEquals(89L, cache.decr("adv-down-counter", 1)); // plain decr also works on the seeded key
    }

    @Test
    public void test_decr_with_default_value_validation() {
        assertThrows(IllegalArgumentException.class, () -> cache.decr(null, 1, 0L));
        assertThrows(IllegalArgumentException.class, () -> cache.decr("k", -1, 0L));
        assertThrows(IllegalArgumentException.class, () -> cache.decr(null, 1, 0L, 1000L));
        assertThrows(IllegalArgumentException.class, () -> cache.decr("k", -1, 0L, 1000L));
    }

    // --- getBulk -------------------------------------------------------------------------------

    @Test
    public void test_getBulk_varargs_returns_only_found_keys() {
        cache.put("a", 1, 60_000);
        cache.put("b", 2, 60_000);

        final Map<String, Object> got = cache.getBulk("a", "b", "missing");

        assertEquals(2, got.size());
        assertEquals(1, got.get("a"));
        assertEquals(2, got.get("b"));
        assertFalse(got.containsKey("missing"));
    }

    @Test
    public void test_getBulk_collection_returns_only_found_keys() {
        cache.put("a", 1, 60_000);

        final List<String> keys = Arrays.asList("a", "b");
        final Map<String, Object> got = cache.getBulk(keys);

        assertEquals(1, got.size());
        assertEquals(1, got.get("a"));
    }

    @Test
    public void test_getBulk_none_found_returns_empty_map() {
        assertTrue(cache.getBulk("zzz1", "zzz2").isEmpty());
    }

    @Test
    public void test_getBulk_rejects_null_keys() {
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk((String[]) null));
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk("a", null));
    }

    @Test
    public void test_getBulk_collection_rejects_null_keys() {
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk((java.util.Collection<String>) null));
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk(Arrays.asList("a", null)));
    }

    /**
     * Validation and request dispatch must consume the caller's collection only once. Iterating the
     * live collection a second time creates a time-of-check/time-of-use gap in which a custom or
     * concurrently changing collection can supply keys that were never validated.
     */
    @Test
    public void test_getBulk_collection_isValidatedAndSnapshottedInOnePass() {
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
    public void test_asyncGetBulk_varargs_forwards() throws Exception {
        cache.put("a", 1, 60_000);
        cache.put("b", 2, 60_000);

        final Map<String, Object> got = cache.asyncGetBulk("a", "b").get();

        assertEquals(2, got.size());
        assertEquals(1, got.get("a"));
    }

    @Test
    public void test_asyncGetBulk_collection_forwards() throws Exception {
        cache.put("a", 1, 60_000);

        final Map<String, Object> got = cache.asyncGetBulk(Arrays.asList("a")).get();

        assertEquals(1, got.size());
        assertEquals(1, got.get("a"));
    }

    // --- flushAll ------------------------------------------------------------------------------

    @Test
    public void test_flushAll_clears_all_keys() {
        cache.put("a", 1, 60_000);
        cache.put("b", 2, 60_000);

        cache.flushAll();

        assertNull(cache.get("a"));
        assertNull(cache.get("b"));
    }

    @Test
    public void test_flushAll_with_immediate_delay_clears() {
        cache.put("a", 1, 60_000);

        assertTrue(cache.flushAll(0));

        assertNull(cache.get("a"));
    }

    @Test
    public void test_flushAll_large_delay_is_scheduled_not_immediate() {
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
    public void test_asyncFlushAll_forwards() throws Exception {
        cache.put("a", 1, 60_000);

        assertTrue(cache.asyncFlushAll().get());

        assertNull(cache.get("a"));
    }

    @Test
    public void test_asyncFlushAll_with_delay_forwards() throws Exception {
        assertTrue(cache.asyncFlushAll(0).get());
    }

    // --- lifecycle / construction --------------------------------------------------------------

    @Test
    public void test_serverUrl_returns_constructor_value() {
        assertEquals(SERVER_URL, cache.serverUrl());
    }

    @Test
    public void test_constructor_rejects_invalid_timeout() {
        assertThrows(IllegalArgumentException.class, () -> new SpyMemcached<>(SERVER_URL, 0L));
        assertThrows(IllegalArgumentException.class, () -> new SpyMemcached<>(SERVER_URL, -1L));
    }

    @Test
    public void test_constructor_rejects_blank_server_url() {
        assertThrows(IllegalArgumentException.class, () -> new SpyMemcached<>((String) null));
        assertThrows(IllegalArgumentException.class, () -> new SpyMemcached<>(""));
        assertThrows(IllegalArgumentException.class, () -> new SpyMemcached<>("   "));
    }

    @Test
    public void test_disconnect_is_idempotent() {
        final SpyMemcached<Object> local = new SpyMemcached<>(SERVER_URL);
        local.disconnect();
        local.disconnect(); // safe to call again

        // After shutdown every operation fails in the wrapper, before argument validation or
        // dispatch into spymemcached. This gives all operation families one lifecycle contract.
        assertThrows(IllegalStateException.class, () -> local.get("k"));
        assertThrows(IllegalStateException.class, () -> local.get(null));
        assertThrows(IllegalStateException.class, () -> local.getBulk((String[]) null));
        assertThrows(IllegalStateException.class, () -> local.asyncPut("k", "v", 60_000));
        assertThrows(IllegalStateException.class, () -> local.incr("counter", -1));
        assertThrows(IllegalStateException.class, local::flushAll);
    }

    @Test
    public void test_disconnect_with_timeout() {
        final SpyMemcached<Object> local = new SpyMemcached<>(SERVER_URL);
        local.disconnect(5_000);
        assertThrows(IllegalStateException.class, () -> local.get("k"));
    }

    @Test
    public void test_disconnect_with_timeout_rejects_negative() {
        assertThrows(IllegalArgumentException.class, () -> cache.disconnect(-1));
        // The shared client must remain usable (the negative timeout was rejected before shutdown).
        assertNotNull(cache.serverUrl());
    }

    @Test
    public void test_disconnect_remains_idempotent_on_failure() throws Exception {
        final SpyMemcached<Object> local = new SpyMemcached<>(SERVER_URL);
        final Field clientField = SpyMemcached.class.getDeclaredField("mc");
        clientField.setAccessible(true);
        final MemcachedClient realClient = (MemcachedClient) clientField.get(local);
        final MemcachedClient failingClient = mock(MemcachedClient.class);
        clientField.set(local, failingClient);

        final RuntimeException failure = new RuntimeException("unexpected shutdown failure");
        doThrow(failure).when(failingClient).shutdown();

        try {
            assertEquals(failure, assertThrows(RuntimeException.class, local::disconnect));

            // The delegate has already entered its terminal shutdown sequence; retrying can only
            // repeat teardown against a partially released client.
            local.disconnect();
            verify(failingClient, times(1)).shutdown();
        } finally {
            realClient.shutdown();
        }
    }

    /**
     * Regression coverage for graceful-shutdown interruption. SpyMemcached's delegate wraps the
     * {@link InterruptedException} raised by its queue wait in a plain RuntimeException and clears
     * the flag. The wrapper must restore the flag and remember that the delegate has nevertheless
     * entered its terminal shutdown sequence, so a second disconnect is a no-op.
     */
    @Test
    public void test_disconnect_with_timeout_restores_interrupt_and_remains_idempotent_on_failure() throws Exception {
        final SpyMemcached<Object> local = new SpyMemcached<>(SERVER_URL);
        final Field clientField = SpyMemcached.class.getDeclaredField("mc");
        clientField.setAccessible(true);
        final MemcachedClient realClient = (MemcachedClient) clientField.get(local);
        final MemcachedClient failingClient = mock(MemcachedClient.class);
        clientField.set(local, failingClient);

        final RuntimeException interruption = new RuntimeException("interrupted graceful shutdown", new InterruptedException("test interrupt"));
        doThrow(interruption).when(failingClient).shutdown(anyLong(), eq(TimeUnit.MILLISECONDS));

        try {
            assertEquals(interruption, assertThrows(RuntimeException.class, () -> local.disconnect(1_000L)));
            assertTrue(Thread.currentThread().isInterrupted());

            // The failed call still terminally shut down the delegate; do not invoke it again.
            local.disconnect(1_000L);
            verify(failingClient, times(1)).shutdown(1_000L, TimeUnit.MILLISECONDS);
        } finally {
            Thread.interrupted(); // Do not leak the intentional interrupt into the JUnit worker.
            realClient.shutdown();
        }
    }

    private static boolean hasFutureBridge(final String name, final Class<?>... parameterTypes) {
        return Arrays.stream(SpyMemcached.class.getDeclaredMethods())
                .anyMatch(method -> method.isBridge() && method.getName().equals(name) && method.getReturnType() == Future.class
                        && Arrays.equals(method.getParameterTypes(), parameterTypes));
    }

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
    public void test_constructor_with_huge_timeout_operationsStillWork() {
        final SpyMemcached<Object> local = new SpyMemcached<>(SERVER_URL, Long.MAX_VALUE);
        try {
            assertTrue(local.put("huge-timeout-key", "v", 60_000));
            assertEquals("v", local.get("huge-timeout-key"));
        } finally {
            local.disconnect();
        }
    }
}
