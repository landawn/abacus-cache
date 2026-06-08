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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
 * operate correctly on counters created through the increment-with-default seeding path, and only for
 * the initial seed — attempting to increment a Kryo-encoded value yields a server-side
 * "cannot increment or decrement non-numeric value" error. The counter tests below stay within the
 * behavior the real server actually supports (missing-key sentinel and first-seed).
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
        cache.set("user:1", "hello", 60_000);
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
        assertTrue(cache.set("k", "value", 60_000));
        assertEquals("value", cache.get("k"));
    }

    @Test
    public void test_set_overwrites_existing_value() {
        cache.set("k", "v1", 60_000);
        assertTrue(cache.set("k", "v2", 60_000));
        assertEquals("v2", cache.get("k"));
    }

    @Test
    public void test_set_null_value_is_stored_and_reads_back_null() {
        // A null value is accepted (memcached stores the empty Kryo payload); get maps it back to null.
        assertTrue(cache.set("maybe-null", null, 60_000));
        assertNull(cache.get("maybe-null"));
    }

    @Test
    public void test_set_rejects_null_key() {
        assertThrows(IllegalArgumentException.class, () -> cache.set(null, "v", 60_000));
    }

    @Test
    public void test_set_long_ttl_is_stored_and_retrievable() {
        // A TTL beyond 30 days is converted to an absolute Unix expiration timestamp. The value must be
        // immediately retrievable (i.e. NOT stored already-expired, which is what a botched conversion
        // would produce).
        final long liveTime = 31L * 24 * 60 * 60 * 1000; // 31 days
        assertTrue(cache.set("k", "value", liveTime));
        assertEquals("value", cache.get("k"));
    }

    @Test
    public void test_set_short_ttl_expires() throws Exception {
        // 1_000 ms -> 1 s. The value is present immediately and gone shortly after the TTL elapses,
        // confirming the ms->s conversion is honored end-to-end by the real server.
        cache.set("ttl", "v", 1_000);
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
        assertThrows(IllegalArgumentException.class, () -> cache.set("k", "v", 2_000_000_000_000L));
    }

    @Test
    public void test_asyncSet_then_asyncGet() throws Exception {
        assertTrue(cache.asyncSet("k", "v", 60_000).get());
        assertEquals("v", cache.asyncGet("k").get());
    }

    @Test
    public void test_asyncSet_rejects_null_key() {
        assertThrows(IllegalArgumentException.class, () -> cache.asyncSet(null, "v", 60_000));
    }

    @Test
    public void test_asyncGet_rejects_null_key() {
        assertThrows(IllegalArgumentException.class, () -> cache.asyncGet(null));
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

        cache.set("k", "v1", 60_000);
        assertTrue(cache.replace("k", "v2", 60_000));
        assertEquals("v2", cache.get("k"));
    }

    @Test
    public void test_replace_rejects_null_key() {
        assertThrows(IllegalArgumentException.class, () -> cache.replace(null, "v", 60_000));
    }

    @Test
    public void test_asyncReplace_forwards() throws Exception {
        cache.set("k", "v1", 60_000);
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
        cache.set("k", "v", 60_000);
        assertTrue(cache.delete("k"));
        assertNull(cache.get("k"));
    }

    @Test
    public void test_delete_missing_returns_false() {
        assertFalse(cache.delete("never-existed"));
    }

    @Test
    public void test_delete_rejects_null_key() {
        assertThrows(IllegalArgumentException.class, () -> cache.delete(null));
    }

    @Test
    public void test_asyncDelete_forwards() throws Exception {
        cache.set("k", "v", 60_000);
        assertTrue(cache.asyncDelete("k").get());
        assertNull(cache.get("k"));
    }

    @Test
    public void test_asyncDelete_rejects_null_key() {
        assertThrows(IllegalArgumentException.class, () -> cache.asyncDelete(null));
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

    @Test
    public void test_incr_with_default_value_validation() {
        assertThrows(IllegalArgumentException.class, () -> cache.incr(null, 1, 0L));
        assertThrows(IllegalArgumentException.class, () -> cache.incr("k", -1, 0L));
        assertThrows(IllegalArgumentException.class, () -> cache.incr(null, 1, 0L, 1000L));
        assertThrows(IllegalArgumentException.class, () -> cache.incr("k", -1, 0L, 1000L));
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
        cache.set("a", 1, 60_000);
        cache.set("b", 2, 60_000);

        final Map<String, Object> got = cache.getBulk("a", "b", "missing");

        assertEquals(2, got.size());
        assertEquals(1, got.get("a"));
        assertEquals(2, got.get("b"));
        assertFalse(got.containsKey("missing"));
    }

    @Test
    public void test_getBulk_collection_returns_only_found_keys() {
        cache.set("a", 1, 60_000);

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

    @Test
    public void test_asyncGetBulk_varargs_forwards() throws Exception {
        cache.set("a", 1, 60_000);
        cache.set("b", 2, 60_000);

        final Map<String, Object> got = cache.asyncGetBulk("a", "b").get();

        assertEquals(2, got.size());
        assertEquals(1, got.get("a"));
    }

    @Test
    public void test_asyncGetBulk_collection_forwards() throws Exception {
        cache.set("a", 1, 60_000);

        final Map<String, Object> got = cache.asyncGetBulk(Arrays.asList("a")).get();

        assertEquals(1, got.size());
        assertEquals(1, got.get("a"));
    }

    // --- flushAll ------------------------------------------------------------------------------

    @Test
    public void test_flushAll_clears_all_keys() {
        cache.set("a", 1, 60_000);
        cache.set("b", 2, 60_000);

        cache.flushAll();

        assertNull(cache.get("a"));
        assertNull(cache.get("b"));
    }

    @Test
    public void test_flushAll_with_immediate_delay_clears() {
        cache.set("a", 1, 60_000);

        assertTrue(cache.flushAll(0));

        assertNull(cache.get("a"));
    }

    @Test
    public void test_flushAll_large_delay_is_scheduled_not_immediate() {
        // memcached's `flush_all <delay>` interprets its argument as a RELATIVE number of seconds and
        // never as an absolute timestamp (the >30-day rewrite that applies to set/add/replace must NOT
        // apply here). A 40-day delay therefore schedules a far-future flush, so the value is still
        // present right after the call.
        cache.set("a", 1, 60_000);

        assertTrue(cache.flushAll(3_456_000_000L)); // 40 days

        assertEquals(1, cache.get("a"));
    }

    @Test
    public void test_asyncFlushAll_forwards() throws Exception {
        cache.set("a", 1, 60_000);

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

        // After shutdown the client is unusable: a further operation fails fast.
        assertThrows(IllegalStateException.class, () -> local.get("k"));
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
}
