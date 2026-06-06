/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.landawn.abacus.cache.SpyMemcached;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

/**
 * Mock-based unit tests for {@link SpyMemcached}.
 *
 * <p>The previous version of this test required a real Memcached server (localhost:11211)
 * and was skipped whenever the server was unreachable. This rewrite intercepts the
 * construction of {@link MemcachedClient} and substitutes a Mockito mock so the
 * SpyMemcached dispatch logic (key validation, TTL ms→s conversion, future blocking,
 * exception conversion) can be exercised without network I/O.
 */
@Tag("2025")
public class SpyMemcachedTest {

    private MockedConstruction<MemcachedClient> ctorIntercept;
    private SpyMemcached<Object> cache;
    private MemcachedClient mockMc;

    @BeforeEach
    public void setUp() throws Exception {
        ctorIntercept = Mockito.mockConstruction(MemcachedClient.class);
        cache = new SpyMemcached<>("localhost:11211");
        mockMc = (MemcachedClient) readField(cache, "mc");
        assertNotNull(mockMc, "mocked memcached client should have been injected");
    }

    @AfterEach
    public void tearDown() {
        if (ctorIntercept != null) {
            ctorIntercept.close();
        }
    }

    @Test
    public void test_get_returns_value() {
        when(mockMc.get("user:1")).thenReturn("hello");

        Object result = cache.get("user:1");

        assertEquals("hello", result);
        verify(mockMc).get("user:1");
    }

    @Test
    public void test_get_returns_null_for_missing_key() {
        when(mockMc.get("missing")).thenReturn(null);

        assertNull(cache.get("missing"));
    }

    @Test
    public void test_get_rejects_null_key() {
        assertThrows(IllegalArgumentException.class, () -> cache.get(null));
    }

    @Test
    public void test_set_converts_millis_to_seconds_and_returns_true() {
        OperationFuture<Boolean> ok = immediateBooleanFuture(true);
        when(mockMc.set(eq("k"), anyInt(), any())).thenReturn(ok);

        boolean result = cache.set("k", "value", 60_000); // 60 seconds

        assertTrue(result);
        verify(mockMc).set("k", 60, "value");
    }

    @Test
    public void test_set_rounds_up_partial_seconds() {
        OperationFuture<Boolean> ok = immediateBooleanFuture(true);
        when(mockMc.set(eq("k"), anyInt(), any())).thenReturn(ok);

        cache.set("k", "value", 1_500); // 1.5s -> rounds up to 2s

        verify(mockMc).set("k", 2, "value");
    }

    @Test
    public void test_set_long_ttl_uses_memcached_absolute_expiration() {
        OperationFuture<Boolean> ok = immediateBooleanFuture(true);
        when(mockMc.set(eq("k"), anyInt(), any())).thenReturn(ok);

        final long liveTime = 31L * 24 * 60 * 60 * 1000;
        final long expectedEarliest = System.currentTimeMillis() / 1000L + (liveTime / 1000);

        assertTrue(cache.set("k", "value", liveTime));

        final long expectedLatest = System.currentTimeMillis() / 1000L + (liveTime / 1000);
        final ArgumentCaptor<Integer> expirationCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(mockMc).set(eq("k"), expirationCaptor.capture(), eq("value"));

        assertTrue(expirationCaptor.getValue() > 30 * 24 * 60 * 60);
        assertTrue(expirationCaptor.getValue() >= expectedEarliest);
        assertTrue(expirationCaptor.getValue() <= expectedLatest);
    }

    @Test
    public void test_set_returns_false_when_server_says_no() {
        OperationFuture<Boolean> no = immediateBooleanFuture(false);
        when(mockMc.set(eq("k"), anyInt(), any())).thenReturn(no);

        assertFalse(cache.set("k", "value", 60_000));
    }

    @Test
    public void test_set_rejects_null_key() {
        assertThrows(IllegalArgumentException.class, () -> cache.set(null, "v", 60_000));
    }

    @Test
    public void test_add_forwards_to_underlying_client() {
        OperationFuture<Boolean> ok = immediateBooleanFuture(true);
        when(mockMc.add(eq("k"), anyInt(), any())).thenReturn(ok);

        assertTrue(cache.add("k", "v", 60_000));
        verify(mockMc).add("k", 60, "v");
    }

    @Test
    public void test_replace_forwards_to_underlying_client() {
        OperationFuture<Boolean> ok = immediateBooleanFuture(true);
        when(mockMc.replace(eq("k"), anyInt(), any())).thenReturn(ok);

        assertTrue(cache.replace("k", "v", 60_000));
        verify(mockMc).replace("k", 60, "v");
    }

    @Test
    public void test_delete_forwards_and_returns_result() {
        OperationFuture<Boolean> ok = immediateBooleanFuture(true);
        when(mockMc.delete("k")).thenReturn(ok);

        assertTrue(cache.delete("k"));
        verify(mockMc).delete("k");
    }

    @Test
    public void test_delete_rejects_null_key() {
        assertThrows(IllegalArgumentException.class, () -> cache.delete(null));
    }

    @Test
    public void test_incr_delegates_to_mc_incr() {
        when(mockMc.incr("counter", 1)).thenReturn(42L);

        assertEquals(42L, cache.incr("counter"));
        verify(mockMc).incr("counter", 1);
    }

    @Test
    public void test_incr_with_delta_delegates() {
        when(mockMc.incr("counter", 5)).thenReturn(50L);

        assertEquals(50L, cache.incr("counter", 5));
        verify(mockMc).incr("counter", 5);
    }

    @Test
    public void test_incr_rejects_negative_delta() {
        assertThrows(IllegalArgumentException.class, () -> cache.incr("counter", -1));
    }

    @Test
    public void test_incr_with_default_value() {
        when(mockMc.incr("counter", 1, 0L, -1)).thenReturn(1L);

        assertEquals(1L, cache.incr("counter", 1, 0L));
        verify(mockMc).incr("counter", 1, 0L, -1);
    }

    @Test
    public void test_decr_delegates_to_mc_decr() {
        when(mockMc.decr("counter", 1)).thenReturn(0L);

        assertEquals(0L, cache.decr("counter"));
        verify(mockMc).decr("counter", 1);
    }

    @Test
    public void test_decr_with_delta_delegates() {
        when(mockMc.decr("counter", 3)).thenReturn(7L);

        assertEquals(7L, cache.decr("counter", 3));
        verify(mockMc).decr("counter", 3);
    }

    @Test
    public void test_decr_rejects_negative_delta() {
        assertThrows(IllegalArgumentException.class, () -> cache.decr("counter", -1));
    }

    @Test
    public void test_getBulk_with_varargs() {
        Map<String, Object> expected = new HashMap<>();
        expected.put("a", 1);
        expected.put("b", 2);
        when(mockMc.getBulk(any(String[].class))).thenReturn((Map) expected);

        Map<String, Object> got = cache.getBulk("a", "b");

        assertEquals(expected, got);
    }

    @Test
    public void test_getBulk_with_collection() {
        Map<String, Object> expected = new HashMap<>();
        expected.put("a", 1);
        when(mockMc.getBulk(any(java.util.Collection.class))).thenReturn((Map) expected);

        List<String> keys = Arrays.asList("a");
        Map<String, Object> got = cache.getBulk(keys);

        assertEquals(expected, got);
    }

    @Test
    public void test_getBulk_rejects_null_keys() {
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk((String[]) null));
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk("a", null));
    }

    @Test
    public void test_asyncGet_returns_future() {
        @SuppressWarnings("unchecked")
        Future<Object> mockFuture = mock(Future.class);
        when(mockMc.asyncGet("k")).thenReturn((net.spy.memcached.internal.GetFuture) null);
        // Use the underlying method directly: spymemcached returns GetFuture, we wrap to Future<T>.
        // Easier: just verify the call happens.
        try {
            cache.asyncGet("k");
        } catch (NullPointerException ignore) {
            // OK: returning null from the mock is fine; we only care it dispatched correctly.
        }
        verify(mockMc).asyncGet("k");
    }

    @Test
    public void test_asyncGet_rejects_null_key() {
        assertThrows(IllegalArgumentException.class, () -> cache.asyncGet(null));
    }

    @Test
    public void test_flushAll_delegates() {
        OperationFuture<Boolean> ok = immediateBooleanFuture(true);
        when(mockMc.flush()).thenReturn(ok);

        cache.flushAll();

        verify(mockMc).flush();
    }

    @Test
    public void test_flushAll_throws_when_server_returns_false() {
        OperationFuture<Boolean> no = immediateBooleanFuture(false);
        when(mockMc.flush()).thenReturn(no);

        assertThrows(IllegalStateException.class, cache::flushAll);
    }

    @Test
    public void test_disconnect_calls_shutdown_once() {
        cache.disconnect();
        cache.disconnect(); // should be idempotent

        verify(mockMc, times(1)).shutdown();
    }

    @Test
    public void test_constructor_rejects_invalid_timeout() {
        assertThrows(IllegalArgumentException.class, () -> new SpyMemcached<>("localhost:11211", 0L));
        assertThrows(IllegalArgumentException.class, () -> new SpyMemcached<>("localhost:11211", -1L));
    }

    @Test
    public void test_resultOf_rejects_null_future() {
        // Use a small subclass test by triggering the path indirectly: set() with a mock returning null.
        when(mockMc.set(eq("k"), anyInt(), any())).thenReturn(null);
        assertThrows(IllegalArgumentException.class, () -> cache.set("k", "v", 60_000));
    }

    @Test
    public void test_serverUrl_returns_constructor_value() {
        assertEquals("localhost:11211", cache.serverUrl());
    }

    @Test
    public void test_asyncSet_forwards() {
        OperationFuture<Boolean> ok = immediateBooleanFuture(true);
        when(mockMc.set(eq("k"), anyInt(), any())).thenReturn(ok);
        assertNotNull(cache.asyncSet("k", "v", 60_000));
        verify(mockMc).set("k", 60, "v");
    }

    @Test
    public void test_asyncSet_EdgeCase_NullKey() {
        assertThrows(IllegalArgumentException.class, () -> cache.asyncSet(null, "v", 60_000));
    }

    @Test
    public void test_add_EdgeCase_NullKey() {
        assertThrows(IllegalArgumentException.class, () -> cache.add(null, "v", 60_000));
    }

    @Test
    public void test_asyncAdd_forwards() {
        OperationFuture<Boolean> ok = immediateBooleanFuture(true);
        when(mockMc.add(eq("k"), anyInt(), any())).thenReturn(ok);
        assertNotNull(cache.asyncAdd("k", "v", 60_000));
        verify(mockMc).add("k", 60, "v");
    }

    @Test
    public void test_asyncAdd_EdgeCase_NullKey() {
        assertThrows(IllegalArgumentException.class, () -> cache.asyncAdd(null, "v", 60_000));
    }

    @Test
    public void test_replace_EdgeCase_NullKey() {
        assertThrows(IllegalArgumentException.class, () -> cache.replace(null, "v", 60_000));
    }

    @Test
    public void test_asyncReplace_forwards() {
        OperationFuture<Boolean> ok = immediateBooleanFuture(true);
        when(mockMc.replace(eq("k"), anyInt(), any())).thenReturn(ok);
        assertNotNull(cache.asyncReplace("k", "v", 60_000));
        verify(mockMc).replace("k", 60, "v");
    }

    @Test
    public void test_asyncReplace_EdgeCase_NullKey() {
        assertThrows(IllegalArgumentException.class, () -> cache.asyncReplace(null, "v", 60_000));
    }

    @Test
    public void test_asyncDelete_forwards() {
        OperationFuture<Boolean> ok = immediateBooleanFuture(true);
        when(mockMc.delete("k")).thenReturn(ok);
        assertNotNull(cache.asyncDelete("k"));
        verify(mockMc).delete("k");
    }

    @Test
    public void test_asyncDelete_EdgeCase_NullKey() {
        assertThrows(IllegalArgumentException.class, () -> cache.asyncDelete(null));
    }

    @Test
    public void test_incr_EdgeCase_NullKey() {
        assertThrows(IllegalArgumentException.class, () -> cache.incr(null));
        assertThrows(IllegalArgumentException.class, () -> cache.incr(null, 1));
    }

    @Test
    public void test_incr_with_default_value_and_liveTime() {
        when(mockMc.incr("counter", 1, 0L, 60)).thenReturn(1L);
        assertEquals(1L, cache.incr("counter", 1, 0L, 60_000L));
        verify(mockMc).incr("counter", 1, 0L, 60);
    }

    @Test
    public void test_incr_with_default_value_EdgeCase_NullKeyAndNegativeDelta() {
        assertThrows(IllegalArgumentException.class, () -> cache.incr(null, 1, 0L));
        assertThrows(IllegalArgumentException.class, () -> cache.incr("k", -1, 0L));
        assertThrows(IllegalArgumentException.class, () -> cache.incr(null, 1, 0L, 1000L));
        assertThrows(IllegalArgumentException.class, () -> cache.incr("k", -1, 0L, 1000L));
    }

    @Test
    public void test_decr_EdgeCase_NullKey() {
        assertThrows(IllegalArgumentException.class, () -> cache.decr(null));
        assertThrows(IllegalArgumentException.class, () -> cache.decr(null, 1));
    }

    @Test
    public void test_decr_with_default_value() {
        when(mockMc.decr("counter", 1, 100L, -1)).thenReturn(99L);
        assertEquals(99L, cache.decr("counter", 1, 100L));
        verify(mockMc).decr("counter", 1, 100L, -1);
    }

    @Test
    public void test_decr_with_default_value_and_liveTime() {
        when(mockMc.decr("counter", 1, 100L, 60)).thenReturn(99L);
        assertEquals(99L, cache.decr("counter", 1, 100L, 60_000L));
        verify(mockMc).decr("counter", 1, 100L, 60);
    }

    @Test
    public void test_decr_with_default_value_EdgeCase_NullKeyAndNegativeDelta() {
        assertThrows(IllegalArgumentException.class, () -> cache.decr(null, 1, 0L));
        assertThrows(IllegalArgumentException.class, () -> cache.decr("k", -1, 0L));
        assertThrows(IllegalArgumentException.class, () -> cache.decr(null, 1, 0L, 1000L));
        assertThrows(IllegalArgumentException.class, () -> cache.decr("k", -1, 0L, 1000L));
    }

    @Test
    public void test_getBulk_collection_EdgeCase_NullKeys() {
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk((java.util.Collection<String>) null));
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk(Arrays.asList("a", null)));
    }

    @Test
    public void test_asyncGetBulk_varargs_forwards() {
        when(mockMc.asyncGetBulk(any(String[].class))).thenReturn((net.spy.memcached.internal.BulkFuture) null);
        try {
            cache.asyncGetBulk("a", "b");
        } catch (NullPointerException ignore) {
            // The mock returns null; we only care the dispatch happens.
        }
        verify(mockMc).asyncGetBulk(any(String[].class));
    }

    @Test
    public void test_asyncGetBulk_collection_forwards() {
        when(mockMc.asyncGetBulk(any(java.util.Collection.class))).thenReturn((net.spy.memcached.internal.BulkFuture) null);
        try {
            cache.asyncGetBulk(Arrays.asList("a"));
        } catch (NullPointerException ignore) {
            // ok
        }
        verify(mockMc).asyncGetBulk(any(java.util.Collection.class));
    }

    @Test
    public void test_flushAll_with_delay() {
        OperationFuture<Boolean> ok = immediateBooleanFuture(true);
        when(mockMc.flush(anyInt())).thenReturn(ok);
        assertTrue(cache.flushAll(5_000));
        verify(mockMc).flush(5);
    }

    @Test
    public void test_asyncFlushAll_forwards() {
        OperationFuture<Boolean> ok = immediateBooleanFuture(true);
        when(mockMc.flush()).thenReturn(ok);
        assertNotNull(cache.asyncFlushAll());
        verify(mockMc).flush();
    }

    @Test
    public void test_asyncFlushAll_with_delay_forwards() {
        OperationFuture<Boolean> ok = immediateBooleanFuture(true);
        when(mockMc.flush(anyInt())).thenReturn(ok);
        assertNotNull(cache.asyncFlushAll(10_000));
        verify(mockMc).flush(10);
    }

    @Test
    public void test_disconnect_with_timeout() {
        cache.disconnect(5000);
        verify(mockMc).shutdown(5000, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    @Test
    public void test_disconnect_with_timeout_EdgeCase_Negative() {
        assertThrows(IllegalArgumentException.class, () -> cache.disconnect(-1));
    }

    private static Object readField(Object target, String fieldName) throws Exception {
        Field f = target.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        return f.get(target);
    }

    /**
     * Returns an immediately-complete {@link OperationFuture}-style future yielding the
     * supplied value. For test purposes we use a simple in-memory {@link Future} via a
     * mock so resultOf() returns the value without blocking.
     */
    @SuppressWarnings("unchecked")
    private static <T> OperationFuture<T> immediateBooleanFuture(T value) {
        OperationFuture<T> f = mock(OperationFuture.class);
        try {
            when(f.get(anyLong(), any())).thenReturn(value);
            when(f.get()).thenReturn(value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return f;
    }
}
