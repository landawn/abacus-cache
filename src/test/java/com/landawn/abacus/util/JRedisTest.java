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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.landawn.abacus.cache.JRedis;
import com.landawn.abacus.parser.KryoParser;
import com.landawn.abacus.parser.ParserFactory;

import redis.clients.jedis.BinaryShardedJedis;
import redis.clients.jedis.Jedis;

/**
 * Mock-based unit tests for {@link JRedis}.
 *
 * <p>The previous version of this test required a real Redis server and was skipped
 * whenever the server was unreachable, which meant it never ran in CI. This rewrite
 * intercepts the construction of {@link BinaryShardedJedis} (which would otherwise
 * open a real socket) and substitutes a Mockito mock, so the JRedis dispatch logic
 * (key encoding, value encoding/decoding, TTL handling, null reply handling) can be
 * exercised without network I/O.
 */
@Tag("2025")
public class JRedisTest {

    private static final KryoParser KRYO = ParserFactory.createKryoParser();

    private MockedConstruction<BinaryShardedJedis> ctorIntercept;
    private JRedis<Object> cache;
    private BinaryShardedJedis mockJedis;

    @BeforeEach
    public void setUp() throws Exception {
        // mockConstruction intercepts `new BinaryShardedJedis(...)` and returns a mock
        // instead of opening a real socket connection.
        ctorIntercept = Mockito.mockConstruction(BinaryShardedJedis.class);
        cache = new JRedis<>("localhost:6379");
        mockJedis = (BinaryShardedJedis) readField(cache, "jedis");
        assertNotNull(mockJedis, "mocked jedis client should have been injected");
    }

    @AfterEach
    public void tearDown() {
        if (ctorIntercept != null) {
            ctorIntercept.close();
        }
    }

    @Test
    public void test_get_returns_decoded_value() {
        Account account = createAccount();
        byte[] encoded = KRYO.encode(account);
        when(mockJedis.get(utf8("user:1"))).thenReturn(encoded);

        Object result = cache.get("user:1");

        assertNotNull(result);
        assertTrue(result instanceof Account);
        assertEquals(account.getFirstName(), ((Account) result).getFirstName());
        verify(mockJedis).get(utf8("user:1"));
    }

    @Test
    public void test_get_returns_null_when_key_missing() {
        when(mockJedis.get(any(byte[].class))).thenReturn(null);

        assertNull(cache.get("missing"));
    }

    @Test
    public void test_get_returns_null_for_empty_byte_array() {
        when(mockJedis.get(any(byte[].class))).thenReturn(new byte[0]);

        assertNull(cache.get("empty"));
    }

    @Test
    public void test_set_with_ttl_uses_setex() {
        when(mockJedis.setex(any(byte[].class), anyInt(), any(byte[].class))).thenReturn("OK");

        Account account = createAccount();
        boolean ok = cache.set("user:1", account, 60_000); // 60 seconds

        assertTrue(ok);
        // 60_000 ms should be 60 seconds
        verify(mockJedis).setex(eq(utf8("user:1")), eq(60), any(byte[].class));
        verify(mockJedis, never()).set(any(byte[].class), any(byte[].class));
    }

    @Test
    public void test_set_without_ttl_uses_set() {
        when(mockJedis.set(any(byte[].class), any(byte[].class))).thenReturn("OK");

        boolean ok = cache.set("forever", "value", 0);

        assertTrue(ok);
        verify(mockJedis).set(eq(utf8("forever")), any(byte[].class));
        verify(mockJedis, never()).setex(any(byte[].class), anyInt(), any(byte[].class));
    }

    @Test
    public void test_set_returns_false_when_server_does_not_say_OK() {
        when(mockJedis.setex(any(byte[].class), anyInt(), any(byte[].class))).thenReturn("NOT_OK");

        assertFalse(cache.set("k", "v", 60_000));
    }

    @Test
    public void test_set_null_value_encodes_as_empty_byte_array() {
        when(mockJedis.setex(any(byte[].class), anyInt(), any(byte[].class))).thenReturn("OK");

        cache.set("k", null, 60_000);

        verify(mockJedis).setex(eq(utf8("k")), anyInt(), eq(new byte[0]));
    }

    @Test
    public void test_delete_calls_del_and_returns_true() {
        when(mockJedis.del(any(byte[].class))).thenReturn(1L);

        assertTrue(cache.delete("k"));
        verify(mockJedis).del(utf8("k"));
    }

    /**
     * Regression coverage for the {@code delete()} return value defect. Previously the method
     * returned {@code true} unconditionally on a successful DEL call, conflating "key existed and
     * was removed" with "key did not exist". The fix returns {@code true} only when Redis reports
     * that at least one key was actually removed (DEL > 0), matching the documented Redis semantics.
     */
    @Test
    public void test_delete_returns_false_when_key_did_not_exist() {
        when(mockJedis.del(any(byte[].class))).thenReturn(0L);

        assertFalse(cache.delete("missing"));
        verify(mockJedis).del(utf8("missing"));
    }

    @Test
    public void test_delete_returns_false_on_null_reply() {
        when(mockJedis.del(any(byte[].class))).thenReturn(null);

        assertFalse(cache.delete("k"));
    }

    @Test
    public void test_delete_returns_true_when_multiple_removed() {
        // A single-key DEL would report 1; just verify any positive count maps to true.
        when(mockJedis.del(any(byte[].class))).thenReturn(2L);

        assertTrue(cache.delete("k"));
    }

    @Test
    public void test_incr_returns_zero_for_null_reply() {
        when(mockJedis.incr(any(byte[].class))).thenReturn(null);

        assertEquals(0L, cache.incr("counter"));
    }

    @Test
    public void test_incr_returns_server_value() {
        when(mockJedis.incr(utf8("counter"))).thenReturn(42L);

        assertEquals(42L, cache.incr("counter"));
    }

    @Test
    public void test_incr_with_delta_calls_incrBy() {
        when(mockJedis.incrBy(utf8("counter"), 5L)).thenReturn(5L);

        assertEquals(5L, cache.incr("counter", 5));
        verify(mockJedis).incrBy(utf8("counter"), 5L);
    }

    @Test
    public void test_incr_with_delta_rejects_negative_delta() {
        assertThrows(IllegalArgumentException.class, () -> cache.incr("counter", -1));
        verify(mockJedis, never()).incrBy(any(byte[].class), eq(-1L));
    }

    @Test
    public void test_decr_returns_zero_for_null_reply() {
        when(mockJedis.decr(any(byte[].class))).thenReturn(null);

        assertEquals(0L, cache.decr("counter"));
    }

    @Test
    public void test_decr_returns_negative_when_redis_does() {
        when(mockJedis.decr(utf8("counter"))).thenReturn(-1L);

        assertEquals(-1L, cache.decr("counter"));
    }

    @Test
    public void test_decr_with_delta_calls_decrBy() {
        when(mockJedis.decrBy(utf8("counter"), 3L)).thenReturn(-3L);

        assertEquals(-3L, cache.decr("counter", 3));
        verify(mockJedis).decrBy(utf8("counter"), 3L);
    }

    @Test
    public void test_decr_with_delta_rejects_negative_delta() {
        assertThrows(IllegalArgumentException.class, () -> cache.decr("counter", -1));
        verify(mockJedis, never()).decrBy(any(byte[].class), eq(-1L));
    }

    @Test
    public void test_flushAll_iterates_all_shards() {
        Jedis shard1 = mock(Jedis.class);
        Jedis shard2 = mock(Jedis.class);
        when(mockJedis.getAllShards()).thenReturn(Arrays.asList(shard1, shard2));

        cache.flushAll();

        verify(shard1).flushAll();
        verify(shard2).flushAll();
    }

    @Test
    public void test_flushAll_continues_when_one_shard_throws() {
        Jedis shard1 = mock(Jedis.class);
        Jedis shard2 = mock(Jedis.class);
        when(mockJedis.getAllShards()).thenReturn(Arrays.asList(shard1, shard2));
        when(shard1.flushAll()).thenThrow(new RuntimeException("shard 1 down"));
        when(shard2.flushAll()).thenReturn("OK");

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> cache.flushAll());
        assertEquals("shard 1 down", thrown.getMessage());
        // shard2 must still have been called even though shard1 threw
        verify(shard2).flushAll();
    }

    @Test
    public void test_flushAll_tolerates_null_shards_collection() {
        when(mockJedis.getAllShards()).thenReturn(null);

        cache.flushAll();
        // no exception
    }

    @Test
    public void test_flushAll_tolerates_null_shard_element() {
        Jedis shard = mock(Jedis.class);
        when(mockJedis.getAllShards()).thenReturn(Arrays.asList(null, shard));

        cache.flushAll();

        verify(shard).flushAll();
    }

    @Test
    public void test_disconnect_is_idempotent() {
        cache.disconnect();
        cache.disconnect(); // safe to call again

        // The underlying client should only be disconnected once.
        verify(mockJedis, times(1)).disconnect();
    }

    @Test
    public void test_constructor_rejects_invalid_timeout() {
        assertThrows(IllegalArgumentException.class, () -> new JRedis<>("localhost:6379", 0L));
        assertThrows(IllegalArgumentException.class, () -> new JRedis<>("localhost:6379", -1L));
        assertThrows(IllegalArgumentException.class, () -> new JRedis<>("localhost:6379", (long) Integer.MAX_VALUE + 1L));
    }

    @Test
    public void test_set_round_trip_uses_kryo_encoding() {
        // Confirm the byte payload SET sends is what Kryo would produce, so callers reading
        // the same key from a real Redis would get an equal Account back.
        when(mockJedis.setex(any(byte[].class), anyInt(), any(byte[].class))).thenReturn("OK");

        Account account = createAccount();
        cache.set("k", account, 30_000);

        org.mockito.ArgumentCaptor<byte[]> payload = org.mockito.ArgumentCaptor.forClass(byte[].class);
        verify(mockJedis).setex(eq(utf8("k")), eq(30), payload.capture());

        Object roundTripped = KRYO.decode(payload.getValue());
        assertTrue(roundTripped instanceof Account);
        assertEquals(account.getFirstName(), ((Account) roundTripped).getFirstName());
        assertEquals(account.getLastName(), ((Account) roundTripped).getLastName());
    }

    private static Object readField(Object target, String fieldName) throws Exception {
        Field f = target.getClass().getDeclaredField(fieldName);
        f.setAccessible(true);
        return f.get(target);
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static Account createAccount() {
        Account a = new Account();
        a.setGui(Strings.uuid());
        a.setFirstName("firstName");
        a.setMiddleName("MN");
        a.setLastName("lastName");
        a.setEmailAddress(a.getGui() + "@email");
        a.setBirthDate(Dates.currentTimestamp());
        return a;
    }

    @Test
    public void test_serverUrl_returns_constructor_value() {
        assertEquals("localhost:6379", cache.serverUrl());
    }

    @Test
    public void test_constructor_with_default_timeout_does_not_throw() {
        // Default-timeout constructor (single-arg) is exercised here, reusing the BeforeEach mockConstruction.
        final JRedis<Object> c = new JRedis<>("localhost:6379");
        assertNotNull(c);
    }

    @Test
    public void test_constructor_rejects_invalid_server_url() {
        // No host:port in the URL → AddrUtil returns empty → IAE.
        assertThrows(IllegalArgumentException.class, () -> new JRedis<>(""));
    }

    @Test
    public void test_getKeyBytes_EdgeCase_NullKey() throws Exception {
        // Reach the protected getKeyBytes via reflection so we can verify the contract.
        final java.lang.reflect.Method m = JRedis.class.getDeclaredMethod("getKeyBytes", String.class);
        m.setAccessible(true);
        try {
            m.invoke(cache, (Object) null);
            org.junit.jupiter.api.Assertions.fail("expected IllegalArgumentException");
        } catch (final java.lang.reflect.InvocationTargetException ite) {
            org.junit.jupiter.api.Assertions.assertTrue(ite.getCause() instanceof IllegalArgumentException);
        }
    }
}
