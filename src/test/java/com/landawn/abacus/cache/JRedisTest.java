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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.landawn.abacus.parser.KryoParser;
import com.landawn.abacus.parser.ParserFactory;
import com.landawn.abacus.util.Dates;
import com.landawn.abacus.util.Strings;

import redis.clients.jedis.RedisClient;
import redis.clients.jedis.params.SetParams;

/**
 * Mock-based unit tests for {@link JRedis}.
 *
 * <p>The previous version of this test required a real Redis server and was skipped
 * whenever the server was unreachable, which meant it never ran in CI. This rewrite
 * intercepts the construction of {@link RedisClient} (which would otherwise open a real
 * socket on first use) and substitutes a Mockito mock, so the JRedis dispatch logic (key
 * encoding, value encoding/decoding, TTL handling, shard routing) can be exercised without
 * any network I/O.
 *
 * <p>After the Jedis 3.x → 7.x migration, {@code JRedis} no longer wraps the removed
 * {@code BinaryShardedJedis}; instead it holds one {@link RedisClient} per shard and invokes
 * commands directly on it ({@code RedisClient} pools connections internally). Each constructed
 * client is captured (in construction order) in {@link #shards} so individual shards can be
 * stubbed and verified.
 */
@Tag("2025")
public class JRedisTest {

    private static final KryoParser KRYO = ParserFactory.createKryoParser();

    private MockedConstruction<RedisClient> ctorIntercept;
    /** One mocked RedisClient per constructed shard, in construction order. */
    private List<RedisClient> shards;
    private JRedis<Object> cache;
    /** The single shard's client for the default single-node {@link #cache}. */
    private RedisClient mockJedis;

    @BeforeEach
    public void setUp() {
        shards = new ArrayList<>();
        // mockConstruction intercepts `new RedisClient(...)` so no real socket is ever opened.
        // Commands are invoked directly on the (mocked) pooled client.
        ctorIntercept = Mockito.mockConstruction(RedisClient.class, (poolMock, context) -> shards.add(poolMock));
        cache = new JRedis<>("localhost:6379");
        assertEquals(1, shards.size(), "single-node URL should build exactly one shard");
        mockJedis = shards.get(0);
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

    // ---- getBulk: previously JRedis inherited UnsupportedOperationException; now implemented (B1) ----

    @Test
    public void test_getBulk_varargs_returns_only_found_keys() {
        Account a1 = createAccount();
        when(mockJedis.get(utf8("user:1"))).thenReturn(KRYO.encode(a1));
        when(mockJedis.get(utf8("user:2"))).thenReturn(null); // missing key
        when(mockJedis.get(utf8("user:3"))).thenReturn(new byte[0]); // empty -> decoded as null

        Map<String, Object> result = cache.getBulk("user:1", "user:2", "user:3");

        assertEquals(1, result.size());
        assertTrue(result.containsKey("user:1"));
        assertFalse(result.containsKey("user:2"));
        assertFalse(result.containsKey("user:3"));
        assertTrue(result.get("user:1") instanceof Account);
    }

    @Test
    public void test_getBulk_collection_returns_only_found_keys() {
        Account a1 = createAccount();
        when(mockJedis.get(utf8("k1"))).thenReturn(KRYO.encode(a1));
        when(mockJedis.get(utf8("k2"))).thenReturn(null);

        Map<String, Object> result = cache.getBulk(List.of("k1", "k2"));

        assertEquals(1, result.size());
        assertTrue(result.containsKey("k1"));
    }

    @Test
    public void test_getBulk_empty_returns_empty_map() {
        assertTrue(cache.getBulk().isEmpty());
        assertTrue(cache.getBulk(List.of()).isEmpty());
    }

    @Test
    public void test_getBulk_rejects_null_keys() {
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk((String[]) null));
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk((List<String>) null));
    }

    @Test
    public void test_getBulk_rejects_null_element() {
        // Validation happens up-front, before any GET is issued.
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk("a", null, "c"));
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk(Arrays.asList("a", null)));
        verify(mockJedis, never()).get(any(byte[].class));
    }

    @Test
    public void test_set_with_ttl_uses_set_with_expiry() {
        when(mockJedis.set(any(byte[].class), any(byte[].class), any(SetParams.class))).thenReturn("OK");

        Account account = createAccount();
        boolean ok = cache.set("user:1", account, 60_000); // 60 seconds

        assertTrue(ok);
        // 60_000 ms should be SET ... PX 60000 (SetParams.equals compares the param content)
        verify(mockJedis).set(eq(utf8("user:1")), any(byte[].class), eq(SetParams.setParams().px(60_000L)));
        verify(mockJedis, never()).set(any(byte[].class), any(byte[].class));
    }

    /**
     * Regression test for the TTL-coarsening bug: {@code liveTime} was previously converted to whole
     * seconds ({@code SET ... EX}, rounding up), so a 300 ms TTL silently became a full second —
     * over 3x longer than requested for sub-second entries. The TTL is now passed through with
     * millisecond precision ({@code SET ... PX}).
     */
    @Test
    public void test_set_subSecondTtl_honoredWithMillisecondPrecision() {
        when(mockJedis.set(any(byte[].class), any(byte[].class), any(SetParams.class))).thenReturn("OK");

        assertTrue(cache.set("short-lived", "v", 300));

        verify(mockJedis).set(eq(utf8("short-lived")), any(byte[].class), eq(SetParams.setParams().px(300L)));
    }

    @Test
    public void test_set_without_ttl_uses_plain_set() {
        when(mockJedis.set(any(byte[].class), any(byte[].class))).thenReturn("OK");

        boolean ok = cache.set("forever", "value", 0);

        assertTrue(ok);
        verify(mockJedis).set(eq(utf8("forever")), any(byte[].class));
        verify(mockJedis, never()).set(any(byte[].class), any(byte[].class), any(SetParams.class));
    }

    @Test
    public void test_set_returns_false_when_server_does_not_say_OK() {
        when(mockJedis.set(any(byte[].class), any(byte[].class), any(SetParams.class))).thenReturn("NOT_OK");

        assertFalse(cache.set("k", "v", 60_000));
    }

    @Test
    public void test_set_null_value_encodes_as_empty_byte_array() {
        when(mockJedis.set(any(byte[].class), any(byte[].class), any(SetParams.class))).thenReturn("OK");

        cache.set("k", null, 60_000);

        verify(mockJedis).set(eq(utf8("k")), eq(new byte[0]), any(SetParams.class));
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
    public void test_delete_returns_true_when_multiple_removed() {
        // A single-key DEL would report 1; just verify any positive count maps to true.
        when(mockJedis.del(any(byte[].class))).thenReturn(2L);

        assertTrue(cache.delete("k"));
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
    public void test_flushAll_single_shard_calls_flushAll() {
        when(mockJedis.flushAll()).thenReturn("OK");

        cache.flushAll();

        verify(mockJedis).flushAll();
    }

    @Test
    public void test_flushAll_iterates_all_shards() {
        final JRedis<Object> sharded = newShardedCache("h1:6379,h2:6379");
        assertEquals(2, shards.size());
        when(shards.get(0).flushAll()).thenReturn("OK");
        when(shards.get(1).flushAll()).thenReturn("OK");

        sharded.flushAll();

        verify(shards.get(0)).flushAll();
        verify(shards.get(1)).flushAll();
    }

    @Test
    public void test_flushAll_continues_when_one_shard_throws() {
        final JRedis<Object> sharded = newShardedCache("h1:6379,h2:6379");
        when(shards.get(0).flushAll()).thenThrow(new RuntimeException("shard 1 down"));
        when(shards.get(1).flushAll()).thenReturn("OK");

        RuntimeException thrown = assertThrows(RuntimeException.class, sharded::flushAll);
        assertEquals("shard 1 down", thrown.getMessage());
        // shard2 must still have been flushed even though shard1 threw
        verify(shards.get(1)).flushAll();
    }

    // ---- shard routing ----

    @Test
    public void test_routing_same_key_is_consistent_across_calls() {
        final JRedis<Object> sharded = newShardedCache("h1:6379,h2:6379");
        when(shards.get(0).get(any(byte[].class))).thenReturn(null);
        when(shards.get(1).get(any(byte[].class))).thenReturn(null);

        sharded.get("same-key");
        sharded.get("same-key");

        int hits0 = invocationCount(shards.get(0), "get");
        int hits1 = invocationCount(shards.get(1), "get");

        // Both reads of the same key must land on exactly one shard (deterministic routing).
        assertEquals(2, hits0 + hits1);
        assertTrue(hits0 == 0 || hits1 == 0, "the same key must always route to the same shard");
    }

    @Test
    public void test_routing_distributes_keys_across_shards() {
        final JRedis<Object> sharded = newShardedCache("h1:6379,h2:6379");
        when(shards.get(0).get(any(byte[].class))).thenReturn(null);
        when(shards.get(1).get(any(byte[].class))).thenReturn(null);

        for (int i = 0; i < 100; i++) {
            sharded.get("key-" + i);
        }

        // CRC-32 over 100 distinct keys must exercise both shards.
        assertTrue(invocationCount(shards.get(0), "get") > 0);
        assertTrue(invocationCount(shards.get(1), "get") > 0);
    }

    @Test
    public void test_disconnect_is_idempotent() {
        cache.disconnect();
        cache.disconnect(); // safe to call again

        // The underlying shard client should only be closed once.
        verify(mockJedis, times(1)).close();
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
        when(mockJedis.set(any(byte[].class), any(byte[].class), any(SetParams.class))).thenReturn("OK");

        Account account = createAccount();
        cache.set("k", account, 30_000);

        org.mockito.ArgumentCaptor<byte[]> payload = org.mockito.ArgumentCaptor.forClass(byte[].class);
        verify(mockJedis).set(eq(utf8("k")), payload.capture(), eq(SetParams.setParams().px(30_000L)));

        Object roundTripped = KRYO.decode(payload.getValue());
        assertTrue(roundTripped instanceof Account);
        assertEquals(account.getFirstName(), ((Account) roundTripped).getFirstName());
        assertEquals(account.getLastName(), ((Account) roundTripped).getLastName());
    }

    /**
     * Builds a multi-node {@link JRedis} and resets {@link #shards} first so that
     * {@code shards.get(0)} / {@code shards.get(1)} line up with the new cache's shard order.
     */
    private JRedis<Object> newShardedCache(final String serverUrl) {
        shards.clear();
        final JRedis<Object> sharded = new JRedis<>(serverUrl);
        assertEquals(serverUrl.split(",").length, shards.size());
        return sharded;
    }

    private static int invocationCount(final Object mock, final String methodName) {
        return (int) Mockito.mockingDetails(mock).getInvocations().stream().filter(i -> i.getMethod().getName().equals(methodName)).count();
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
        // Reach the protected getKeyBytes via reflection so we can verify the contract. It is declared
        // on the abstract base class (AbstractJedisCacheClient), so look it up on the superclass.
        final java.lang.reflect.Method m = JRedis.class.getSuperclass().getDeclaredMethod("getKeyBytes", String.class);
        m.setAccessible(true);
        try {
            m.invoke(cache, (Object) null);
            org.junit.jupiter.api.Assertions.fail("expected IllegalArgumentException");
        } catch (final java.lang.reflect.InvocationTargetException ite) {
            org.junit.jupiter.api.Assertions.assertTrue(ite.getCause() instanceof IllegalArgumentException);
        }
    }
}
