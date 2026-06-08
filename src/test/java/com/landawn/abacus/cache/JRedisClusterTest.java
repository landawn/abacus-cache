/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.landawn.abacus.parser.KryoParser;
import com.landawn.abacus.parser.ParserFactory;

import redis.clients.jedis.RedisClusterClient;
import redis.clients.jedis.params.SetParams;

/**
 * Mock-based unit tests for {@link JRedisCluster}.
 *
 * <p>The public constructors build a real {@link RedisClusterClient}, which eagerly discovers the
 * cluster topology (a network call) — unsuitable for a hermetic unit test. These tests therefore use
 * the package-private injection constructor to hand in a mocked {@code RedisClusterClient}, so the
 * cluster-cache dispatch logic (key encoding, value encoding/decoding, TTL handling, command
 * dispatch, flushAll broadcast) can be exercised without any network I/O.
 *
 * <p>Constructor-validation tests still call the public constructors: their argument checks all run
 * <em>before</em> the cluster client is built, so they throw without opening a connection.
 */
@Tag("2025")
public class JRedisClusterTest {

    private static final KryoParser KRYO = ParserFactory.createKryoParser();

    private RedisClusterClient mockCluster;
    private JRedisCluster<Object> cache;

    @BeforeEach
    public void setUp() {
        mockCluster = mock(RedisClusterClient.class);
        // Injection constructor: no real cluster client is built, so no topology discovery happens.
        cache = new JRedisCluster<>("10.0.0.1:7000", mockCluster);
    }

    @Test
    public void test_get_returns_decoded_value() {
        when(mockCluster.get(utf8("user:1"))).thenReturn(KRYO.encode("Alice"));

        assertEquals("Alice", cache.get("user:1"));
        verify(mockCluster).get(utf8("user:1"));
    }

    @Test
    public void test_get_returns_null_when_key_missing() {
        when(mockCluster.get(any(byte[].class))).thenReturn(null);

        assertNull(cache.get("missing"));
    }

    @Test
    public void test_get_returns_null_for_empty_byte_array() {
        when(mockCluster.get(any(byte[].class))).thenReturn(new byte[0]);

        assertNull(cache.get("empty"));
    }

    @Test
    public void test_getBulk_returns_only_found_keys() {
        when(mockCluster.get(utf8("k1"))).thenReturn(KRYO.encode("v1"));
        when(mockCluster.get(utf8("k2"))).thenReturn(null);          // missing
        when(mockCluster.get(utf8("k3"))).thenReturn(new byte[0]);    // empty -> null

        Map<String, Object> result = cache.getBulk("k1", "k2", "k3");

        assertEquals(1, result.size());
        assertEquals("v1", result.get("k1"));
    }

    @Test
    public void test_getBulk_collection_and_empty() {
        when(mockCluster.get(utf8("a"))).thenReturn(KRYO.encode("x"));

        assertEquals(1, cache.getBulk(List.of("a")).size());
        assertTrue(cache.getBulk(List.of()).isEmpty());
        assertTrue(cache.getBulk().isEmpty());
    }

    @Test
    public void test_getBulk_rejects_null_element() {
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk("a", null));
        verify(mockCluster, never()).get(any(byte[].class));
    }

    @Test
    public void test_set_with_ttl_uses_set_with_expiry() {
        when(mockCluster.set(any(byte[].class), any(byte[].class), any(SetParams.class))).thenReturn("OK");

        assertTrue(cache.set("k", "v", 60_000)); // 60 seconds
        verify(mockCluster).set(eq(utf8("k")), any(byte[].class), eq(SetParams.setParams().ex(60L)));
        verify(mockCluster, never()).set(any(byte[].class), any(byte[].class));
    }

    @Test
    public void test_set_without_ttl_uses_plain_set() {
        when(mockCluster.set(any(byte[].class), any(byte[].class))).thenReturn("OK");

        assertTrue(cache.set("forever", "v", 0));
        verify(mockCluster).set(eq(utf8("forever")), any(byte[].class));
        verify(mockCluster, never()).set(any(byte[].class), any(byte[].class), any(SetParams.class));
    }

    @Test
    public void test_set_returns_false_when_server_does_not_say_OK() {
        when(mockCluster.set(any(byte[].class), any(byte[].class), any(SetParams.class))).thenReturn("NOT_OK");

        assertFalse(cache.set("k", "v", 60_000));
    }

    @Test
    public void test_set_null_value_encodes_as_empty_byte_array() {
        when(mockCluster.set(any(byte[].class), any(byte[].class), any(SetParams.class))).thenReturn("OK");

        cache.set("k", null, 60_000);

        verify(mockCluster).set(eq(utf8("k")), eq(new byte[0]), any(SetParams.class));
    }

    @Test
    public void test_set_round_trip_uses_kryo_encoding() {
        when(mockCluster.set(any(byte[].class), any(byte[].class), any(SetParams.class))).thenReturn("OK");

        cache.set("k", "hello", 30_000);

        ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
        verify(mockCluster).set(eq(utf8("k")), payload.capture(), eq(SetParams.setParams().ex(30L)));
        assertEquals("hello", KRYO.decode(payload.getValue()));
    }

    @Test
    public void test_delete_returns_true_when_removed() {
        when(mockCluster.del(any(byte[].class))).thenReturn(1L);

        assertTrue(cache.delete("k"));
        verify(mockCluster).del(utf8("k"));
    }

    @Test
    public void test_delete_returns_false_when_key_did_not_exist() {
        when(mockCluster.del(any(byte[].class))).thenReturn(0L);

        assertFalse(cache.delete("missing"));
    }

    @Test
    public void test_incr_returns_server_value() {
        when(mockCluster.incr(utf8("counter"))).thenReturn(1L);

        assertEquals(1L, cache.incr("counter"));
    }

    @Test
    public void test_incr_with_delta_calls_incrBy() {
        when(mockCluster.incrBy(utf8("counter"), 5L)).thenReturn(5L);

        assertEquals(5L, cache.incr("counter", 5));
        verify(mockCluster).incrBy(utf8("counter"), 5L);
    }

    @Test
    public void test_incr_with_delta_rejects_negative_delta() {
        assertThrows(IllegalArgumentException.class, () -> cache.incr("counter", -1));
        verify(mockCluster, never()).incrBy(any(byte[].class), eq(-1L));
    }

    @Test
    public void test_decr_returns_negative_when_redis_does() {
        when(mockCluster.decr(utf8("counter"))).thenReturn(-1L);

        assertEquals(-1L, cache.decr("counter"));
    }

    @Test
    public void test_decr_with_delta_calls_decrBy() {
        when(mockCluster.decrBy(utf8("counter"), 3L)).thenReturn(-3L);

        assertEquals(-3L, cache.decr("counter", 3));
        verify(mockCluster).decrBy(utf8("counter"), 3L);
    }

    @Test
    public void test_decr_with_delta_rejects_negative_delta() {
        assertThrows(IllegalArgumentException.class, () -> cache.decr("counter", -1));
        verify(mockCluster, never()).decrBy(any(byte[].class), eq(-1L));
    }

    @Test
    public void test_all_keys_route_to_the_single_cluster_client() {
        // JRedisCluster does no client-side sharding: every key is dispatched to the one cluster
        // client, which routes by hash slot server-side.
        when(mockCluster.get(any(byte[].class))).thenReturn(null);

        cache.get("a");
        cache.get("b");
        cache.get("c");

        verify(mockCluster, times(3)).get(any(byte[].class));
    }

    @Test
    public void test_flushAll_broadcasts_to_cluster() {
        when(mockCluster.flushAll()).thenReturn("OK");

        cache.flushAll();

        verify(mockCluster).flushAll();
    }

    @Test
    public void test_flushAll_propagates_broadcast_failure() {
        when(mockCluster.flushAll()).thenThrow(new RuntimeException("cluster down"));

        RuntimeException thrown = assertThrows(RuntimeException.class, cache::flushAll);
        assertEquals("cluster down", thrown.getMessage());
    }

    @Test
    public void test_disconnect_is_idempotent() {
        cache.disconnect();
        cache.disconnect(); // safe to call again

        verify(mockCluster, times(1)).close();
    }

    @Test
    public void test_serverUrl_returns_constructor_value() {
        assertEquals("10.0.0.1:7000", cache.serverUrl());
    }

    @Test
    public void test_injection_constructor_rejects_null_client() {
        assertThrows(IllegalArgumentException.class, () -> new JRedisCluster<>("10.0.0.1:7000", (RedisClusterClient) null));
    }

    // ---- constructor validation: these run before any cluster client is built, so no network I/O ----

    @Test
    public void test_constructor_rejects_invalid_timeout() {
        assertThrows(IllegalArgumentException.class, () -> new JRedisCluster<>("10.0.0.1:7000", 0L));
        assertThrows(IllegalArgumentException.class, () -> new JRedisCluster<>("10.0.0.1:7000", -1L));
        assertThrows(IllegalArgumentException.class, () -> new JRedisCluster<>("10.0.0.1:7000", (long) Integer.MAX_VALUE + 1L));
    }

    @Test
    public void test_constructor_rejects_blank_server_url() {
        assertThrows(IllegalArgumentException.class, () -> new JRedisCluster<>(""));
        assertThrows(IllegalArgumentException.class, () -> new JRedisCluster<>("   "));
    }

    private static byte[] utf8(final String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
