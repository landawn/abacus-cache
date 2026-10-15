/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
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

class JRedisClusterTest {

    private static final KryoParser KRYO = ParserFactory.createKryoParser();

    private RedisClusterClient mockCluster;
    private JRedisCluster<Object> cache;

    @BeforeEach
    public void setUp() {
        mockCluster = mock(RedisClusterClient.class);
        // Injection constructor: no real cluster client is built, so no topology discovery happens.
        cache = new JRedisCluster<>("10.0.0.1:7000", mockCluster);
    }

    // Construction

    @Test
    void constructorRejectsBlankServerUrl() {
        assertThrows(IllegalArgumentException.class, () -> new JRedisCluster<>(""));
        assertThrows(IllegalArgumentException.class, () -> new JRedisCluster<>("   "));
    }

    @Test
    void constructorRejectsInvalidTimeout() {
        assertThrows(IllegalArgumentException.class, () -> new JRedisCluster<>("10.0.0.1:7000", 0L));
        assertThrows(IllegalArgumentException.class, () -> new JRedisCluster<>("10.0.0.1:7000", -1L));
        assertThrows(IllegalArgumentException.class, () -> new JRedisCluster<>("10.0.0.1:7000", (long) Integer.MAX_VALUE + 1L));
    }

    @Test
    void constructorRejectsNullServerUrl() {
        assertThrows(IllegalArgumentException.class, () -> new JRedisCluster<>((String) null));
        assertThrows(IllegalArgumentException.class, () -> new JRedisCluster<>((String) null, 1000L));
        assertThrows(IllegalArgumentException.class, () -> new JRedisCluster<>((String) null, mock(RedisClusterClient.class)));
    }

    // Reads

    @Test
    void getReturnsDecodedValue() {
        when(mockCluster.get(utf8("user:1"))).thenReturn(KRYO.encode("Alice"));

        assertEquals("Alice", cache.get("user:1"));
        verify(mockCluster).get(utf8("user:1"));
    }

    // Bulk Reads

    @Test
    void getBulkCollectionAndEmpty() {
        when(mockCluster.get(utf8("a"))).thenReturn(KRYO.encode("x"));

        assertEquals(1, cache.getBulk(List.of("a")).size());
        assertTrue(cache.getBulk(List.of()).isEmpty());
        assertTrue(cache.getBulk().isEmpty());
    }

    @Test
    void getBulkReturnsOnlyFoundKeys() {
        when(mockCluster.get(utf8("k1"))).thenReturn(KRYO.encode("v1"));
        when(mockCluster.get(utf8("k2"))).thenReturn(null); // missing
        when(mockCluster.get(utf8("k3"))).thenReturn(new byte[0]); // empty -> null

        Map<String, Object> result = cache.getBulk("k1", "k2", "k3");

        assertEquals(1, result.size());
        assertEquals("v1", result.get("k1"));
    }

    // Writes

    @Test
    void incrReturnsServerValue() {
        when(mockCluster.incr(utf8("counter"))).thenReturn(1L);

        assertEquals(1L, cache.incr("counter"));
    }

    @Test
    void setRoundTripUsesKryoEncoding() {
        when(mockCluster.set(any(byte[].class), any(byte[].class), any(SetParams.class))).thenReturn("OK");

        cache.put("k", "hello", 30_000);

        ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
        verify(mockCluster).set(eq(utf8("k")), payload.capture(), eq(SetParams.setParams().px(30_000L)));
        assertEquals("hello", KRYO.decode(payload.getValue()));
    }

    @Test
    void setWithoutTtlUsesPlainSet() {
        when(mockCluster.set(any(byte[].class), any(byte[].class))).thenReturn("OK");

        assertTrue(cache.put("forever", "v", 0));
        verify(mockCluster).set(eq(utf8("forever")), any(byte[].class));
        verify(mockCluster, never()).set(any(byte[].class), any(byte[].class), any(SetParams.class));
    }

    @Test
    void setWithTtlUsesSetWithExpiry() {
        when(mockCluster.set(any(byte[].class), any(byte[].class), any(SetParams.class))).thenReturn("OK");

        assertTrue(cache.put("k", "v", 60_000)); // 60 seconds, sent with millisecond precision (PX)
        verify(mockCluster).set(eq(utf8("k")), any(byte[].class), eq(SetParams.setParams().px(60_000L)));
        verify(mockCluster, never()).set(any(byte[].class), any(byte[].class));
    }

    // Removals

    @Test
    void deleteReturnsFalseWhenKeyDidNotExist() {
        when(mockCluster.del(any(byte[].class))).thenReturn(0L);

        assertFalse(cache.remove("missing"));
    }

    @Test
    void flushAllBroadcastsToCluster() {
        when(mockCluster.flushAll()).thenReturn("OK");

        cache.flushAll();

        verify(mockCluster).flushAll();
    }

    @Test
    void flushAllPropagatesBroadcastFailure() {
        when(mockCluster.flushAll()).thenThrow(new RuntimeException("cluster down"));

        RuntimeException thrown = assertThrows(RuntimeException.class, cache::flushAll);
        assertEquals("cluster down", thrown.getMessage());
    }

    // Lifecycle

    @Test
    void disconnectIsIdempotent() {
        cache.disconnect();
        cache.disconnect(); // safe to call again

        verify(mockCluster, times(1)).close();
    }

    @Test
    void disconnectPublishesShutdownBeforeClosingClusterClient() {
        doAnswer(invocation -> {
            assertThrows(IllegalStateException.class, cache::flushAll);
            assertThrows(IllegalStateException.class, () -> cache.get("during-close"));
            return null;
        }).when(mockCluster).close();

        cache.disconnect();

        verify(mockCluster, never()).flushAll();
        verify(mockCluster, never()).get(utf8("during-close"));
    }

    /**
     * A RuntimeException from the cluster client's close() is swallowed: disconnect() still
     * returns normally and the instance stays marked shut down.
     */
    @Test
    void disconnectSwallowsClusterCloseFailure() {
        org.mockito.Mockito.doThrow(new RuntimeException("close failed")).when(mockCluster).close();

        cache.disconnect(); // must not throw

        assertThrows(IllegalStateException.class, cache::flushAll);
    }

    // Validation And Failures

    @Test
    void clientForPreservesIgnoredNullKey() {
        assertSame(mockCluster, cache.clientFor(null));
    }

    @Test
    void injectionConstructorRejectsNullClient() {
        assertThrows(IllegalArgumentException.class, () -> new JRedisCluster<>("10.0.0.1:7000", (RedisClusterClient) null));
    }

    // Other Behavior

    @Test
    void allKeysRouteToTheSingleClusterClient() {
        // JRedisCluster does no client-side sharding: every key is dispatched to the one cluster
        // client, which routes by hash slot server-side.
        when(mockCluster.get(any(byte[].class))).thenReturn(null);

        cache.get("a");
        cache.get("b");
        cache.get("c");

        verify(mockCluster, times(3)).get(any(byte[].class));
    }

    @Test
    void serverUrlReturnsConstructorValue() {
        assertEquals("10.0.0.1:7000", cache.serverUrl());
    }

    @Test
    void validSupplementaryUnicodeKeyPreservesUtf8Bytes() {
        final String key = "user:\uD83D\uDE00:\uD800\uDC00";
        when(mockCluster.get(utf8(key))).thenReturn(KRYO.encode("value"));

        assertEquals("value", cache.get(key));
        assertEquals(Map.of(key, "value"), cache.getBulk(List.of("missing", key)));
        verify(mockCluster, times(2)).get(utf8(key));
    }

    private static byte[] utf8(final String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
