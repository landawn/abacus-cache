/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.landawn.abacus.LogCapture;

import redis.clients.jedis.RedisClusterClient;
import redis.clients.jedis.providers.ClusterConnectionProvider;

/**
 * Cluster construction, routing, broadcast, and lifecycle tests. Most cases inject a mock client;
 * constructor cases also intercept topology discovery. Inherited operation contracts belong to
 * {@link AbstractJedisCacheClientTest}.
 */
class JRedisClusterTest {

    private RedisClusterClient mockCluster;
    private JRedisCluster<Object> cache;

    @BeforeEach
    void setUp() {
        mockCluster = mock(RedisClusterClient.class);
        // Injection constructor: no real cluster client is built, so no topology discovery happens.
        cache = new JRedisCluster<>("10.0.0.1:7000", mockCluster);
    }

    @AfterEach
    void closeClient() {
        cache.disconnect();
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
        assertThrows(IllegalArgumentException.class, () -> new JRedisCluster<>("10.0.0.1:7000", Integer.MAX_VALUE + 1L));
    }

    @Test
    void constructorRejectsNullServerUrl() {
        assertThrows(IllegalArgumentException.class, () -> new JRedisCluster<>((String) null));
        assertThrows(IllegalArgumentException.class, () -> new JRedisCluster<>((String) null, 1000L));
        assertThrows(IllegalArgumentException.class, () -> new JRedisCluster<>((String) null, mock(RedisClusterClient.class)));
    }

    @Test
    void defaultTimeoutConstructorCreatesAndClosesItsClusterClient() {
        try (MockedConstruction<ClusterConnectionProvider> providers = Mockito.mockConstruction(ClusterConnectionProvider.class);
             MockedConstruction<RedisClusterClient> clients = Mockito.mockConstruction(RedisClusterClient.class)) {
            final JRedisCluster<Object> local = new JRedisCluster<>("127.0.0.1:7000");
            try {
                assertEquals("127.0.0.1:7000", local.serverUrl());
                assertEquals(1, clients.constructed().size());
            } finally {
                local.disconnect();
            }
            Mockito.verify(clients.constructed().get(0)).close();
        }
    }

    @Test
    void injectionConstructorRejectsNullClient() {
        assertThrows(IllegalArgumentException.class, () -> new JRedisCluster<>("10.0.0.1:7000", (RedisClusterClient) null));
    }

    @Test
    void serverUrlReturnsConstructorValue() {
        assertEquals("10.0.0.1:7000", cache.serverUrl());
    }

    // Cluster routing

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
    void clientForPreservesIgnoredNullKey() {
        assertSame(mockCluster, cache.clientFor(null));
    }

    // Removals

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

    @ParameterizedTest
    @ValueSource(strings = { "WARN", "OFF" })
    @ResourceLock(LogCapture.RESOURCE)
    void disconnectRemainsTerminalWhenTheClusterCloseFails(final String level) {
        final RedisClusterClient delegate = Mockito.mock(RedisClusterClient.class);
        Mockito.doThrow(new IllegalStateException("close failed")).when(delegate).close();
        final JRedisCluster<String> cache = new JRedisCluster<>("127.0.0.1:7000", delegate);
        try (LogCapture logs = new LogCapture(JRedisCluster.class, Level.valueOf(level))) {
            cache.disconnect();
            cache.disconnect();
            Mockito.verify(delegate, Mockito.times(1)).close();
            assertThrows(IllegalStateException.class, () -> cache.get("key"));
            assertThrows(IllegalStateException.class, cache::flushAll);
            assertEquals("WARN".equals(level) ? 1 : 0, logs.messages().size());
            if ("WARN".equals(level)) {
                assertTrue(logs.messages().get(0).contains("Failed to close the Redis Cluster client"));
            }
        }
    }

    private static byte[] utf8(final String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
