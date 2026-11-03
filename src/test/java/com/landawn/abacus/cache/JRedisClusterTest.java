/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import java.time.Duration;
import java.util.List;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingConsumer;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.landawn.abacus.LogCapture;

import redis.clients.jedis.Connection;
import redis.clients.jedis.ConnectionPool;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClusterInfoCache;
import redis.clients.jedis.RedisClusterClient;
import redis.clients.jedis.exceptions.JedisBroadcastException;
import redis.clients.jedis.exceptions.JedisClusterOperationException;
import redis.clients.jedis.exceptions.JedisException;
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

    /** A node's interrupted pool wait arrives as a broadcast reply; the interrupt must be restored. */
    @Test
    void flushAllRestoresTheInterruptReportedInABroadcastReply() {
        final JedisBroadcastException broadcast = new JedisBroadcastException();
        broadcast.addReply(new HostAndPort("10.0.0.1", 7000), "OK");
        broadcast.addReply(new HostAndPort("10.0.0.2", 7000), new JedisException("Could not get a resource from the pool", new InterruptedException()));
        when(mockCluster.flushAll()).thenThrow(broadcast);
        Thread.interrupted();

        try {
            assertSame(broadcast, assertThrows(JedisBroadcastException.class, cache::flushAll));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    /**
     * Regression test: when a key's slot has no pool, Jedis falls back to a random primary
     * ({@code ClusterConnectionProvider.getConnection()}), which reports a node's failed pool wait
     * only as a <em>suppressed</em> exception of "No reachable node in cluster.", not as a cause, so
     * the interrupt that wait consumed used to be lost. Real Jedis client, executor, connection
     * provider, and pool; only the topology cache and the seed connection are stubbed.
     */
    @Test
    void interruptConsumedByTheRandomPrimaryFallbackIsRestored() throws Throwable {
        withRandomPrimaryFallbackIntoAnExhaustedPool(real -> {
            Thread.interrupted();

            try {
                Thread.currentThread().interrupt();
                final JedisClusterOperationException failure = assertThrows(JedisClusterOperationException.class, () -> real.get("key"));

                // Pin the Jedis shape this fix depends on: the interrupt is reachable only through a suppressed exception.
                assertNull(failure.getCause());
                assertInstanceOf(InterruptedException.class, failure.getSuppressed()[0].getCause());
                assertTrue(Thread.currentThread().isInterrupted(), "the interrupt the pool wait consumed must be restored");
            } finally {
                Thread.interrupted();
            }
        });
    }

    /**
     * Regression test, end to end through {@link DistributedCache}: the interrupt that the
     * random-primary fallback reports only as a suppressed exception must also keep the read out of the
     * circuit breaker. Recognizing it only in the cause chain counted the caller's interrupt as a
     * backend failure, which with a threshold of 1 opened the circuit for every thread.
     */
    @Test
    void interruptReportedOnlyAsSuppressedByTheRandomPrimaryFallbackIsNotCountedByTheBreaker() throws Throwable {
        withRandomPrimaryFallbackIntoAnExhaustedPool(real -> {
            final DistributedCache<String, Object> distributed = new DistributedCache<>(real, "", 1, 60_000L);

            try {
                final Object closedState = DistributedCacheBreakerAccess.breakerState(distributed);
                Thread.interrupted();

                try {
                    Thread.currentThread().interrupt();
                    assertNull(distributed.getOrNull("key"));
                    assertTrue(Thread.currentThread().isInterrupted(), "the interrupt the pool wait consumed must be restored");
                } finally {
                    Thread.interrupted();
                }

                assertSame(closedState, DistributedCacheBreakerAccess.breakerState(distributed));

                // The circuit is still closed, so an uninterrupted read reaches the cluster again; its genuine
                // pool timeout is then recorded, which an open circuit (no backend call) would not do.
                assertNull(distributed.getOrNull("key"));
                assertTrue(DistributedCacheBreakerAccess.booleanField(DistributedCacheBreakerAccess.breakerState(distributed), "hasFailure"));
            } finally {
                distributed.close();
            }
        });
    }

    /**
     * Runs {@code test} against a real cluster client (real Jedis client, executor, connection provider,
     * and pool; only the topology cache and the seed connection are stubbed) whose only node pool is
     * exhausted. No slot is mapped, so every keyed command takes the random-primary fallback, and its pool
     * wait times out after 10 ms, or fails at once on an interrupted thread.
     */
    private static void withRandomPrimaryFallbackIntoAnExhaustedPool(final ThrowingConsumer<JRedisCluster<Object>> test) throws Throwable {
        final GenericObjectPoolConfig<Connection> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(1);
        poolConfig.setMaxWait(Duration.ofMillis(10));
        poolConfig.setJmxEnabled(false);
        @SuppressWarnings("unchecked")
        final PooledObjectFactory<Connection> factory = mock(PooledObjectFactory.class);
        when(factory.makeObject()).thenAnswer(invocation -> new DefaultPooledObject<>(mock(Connection.class)));

        try (ConnectionPool exhausted = new ConnectionPool(factory, poolConfig);
             MockedConstruction<JedisClusterInfoCache> topology = Mockito.mockConstruction(JedisClusterInfoCache.class,
                     (slots, context) -> when(slots.getShuffledPrimaryNodesPool()).thenReturn(List.of(exhausted)));
             MockedConstruction<Connection> seed = Mockito.mockConstruction(Connection.class)) {
            exhausted.getResource(); // the pool's only connection stays borrowed, so the next borrow waits

            final JRedisCluster<Object> real = new JRedisCluster<>("10.0.0.1:7000", 1_000);

            try {
                test.accept(real);
            } finally {
                real.disconnect();
            }
        }
    }

    /** End to end: a get whose pool wait was interrupted is a miss that leaves the breaker closed. */
    @Test
    void interruptedPoolWaitIsNotCountedByTheDistributedCacheBreaker() throws Exception {
        when(mockCluster.get(any(byte[].class))).thenThrow(new JedisException("Could not get a resource from the pool", new InterruptedException()));
        final DistributedCache<String, Object> distributed = new DistributedCache<>(cache, "", 0, 60_000L);
        final Object closedState = DistributedCacheBreakerAccess.breakerState(distributed);
        Thread.interrupted();

        try {
            assertEquals(null, distributed.getOrNull("key"));
            assertTrue(Thread.currentThread().isInterrupted(), "the interrupt Jedis consumed must be restored");
        } finally {
            Thread.interrupted();
        }

        assertSame(closedState, DistributedCacheBreakerAccess.breakerState(distributed));
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
