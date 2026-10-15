/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.landawn.abacus.LogCapture;

import redis.clients.jedis.RedisClient;
import redis.clients.jedis.params.SetParams;

/**
 * Standalone Redis construction, shard routing, broadcast, and lifecycle tests. Client construction
 * is intercepted so these cases do not open server connections. Inherited serialization, TTL,
 * counters, and key validation belong to {@link AbstractJedisCacheClientTest}.
 */
class JRedisTest {

    private MockedConstruction<RedisClient> ctorIntercept;
    /** One mocked RedisClient per constructed shard, in construction order. */
    private List<RedisClient> shards;
    private JRedis<Object> cache;
    private List<JRedis<Object>> additionalCaches;
    /** The single shard's client for the default single-node {@link #cache}. */
    private RedisClient mockJedis;

    @BeforeEach
    void setUp() {
        shards = new ArrayList<>();
        additionalCaches = new ArrayList<>();
        // mockConstruction intercepts `new RedisClient(...)` so no real socket is ever opened.
        // Commands are invoked directly on the (mocked) pooled client.
        ctorIntercept = Mockito.mockConstruction(RedisClient.class, (poolMock, context) -> shards.add(poolMock));
        cache = new JRedis<>("localhost:6379");
        assertEquals(1, shards.size(), "single-node URL should build exactly one shard");
        mockJedis = shards.get(0);
        assertNotNull(mockJedis, "mocked jedis client should have been injected");
    }

    @AfterEach
    void tearDown() {
        try {
            for (final JRedis<Object> additional : additionalCaches) {
                additional.disconnect();
            }
            cache.disconnect();
        } finally {
            if (ctorIntercept != null) {
                ctorIntercept.close();
            }
        }
    }

    // Construction

    /** A shard-construction failure must close the shard clients that were already built. */
    @Test
    void constructorPartialFailureClosesAlreadyBuiltShards() {
        ctorIntercept.close();
        final List<RedisClient> built = new ArrayList<>();
        final RuntimeException failure = new IllegalStateException("third shard fails");

        ctorIntercept = Mockito.mockConstruction(RedisClient.class, (poolMock, context) -> {
            if (built.size() == 2) {
                throw failure;
            }

            built.add(poolMock);
        });

        final RuntimeException thrown = assertThrows(RuntimeException.class, () -> new JRedis<>("h1:6379,h2:6379,h3:6379"));

        assertTrue(thrown == failure || thrown.getCause() == failure, String.valueOf(thrown));
        assertEquals(2, built.size());
        verify(built.get(0)).close();
        verify(built.get(1)).close();
    }

    @Test
    void constructorRejectsInvalidServerUrl() {
        // An empty URL is rejected by the base-class blank check; a token without host:port by AddrUtil.
        assertThrows(IllegalArgumentException.class, () -> new JRedis<>(""));
        assertThrows(IllegalArgumentException.class, () -> new JRedis<>("localhost"));
        assertEquals(1, shards.size(), "only setUp's shard may exist; none is built for a malformed URL");
    }

    @Test
    void constructorRejectsInvalidTimeout() {
        assertThrows(IllegalArgumentException.class, () -> new JRedis<>("localhost:6379", 0L));
        assertThrows(IllegalArgumentException.class, () -> new JRedis<>("localhost:6379", -1L));
        assertThrows(IllegalArgumentException.class, () -> new JRedis<>("localhost:6379", Integer.MAX_VALUE + 1L));
    }

    @Test
    void constructorRejectsNullServerUrl() {
        assertThrows(IllegalArgumentException.class, () -> new JRedis<>((String) null));
        assertThrows(IllegalArgumentException.class, () -> new JRedis<>((String) null, 1000L));
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void constructorRetainsFailureWhileClosingEveryBuiltShard(final boolean reusedFailure) {
        final RuntimeException failure = new IllegalStateException("third shard failed");
        final Throwable cleanupFailure = reusedFailure ? failure : new AssertionError("second shard close failed");
        final RedisClient first = Mockito.mock(RedisClient.class);
        final RedisClient second = Mockito.mock(RedisClient.class);
        final RedisClient.Builder builder = Mockito.mock(RedisClient.Builder.class, Mockito.RETURNS_SELF);
        when(builder.build()).thenReturn(first, second).thenThrow(failure);
        doThrow(cleanupFailure).when(second).close();

        try (MockedStatic<RedisClient> factory = Mockito.mockStatic(RedisClient.class)) {
            factory.when(RedisClient::builder).thenReturn(builder);
            final RuntimeException thrown = assertThrows(RuntimeException.class, () -> new JRedis<>("h1:6379,h2:6379,h3:6379"));

            assertSame(failure, thrown);
            assertArrayEquals(reusedFailure ? new Throwable[0] : new Throwable[] { cleanupFailure }, thrown.getSuppressed());
            verify(first).close();
            verify(second).close();
        }
    }

    /**
     * Regression test: without Kryo on the classpath, every construction attempt must fail with the
     * documented, actionable {@link IllegalStateException}. The check used to run in a static field
     * initializer, so the first attempt surfaced as {@link ExceptionInInitializerError} and every
     * later attempt as a bare {@link NoClassDefFoundError} ("Could not initialize class ...").
     *
     * <p>The abacus classes (including abacus-common's {@code ParserFactory}, whose Kryo probe is
     * computed once per class loader) are reloaded in an isolated loader that hides Kryo.
     */
    @Test
    void missingKryoEveryConstructionFailsWithIllegalStateException() throws Exception {
        final ClassLoader loader = new KryoHidingClassLoader(JRedisTest.class.getClassLoader());
        // Reflect only on JRedis itself: reflecting on ParserFactory's public methods would fail on
        // the Kryo types in their signatures. Initialization is deferred to the constructor call.
        final Class<?> jredis = Class.forName(JRedis.class.getName(), false, loader);
        final java.lang.reflect.Constructor<?> ctor = jredis.getConstructor(String.class);

        for (int attempt = 1; attempt <= 2; attempt++) {
            final Throwable thrown = assertThrows(Throwable.class, () -> ctor.newInstance("localhost:6379"));
            final Throwable cause = thrown instanceof InvocationTargetException ? thrown.getCause() : thrown;

            assertEquals(IllegalStateException.class, cause.getClass(), "attempt " + attempt + ": " + cause);
            assertTrue(cause.getMessage().contains("com.esotericsoftware:kryo"), cause.getMessage());
        }

        assertEquals(1, shards.size(), "only setUp's shard may exist; none is built when Kryo is missing");
    }

    @Test
    void serverUrlReturnsConstructorValue() {
        assertEquals("localhost:6379", cache.serverUrl());
    }

    // Shard routing

    @Test
    void clientForRejectsNullBeforeSingleShardShortcutOrHashing() {
        final IllegalArgumentException singleShardFailure = assertThrows(IllegalArgumentException.class, () -> cache.clientFor(null));
        assertTrue(singleShardFailure.getMessage().contains("keyBytes"));
        Mockito.verifyNoInteractions(mockJedis);

        final JRedis<Object> sharded = newShardedCache("h1:6379,h2:6379");
        final IllegalArgumentException multipleShardFailure = assertThrows(IllegalArgumentException.class, () -> sharded.clientFor(null));
        assertEquals(singleShardFailure.getMessage(), multipleShardFailure.getMessage());
        shards.forEach(Mockito::verifyNoInteractions);
    }

    @Test
    void routingDistributesKeysAcrossShards() {
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
    void routingSameKeyIsConsistentAcrossCalls() {
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

    // Removals

    @Test
    void flushAllContinuesWhenOneShardThrows() {
        final JRedis<Object> sharded = newShardedCache("h1:6379,h2:6379");
        when(shards.get(0).flushAll()).thenThrow(new RuntimeException("shard 1 down"));
        when(shards.get(1).flushAll()).thenReturn("OK");

        RuntimeException thrown = assertThrows(RuntimeException.class, sharded::flushAll);
        assertEquals("shard 1 down", thrown.getMessage());
        // shard2 must still have been flushed even though shard1 threw
        verify(shards.get(1)).flushAll();
    }

    @Test
    void flushAllIteratesAllShards() {
        final JRedis<Object> sharded = newShardedCache("h1:6379,h2:6379");
        assertEquals(2, shards.size());
        when(shards.get(0).flushAll()).thenReturn("OK");
        when(shards.get(1).flushAll()).thenReturn("OK");

        sharded.flushAll();

        verify(shards.get(0)).flushAll();
        verify(shards.get(1)).flushAll();
    }

    @Test
    void flushAllRetainsAdditionalFailuresAsSuppressed() {
        final JRedis<Object> sharded = newShardedCache("h1:6379,h2:6379,h3:6379");
        final RuntimeException first = new RuntimeException("shard 1 down");
        final RuntimeException second = new RuntimeException("shard 2 down");
        when(shards.get(0).flushAll()).thenThrow(first);
        when(shards.get(1).flushAll()).thenThrow(second);
        when(shards.get(2).flushAll()).thenReturn("OK");

        final RuntimeException thrown = assertThrows(RuntimeException.class, sharded::flushAll);

        assertTrue(thrown == first);
        assertEquals(1, thrown.getSuppressed().length);
        assertTrue(thrown.getSuppressed()[0] == second);
        verify(shards.get(2)).flushAll();
    }

    @Test
    void flushAllSingleShardCallsFlushAll() {
        when(mockJedis.flushAll()).thenReturn("OK");

        cache.flushAll();

        verify(mockJedis).flushAll();
    }

    // Lifecycle

    @Test
    void disconnectIsIdempotent() {
        cache.disconnect();
        cache.disconnect(); // safe to call again

        // The underlying shard client should only be closed once.
        verify(mockJedis, times(1)).close();
    }

    @Test
    void disconnectPublishesShutdownBeforeClosingClient() {
        doAnswer(invocation -> {
            assertThrows(IllegalStateException.class, () -> cache.get("during-close"));
            return null;
        }).when(mockJedis).close();

        cache.disconnect();

        verify(mockJedis, never()).get(utf8("during-close"));
        assertThrows(IllegalStateException.class, () -> cache.getBulk());
        assertThrows(IllegalStateException.class, cache::flushAll);
    }

    /**
     * {@link Throwable#addSuppressed(Throwable)} rejects self-suppression. If two shard clients
     * happen to throw the same reusable {@link Error} instance, cleanup must still attempt every
     * remaining shard and eventually rethrow that original error.
     */
    @Test
    void disconnectReusedErrorDoesNotAbortRemainingShardCleanup() {
        final JRedis<Object> sharded = newShardedCache("h1:6379,h2:6379,h3:6379");
        final AssertionError sharedFailure = new AssertionError("shared close failure");
        doThrow(sharedFailure).when(shards.get(0)).close();
        doThrow(sharedFailure).when(shards.get(1)).close();

        final Error thrown = assertThrows(Error.class, sharded::disconnect);

        assertTrue(thrown == sharedFailure);
        assertEquals(0, thrown.getSuppressed().length, "the same Error instance cannot be suppressed onto itself");
        verify(shards.get(0)).close();
        verify(shards.get(1)).close();
        verify(shards.get(2)).close();
    }

    @ParameterizedTest
    @ValueSource(strings = { "WARN", "OFF" })
    @ResourceLock(LogCapture.RESOURCE)
    void shardFailuresPreserveDiagnosticsAndContinueCleanup(final String level) {
        final JRedis<Object> sharded = newShardedCache("h1:6379,h2:6379,h3:6379,h4:6379");
        final RuntimeException sharedFlushFailure = new IllegalStateException("unavailable");
        when(shards.get(0).flushAll()).thenThrow(sharedFlushFailure);
        when(shards.get(1).flushAll()).thenThrow(sharedFlushFailure);
        final AssertionError firstCloseFailure = new AssertionError("second shard close");
        final AssertionError secondCloseFailure = new AssertionError("third shard close");
        doThrow(new IllegalStateException("first shard close")).when(shards.get(0)).close();
        doThrow(firstCloseFailure).when(shards.get(1)).close();
        doThrow(secondCloseFailure).when(shards.get(2)).close();

        try (LogCapture logs = new LogCapture(JRedis.class, Level.valueOf(level))) {
            assertSame(sharedFlushFailure, assertThrows(RuntimeException.class, sharded::flushAll));
            assertEquals(0, sharedFlushFailure.getSuppressed().length);
            for (final RedisClient shard : shards) {
                verify(shard).flushAll();
            }

            assertSame(firstCloseFailure, assertThrows(AssertionError.class, sharded::disconnect));
            assertArrayEquals(new Throwable[] { secondCloseFailure }, firstCloseFailure.getSuppressed());
            sharded.disconnect();
            for (final RedisClient shard : shards) {
                verify(shard, times(1)).close();
            }
            assertThrows(IllegalStateException.class, () -> sharded.get("after-close"));

            if ("WARN".equals(level)) {
                assertEquals(3, logs.messages().size());
                assertTrue(logs.messages().get(0).contains("shard index 0"));
                assertTrue(logs.messages().get(1).contains("shard index 1"));
                assertTrue(logs.messages().get(2).contains("Failed to close Redis client for shard index 0"));
            } else {
                assertTrue(logs.messages().isEmpty());
            }
        }
    }

    // Compatibility

    /**
     * The deprecated {@code set}/{@code delete} names are interface compatibility defaults. They must
     * bridge to the {@code put}/{@code remove} implementations, which are declared on the
     * package-private {@code AbstractJedisCacheClient} rather than on {@code JRedis} itself.
     */
    @SuppressWarnings("deprecation")
    @Test
    void legacySetAndDeleteBridgeToPutAndRemove() {
        when(mockJedis.set(any(byte[].class), any(byte[].class), any(SetParams.class))).thenReturn("OK");
        when(mockJedis.del(any(byte[].class))).thenReturn(1L);

        assertTrue(cache.set("legacy", "v", 60_000));
        assertTrue(cache.delete("legacy"));

        verify(mockJedis).set(eq(utf8("legacy")), any(byte[].class), eq(SetParams.setParams().px(60_000L)));
        verify(mockJedis).del(utf8("legacy"));
    }

    /**
     * Builds a multi-node {@link JRedis} and resets {@link #shards} first so that
     * {@code shards.get(0)} / {@code shards.get(1)} line up with the new cache's shard order.
     */
    private JRedis<Object> newShardedCache(final String serverUrl) {
        shards.clear();
        final JRedis<Object> sharded = new JRedis<>(serverUrl);
        additionalCaches.add(sharded);
        assertEquals(serverUrl.split(",").length, shards.size());
        return sharded;
    }

    private static int invocationCount(final Object mock, final String methodName) {
        return (int) Mockito.mockingDetails(mock).getInvocations().stream().filter(i -> i.getMethod().getName().equals(methodName)).count();
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Child-first loader for {@code com.landawn.abacus.*} classes that makes every Kryo class
     * ({@code com.esotericsoftware.*}) unloadable; all other classes are delegated to the parent.
     */
    private static final class KryoHidingClassLoader extends ClassLoader {
        KryoHidingClassLoader(final ClassLoader parent) {
            super(parent);
        }

        @Override
        protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
            if (name.startsWith("com.esotericsoftware.")) {
                throw new ClassNotFoundException(name);
            }

            if (!name.startsWith("com.landawn.abacus.")) {
                return super.loadClass(name, resolve);
            }

            synchronized (getClassLoadingLock(name)) {
                Class<?> cls = findLoadedClass(name);

                if (cls == null) {
                    try (java.io.InputStream in = getParent().getResourceAsStream(name.replace('.', '/') + ".class")) {
                        if (in == null) {
                            throw new ClassNotFoundException(name);
                        }

                        final byte[] bytes = in.readAllBytes();
                        cls = defineClass(name, bytes, 0, bytes.length);
                    } catch (final java.io.IOException e) {
                        throw new ClassNotFoundException(name, e);
                    }
                }

                if (resolve) {
                    resolveClass(cls);
                }

                return cls;
            }
        }
    }
}
