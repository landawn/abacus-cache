/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.tools.ToolProvider;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.landawn.abacus.pool.KeyedObjectPool;
import com.landawn.abacus.pool.PoolFactory;
import com.landawn.abacus.pool.PoolableAdapter;
import com.landawn.abacus.util.Suppliers;
import com.landawn.abacus.util.TypeAttrParser;

import net.spy.memcached.MemcachedClient;

import redis.clients.jedis.RedisClient;
import redis.clients.jedis.RedisClusterClient;
import redis.clients.jedis.providers.ClusterConnectionProvider;

class CacheFactoryTest {

    private static boolean nonCacheProviderInitialized;

    @Nested
    class InProcessCaches {

        /** createCaffeineCache forwards to the CaffeineCache wrapper constructor, which rejects a null delegate. */
        @Test
        void createCaffeineCacheNullRejected() {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCaffeineCache(null));
        }

        /** Factory smoke test: the createCaffeineCache delegation wraps the supplied Caffeine cache. */
        @Test
        void createCaffeineCacheSmoke() {
            final CaffeineCache<String, String> cache = CacheFactory
                    .createCaffeineCache(com.github.benmanes.caffeine.cache.Caffeine.newBuilder().maximumSize(10).build());

            try {
                assertTrue(cache.put("k", "v"));
                assertEquals("v", cache.getOrNull("k"));
            } finally {
                cache.close();
            }
        }

        /** createEhcache forwards to the Ehcache wrapper constructor, which rejects a null delegate. */
        @Test
        void createEhcacheNullRejected() {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createEhcache(null));
        }

        @Test
        void createLocalCache() {
            final LocalCache<String, String> cache = CacheFactory.createLocalCache(100, 0);

            try {
                assertNotNull(cache);
                assertTrue(cache.put("k", "v"));
                assertEquals("v", cache.getOrNull("k"));
            } finally {
                cache.close();
            }
        }

        @Test
        void createLocalCacheFourArg() {
            final LocalCache<String, String> cache = CacheFactory.createLocalCache(100, 0, 60000L, 30000L);

            try {
                assertNotNull(cache);
                assertTrue(cache.put("a", "b"));
            } finally {
                cache.close();
            }
        }

        @Test
        void createLocalCacheInvalidCapacity() {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createLocalCache(0, 0));
        }

        @Test
        void createLocalCacheWithPool() {
            // Use IntFunctions to create the capacity supplier indirectly.
            final IntFunction<KeyedObjectPool<String, PoolableAdapter<String>>> poolFactory = cap -> PoolFactory.createKeyedObjectPool(cap, 0);
            final KeyedObjectPool<String, PoolableAdapter<String>> pool = poolFactory.apply(64);

            final LocalCache<String, String> cache = CacheFactory.createLocalCache(pool, 60000L, 30000L);

            try {
                assertNotNull(cache);
                assertTrue(cache.put("x", "y"));
                assertEquals("y", cache.getOrNull("x"));
            } finally {
                cache.close();
            }
        }

        @Test
        void createLocalCacheWithPoolNullPool() {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createLocalCache((KeyedObjectPool<String, PoolableAdapter<String>>) null, 0L, 0L));
        }

        /** Factory smoke test: the createOffHeapCache delegation produces a working cache. */
        @Test
        void createOffHeapCacheSmoke() {
            final OffHeapCache<String, byte[]> cache = CacheFactory.createOffHeapCache(1);
            try {
                assertTrue(cache.put("k", new byte[] { 1, 2 }));
                assertArrayEquals(new byte[] { 1, 2 }, cache.getOrNull("k"));
            } finally {
                cache.close();
            }
        }

        @Test
        void ehcacheFactoryUsesTheSuppliedDelegate() {
            final org.ehcache.Cache<String, String> delegate = Mockito.mock(org.ehcache.Cache.class);
            Mockito.when(delegate.get("key")).thenReturn("value");
            final Ehcache<String, String> cache = CacheFactory.createEhcache(delegate);
            try {
                assertTrue(cache.put("key", "value"));
                assertEquals("value", cache.getOrNull("key"));
                Mockito.verify(delegate).put("key", "value");
            } finally {
                cache.close();
            }
        }

        @ParameterizedTest
        @ValueSource(booleans = { false, true })
        void offHeapFactoryOverloadsCreateUsableCaches(final boolean customLifetimes) {
            final OffHeapCache<String, byte[]> cache = customLifetimes ? CacheFactory.createOffHeapCache(1, 0, 60_000, 30_000)
                    : CacheFactory.createOffHeapCache(1, 0);
            try {
                assertTrue(cache.put("key", new byte[] { 1, 2, 3 }));
                assertArrayEquals(new byte[] { 1, 2, 3 }, cache.getOrNull("key"));
            } finally {
                cache.close();
            }
            assertTrue(cache.isClosed());
        }

    }

    @Nested
    class DistributedCaches {

        @Test
        void createDistributedCacheFourArg() {
            try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
                final SpyMemcached<Object> dcc = new SpyMemcached<>("localhost:11211");
                final DistributedCache<String, Object> dc = CacheFactory.createDistributedCache(dcc, "p:", 50, 2000);
                assertNotNull(dc);
                dc.close();
            }
        }

        /** Every createDistributedCache overload rejects a null client eagerly with IllegalArgumentException. */
        @Test
        void createDistributedCacheNullClientRejected() {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createDistributedCache((DistributedCacheClient<Object>) null));
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createDistributedCache((DistributedCacheClient<Object>) null, "p:"));
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createDistributedCache((DistributedCacheClient<Object>) null, null));
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createDistributedCache((DistributedCacheClient<Object>) null, "p:", 50, 2000));
        }

        /** A null key prefix means "no prefix" (null-as-default), identical to an empty prefix. */
        @Test
        void createDistributedCacheNullKeyPrefixMeansNoPrefix() {
            try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
                final DistributedCache<String, Object> nullPrefix = CacheFactory.createDistributedCache(new SpyMemcached<>("localhost:11211"), null);
                final DistributedCache<String, Object> emptyPrefix = CacheFactory.createDistributedCache(new SpyMemcached<>("localhost:11211"), "", 50, 2000);

                try {
                    assertEquals(emptyPrefix.generateKey("k"), nullPrefix.generateKey("k"));
                } finally {
                    nullPrefix.close();
                    emptyPrefix.close();
                }
            }
        }

        @Test
        void createDistributedCacheOneArg() {
            try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
                final Supplier<DistributedCacheClient<Object>> supplier = Suppliers.of(() -> new SpyMemcached<>("localhost:11211"));
                final DistributedCacheClient<Object> dcc = supplier.get();
                final DistributedCache<String, Object> dc = CacheFactory.createDistributedCache(dcc);
                assertNotNull(dc);
                dc.close();
            }
        }

        @Test
        void createDistributedCacheTwoArg() {
            try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
                final SpyMemcached<Object> dcc = new SpyMemcached<>("localhost:11211");
                final DistributedCache<String, Object> dc = CacheFactory.createDistributedCache(dcc, "myapp:");
                assertNotNull(dc);
                dc.close();
            }
        }

    }

    @Nested
    class MemcachedProviders {

        @Test
        void createCacheMemcached() {
            try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
                final Cache<String, Object> cache = CacheFactory.createCache("Memcached(localhost:11211)");
                try {
                    assertNotNull(cache);
                    assertTrue(cache instanceof DistributedCache);
                } finally {
                    cache.close();
                }
            }
        }

        @Test
        void createCacheMemcachedWithPrefix() {
            try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
                final Cache<String, Object> cache = CacheFactory.createCache("Memcached(localhost:11211,prefix:)");
                try {
                    assertNotNull(cache);
                } finally {
                    cache.close();
                }
            }
        }

        @Test
        void createCacheMemcachedWithTimeout() {
            try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
                final Cache<String, Object> cache = CacheFactory.createCache("Memcached(localhost:11211,prefix:,5000)");
                try {
                    assertNotNull(cache);
                } finally {
                    cache.close();
                }
            }
        }

        @ParameterizedTest
        @ValueSource(booleans = { false, true })
        void wrapperConstructionFailureDisconnectsTheCreatedClient(final boolean cleanupFails) {
            final IllegalStateException cleanupFailure = new IllegalStateException("disconnect failed");
            try (MockedConstruction<SpyMemcached> clients = Mockito.mockConstruction(SpyMemcached.class, (client, context) -> {
                if (cleanupFails) {
                    Mockito.doThrow(cleanupFailure).when(client).disconnect();
                }
            })) {
                final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                        () -> CacheFactory.createCache("Memcached(127.0.0.1:11211,bad prefix)"));
                assertTrue(failure.getMessage().contains("keyPrefix"));
                assertEquals(1, clients.constructed().size());
                Mockito.verify(clients.constructed().get(0)).disconnect();
                assertEquals(cleanupFails ? 1 : 0, failure.getSuppressed().length);
                if (cleanupFails) {
                    assertSame(cleanupFailure, failure.getSuppressed()[0]);
                }
            }
        }

    }

    @Nested
    class RedisProviders {

        @Test
        void createCacheRedis() {
            try (MockedConstruction<RedisClient> ctorIntercept = Mockito.mockConstruction(RedisClient.class)) {
                final Cache<String, Object> cache = CacheFactory.createCache("Redis(localhost:6379)");
                try {
                    assertNotNull(cache);
                    assertTrue(cache instanceof DistributedCache);
                } finally {
                    cache.close();
                }
            }
        }

        /** Same all-digit rule for the standalone Redis two-parameter form. */
        @Test
        void createCacheRedisTwoParamAllDigitSecondParameterIsTimeout() throws ReflectiveOperationException {
            final Cache<String, Object> cache = CacheFactory.createCache("Redis(localhost:6379,5000)");
            try {
                assertEquals("localhost:6379", distributedClient(cache).serverUrl());

                final Field prefixField = DistributedCache.class.getDeclaredField("keyPrefix");
                prefixField.setAccessible(true);
                assertEquals("", prefixField.get(cache), "the all-digit token must configure the timeout, not the key prefix");
            } finally {
                cache.close();
            }

            // A non-positive all-digit token is a timeout and is rejected as such, not bound as a prefix.
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Redis(localhost:6379,-5000)"));
        }

        @Test
        void createCacheRedisWithPrefix() {
            try (MockedConstruction<RedisClient> ctorIntercept = Mockito.mockConstruction(RedisClient.class)) {
                final Cache<String, Object> cache = CacheFactory.createCache("Redis(localhost:6379,prefix:)");
                try {
                    assertNotNull(cache);
                } finally {
                    cache.close();
                }
            }
        }

        @Test
        void createCacheRedisWithTimeout() {
            try (MockedConstruction<RedisClient> ctorIntercept = Mockito.mockConstruction(RedisClient.class)) {
                final Cache<String, Object> cache = CacheFactory.createCache("Redis(localhost:6379,prefix:,5000)");
                try {
                    assertNotNull(cache);
                } finally {
                    cache.close();
                }
            }
        }

    }

    @Nested
    class RedisClusterProviders {

        @Test
        void createCacheRedisCluster() {
            try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class);
                 MockedConstruction<RedisClusterClient> clientIntercept = Mockito.mockConstruction(RedisClusterClient.class)) {
                final Cache<String, Object> cache = CacheFactory.createCache("RedisCluster(10.0.0.1:7000)");
                try {
                    assertNotNull(cache);
                    assertTrue(cache instanceof DistributedCache);
                } finally {
                    cache.close();
                }
            }
        }

        /** The documented escape hatches for both readings of an endpoint-shaped token must work. */
        @Test
        void createCacheRedisClusterAmbiguityEscapeHatches() throws ReflectiveOperationException {
            try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class);
                 MockedConstruction<RedisClusterClient> clientIntercept = Mockito.mockConstruction(RedisClusterClient.class)) {
                final Field prefixField = DistributedCache.class.getDeclaredField("keyPrefix");
                prefixField.setAccessible(true);

                // Seed intent: all seed nodes space-separated in the first parameter.
                final Cache<String, Object> seedCache = CacheFactory.createCache("RedisCluster(10.0.0.1:7000 10.0.0.2:7000,3000)");
                try {
                    assertEquals("10.0.0.1:7000 10.0.0.2:7000", distributedClient(seedCache).serverUrl());
                    assertEquals("", prefixField.get(seedCache));
                } finally {
                    seedCache.close();
                }

                // Prefix intent: a trailing ':' makes the prefix non-endpoint-shaped.
                final Cache<String, Object> prefixCache = CacheFactory.createCache("RedisCluster(10.0.0.1:7000,tenant:1:,1000)");
                try {
                    assertEquals("10.0.0.1:7000", distributedClient(prefixCache).serverUrl());
                    assertEquals("tenant:1:", prefixField.get(prefixCache));
                } finally {
                    prefixCache.close();
                }
            }
        }

        /**
         * An endpoint-shaped token directly before the numeric timeout is irreducibly ambiguous —
         * {@code (seed, seed, timeout)} and {@code (seed, endpoint-shaped-prefix, timeout)} are the
         * same shape with opposite intents, and either silent reading breaks the other silently (a
         * demoted seed changes the key namespace; a promoted prefix is quietly tolerated as one
         * unreachable seed). The parser must reject it with guidance for expressing both intents.
         */
        @Test
        void createCacheRedisClusterEndpointShapedTokenBeforeTimeoutRejectedAsAmbiguous() {
            try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class)) {
                // Seed-list intent: two seeds plus a timeout.
                final IllegalArgumentException seedIntent = assertThrows(IllegalArgumentException.class,
                        () -> CacheFactory.createCache("RedisCluster(10.0.0.1:7000,10.0.0.2:7000,3000)"));
                assertTrue(seedIntent.getMessage().contains("Ambiguous RedisCluster parameters"));

                // Prefix intent: an endpoint-shaped prefix such as "tenant:1".
                final IllegalArgumentException prefixIntent = assertThrows(IllegalArgumentException.class,
                        () -> CacheFactory.createCache("RedisCluster(10.0.0.1:7000,tenant:1,1000)"));
                assertTrue(prefixIntent.getMessage().contains("Ambiguous RedisCluster parameters"));
            }
        }

        /**
         * A final all-digit token following nothing but seed nodes is the timeout, not the key prefix:
         * silently binding "3000" as the key namespace would corrupt every generated key while the
         * intended timeout silently stayed at the default.
         */
        @Test
        void createCacheRedisClusterFinalNumericTokenIsTimeoutNotPrefix() throws ReflectiveOperationException {
            try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class);
                 MockedConstruction<RedisClusterClient> clientIntercept = Mockito.mockConstruction(RedisClusterClient.class)) {
                final Cache<String, Object> cache = CacheFactory.createCache("RedisCluster(10.0.0.1:7000,3000)");
                try {
                    assertEquals("10.0.0.1:7000", distributedClient(cache).serverUrl());

                    final Field prefixField = DistributedCache.class.getDeclaredField("keyPrefix");
                    prefixField.setAccessible(true);
                    assertEquals("", prefixField.get(cache), "the all-digit token must configure the timeout, not the key prefix");
                } finally {
                    cache.close();
                }
            }
        }

        /**
         * Regression: a later Redis Cluster seed may be an ordinary alphabetic DNS hostname. The
         * parser previously required a dot, digit, {@code localhost}, or bracketed IPv6 address in
         * later seed tokens, so {@code primary:6379} was silently treated as the cache key prefix.
         */
        @Test
        void createCacheRedisClusterWithAlphabeticSeedHostname() throws ReflectiveOperationException {
            try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class);
                 MockedConstruction<RedisClusterClient> clientIntercept = Mockito.mockConstruction(RedisClusterClient.class)) {
                final Cache<String, Object> cache = CacheFactory.createCache("RedisCluster(10.0.0.1:7000,primary:6379)");
                try {
                    assertEquals("10.0.0.1:7000,primary:6379", distributedClient(cache).serverUrl());
                } finally {
                    cache.close();
                }
            }
        }

        @Test
        void createCacheRedisClusterWithAlphabeticSeedHostnameAndPrefix() throws ReflectiveOperationException {
            try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class);
                 MockedConstruction<RedisClusterClient> clientIntercept = Mockito.mockConstruction(RedisClusterClient.class)) {
                final Cache<String, Object> cache = CacheFactory.createCache("RedisCluster(10.0.0.1:7000,primary:6379,prefix:)");
                try {
                    assertEquals("10.0.0.1:7000,primary:6379", distributedClient(cache).serverUrl());

                    final Field prefixField = DistributedCache.class.getDeclaredField("keyPrefix");
                    prefixField.setAccessible(true);
                    assertEquals("prefix:", prefixField.get(cache));
                } finally {
                    cache.close();
                }
            }
        }

        @Test
        void createCacheRedisClusterWithMultipleSeedNodes() throws ReflectiveOperationException {
            try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class);
                 MockedConstruction<RedisClusterClient> clientIntercept = Mockito.mockConstruction(RedisClusterClient.class)) {
                final Cache<String, Object> cache = CacheFactory.createCache("RedisCluster(10.0.0.1:7000,10.0.0.2:7000)");
                try {
                    assertNotNull(cache);
                    assertTrue(cache instanceof DistributedCache);
                    assertEquals("10.0.0.1:7000,10.0.0.2:7000", distributedClient(cache).serverUrl());
                } finally {
                    cache.close();
                }
            }
        }

        @Test
        void createCacheRedisClusterWithMultipleSeedNodesPrefixAndTimeout() throws ReflectiveOperationException {
            try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class);
                 MockedConstruction<RedisClusterClient> clientIntercept = Mockito.mockConstruction(RedisClusterClient.class)) {
                final Cache<String, Object> cache = CacheFactory.createCache("RedisCluster(10.0.0.1:7000,10.0.0.2:7000,prefix:,5000)");
                try {
                    assertNotNull(cache);
                    assertTrue(cache instanceof DistributedCache);
                    assertEquals("10.0.0.1:7000,10.0.0.2:7000", distributedClient(cache).serverUrl());
                } finally {
                    cache.close();
                }
            }
        }

        @Test
        void createCacheRedisClusterWithNumericLeadingPrefix() throws ReflectiveOperationException {
            try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class);
                 MockedConstruction<RedisClusterClient> clientIntercept = Mockito.mockConstruction(RedisClusterClient.class)) {
                final Cache<String, Object> cache = CacheFactory.createCache("RedisCluster(10.0.0.1:7000,primary:6379,123prefix)");
                try {
                    assertEquals("10.0.0.1:7000,primary:6379", distributedClient(cache).serverUrl());

                    final Field prefixField = DistributedCache.class.getDeclaredField("keyPrefix");
                    prefixField.setAccessible(true);
                    assertEquals("123prefix", prefixField.get(cache));
                } finally {
                    cache.close();
                }
            }
        }

        @Test
        void createCacheRedisClusterWithPrefix() {
            try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class);
                 MockedConstruction<RedisClusterClient> clientIntercept = Mockito.mockConstruction(RedisClusterClient.class)) {
                final Cache<String, Object> cache = CacheFactory.createCache("RedisCluster(10.0.0.1:7000,prefix:)");
                try {
                    assertNotNull(cache);
                } finally {
                    cache.close();
                }
            }
        }

        @Test
        void createCacheRedisClusterWithTimeout() {
            try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class);
                 MockedConstruction<RedisClusterClient> clientIntercept = Mockito.mockConstruction(RedisClusterClient.class)) {
                final Cache<String, Object> cache = CacheFactory.createCache("RedisCluster(10.0.0.1:7000,prefix:,5000)");
                try {
                    assertNotNull(cache);
                } finally {
                    cache.close();
                }
            }
        }

        @ParameterizedTest
        @CsvSource(delimiter = '|', value = { "host:7001 | true", "host.name-_1:65535 | true", "[::1]:7002 | true", "[a,b]:7002 | false", "[a[b]:7002 | false",
                "[a]b]:7002 | false", "[]:7002 | false", "[a:7002 | false", "a]:7002 | false", "abc]:7002 | false", "[abc:7002 | false", "host:0 | false",
                "host:65536 | false", "host:9a | false", "host:/ | false", "a::1 | false", "a!:1 | false", "'' | false", "+ | false", "- | false" })
        void distinguishesAdditionalSeedNodesFromPrefixes(final String token, final boolean isSeed) throws Exception {
            final AtomicReference<List<?>> arguments = new AtomicReference<>();
            try (MockedConstruction<JRedisCluster> clients = Mockito.mockConstruction(JRedisCluster.class,
                    (client, context) -> arguments.set(context.arguments()))) {
                final Cache<String, Object> cache = CacheFactory.createCache("RedisCluster(seed:7000,\"" + token + "\")");
                try {
                    assertEquals(isSeed ? "seed:7000," + token : "seed:7000", arguments.get().get(0));
                    assertEquals(DistributedCacheClient.DEFAULT_TIMEOUT, arguments.get().get(1));
                    assertEquals((isSeed ? "" : token) + "a2V5", ((DistributedCache<String, Object>) cache).generateKey("key"));
                } finally {
                    cache.close();
                }
                Mockito.verify(clients.constructed().get(0)).disconnect();
            }
        }

        @Test
        void rejectsExtraOptionsAfterTheKeyPrefix() {
            final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> CacheFactory.createCache("RedisCluster(seed:7000,prefix:,other:7002,last:7003)"));
            assertTrue(failure.getMessage().contains("Unsupported parameters"));
        }

        @Test
        void rejectsMalformedEarlierSeedBeforeAnExplicitPrefixAndTimeout() {
            final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> CacheFactory.createCache("RedisCluster(seed:7000,invalid,prefix:,1000)"));
            assertTrue(failure.getMessage().contains("seed"));
        }

        @ParameterizedTest
        @ValueSource(strings = { " ", "[a b]:7002" })
        void rejectsWhitespacePrefixesAfterClassifyingMalformedEndpoints(final String token) {
            try (MockedConstruction<JRedisCluster> clients = Mockito.mockConstruction(JRedisCluster.class)) {
                assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("RedisCluster(seed:7000,\"" + token + "\")"));
                Mockito.verify(clients.constructed().get(0)).disconnect();
            }
        }

    }

    @Nested
    class ProviderValidation {

        @ParameterizedTest
        @NullAndEmptySource
        @ValueSource(strings = { " ", "\t" })
        void blankProviderIsRejected(final String provider) {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache(provider));
        }

        /**
         * A provider specification with an empty class name (the text before the parenthesis) is rejected
         * with an {@link IllegalArgumentException} naming the missing class name. Since abacus-common
         * 7.8.8, {@code TypeAttrParser.parse} rejects the missing class name itself ("Malformed type
         * attribute: missing class name"); {@code createCache}'s own "class name cannot be empty" branch
         * remains as defense should the parser's contract change again.
         */
        @Test
        void createCacheEmptyClassName() {
            final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("(localhost:11211)"));
            assertTrue(ex.getMessage() != null && (ex.getMessage().contains("missing class name") || ex.getMessage().contains("class name cannot be empty")),
                    "expected a missing/empty class name message but was: " + ex.getMessage());
        }

        /**
         * A non-empty parameter list whose first (server-URL) parameter is empty is rejected with the
         * "server URL cannot be empty" message. {@code Memcached(,prefix:)} parses to a two-element
         * parameter array with an empty first element (distinct from {@code Memcached()}, which has no
         * parameters at all).
         */
        @Test
        void createCacheEmptyServerUrlWithTrailingParam() {
            final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Memcached(,prefix:)"));
            assertTrue(ex.getMessage() != null && ex.getMessage().contains("server URL cannot be empty"),
                    "expected a 'server URL cannot be empty' message but was: " + ex.getMessage());
        }

        @Test
        void createCacheEmptyUrl() {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Memcached()"));
        }

        /**
         * A quoted DSL argument keeps its surrounding whitespace (unquoted ones are stripped by the
         * parser). The timeout predicate classifies the trimmed token, so the timeout parser must parse
         * that same trimmed token instead of rejecting a token it had just recognized as a timeout.
         */
        @Test
        void createCacheQuotedTimeoutWithSurroundingWhitespaceIsParsedLikeThePredicateSeesIt() throws ReflectiveOperationException {
            final Field timeoutField = SpyMemcached.class.getDeclaredField("operationTimeoutMillis");
            timeoutField.setAccessible(true);

            try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
                for (final String provider : new String[] { "Memcached(localhost:11211,prefix:,\" 5000 \")", "Memcached(localhost:11211,\" 5000\")" }) {
                    final Cache<String, Object> cache = CacheFactory.createCache(provider);
                    try {
                        assertEquals(5000L, timeoutField.getLong(distributedClient(cache)), provider);
                    } finally {
                        cache.close();
                    }
                }
            }
        }

        /**
         * Malformed DSL (an unbalanced parenthesis) must be reported as the documented
         * IllegalArgumentException rather than leaking a low-level parser exception such as
         * StringIndexOutOfBoundsException.
         */
        @Test
        void createCacheUnbalancedParenthesis() {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Memcached(localhost:11211,app:"));
        }

        @Test
        void createCacheUnparsableProvider() {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("not a valid spec"));
        }

        @ParameterizedTest
        @ValueSource(strings = { "Memcached", "Redis", "RedisCluster" })
        void extraParametersAreRejectedBeforeClientConstruction(final String provider) {
            try (MockedConstruction<SpyMemcached> memcached = Mockito.mockConstruction(SpyMemcached.class);
                 MockedConstruction<JRedis> redis = Mockito.mockConstruction(JRedis.class);
                 MockedConstruction<JRedisCluster> cluster = Mockito.mockConstruction(JRedisCluster.class)) {
                assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache(provider + "(127.0.0.1:7000,prefix:,1000,extra)"));
                assertTrue(memcached.constructed().isEmpty());
                assertTrue(redis.constructed().isEmpty());
                assertTrue(cluster.constructed().isEmpty());
            }
        }

        @ParameterizedTest(name = "{0}: timeout={1}")
        @MethodSource("com.landawn.abacus.cache.CacheFactoryTest#invalidTimeouts")
        void invalidTimeoutIsRejectedBeforeClientConstruction(final String provider, final String timeout) {
            try (MockedConstruction<SpyMemcached> memcached = Mockito.mockConstruction(SpyMemcached.class);
                 MockedConstruction<JRedis> redis = Mockito.mockConstruction(JRedis.class);
                 MockedConstruction<JRedisCluster> cluster = Mockito.mockConstruction(JRedisCluster.class)) {
                final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                        () -> CacheFactory.createCache(provider + "(127.0.0.1:7000,prefix:," + timeout + ")"));

                assertTrue(failure.getMessage().contains("timeout"), failure.getMessage());
                assertTrue(memcached.constructed().isEmpty());
                assertTrue(redis.constructed().isEmpty());
                assertTrue(cluster.constructed().isEmpty());
            }
        }

        @Test
        void parserFailuresAreReportedAsProviderConfigurationErrors() {
            final IndexOutOfBoundsException parserFailure = new IndexOutOfBoundsException("parser failed");
            try (MockedStatic<TypeAttrParser> parser = Mockito.mockStatic(TypeAttrParser.class)) {
                parser.when(() -> TypeAttrParser.parse("broken")).thenThrow(parserFailure);
                final IllegalArgumentException wrapped = assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("broken"));
                assertSame(parserFailure, wrapped.getCause());
                assertTrue(wrapped.getMessage().contains("Failed to parse provider specification"));

                parser.when(() -> TypeAttrParser.parse("no-result")).thenReturn(null);
                assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("no-result"));

                final TypeAttrParser unnamed = Mockito.mock(TypeAttrParser.class);
                Mockito.when(unnamed.getClassName()).thenReturn("");
                parser.when(() -> TypeAttrParser.parse("unnamed")).thenReturn(unnamed);
                assertTrue(assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("unnamed")).getMessage()
                        .contains("class name cannot be empty"));
            }
        }

    }

    @Nested
    class CustomProviders {

        @Test
        void contextClassLoaderCanSupplyAnApplicationProvider(@TempDir final Path directory) throws Exception {
            final Path javaFile = directory.resolve("ContextOnlyCache.java");
            Files.writeString(javaFile, """
                    package isolated;
                    public final class ContextOnlyCache extends com.landawn.abacus.cache.LocalCache<String, Object> {
                        public ContextOnlyCache() { super(4, 0); }
                    }
                    """);
            final String classPath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
            assertEquals(0, ToolProvider.getSystemJavaCompiler()
                    .run(null, null, null, "-proc:none", "-classpath", classPath, "-d", directory.toString(), javaFile.toString()));
            final ClassLoader previous = Thread.currentThread().getContextClassLoader();
            try (URLClassLoader applicationLoader = new URLClassLoader(new URL[] { directory.toUri().toURL() }, CacheFactory.class.getClassLoader())) {
                Thread.currentThread().setContextClassLoader(applicationLoader);
                final Cache<String, Object> cache = CacheFactory.createCache("isolated.ContextOnlyCache()");
                try {
                    assertSame(applicationLoader, cache.getClass().getClassLoader());
                    assertTrue(cache.put("key", "value"));
                    assertEquals("value", cache.getOrNull("key"));
                } finally {
                    cache.close();
                }
            } finally {
                Thread.currentThread().setContextClassLoader(previous);
            }
        }

        /**
         * Regression: an abstract custom provider with a matching constructor was reflectively invoked and
         * failed with a plain RuntimeException wrapping InstantiationException, unlike every other
         * unusable-class configuration error, which is reported as IllegalArgumentException.
         */
        @Test
        void createCacheAbstractOrInterfaceCustomClassRejectedWithIllegalArgumentException() {
            for (final String provider : new String[] { AbstractProviderCache.class.getName() + "(localhost:9999)",
                    AbstractProviderCache.class.getName() + "()", Cache.class.getName() + "()", Cache.class.getName() + "(localhost:9999)" }) {
                final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache(provider), provider);
                assertTrue(ex.getMessage().contains("must be a concrete class"), provider + ": " + ex.getMessage());
            }
        }

        /**
         * A class-not-found custom provider must surface this method's documented "Cannot find class"
         * IllegalArgumentException. {@code ClassUtil.forName} throws (rather than returning null) for a
         * missing class, so the bespoke message is produced by wrapping that exception.
         */
        @Test
        void createCacheClassNotFoundHasDescriptiveMessage() {
            final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> CacheFactory.createCache("com.example.NonExistentCache(localhost)"));
            assertTrue(ex.getMessage() != null && ex.getMessage().contains("Cannot find class"),
                    "expected a 'Cannot find class' message but was: " + ex.getMessage());
        }

        /**
         * A custom provider class that implements {@link Cache} and exposes a {@code (String)} constructor
         * is resolved and instantiated through the reflective custom-class branch of {@code createCache}.
         */
        @Test
        void createCacheCustomClassImplementingCache() {
            final Cache<String, Object> cache = CacheFactory.createCache(DummyProviderCache.class.getName() + "(localhost:9999)");
            try {
                assertNotNull(cache);
                assertTrue(cache instanceof DummyProviderCache);
                assertEquals("localhost:9999", ((DummyProviderCache<String, Object>) cache).serverUrl());
                assertTrue(cache.put("k", "v"));
                assertEquals("v", cache.getOrNull("k"));
            } finally {
                cache.close();
            }
        }

        @Test
        void createCacheCustomClassMustImplementCache() {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("java.lang.String(localhost)"));
        }

        /**
         * Regression test for the custom-class validation gating bug.
         *
         * <p>Before the fix, the "missing parameters" / "server URL cannot be empty" validations (which
         * only make sense for the built-in Memcached/Redis providers) ran before the class-name dispatch,
         * so a perfectly valid custom {@link Cache} with a no-arg constructor — e.g.
         * {@code "com.example.MyCache()"} — was rejected with "missing parameters". The validations now
         * apply only to the built-in providers.
         */
        @Test
        void createCacheCustomClassWithNoArgConstructor() {
            final Cache<String, Object> cache = CacheFactory.createCache(DummyProviderCache.class.getName() + "()");
            try {
                assertNotNull(cache);
                assertTrue(cache instanceof DummyProviderCache);
                assertTrue(cache.put("k", "v"));
                assertEquals("v", cache.getOrNull("k"));
            } finally {
                cache.close();
            }
        }

        /**
         * Regression: a nested custom provider named by its canonical (dot-separated) name, which
         * {@code ClassUtil.forName} used to resolve and {@code TypeAttrParser} itself produces for
         * {@code Owner<...>.Member} specifications, was reported as "Cannot find class" once class
         * loading switched to the non-initializing {@code Class.forName}, which accepts only binary names.
         */
        @Test
        void createCacheNestedCustomClassByCanonicalOrBinaryName() {
            for (final String className : new String[] { NestedProviderCache.class.getCanonicalName(), NestedProviderCache.class.getName() }) {
                final Cache<String, Object> cache = CacheFactory.createCache(className + "(localhost:9999)");
                try {
                    assertTrue(cache instanceof NestedProviderCache, className);
                    assertEquals("localhost:9999", ((NestedProviderCache<String, Object>) cache).serverUrl());
                } finally {
                    cache.close();
                }
            }

            // The canonical-name retry must stay non-initializing and keep the type check authoritative.
            nonCacheProviderInitialized = false;
            final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> CacheFactory.createCache(NonCacheProviderWithInitializer.class.getCanonicalName() + "()"));
            assertTrue(ex.getMessage().contains("must implement Cache"), ex.getMessage());
            assertFalse(nonCacheProviderInitialized, "type validation must not run an invalid provider's static initializer");

            // A name that matches no spelling still reports the name exactly as given.
            final IllegalArgumentException missing = assertThrows(IllegalArgumentException.class,
                    () -> CacheFactory.createCache(CacheFactoryTest.class.getName() + ".NoSuchNestedCache(localhost:9999)"));
            assertTrue(missing.getMessage().contains("Cannot find class: " + CacheFactoryTest.class.getName() + ".NoSuchNestedCache"), missing.getMessage());
        }

        /**
         * Resolving an invalid custom provider must not execute arbitrary static initialization before
         * the factory has established that the type implements {@link Cache}. Class literals load but
         * do not initialize their class, so the probe remains deterministic until the factory call.
         */
        @Test
        void createCacheNonCacheClassIsRejectedWithoutInitialization() {
            nonCacheProviderInitialized = false;
            final String provider = NonCacheProviderWithInitializer.class.getName() + "()";

            final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache(provider));

            assertTrue(ex.getMessage().contains("must implement Cache"));
            assertFalse(nonCacheProviderInitialized, "type validation must not run an invalid provider's static initializer");
        }

        /**
         * Regression: a custom provider name that differs from the real class only in letter case must be
         * reported as the documented "Cannot find class" IllegalArgumentException. On a case-insensitive
         * file system (Windows, default macOS) the class loader finds the differently-cased class file and
         * rejects it with {@code NoClassDefFoundError (wrong name: ...)}, an Error that used to escape
         * createCache, also from the canonical nested-name retry. A case-sensitive file system reports a
         * plain ClassNotFoundException, so the assertions hold on every platform.
         */
        @Test
        void createCacheWrongCaseCustomClassNameIsReportedAsNotFound() {
            final String wrongCaseTopLevel = DummyProviderCache.class.getName().toLowerCase(java.util.Locale.ROOT);
            final String wrongCaseNested = CacheFactoryTest.class.getName() + ".nestedProviderCache";

            for (final String className : new String[] { wrongCaseTopLevel, wrongCaseNested }) {
                final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache(className + "(localhost:9999)"),
                        className);
                assertTrue(ex.getMessage().contains("Cannot find class: " + className), ex.getMessage());
            }
        }

        /**
         * A NoClassDefFoundError from a class loader (the JDK's "wrong name" rejection, or a class whose
         * superclass is missing) is a failure to load the class under the requested name: it must not
         * escape as an Error, and the context class loader's failure is retained like a
         * ClassNotFoundException.
         */
        @Test
        void contextLoaderNoClassDefFoundErrorIsReportedAsClassNotFound() {
            final NoClassDefFoundError loaderFailure = new NoClassDefFoundError("missing/application/Cache (wrong name: missing/application/CACHE)");
            final ClassLoader previous = Thread.currentThread().getContextClassLoader();

            try {
                Thread.currentThread().setContextClassLoader(new ClassLoader(null) {
                    @Override
                    protected Class<?> findClass(final String name) throws ClassNotFoundException {
                        if (name.startsWith("missing.")) {
                            throw loaderFailure;
                        }

                        return super.findClass(name);
                    }
                });

                final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                        () -> CacheFactory.createCache("missing.application.Cache()"));
                assertTrue(failure.getMessage().contains("Cannot find class: missing.application.Cache"), failure.getMessage());
                assertTrue(failure.getCause() instanceof ClassNotFoundException);
                assertEquals(1, failure.getCause().getSuppressed().length);
                assertSame(loaderFailure, failure.getCause().getSuppressed()[0].getCause());
            } finally {
                Thread.currentThread().setContextClassLoader(previous);
            }
        }

        /**
         * Regression: class loaders may reuse one ClassNotFoundException instance for every miss. When
         * that instance carries a cause, keeping a failed '$' spelling as suppressed attempted
         * self-suppression, whose IllegalArgumentException aborted the remaining spellings, so a valid
         * provider named canonically ({@code Outer.Mid.Provider}) could not be found as
         * {@code Outer$Mid$Provider}, and a missing one was reported as "Self-suppression not permitted".
         */
        @Test
        void contextLoaderReusingOneMissInstanceStillResolvesCanonicalNestedName(@TempDir final Path directory) throws Exception {
            final Path javaFile = directory.resolve("Outer.java");
            Files.writeString(javaFile, """
                    package isolated;
                    public final class Outer {
                        public static final class Mid {
                            public static final class Provider extends com.landawn.abacus.cache.LocalCache<String, Object> {
                                public Provider() { super(4, 0); }
                            }
                        }
                    }
                    """);
            final String classPath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
            assertEquals(0, ToolProvider.getSystemJavaCompiler()
                    .run(null, null, null, "-proc:none", "-classpath", classPath, "-d", directory.toString(), javaFile.toString()));
            final ClassNotFoundException reusedMiss = new ClassNotFoundException("not found", new IllegalStateException("loader detail"));
            final ClassLoader previous = Thread.currentThread().getContextClassLoader();

            try (URLClassLoader applicationLoader = new URLClassLoader(new URL[] { directory.toUri().toURL() }, CacheFactory.class.getClassLoader()) {
                @Override
                protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
                    try {
                        return super.loadClass(name, resolve);
                    } catch (final ClassNotFoundException e) {
                        throw reusedMiss;
                    }
                }
            }) {
                Thread.currentThread().setContextClassLoader(applicationLoader);

                final Cache<String, Object> cache = CacheFactory.createCache("isolated.Outer.Mid.Provider()");
                try {
                    assertEquals("isolated.Outer$Mid$Provider", cache.getClass().getName());
                } finally {
                    cache.close();
                }

                final IllegalArgumentException missing = assertThrows(IllegalArgumentException.class,
                        () -> CacheFactory.createCache("isolated.Outer.Mid.Missing()"));
                assertTrue(missing.getMessage().contains("Cannot find class: isolated.Outer.Mid.Missing"), missing.getMessage());
                assertEquals(0, reusedMiss.getSuppressed().length, "a reused miss must never suppress itself");
            } finally {
                Thread.currentThread().setContextClassLoader(previous);
            }
        }

        /**
         * A NoClassDefFoundError raised for an alternate '$' spelling of a canonical nested name means a
         * class file was found but could not be loaded. It must survive as a suppressed exception on the
         * exact-name failure instead of being discarded with the plain misses of the other spellings.
         */
        @Test
        void alternateSpellingLoadFailureIsKeptAsSuppressed() {
            final NoClassDefFoundError nestedFailure = new NoClassDefFoundError(
                    "missing/application/Outer$Inner (wrong name: missing/application/OUTER$Inner)");
            final ClassLoader previous = Thread.currentThread().getContextClassLoader();

            try {
                Thread.currentThread().setContextClassLoader(new ClassLoader(null) {
                    @Override
                    protected Class<?> findClass(final String name) throws ClassNotFoundException {
                        if (name.equals("missing.application.Outer$Inner")) {
                            throw nestedFailure;
                        }

                        return super.findClass(name);
                    }
                });

                final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                        () -> CacheFactory.createCache("missing.application.Outer.Inner()"));
                assertTrue(failure.getMessage().contains("Cannot find class: missing.application.Outer.Inner"), failure.getMessage());
                final Throwable contextFailure = failure.getCause().getSuppressed()[0];
                assertEquals(1, contextFailure.getSuppressed().length, "only the spelling that found a class file is kept");
                assertSame(nestedFailure, contextFailure.getSuppressed()[0].getCause());
            } finally {
                Thread.currentThread().setContextClassLoader(previous);
            }
        }

        @ParameterizedTest
        @ValueSource(booleans = { false, true })
        void missingCustomClassRetainsTheContextLoaderFailure(final boolean hasContextLoader) {
            final ClassLoader previous = Thread.currentThread().getContextClassLoader();
            try {
                Thread.currentThread().setContextClassLoader(hasContextLoader ? new ClassLoader(null) {
                } : null);
                final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                        () -> CacheFactory.createCache("missing.application.Cache()"));
                assertTrue(failure.getCause() instanceof ClassNotFoundException);
                assertEquals(hasContextLoader ? 1 : 0, failure.getCause().getSuppressed().length);
            } finally {
                Thread.currentThread().setContextClassLoader(previous);
            }
        }

        // Validation: null/empty provider string

    }

    private static Stream<Arguments> invalidTimeouts() {
        return Stream.of("Memcached", "Redis", "RedisCluster")
                .flatMap(provider -> Stream.of("", "abc", "0", "-1", "99999999999999999999", "0x1F4", "5000L", "12/")
                        .map(timeout -> Arguments.of(provider, timeout)));
    }

    private static final class NonCacheProviderWithInitializer {
        static {
            nonCacheProviderInitialized = true;
        }
    }

    @SuppressWarnings("unchecked")
    private static DistributedCacheClient<?> distributedClient(final Cache<?, ?> cache) throws ReflectiveOperationException {
        final Field clientField = DistributedCache.class.getDeclaredField("client");
        clientField.setAccessible(true);

        return (DistributedCacheClient<?>) clientField.get(cache);
    }

    /** A concrete custom provider declared as a nested class. */
    public static class NestedProviderCache<K, V> extends DummyProviderCache<K, V> {
        public NestedProviderCache(final String serverUrl) {
            super(serverUrl);
        }
    }

    /** An abstract {@link Cache} that declares a matching {@code (String)} constructor but cannot be instantiated. */
    public abstract static class AbstractProviderCache<K, V> extends DummyProviderCache<K, V> {
        public AbstractProviderCache(final String serverUrl) {
            super(serverUrl);
        }
    }
}
