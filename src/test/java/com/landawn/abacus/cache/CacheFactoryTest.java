/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.landawn.abacus.pool.KeyedObjectPool;
import com.landawn.abacus.pool.PoolFactory;
import com.landawn.abacus.pool.PoolableAdapter;
import com.landawn.abacus.util.Suppliers;

import net.spy.memcached.MemcachedClient;

import redis.clients.jedis.RedisClient;
import redis.clients.jedis.RedisClusterClient;
import redis.clients.jedis.providers.ClusterConnectionProvider;

class CacheFactoryTest {

    private static boolean nonCacheProviderInitialized;

    @Nested
    class LocalCaches {

        // Two-arg createLocalCache
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

        // Four-arg createLocalCache
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

        // createLocalCache with KeyedObjectPool
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
    }

    @Nested
    class DistributedCaches {

        // createDistributedCache with full circuit breaker configuration
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

        // createDistributedCache one-arg overload
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

        // createDistributedCache with key prefix
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

        // createCache: Memcached with one parameter
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
        void createCacheMemcachedBlankTimeoutRejected() {
            // A trailing comma yields an empty third parameter. The strict decimal-token check must
            // reject it ("Invalid timeout parameter") rather than silently constructing a client with a
            // zero or default timeout.
            try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
                assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Memcached(localhost:11211,prefix:,)"));
            }
        }

        @Test
        void createCacheMemcachedNonNumericTimeout() {
            try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
                assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Memcached(localhost:11211,prefix:,abc)"));
            }
        }

        @Test
        void createCacheMemcachedNonPositiveTimeout() {
            try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
                assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Memcached(localhost:11211,prefix:,0)"));
            }
        }

        @Test
        void createCacheMemcachedOverflowTimeout() {
            // A numeric token that overflows long makes Numbers.toLong throw ArithmeticException
            // (not NumberFormatException). The factory must still surface it as the documented
            // IllegalArgumentException rather than leaking the raw ArithmeticException.
            try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
                assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Memcached(localhost:11211,prefix:,99999999999999999999)"));
            }
        }

        @Test
        void createCacheMemcachedTooManyParameters() {
            try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
                assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Memcached(a,b,1000,extra)"));
            }
        }

        // Memcached with prefix
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

        // Memcached with prefix and explicit timeout
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
    }

    @Nested
    class RedisProviders {

        // Redis
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

        @Test
        void createCacheRedisNonNumericTimeout() {
            try (MockedConstruction<RedisClient> ctorIntercept = Mockito.mockConstruction(RedisClient.class)) {
                assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Redis(localhost:6379,p:,xyz)"));
            }
        }

        @Test
        void createCacheRedisNonPositiveTimeout() {
            try (MockedConstruction<RedisClient> ctorIntercept = Mockito.mockConstruction(RedisClient.class)) {
                assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Redis(localhost:6379,p:,0)"));
            }
        }

        @Test
        void createCacheRedisTooManyParameters() {
            try (MockedConstruction<RedisClient> ctorIntercept = Mockito.mockConstruction(RedisClient.class)) {
                assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Redis(a,b,1000,extra)"));
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

        // Redis Cluster
        //
        // RedisClusterClient eagerly discovers the cluster topology while it is being built (inside the
        // ClusterConnectionProvider constructor), so the happy-path tests must intercept BOTH the provider
        // construction (to suppress the network call) and the client construction (to return a mock).
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

        @Test
        void createCacheRedisClusterNonNumericTimeout() {
            // "Invalid timeout parameter" is thrown by the RedisCluster branch (not the class-loading
            // fallback), which proves the keyword routed to JRedisCluster.
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> CacheFactory.createCache("RedisCluster(10.0.0.1:7000,p:,xyz)"));
            assertTrue(e.getMessage().contains("Invalid timeout parameter"));
        }

        @Test
        void createCacheRedisClusterNonPositiveTimeout() {
            // Validation throws before the cluster client is built; the provider mock is a safety net.
            try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class)) {
                assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("RedisCluster(10.0.0.1:7000,p:,0)"));
            }
        }

        @Test
        void createCacheRedisClusterOverflowTimeout() {
            // The RedisCluster branch routes through parseRedisClusterParameters -> parseTimeoutParameter,
            // so an overflowing numeric timeout must also surface as IllegalArgumentException ("Invalid
            // timeout parameter") rather than a raw ArithmeticException.
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> CacheFactory.createCache("RedisCluster(10.0.0.1:7000,p:,99999999999999999999)"));
            assertTrue(e.getMessage().contains("Invalid timeout parameter"));
        }

        @Test
        void createCacheRedisClusterTooManyParameters() {
            try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class)) {
                assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("RedisCluster(a,b,1000,extra)"));
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
    }

    @Nested
    class CustomProviders {

        /**
         * Regression: an abstract custom provider with a matching constructor was reflectively invoked and
         * failed with a plain RuntimeException wrapping InstantiationException, unlike every other
         * unusable-class configuration error, which is reported as IllegalArgumentException.
         */
        @Test
        void createCacheAbstractOrInterfaceCustomClassRejectedWithIae() {
            for (final String provider : new String[] { AbstractProviderCache.class.getName() + "(localhost:9999)",
                    AbstractProviderCache.class.getName() + "()", Cache.class.getName() + "()", Cache.class.getName() + "(localhost:9999)" }) {
                final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache(provider), provider);
                assertTrue(ex.getMessage().contains("must be a concrete class"), provider + ": " + ex.getMessage());
            }
        }

        @Test
        void createCacheClassNotFound() {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("com.example.NonExistentCache(localhost)"));
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

        @Test
        void createCacheEmptyProvider() {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache(""));
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

        /** Timeout tokens are strictly decimal; Numbers.toLong's hex/octal/trailing-L forms are rejected. */
        @Test
        void createCacheNonDecimalTimeoutRejected() {
            final IllegalArgumentException hex = assertThrows(IllegalArgumentException.class,
                    () -> CacheFactory.createCache("RedisCluster(10.0.0.1:7000,p:,0x1F4)"));
            assertTrue(hex.getMessage().contains("Invalid timeout parameter"));

            final IllegalArgumentException trailingL = assertThrows(IllegalArgumentException.class,
                    () -> CacheFactory.createCache("Redis(localhost:6379,p:,5000L)"));
            assertTrue(trailingL.getMessage().contains("Invalid timeout parameter"));
        }

        // Validation: null/empty provider string
        @Test
        void createCacheNullProvider() {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache(null));
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
