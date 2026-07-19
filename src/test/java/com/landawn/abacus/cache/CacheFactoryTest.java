/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.landawn.abacus.TestBase;
import com.landawn.abacus.pool.KeyedObjectPool;
import com.landawn.abacus.pool.PoolFactory;
import com.landawn.abacus.pool.PoolableAdapter;
import com.landawn.abacus.util.IntFunctions;
import com.landawn.abacus.util.Suppliers;

import net.spy.memcached.MemcachedClient;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.RedisClusterClient;
import redis.clients.jedis.providers.ClusterConnectionProvider;

@Tag("2025")
public class CacheFactoryTest extends TestBase {

    private static boolean nonCacheProviderInitialized;

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

    // Two-arg createLocalCache
    @Test
    public void testCreateLocalCache() {
        try (LocalCache<String, String> cache = CacheFactory.createLocalCache(100, 0)) {
            assertNotNull(cache);
            assertTrue(cache.put("k", "v"));
            assertEquals("v", cache.getOrNull("k"));
        }
    }

    @Test
    public void testCreateLocalCache_EdgeCase_InvalidCapacity() {
        assertThrows(IllegalArgumentException.class, () -> CacheFactory.createLocalCache(0, 0));
    }

    // Four-arg createLocalCache
    @Test
    public void testCreateLocalCache_FourArg() {
        try (LocalCache<String, String> cache = CacheFactory.createLocalCache(100, 0, 60000L, 30000L)) {
            assertNotNull(cache);
            assertTrue(cache.put("a", "b"));
        }
    }

    // createLocalCache with KeyedObjectPool
    @Test
    public void testCreateLocalCache_WithPool() {
        // Use IntFunctions to create the capacity supplier indirectly.
        final IntFunction<KeyedObjectPool<String, PoolableAdapter<String>>> poolFactory = cap -> PoolFactory.createKeyedObjectPool(cap, 0);
        final KeyedObjectPool<String, PoolableAdapter<String>> pool = poolFactory.apply(64);

        try (LocalCache<String, String> cache = CacheFactory.createLocalCache(pool, 60000L, 30000L)) {
            assertNotNull(cache);
            assertTrue(cache.put("x", "y"));
            assertEquals("y", cache.getOrNull("x"));
        }
    }

    @Test
    public void testCreateLocalCache_WithPool_EdgeCase_NullPool() {
        assertThrows(IllegalArgumentException.class, () -> CacheFactory.createLocalCache((KeyedObjectPool<String, PoolableAdapter<String>>) null, 0L, 0L));
    }

    // createDistributedCache one-arg overload
    @Test
    public void testCreateDistributedCache_OneArg() {
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
    public void testCreateDistributedCache_TwoArg() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            final SpyMemcached<Object> dcc = new SpyMemcached<>("localhost:11211");
            final DistributedCache<String, Object> dc = CacheFactory.createDistributedCache(dcc, "myapp:");
            assertNotNull(dc);
            dc.close();
        }
    }

    // createDistributedCache with full circuit breaker configuration
    @Test
    public void testCreateDistributedCache_FourArg() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            final SpyMemcached<Object> dcc = new SpyMemcached<>("localhost:11211");
            final DistributedCache<String, Object> dc = CacheFactory.createDistributedCache(dcc, "p:", 50, 2000);
            assertNotNull(dc);
            dc.close();
        }
    }

    // createCache: Memcached with one parameter
    @Test
    public void testCreateCache_Memcached() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            try (Cache<String, Object> cache = CacheFactory.createCache("Memcached(localhost:11211)")) {
                assertNotNull(cache);
                assertTrue(cache instanceof DistributedCache);
            }
        }
    }

    // Memcached with prefix
    @Test
    public void testCreateCache_MemcachedWithPrefix() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            try (Cache<String, Object> cache = CacheFactory.createCache("Memcached(localhost:11211,prefix:)")) {
                assertNotNull(cache);
            }
        }
    }

    // Memcached with prefix and explicit timeout
    @Test
    public void testCreateCache_MemcachedWithTimeout() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            try (Cache<String, Object> cache = CacheFactory.createCache("Memcached(localhost:11211,prefix:,5000)")) {
                assertNotNull(cache);
            }
        }
    }

    @Test
    public void testCreateCache_Memcached_EdgeCase_NonPositiveTimeout() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Memcached(localhost:11211,prefix:,0)"));
        }
    }

    @Test
    public void testCreateCache_Memcached_EdgeCase_NonNumericTimeout() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Memcached(localhost:11211,prefix:,abc)"));
        }
    }

    @Test
    public void testCreateCache_Memcached_EdgeCase_BlankTimeoutRejected() {
        // A trailing comma yields a blank third parameter. Numbers.toLong("") silently returns 0,
        // so the factory must still reject it via the "timeout must be positive" guard rather than
        // silently constructing a client with a zero timeout.
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Memcached(localhost:11211,prefix:,)"));
        }
    }

    @Test
    public void testCreateCache_Memcached_EdgeCase_OverflowTimeout() {
        // A numeric token that overflows long makes Numbers.toLong throw ArithmeticException
        // (not NumberFormatException). The factory must still surface it as the documented
        // IllegalArgumentException rather than leaking the raw ArithmeticException.
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Memcached(localhost:11211,prefix:,99999999999999999999)"));
        }
    }

    @Test
    public void testCreateCache_Memcached_EdgeCase_TooManyParameters() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Memcached(a,b,1000,extra)"));
        }
    }

    // Redis
    @Test
    public void testCreateCache_Redis() {
        try (MockedConstruction<RedisClient> ctorIntercept = Mockito.mockConstruction(RedisClient.class)) {
            try (Cache<String, Object> cache = CacheFactory.createCache("Redis(localhost:6379)")) {
                assertNotNull(cache);
                assertTrue(cache instanceof DistributedCache);
            }
        }
    }

    @Test
    public void testCreateCache_RedisWithPrefix() {
        try (MockedConstruction<RedisClient> ctorIntercept = Mockito.mockConstruction(RedisClient.class)) {
            try (Cache<String, Object> cache = CacheFactory.createCache("Redis(localhost:6379,prefix:)")) {
                assertNotNull(cache);
            }
        }
    }

    @Test
    public void testCreateCache_RedisWithTimeout() {
        try (MockedConstruction<RedisClient> ctorIntercept = Mockito.mockConstruction(RedisClient.class)) {
            try (Cache<String, Object> cache = CacheFactory.createCache("Redis(localhost:6379,prefix:,5000)")) {
                assertNotNull(cache);
            }
        }
    }

    @Test
    public void testCreateCache_Redis_EdgeCase_NonPositiveTimeout() {
        try (MockedConstruction<RedisClient> ctorIntercept = Mockito.mockConstruction(RedisClient.class)) {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Redis(localhost:6379,p:,0)"));
        }
    }

    @Test
    public void testCreateCache_Redis_EdgeCase_NonNumericTimeout() {
        try (MockedConstruction<RedisClient> ctorIntercept = Mockito.mockConstruction(RedisClient.class)) {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Redis(localhost:6379,p:,xyz)"));
        }
    }

    @Test
    public void testCreateCache_Redis_EdgeCase_TooManyParameters() {
        try (MockedConstruction<RedisClient> ctorIntercept = Mockito.mockConstruction(RedisClient.class)) {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Redis(a,b,1000,extra)"));
        }
    }

    // Redis Cluster
    //
    // RedisClusterClient eagerly discovers the cluster topology while it is being built (inside the
    // ClusterConnectionProvider constructor), so the happy-path tests must intercept BOTH the provider
    // construction (to suppress the network call) and the client construction (to return a mock).
    @Test
    public void testCreateCache_RedisCluster() {
        try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class);
             MockedConstruction<RedisClusterClient> clientIntercept = Mockito.mockConstruction(RedisClusterClient.class)) {
            try (Cache<String, Object> cache = CacheFactory.createCache("RedisCluster(10.0.0.1:7000)")) {
                assertNotNull(cache);
                assertTrue(cache instanceof DistributedCache);
            }
        }
    }

    @Test
    public void testCreateCache_RedisClusterWithMultipleSeedNodes() throws ReflectiveOperationException {
        try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class);
             MockedConstruction<RedisClusterClient> clientIntercept = Mockito.mockConstruction(RedisClusterClient.class)) {
            try (Cache<String, Object> cache = CacheFactory.createCache("RedisCluster(10.0.0.1:7000,10.0.0.2:7000)")) {
                assertNotNull(cache);
                assertTrue(cache instanceof DistributedCache);
                assertEquals("10.0.0.1:7000,10.0.0.2:7000", distributedClient(cache).serverUrl());
            }
        }
    }

    /**
     * Regression: a later Redis Cluster seed may be an ordinary alphabetic DNS hostname. The
     * parser previously required a dot, digit, {@code localhost}, or bracketed IPv6 address in
     * later seed tokens, so {@code primary:6379} was silently treated as the cache key prefix.
     */
    @Test
    public void testCreateCache_RedisClusterWithAlphabeticSeedHostname() throws ReflectiveOperationException {
        try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class);
             MockedConstruction<RedisClusterClient> clientIntercept = Mockito.mockConstruction(RedisClusterClient.class)) {
            try (Cache<String, Object> cache = CacheFactory.createCache("RedisCluster(10.0.0.1:7000,primary:6379)")) {
                assertEquals("10.0.0.1:7000,primary:6379", distributedClient(cache).serverUrl());
            }
        }
    }

    @Test
    public void testCreateCache_RedisClusterWithAlphabeticSeedHostnameAndPrefix() throws ReflectiveOperationException {
        try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class);
             MockedConstruction<RedisClusterClient> clientIntercept = Mockito.mockConstruction(RedisClusterClient.class)) {
            try (Cache<String, Object> cache = CacheFactory.createCache("RedisCluster(10.0.0.1:7000,primary:6379,prefix:)")) {
                assertEquals("10.0.0.1:7000,primary:6379", distributedClient(cache).serverUrl());

                final Field prefixField = DistributedCache.class.getDeclaredField("keyPrefix");
                prefixField.setAccessible(true);
                assertEquals("prefix:", prefixField.get(cache));
            }
        }
    }

    @Test
    public void testCreateCache_RedisClusterWithNumericLeadingPrefix() throws ReflectiveOperationException {
        try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class);
             MockedConstruction<RedisClusterClient> clientIntercept = Mockito.mockConstruction(RedisClusterClient.class)) {
            try (Cache<String, Object> cache = CacheFactory.createCache("RedisCluster(10.0.0.1:7000,primary:6379,123prefix)")) {
                assertEquals("10.0.0.1:7000,primary:6379", distributedClient(cache).serverUrl());

                final Field prefixField = DistributedCache.class.getDeclaredField("keyPrefix");
                prefixField.setAccessible(true);
                assertEquals("123prefix", prefixField.get(cache));
            }
        }
    }

    @Test
    public void testCreateCache_RedisClusterWithMultipleSeedNodesPrefixAndTimeout() throws ReflectiveOperationException {
        try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class);
             MockedConstruction<RedisClusterClient> clientIntercept = Mockito.mockConstruction(RedisClusterClient.class)) {
            try (Cache<String, Object> cache = CacheFactory.createCache("RedisCluster(10.0.0.1:7000,10.0.0.2:7000,prefix:,5000)")) {
                assertNotNull(cache);
                assertTrue(cache instanceof DistributedCache);
                assertEquals("10.0.0.1:7000,10.0.0.2:7000", distributedClient(cache).serverUrl());
            }
        }
    }

    @Test
    public void testCreateCache_RedisClusterWithPrefix() {
        try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class);
             MockedConstruction<RedisClusterClient> clientIntercept = Mockito.mockConstruction(RedisClusterClient.class)) {
            try (Cache<String, Object> cache = CacheFactory.createCache("RedisCluster(10.0.0.1:7000,prefix:)")) {
                assertNotNull(cache);
            }
        }
    }

    @Test
    public void testCreateCache_RedisClusterWithTimeout() {
        try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class);
             MockedConstruction<RedisClusterClient> clientIntercept = Mockito.mockConstruction(RedisClusterClient.class)) {
            try (Cache<String, Object> cache = CacheFactory.createCache("RedisCluster(10.0.0.1:7000,prefix:,5000)")) {
                assertNotNull(cache);
            }
        }
    }

    @Test
    public void testCreateCache_RedisCluster_EdgeCase_NonPositiveTimeout() {
        // Validation throws before the cluster client is built; the provider mock is a safety net.
        try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class)) {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("RedisCluster(10.0.0.1:7000,p:,0)"));
        }
    }

    @Test
    public void testCreateCache_RedisCluster_EdgeCase_NonNumericTimeout() {
        // "Invalid timeout parameter" is thrown by the RedisCluster branch (not the class-loading
        // fallback), which proves the keyword routed to JRedisCluster.
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("RedisCluster(10.0.0.1:7000,p:,xyz)"));
        assertTrue(e.getMessage().contains("Invalid timeout parameter"));
    }

    @Test
    public void testCreateCache_RedisCluster_EdgeCase_OverflowTimeout() {
        // The RedisCluster branch routes through parseRedisClusterParameters -> parseTimeoutParameter,
        // so an overflowing numeric timeout must also surface as IllegalArgumentException ("Invalid
        // timeout parameter") rather than a raw ArithmeticException.
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> CacheFactory.createCache("RedisCluster(10.0.0.1:7000,p:,99999999999999999999)"));
        assertTrue(e.getMessage().contains("Invalid timeout parameter"));
    }

    @Test
    public void testCreateCache_RedisCluster_EdgeCase_TooManyParameters() {
        try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class)) {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("RedisCluster(a,b,1000,extra)"));
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
    public void testCreateCache_RedisCluster_EdgeCase_EndpointShapedTokenBeforeTimeout_rejectedAsAmbiguous() {
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

    /** The documented escape hatches for both readings of an endpoint-shaped token must work. */
    @Test
    public void testCreateCache_RedisCluster_AmbiguityEscapeHatches() throws ReflectiveOperationException {
        try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class);
             MockedConstruction<RedisClusterClient> clientIntercept = Mockito.mockConstruction(RedisClusterClient.class)) {
            final Field prefixField = DistributedCache.class.getDeclaredField("keyPrefix");
            prefixField.setAccessible(true);

            // Seed intent: all seed nodes space-separated in the first parameter.
            try (Cache<String, Object> cache = CacheFactory.createCache("RedisCluster(10.0.0.1:7000 10.0.0.2:7000,3000)")) {
                assertEquals("10.0.0.1:7000 10.0.0.2:7000", distributedClient(cache).serverUrl());
                assertEquals("", prefixField.get(cache));
            }

            // Prefix intent: a trailing ':' makes the prefix non-endpoint-shaped.
            try (Cache<String, Object> cache = CacheFactory.createCache("RedisCluster(10.0.0.1:7000,tenant:1:,1000)")) {
                assertEquals("10.0.0.1:7000", distributedClient(cache).serverUrl());
                assertEquals("tenant:1:", prefixField.get(cache));
            }
        }
    }

    /**
     * A final all-digit token following nothing but seed nodes is the timeout, not the key prefix:
     * silently binding "3000" as the key namespace would corrupt every generated key while the
     * intended timeout silently stayed at the default.
     */
    @Test
    public void testCreateCache_RedisCluster_FinalNumericTokenIsTimeoutNotPrefix() throws ReflectiveOperationException {
        try (MockedConstruction<ClusterConnectionProvider> providerIntercept = Mockito.mockConstruction(ClusterConnectionProvider.class);
             MockedConstruction<RedisClusterClient> clientIntercept = Mockito.mockConstruction(RedisClusterClient.class)) {
            try (Cache<String, Object> cache = CacheFactory.createCache("RedisCluster(10.0.0.1:7000,3000)")) {
                assertEquals("10.0.0.1:7000", distributedClient(cache).serverUrl());

                final Field prefixField = DistributedCache.class.getDeclaredField("keyPrefix");
                prefixField.setAccessible(true);
                assertEquals("", prefixField.get(cache), "the all-digit token must configure the timeout, not the key prefix");
            }
        }
    }

    /** Same all-digit rule for the standalone Redis two-parameter form. */
    @Test
    public void testCreateCache_Redis_TwoParam_AllDigitSecondParameterIsTimeout() throws ReflectiveOperationException {
        try (Cache<String, Object> cache = CacheFactory.createCache("Redis(localhost:6379,5000)")) {
            assertEquals("localhost:6379", distributedClient(cache).serverUrl());

            final Field prefixField = DistributedCache.class.getDeclaredField("keyPrefix");
            prefixField.setAccessible(true);
            assertEquals("", prefixField.get(cache), "the all-digit token must configure the timeout, not the key prefix");
        }

        // A non-positive all-digit token is a timeout and is rejected as such, not bound as a prefix.
        assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Redis(localhost:6379,-5000)"));
    }

    /** Timeout tokens are strictly decimal; Numbers.toLong's hex/octal/trailing-L forms are rejected. */
    @Test
    public void testCreateCache_EdgeCase_NonDecimalTimeoutRejected() {
        final IllegalArgumentException hex = assertThrows(IllegalArgumentException.class,
                () -> CacheFactory.createCache("RedisCluster(10.0.0.1:7000,p:,0x1F4)"));
        assertTrue(hex.getMessage().contains("Invalid timeout parameter"));

        final IllegalArgumentException trailingL = assertThrows(IllegalArgumentException.class,
                () -> CacheFactory.createCache("Redis(localhost:6379,p:,5000L)"));
        assertTrue(trailingL.getMessage().contains("Invalid timeout parameter"));
    }

    // Validation: null/empty provider string
    @Test
    public void testCreateCache_EdgeCase_NullProvider() {
        assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache(null));
    }

    @Test
    public void testCreateCache_EdgeCase_EmptyProvider() {
        assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache(""));
    }

    @Test
    public void testCreateCache_EdgeCase_UnparsableProvider() {
        assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("not a valid spec"));
    }

    @Test
    public void testCreateCache_EdgeCase_EmptyUrl() {
        assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Memcached()"));
    }

    @Test
    public void testCreateCache_EdgeCase_ClassNotFound() {
        assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("com.example.NonExistentCache(localhost)"));
    }

    /**
     * A class-not-found custom provider must surface this method's documented "Cannot find class"
     * IllegalArgumentException. {@code ClassUtil.forName} throws (rather than returning null) for a
     * missing class, so the bespoke message is produced by wrapping that exception.
     */
    @Test
    public void testCreateCache_ClassNotFound_HasDescriptiveMessage() {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> CacheFactory.createCache("com.example.NonExistentCache(localhost)"));
        assertTrue(ex.getMessage() != null && ex.getMessage().contains("Cannot find class"),
                "expected a 'Cannot find class' message but was: " + ex.getMessage());
    }

    /**
     * Malformed DSL (an unbalanced parenthesis) must be reported as the documented
     * IllegalArgumentException rather than leaking a low-level parser exception such as
     * StringIndexOutOfBoundsException.
     */
    @Test
    public void testCreateCache_EdgeCase_UnbalancedParenthesis() {
        assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Memcached(localhost:11211,app:"));
    }

    @Test
    public void testCreateCache_EdgeCase_CustomClassMustImplementCache() {
        assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("java.lang.String(localhost)"));
    }

    /**
     * Resolving an invalid custom provider must not execute arbitrary static initialization before
     * the factory has established that the type implements {@link Cache}. Class literals load but
     * do not initialize their class, so the probe remains deterministic until the factory call.
     */
    @Test
    public void testCreateCache_NonCacheClassIsRejectedWithoutInitialization() {
        nonCacheProviderInitialized = false;
        final String provider = NonCacheProviderWithInitializer.class.getName() + "()";

        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache(provider));

        assertTrue(ex.getMessage().contains("must implement Cache"));
        assertFalse(nonCacheProviderInitialized, "type validation must not run an invalid provider's static initializer");
    }

    /**
     * A non-empty parameter list whose first (server-URL) parameter is empty is rejected with the
     * "server URL cannot be empty" message. {@code Memcached(,prefix:)} parses to a two-element
     * parameter array with an empty first element (distinct from {@code Memcached()}, which has no
     * parameters at all).
     */
    @Test
    public void testCreateCache_EdgeCase_EmptyServerUrlWithTrailingParam() {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Memcached(,prefix:)"));
        assertTrue(ex.getMessage() != null && ex.getMessage().contains("server URL cannot be empty"),
                "expected a 'server URL cannot be empty' message but was: " + ex.getMessage());
    }

    /**
     * A provider specification with an empty class name (the text before the parenthesis) is rejected
     * with an {@link IllegalArgumentException} naming the missing class name. Since abacus-common
     * 7.8.8, {@code TypeAttrParser.parse} rejects the missing class name itself ("Malformed type
     * attribute: missing class name"); {@code createCache}'s own "class name cannot be empty" branch
     * remains as defense should the parser's contract change again.
     */
    @Test
    public void testCreateCache_EdgeCase_EmptyClassName() {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("(localhost:11211)"));
        assertTrue(ex.getMessage() != null && (ex.getMessage().contains("missing class name") || ex.getMessage().contains("class name cannot be empty")),
                "expected a missing/empty class name message but was: " + ex.getMessage());
    }

    /**
     * A custom provider class that implements {@link Cache} and exposes a {@code (String)} constructor
     * is resolved and instantiated through the reflective custom-class branch of {@code createCache}.
     */
    @Test
    public void testCreateCache_CustomClassImplementingCache() {
        try (Cache<String, Object> cache = CacheFactory.createCache(DummyProviderCache.class.getName() + "(localhost:9999)")) {
            assertNotNull(cache);
            assertTrue(cache instanceof DummyProviderCache);
            assertEquals("localhost:9999", ((DummyProviderCache<String, Object>) cache).serverUrl());
            assertTrue(cache.put("k", "v"));
            assertEquals("v", cache.getOrNull("k"));
        }
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
    public void testCreateCache_CustomClassWithNoArgConstructor() {
        try (Cache<String, Object> cache = CacheFactory.createCache(DummyProviderCache.class.getName() + "()")) {
            assertNotNull(cache);
            assertTrue(cache instanceof DummyProviderCache);
            assertTrue(cache.put("k", "v"));
            assertEquals("v", cache.getOrNull("k"));
        }
    }

    /** Factory smoke test: the createOffHeapCache delegation produces a working cache. */
    @Test
    public void testCreateOffHeapCache_Smoke() {
        final OffHeapCache<String, byte[]> cache = CacheFactory.createOffHeapCache(1);
        try {
            assertTrue(cache.put("k", new byte[] { 1, 2 }));
            org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[] { 1, 2 }, cache.getOrNull("k"));
        } finally {
            cache.close();
        }
    }

    /** Factory smoke test: the createCaffeineCache delegation wraps the supplied Caffeine cache. */
    @Test
    public void testCreateCaffeineCache_Smoke() {
        try (CaffeineCache<String, String> cache = CacheFactory
                .createCaffeineCache(com.github.benmanes.caffeine.cache.Caffeine.newBuilder().maximumSize(10).build())) {
            assertTrue(cache.put("k", "v"));
            assertEquals("v", cache.getOrNull("k"));
        }
    }

    /** createEhcache forwards to the Ehcache wrapper constructor, which rejects a null delegate. */
    @Test
    public void testCreateEhcache_EdgeCase_NullRejected() {
        assertThrows(IllegalArgumentException.class, () -> CacheFactory.createEhcache(null));
    }

    // TypeAttrParser.parse currently never returns null, so createCache's defensive attrResult-null
    // guard is not directly reachable through the dependency's public parser implementation.
}
