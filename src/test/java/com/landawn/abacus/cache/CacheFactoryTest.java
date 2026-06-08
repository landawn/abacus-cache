/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

@Tag("2025")
public class CacheFactoryTest extends TestBase {

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

        try (LocalCache<String, String> cache = CacheFactory.createLocalCache(60000L, 30000L, pool)) {
            assertNotNull(cache);
            assertTrue(cache.put("x", "y"));
            assertEquals("y", cache.getOrNull("x"));
        }
    }

    @Test
    public void testCreateLocalCache_WithPool_EdgeCase_NullPool() {
        assertThrows(IllegalArgumentException.class, () -> CacheFactory.createLocalCache(0L, 0L, null));
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
     * with the "class name cannot be empty" message.
     */
    @Test
    public void testCreateCache_EdgeCase_EmptyClassName() {
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("(localhost:11211)"));
        assertTrue(ex.getMessage() != null && ex.getMessage().contains("class name cannot be empty"),
                "expected a 'class name cannot be empty' message but was: " + ex.getMessage());
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

    // TODO: createCache's "attrResult == null" and "Cannot find class (cls == null)" guards are
    // unreachable — TypeAttrParser.parse never returns null and ClassUtil.forName throws (rather than
    // returning null) for a missing class. The "catch (IllegalArgumentException) -> rethrow" guard is
    // likewise unreachable because TypeAttrParser.parse only throws non-IAE RuntimeExceptions
    // (ParsingException / StringIndexOutOfBoundsException), which are handled by the RuntimeException
    // branch instead.
}
