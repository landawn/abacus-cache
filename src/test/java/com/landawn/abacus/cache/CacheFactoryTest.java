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
import redis.clients.jedis.BinaryShardedJedis;

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
    public void testCreateCache_Memcached_EdgeCase_TooManyParameters() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Memcached(a,b,1000,extra)"));
        }
    }

    // Redis
    @Test
    public void testCreateCache_Redis() {
        try (MockedConstruction<BinaryShardedJedis> ctorIntercept = Mockito.mockConstruction(BinaryShardedJedis.class)) {
            try (Cache<String, Object> cache = CacheFactory.createCache("Redis(localhost:6379)")) {
                assertNotNull(cache);
                assertTrue(cache instanceof DistributedCache);
            }
        }
    }

    @Test
    public void testCreateCache_RedisWithPrefix() {
        try (MockedConstruction<BinaryShardedJedis> ctorIntercept = Mockito.mockConstruction(BinaryShardedJedis.class)) {
            try (Cache<String, Object> cache = CacheFactory.createCache("Redis(localhost:6379,prefix:)")) {
                assertNotNull(cache);
            }
        }
    }

    @Test
    public void testCreateCache_RedisWithTimeout() {
        try (MockedConstruction<BinaryShardedJedis> ctorIntercept = Mockito.mockConstruction(BinaryShardedJedis.class)) {
            try (Cache<String, Object> cache = CacheFactory.createCache("Redis(localhost:6379,prefix:,5000)")) {
                assertNotNull(cache);
            }
        }
    }

    @Test
    public void testCreateCache_Redis_EdgeCase_NonPositiveTimeout() {
        try (MockedConstruction<BinaryShardedJedis> ctorIntercept = Mockito.mockConstruction(BinaryShardedJedis.class)) {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Redis(localhost:6379,p:,0)"));
        }
    }

    @Test
    public void testCreateCache_Redis_EdgeCase_NonNumericTimeout() {
        try (MockedConstruction<BinaryShardedJedis> ctorIntercept = Mockito.mockConstruction(BinaryShardedJedis.class)) {
            assertThrows(IllegalArgumentException.class, () -> CacheFactory.createCache("Redis(localhost:6379,p:,xyz)"));
        }
    }

    @Test
    public void testCreateCache_Redis_EdgeCase_TooManyParameters() {
        try (MockedConstruction<BinaryShardedJedis> ctorIntercept = Mockito.mockConstruction(BinaryShardedJedis.class)) {
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
}
