package com.landawn.abacus.cache;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Field;

import net.spy.memcached.MemcachedClient;

final class SpyMemcachedTestSupport {
    private SpyMemcachedTestSupport() {
    }

    @SuppressWarnings("unchecked")
    static SpyMemcached<Object> clientWithDelegate(final MemcachedClient delegate) throws Exception {
        final SpyMemcached<Object> cache = mock(SpyMemcached.class, CALLS_REAL_METHODS);
        final Field clientField = SpyMemcached.class.getDeclaredField("mc");
        clientField.setAccessible(true);
        clientField.set(cache, delegate);
        final Field timeoutField = SpyMemcached.class.getDeclaredField("operationTimeoutMillis");
        timeoutField.setAccessible(true);
        timeoutField.setLong(cache, 25L);
        return cache;
    }
}
