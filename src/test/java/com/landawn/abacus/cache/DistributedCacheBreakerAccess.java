package com.landawn.abacus.cache;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;

/** Reflective access to {@link DistributedCache}'s private circuit-breaker state, shared by its tests. */
final class DistributedCacheBreakerAccess {

    private DistributedCacheBreakerAccess() {
    }

    /**
     * Publishes a recorded-failure state with the given count and {@link System#nanoTime()}-based
     * last-failure timestamp.
     */
    static void setBreakerState(final DistributedCache<?, ?> cache, final int failedCount, final long lastFailedTime) throws ReflectiveOperationException {
        final Field field = DistributedCache.class.getDeclaredField("circuitBreaker");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        final AtomicReference<Object> stateRef = (AtomicReference<Object>) field.get(cache);
        final Class<?> stateType = Class.forName(DistributedCache.class.getName() + "$CircuitBreakerState");
        final Constructor<?> constructor = stateType.getDeclaredConstructor(int.class, long.class, boolean.class);
        constructor.setAccessible(true);
        stateRef.set(constructor.newInstance(failedCount, lastFailedTime, true));
    }

    /** Returns the currently published breaker state record. */
    static Object breakerState(final DistributedCache<?, ?> cache) throws ReflectiveOperationException {
        final Field field = DistributedCache.class.getDeclaredField("circuitBreaker");
        field.setAccessible(true);
        return ((AtomicReference<?>) field.get(cache)).get();
    }

    /** Returns the current consecutive-failure count. */
    static int failedCounter(final DistributedCache<?, ?> cache) throws ReflectiveOperationException {
        return intField(breakerState(cache), "failedCount");
    }

    static int intField(final Object target, final String name) throws ReflectiveOperationException {
        final Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.getInt(target);
    }

    static long longField(final Object target, final String name) throws ReflectiveOperationException {
        final Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.getLong(target);
    }

    static boolean booleanField(final Object target, final String name) throws ReflectiveOperationException {
        final Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.getBoolean(target);
    }
}
