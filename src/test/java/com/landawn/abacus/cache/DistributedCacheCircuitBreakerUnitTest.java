package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Service-free regression coverage for {@link DistributedCache}'s circuit-breaker state. */
@Tag("2025")
public class DistributedCacheCircuitBreakerUnitTest {

    @Test
    public void failurePublishesMarkerAndSuccessResetsWholeState() throws Exception {
        @SuppressWarnings("unchecked")
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        when(client.get(anyString())).thenThrow(new IllegalStateException("backend unavailable"))
                .thenThrow(new IllegalStateException("backend unavailable"))
                .thenReturn("value");

        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 2, 0);

        try {
            assertNull(cache.getOrNull("key"));
            assertNull(cache.getOrNull("key"));

            final Object failedState = breakerState(cache);
            assertEquals(2, intField(failedState, "failedCount"));
            assertTrue(booleanField(failedState, "hasFailure"));

            // A zero retry delay lets the next read probe the backend immediately.
            assertEquals("value", cache.getOrNull("key"));

            final Object recoveredState = breakerState(cache);
            assertEquals(0, intField(recoveredState, "failedCount"));
            assertEquals(0L, longField(recoveredState, "lastFailedTime"));
            assertFalse(booleanField(recoveredState, "hasFailure"));
            verify(client, times(3)).get(anyString());
        } finally {
            cache.close();
        }
    }

    @Test
    public void zeroThresholdOpensAfterFirstRecordedFailure() {
        @SuppressWarnings("unchecked")
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        when(client.get(anyString())).thenThrow(new IllegalStateException("backend unavailable"));

        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 0, 60_000L);

        try {
            assertNull(cache.getOrNull("key")); // records the first failure
            assertNull(cache.getOrNull("key")); // short-circuited; no second backend call
            verify(client, times(1)).get(anyString());
        } finally {
            cache.close();
        }
    }

    private static Object breakerState(final DistributedCache<?, ?> cache) throws Exception {
        final Field field = DistributedCache.class.getDeclaredField("circuitBreaker");
        field.setAccessible(true);
        return ((AtomicReference<?>) field.get(cache)).get();
    }

    private static int intField(final Object target, final String name) throws Exception {
        final Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.getInt(target);
    }

    private static long longField(final Object target, final String name) throws Exception {
        final Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.getLong(target);
    }

    private static boolean booleanField(final Object target, final String name) throws Exception {
        final Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.getBoolean(target);
    }
}
