package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Service-free regression coverage for {@link DistributedCache}'s circuit-breaker state. */
@Tag("2025")
public class DistributedCacheCircuitBreakerUnitTest {

    @Test
    public void malformedUnicodeKeysCannotAddressAnotherEntryOrChangeBreakerState() throws Exception {
        @SuppressWarnings("unchecked")
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        final DistributedCache<Object, String> cache = new DistributedCache<>(client, "prefix:", 1, 60_000L);

        try {
            final Object initialState = breakerState(cache);
            for (final String malformed : List.of("\uD800", "\uDC00", "key:\uD800suffix", "key:\uDC00suffix")) {
                for (final Object key : List.of(malformed, new StringBuilder(malformed))) {
                    assertThrows(IllegalArgumentException.class, () -> cache.getOrNull(key));
                    assertThrows(IllegalArgumentException.class, () -> cache.put(key, "value", 0, 0));
                    assertThrows(IllegalArgumentException.class, () -> cache.remove(key));
                    assertThrows(IllegalArgumentException.class, () -> cache.containsKey(key));
                }
            }

            assertSame(initialState, breakerState(cache));
            verifyNoInteractions(client);

            // Valid supplementary characters keep the established UTF-8/Base64 wire encoding.
            final String valid = "user:\uD83D\uDE00:\uD800\uDC00";
            final String encoded = "prefix:" + Base64.getEncoder().encodeToString(valid.getBytes(StandardCharsets.UTF_8));
            assertEquals(encoded, cache.generateKey(valid));
            assertEquals(encoded, cache.generateKey(new StringBuilder(valid)));
            assertEquals("prefix:Pz8=", cache.generateKey("??"));
            assertEquals("prefix:-", cache.generateKey(""));
        } finally {
            cache.close();
        }
    }

    @Test
    public void malformedUnicodeKeysAreRejectedWhileCircuitIsOpen() throws Exception {
        @SuppressWarnings("unchecked")
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        when(client.get(anyString())).thenThrow(new IllegalStateException("backend unavailable"));
        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 1, 60_000L);

        try {
            assertNull(cache.getOrNull("valid"));
            final Object failedState = breakerState(cache);

            assertThrows(IllegalArgumentException.class, () -> cache.getOrNull("\uD800"));
            assertThrows(IllegalArgumentException.class, () -> cache.containsKey("\uDC00"));
            assertSame(failedState, breakerState(cache));
            verify(client, times(1)).get(anyString());
        } finally {
            cache.close();
        }
    }

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

    @Test
    public void nullArgumentsAreRejectedWithIaeWithoutTouchingClientOrBreaker() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new DistributedCache<String, String>(null));
        assertThrows(IllegalArgumentException.class, () -> new DistributedCache<String, String>(null, "p:"));
        assertThrows(IllegalArgumentException.class, () -> new DistributedCache<String, String>(null, "p:", 1, 1_000L));

        @SuppressWarnings("unchecked")
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        final DistributedCache<String, String> cache = new DistributedCache<>(client, "p:", 1, 60_000L);

        try {
            final Object initialState = breakerState(cache);

            assertThrows(IllegalArgumentException.class, () -> cache.getOrNull(null));
            assertThrows(IllegalArgumentException.class, () -> cache.containsKey(null));
            assertThrows(IllegalArgumentException.class, () -> cache.put(null, "v", 1_000L, 0L));
            assertThrows(IllegalArgumentException.class, () -> cache.remove(null));
            assertThrows(IllegalArgumentException.class, () -> cache.generateKey(null));

            assertSame(initialState, breakerState(cache));
            verifyNoInteractions(client);
        } finally {
            cache.close();
        }
    }

    @Test
    public void nullKeyPrefixMeansNoPrefixAndNullValueIsForwardedToClient() {
        @SuppressWarnings("unchecked")
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        final DistributedCache<String, String> cache = new DistributedCache<>(client, null);

        try {
            final String encoded = Base64.getEncoder().encodeToString("k".getBytes(StandardCharsets.UTF_8));
            assertEquals(encoded, cache.generateKey("k"));

            // The null policy for values belongs to the pluggable client; the wrapper forwards null as-is.
            when(client.put(encoded, null, 1_000L)).thenReturn(true);
            assertTrue(cache.put("k", null, 1_000L, 0L));
            verify(client).put(encoded, null, 1_000L);
        } finally {
            cache.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void delayedFailureCannotMoveCircuitRetryTimestampBackwards() throws Exception {
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        when(client.get(anyString())).thenThrow(new IllegalStateException("backend unavailable"));
        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 2, 60_000L);
        final AtomicReference<Object> state = new AtomicReference<>(breakerState(cache));
        final AtomicReference<Object> controlled = mock(AtomicReference.class);
        final AtomicBoolean firstUpdate = new AtomicBoolean(true);
        final CountDownLatch olderFailureReachedUpdate = new CountDownLatch(1);
        final CountDownLatch publishOlderFailure = new CountDownLatch(1);

        when(controlled.get()).thenAnswer(invocation -> state.get());
        when(controlled.getAndUpdate(any())).thenAnswer(invocation -> {
            final UnaryOperator<Object> update = invocation.getArgument(0);
            if (firstUpdate.compareAndSet(true, false)) {
                olderFailureReachedUpdate.countDown();
                assertTrue(publishOlderFailure.await(5, TimeUnit.SECONDS));
            }
            return state.getAndUpdate(update);
        });
        final Field field = DistributedCache.class.getDeclaredField("circuitBreaker");
        field.setAccessible(true);
        field.set(cache, controlled);

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final Future<?> olderFailure = executor.submit(() -> cache.getOrNull("older"));
            assertTrue(olderFailureReachedUpdate.await(5, TimeUnit.SECONDS));

            // Force a distinct clock tick after the older failure has reached its update boundary.
            final long reachedAt = System.nanoTime();
            while (System.nanoTime() == reachedAt) {
                Thread.onSpinWait();
            }
            assertNull(cache.getOrNull("newer"));
            final long newerTimestamp = longField(state.get(), "lastFailedTime");

            publishOlderFailure.countDown();
            olderFailure.get(5, TimeUnit.SECONDS);

            assertEquals(2, intField(state.get(), "failedCount"));
            assertTrue(longField(state.get(), "lastFailedTime") - newerTimestamp >= 0,
                    "Publishing a delayed failure must not shorten the newer failure's retry window");
        } finally {
            publishOlderFailure.countDown();
            executor.shutdownNow();
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
