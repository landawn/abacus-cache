/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static com.landawn.abacus.cache.DistributedCacheBreakerAccess.setBreakerState;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

class DistributedCacheTest {

    // Construction

    @Test
    void constructorNegativeMaxFailed() {
        // Constructor validation must finish before any backend operation.
        assertThrows(IllegalArgumentException.class, () -> new DistributedCache<>(newClient(), "p:", -1, 1000));
    }

    @Test
    void constructorNegativeRetryDelay() {
        assertThrows(IllegalArgumentException.class, () -> new DistributedCache<>(newClient(), "p:", 100, -1));
    }

    @Test
    void constructorNullClient() {
        assertThrows(IllegalArgumentException.class, () -> new DistributedCache<>(null));
    }

    // Reads

    /**
     * The documented contract of {@link DistributedCache#getOrNull(Object)} rejects a null key with
     * {@link IllegalArgumentException} even when the circuit breaker is open (the null check runs before
     * the breaker short-circuit).
     */
    @Test
    void getOrNullNullKeyThrowsEvenWhenCircuitOpen() throws Exception {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient(), "", 1, 60_000);
        try {
            setBreakerState(cache, 1, System.nanoTime()); // force the circuit open (nanoTime-based window)
            assertThrows(IllegalArgumentException.class, () -> cache.getOrNull(null));
        } finally {
            cache.close();
        }
    }

    // Writes

    /** Null keys are rejected consistently across put/remove, mirroring the getOrNull coverage. */
    @Test
    void putAndRemoveNullKey() {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());
        try {
            assertThrows(IllegalArgumentException.class, () -> cache.put(null, "v", 1000, 0));
            assertThrows(IllegalArgumentException.class, () -> cache.remove(null));
        } finally {
            cache.close();
        }
    }

    // Membership And Views

    @Test
    void keySetUnsupported() {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());
        try {
            final UnsupportedOperationException e = assertThrows(UnsupportedOperationException.class, cache::keySet);
            assertEquals("keySet() is not supported by DistributedCache", e.getMessage());
        } finally {
            cache.close();
        }
    }

    @Test
    void sizeUnsupported() {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());
        try {
            final UnsupportedOperationException e = assertThrows(UnsupportedOperationException.class, cache::size);
            assertEquals("size() is not supported by DistributedCache", e.getMessage());
        } finally {
            cache.close();
        }
    }

    // Lifecycle

    /**
     * Internal lifecycle decisions must read the private field, not dispatch through an overridable
     * accessor. A subclass that decorates {@code isClosed()} must not make {@code close()} disconnect
     * the same client repeatedly.
     */
    @Test
    void closeIdempotenceCannotBeDefeatedByIsClosedOverride() {
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        final DistributedCache<String, String> cache = new DistributedCache<>(client) {
            @Override
            public boolean isClosed() {
                return false;
            }
        };

        cache.close();
        cache.close();

        verify(client, times(1)).disconnect();
    }

    @Test
    void closeIsIdempotent() {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());
        cache.close();
        cache.close(); // idempotent
        assertTrue(cache.isClosed());
    }

    // Validation And Failures

    @Test
    void generateKeyNullKey() {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());
        try {
            assertThrows(IllegalArgumentException.class, () -> cache.getOrNull(null));
        } finally {
            cache.close();
        }
    }

    /**
     * {@code generateKey} rejects a non-null key whose string representation is null. An empty
     * {@link Optional} is non-null yet {@code N.stringOf(Optional.empty())} returns {@code null}.
     * (Uses {@link #newClient()}; {@code generateKey} is a pure transformation and performs no I/O.)
     */
    @Test
    void generateKeyNullStringRepresentation() {
        final DistributedCache<Optional<Object>, String> cache = new DistributedCache<>(newClient());
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> cache.generateKey(Optional.empty()));
        assertTrue(ex.getMessage() != null && ex.getMessage().contains("Key string representation cannot be null"),
                "expected the null-string-representation message but was: " + ex.getMessage());
    }

    /**
     * Local key conversion is deterministic validation and must not be bypassed by an unrelated open
     * backend circuit. Otherwise the same invalid key alternates between throwing and looking absent.
     */
    @Test
    void generateKeyNullStringRepresentationThrowsEvenWhenCircuitOpen() throws Exception {
        final DistributedCache<Optional<Object>, String> cache = new DistributedCache<>(newClient(), "", 1, 60_000);
        setBreakerState(cache, 1, System.nanoTime());

        assertThrows(IllegalArgumentException.class, () -> cache.getOrNull(Optional.empty()));
    }

    // Other Behavior

    /**
     * Regression guard for the torn-state race: the failure count and its timestamp must be
     * published through one atomic holder, not through independently writable atomics.
     */
    @Test
    void circuitBreakerCountAndTimestampUseOneAtomicState() throws Exception {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());
        try {
            final Field stateField = DistributedCache.class.getDeclaredField("circuitBreaker");
            stateField.setAccessible(true);

            assertTrue(AtomicReference.class.isAssignableFrom(stateField.getType()));

            final Object state = ((AtomicReference<?>) stateField.get(cache)).get();
            final Field failedCountField = state.getClass().getDeclaredField("failedCount");
            final Field lastFailedTimeField = state.getClass().getDeclaredField("lastFailedTime");
            final Field hasFailureField = state.getClass().getDeclaredField("hasFailure");
            failedCountField.setAccessible(true);
            lastFailedTimeField.setAccessible(true);
            hasFailureField.setAccessible(true);
            assertEquals(0, failedCountField.getInt(state));
            assertEquals(0L, lastFailedTimeField.getLong(state));
            assertFalse(hasFailureField.getBoolean(state));
            assertThrows(NoSuchFieldException.class, () -> DistributedCache.class.getDeclaredField("failedCounter"));
            assertThrows(NoSuchFieldException.class, () -> DistributedCache.class.getDeclaredField("lastFailedTime"));
        } finally {
            cache.close();
        }
    }

    /**
     * Base64 encodes an empty byte sequence as an empty string, but Memcached rejects an empty key.
     * The wrapper must therefore use a non-empty marker that cannot collide with standard Base64.
     */
    @Test
    void generateKeyEmptyStringUsesMemcachedSafeMarker() {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());

        assertEquals("-", cache.generateKey(""));
    }

    /** The documented non-String key path: {@code generateKey(12345)} base64-encodes "12345". */
    @Test
    void generateKeyNonStringKey() {
        final DistributedCache<Integer, String> cache = new DistributedCache<>(newClient());
        try {
            assertEquals("MTIzNDU=", cache.generateKey(12345));
        } finally {
            cache.close();
        }
    }

    @Test
    void operationsAfterCloseThrow() {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());
        cache.close();
        assertThrows(IllegalStateException.class, () -> cache.getOrNull("k"));
        assertThrows(IllegalStateException.class, () -> cache.put("k", "v", 0, 0));
        assertThrows(IllegalStateException.class, () -> cache.remove("k"));
        assertThrows(IllegalStateException.class, cache::clear);
    }

    @SuppressWarnings("unchecked")
    private static DistributedCacheClient<String> newClient() {
        return mock(DistributedCacheClient.class);
    }
}
