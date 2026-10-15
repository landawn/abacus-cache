/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static com.landawn.abacus.cache.DistributedCacheBreakerAccess.breakerState;
import static com.landawn.abacus.cache.DistributedCacheBreakerAccess.setBreakerState;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.LogCapture;

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

    @ParameterizedTest
    @ValueSource(strings = { "has space", "\n", "\u007F", "\u0080" })
    void constructorRejectsPrefixesUnsafeForBackendKeys(final String prefix) {
        final DistributedCacheClient<String> client = newClient();
        assertThrows(IllegalArgumentException.class, () -> new DistributedCache<>(client, prefix));
        verifyNoInteractions(client);
    }

    // Key encoding and validation

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
    void malformedUnicodeKeysCannotAddressAnotherEntryOrChangeBreakerState() throws Exception {
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
    void nullArgumentsAreRejectedWithIllegalArgumentExceptionWithoutTouchingClientOrBreaker() throws Exception {
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

    @ParameterizedTest(name = "circuit open: {0}")
    @ValueSource(booleans = { false, true })
    void nullKeysAreRejectedRegardlessOfCircuitState(final boolean open) throws Exception {
        final DistributedCacheClient<String> client = newClient();
        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 1, 60_000);
        try {
            if (open) {
                setBreakerState(cache, 1, System.nanoTime());
            }
            final Object before = breakerState(cache);
            assertThrows(IllegalArgumentException.class, () -> cache.getOrNull(null));
            assertSame(before, breakerState(cache));
            verifyNoInteractions(client);
        } finally {
            cache.close();
        }
    }

    @ParameterizedTest(name = "circuit open: {0}")
    @ValueSource(booleans = { false, true })
    void nullStringRepresentationsAreRejectedRegardlessOfCircuitState(final boolean open) throws Exception {
        final DistributedCacheClient<String> client = newClient();
        final DistributedCache<Optional<Object>, String> cache = new DistributedCache<>(client, "", 1, 60_000);
        try {
            if (open) {
                setBreakerState(cache, 1, System.nanoTime());
            }
            final Object before = breakerState(cache);
            final IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> cache.getOrNull(Optional.empty()));
            assertTrue(failure.getMessage().contains("Key string representation cannot be null"));
            assertSame(before, breakerState(cache));
            verifyNoInteractions(client);
        } finally {
            cache.close();
        }
    }

    // Reads and writes

    @Test
    void nullKeyPrefixMeansNoPrefixAndNullValueIsForwardedToClient() {
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

    @ParameterizedTest
    @ValueSource(strings = { "", "tenant:" })
    void operationsUseTheSameEncodedKeyAndOnlyTheTtl(final String prefix) {
        final DistributedCacheClient<String> client = newClient();
        final DistributedCache<String, String> cache = new DistributedCache<>(client, prefix);
        final String encoded = prefix + "a2V5";
        try {
            when(client.put(encoded, "value", 1234)).thenReturn(true);
            when(client.get(encoded)).thenReturn("value", "value", null);
            assertTrue(cache.put("key", "value", 1234, 5678));
            assertEquals("value", cache.getOrNull("key"));
            assertTrue(cache.containsKey("key"));
            assertFalse(cache.containsKey("key"));
            cache.remove("key");
            cache.clear();
            verify(client).put(encoded, "value", 1234);
            verify(client).remove(encoded);
            verify(client).flushAll();
        } finally {
            cache.close();
        }
        verify(client).disconnect();
    }

    // Membership and views

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

    @ParameterizedTest
    @ValueSource(strings = { "DEBUG", "OFF" })
    @ResourceLock(LogCapture.RESOURCE)
    void backendFailuresKeepTheCacheClosableAndReportDiagnostics(final String level) {
        final DistributedCacheClient<String> client = newClient();
        try (LogCapture logs = new LogCapture(DistributedCache.class, Level.valueOf(level))) {
            final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 2, 0);
            when(client.get("a2V5")).thenThrow(new IllegalStateException("server unavailable"));
            doThrow(new IllegalStateException("disconnect failed")).when(client).disconnect();
            assertNull(cache.getOrNull("key"));
            assertNull(cache.getOrNull("key"));
            cache.close();
            cache.close();
            assertTrue(cache.isClosed());
            verify(client, times(1)).disconnect();
            if ("DEBUG".equals(level)) {
                assertTrue(logs.messages().stream().anyMatch(message -> message.contains("2 consecutive read failures")));
                assertTrue(logs.messages().stream().anyMatch(message -> message.contains("Failed to disconnect")));
                assertTrue(logs.messages().stream().anyMatch(message -> message.contains("treated as cache miss")));
            } else {
                assertTrue(logs.messages().isEmpty());
            }
        }
    }

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
        final DistributedCacheClient<String> client = newClient();
        final DistributedCache<String, String> cache = new DistributedCache<>(client);
        cache.close();
        cache.close(); // idempotent
        assertTrue(cache.isClosed());
        verify(client, times(1)).disconnect();
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
