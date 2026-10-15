/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

class AbstractDistributedCacheClientTest {

    private static final String SERVER_URL = "localhost:11211";

    private final TestClient<Object> client = new TestClient<>(SERVER_URL);

    // Construction

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = { " ", "\t", "\n" })
    void constructorRejectsMissingServerUrls(final String serverUrl) {
        assertThrows(IllegalArgumentException.class, () -> new TestClient<String>(serverUrl));
    }

    @Test
    void serverUrl() {
        assertEquals(SERVER_URL, client.serverUrl());
    }

    // Bulk reads

    @Test
    void getBulkCollectionUnsupported() {
        final Collection<String> keys = Arrays.asList("k1", "k2");
        assertThrows(UnsupportedOperationException.class, () -> client.getBulk(keys));
    }

    @Test
    void getBulkVarargsUnsupported() {
        assertThrows(UnsupportedOperationException.class, () -> client.getBulk("k1", "k2"));
    }

    // Removals

    @Test
    void flushAllUnsupported() {
        assertThrows(UnsupportedOperationException.class, client::flushAll);
    }

    // Compatibility

    /**
     * The delegation-cycle guard is keyed on (thread, client, name-pair) only — it cannot tell a
     * true cycle from a nested bridged call with a different key. The javadoc therefore requires
     * that an overriding pair member never call back into its bridged counterpart on {@code this},
     * even for a different key. This test pins that documented fail-fast behavior.
     */
    @Test
    void compatibilityDefaultsNestedDifferentKeyDelegationFailsFast() {
        final class IndexWritingLegacyClient extends AbstractDistributedCacheClient<String> {
            private final Map<String, String> values = new HashMap<>();

            IndexWritingLegacyClient() {
                super("nested.test:1");
            }

            @Override
            public String get(final String key) {
                return values.get(key);
            }

            @Deprecated
            @Override
            public boolean set(final String key, final String value, final long liveTime) {
                values.put(key, value);
                if (!key.startsWith("index:")) {
                    put("index:" + key, value, liveTime); // nested bridged call, different key
                }
                return true;
            }

            @Override
            public long incr(final String key) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long incr(final String key, final long delta) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long decr(final String key) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long decr(final String key, final long delta) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void disconnect() {
            }
        }

        final IndexWritingLegacyClient nested = new IndexWritingLegacyClient();
        final UnsupportedOperationException failure = assertThrows(UnsupportedOperationException.class, () -> nested.put("k", "v", 0));
        assertEquals("A DistributedCacheClient implementation must provide a non-recursive implementation of either put(...) or set(...)",
                failure.getMessage());

        // The guard is released on exit: after the failed call, a direct top-level set(...) still works.
        assertTrue(nested.set("k2", "v2", 0));
        assertEquals("v2", nested.get("k2"));
    }

    /** A proxy handler that genuinely implements the legacy counterpart still uses the defaults. */
    @Test
    void compatibilityDefaultsProxyWithConcreteCounterpartsStillWorks() {
        final AtomicInteger setCalls = new AtomicInteger();
        final AtomicInteger deleteCalls = new AtomicInteger();
        final DistributedCacheClient<String> legacyProxy = newProxy((proxy, method, args) -> {
            if ("set".equals(method.getName())) {
                setCalls.incrementAndGet();
                return true;
            }

            if ("delete".equals(method.getName())) {
                deleteCalls.incrementAndGet();
                return true;
            }

            if (method.isDefault()) {
                return InvocationHandler.invokeDefault(proxy, method, args);
            }

            throw new AssertionError("Unexpected proxy method: " + method);
        });

        assertTrue(legacyProxy.put("k", "v", 0));
        assertTrue(legacyProxy.remove("k"));
        assertEquals(1, setCalls.get());
        assertEquals(1, deleteCalls.get());
    }

    /**
     * A generated proxy declares methods for every interface member, so reflection alone cannot
     * distinguish a real counterpart implementation from a handler that invokes both defaults.
     * Such a cycle must fail predictably instead of overflowing the thread stack.
     */
    @SuppressWarnings("deprecation")
    @Test
    void compatibilityDefaultsRecursiveProxyFailsFast() {
        final DistributedCacheClient<String> recursive = newProxy((proxy, method, args) -> InvocationHandler.invokeDefault(proxy, method, args));

        final UnsupportedOperationException putFailure = assertThrows(UnsupportedOperationException.class, () -> recursive.put("k", "v", 0));
        final UnsupportedOperationException setFailure = assertThrows(UnsupportedOperationException.class, () -> recursive.set("k", "v", 0));
        final UnsupportedOperationException removeFailure = assertThrows(UnsupportedOperationException.class, () -> recursive.remove("k"));
        final UnsupportedOperationException deleteFailure = assertThrows(UnsupportedOperationException.class, () -> recursive.delete("k"));

        assertEquals("A DistributedCacheClient implementation must provide a non-recursive implementation of either put(...) or set(...)",
                putFailure.getMessage());
        assertEquals(putFailure.getMessage(), setFailure.getMessage());
        assertEquals("A DistributedCacheClient implementation must provide a non-recursive implementation of either remove(...) or delete(...)",
                removeFailure.getMessage());
        assertEquals(removeFailure.getMessage(), deleteFailure.getMessage());
    }

    @Test
    void compatibilityGuardsTrackOverlappingPairsIndependently() {
        try {
            assertTrue(DistributedCacheClientCompatibilitySupport.tryEnter(client, DistributedCacheClientCompatibilitySupport.WRITE_PAIR));
            assertTrue(DistributedCacheClientCompatibilitySupport.tryEnter(client, DistributedCacheClientCompatibilitySupport.REMOVE_PAIR));
            DistributedCacheClientCompatibilitySupport.exit(client, DistributedCacheClientCompatibilitySupport.REMOVE_PAIR);
            assertFalse(DistributedCacheClientCompatibilitySupport.tryEnter(client, DistributedCacheClientCompatibilitySupport.WRITE_PAIR));
            assertTrue(DistributedCacheClientCompatibilitySupport.tryEnter(client, DistributedCacheClientCompatibilitySupport.REMOVE_PAIR));
            DistributedCacheClientCompatibilitySupport.exit(client, DistributedCacheClientCompatibilitySupport.WRITE_PAIR);
            assertFalse(DistributedCacheClientCompatibilitySupport.tryEnter(client, DistributedCacheClientCompatibilitySupport.REMOVE_PAIR));
        } finally {
            DistributedCacheClientCompatibilitySupport.exit(client, DistributedCacheClientCompatibilitySupport.WRITE_PAIR);
            DistributedCacheClientCompatibilitySupport.exit(client, DistributedCacheClientCompatibilitySupport.REMOVE_PAIR);
            DistributedCacheClientCompatibilitySupport.exit(client, DistributedCacheClientCompatibilitySupport.REMOVE_PAIR);
        }
        assertTrue(DistributedCacheClientCompatibilitySupport.tryEnter(client, DistributedCacheClientCompatibilitySupport.WRITE_PAIR));
        DistributedCacheClientCompatibilitySupport.exit(client, DistributedCacheClientCompatibilitySupport.WRITE_PAIR);
    }

    @Test
    void compatibilityOverrideLookupRejectsUnknownAndMissingMethods() {
        assertFalse(DistributedCacheClientCompatibilitySupport.isOverridden(DistributedCacheClient.class, "unknown"));
        assertFalse(DistributedCacheClientCompatibilitySupport.isOverridden(Object.class, "put"));
    }

    @Test
    void currentWriteNamesDelegateToLegacyImplementation() {
        final LegacyNamingClient<String> legacy = new LegacyNamingClient<>();
        assertTrue(legacy.put("k", "v", 0));
        assertEquals("v", legacy.get("k"));
        assertTrue(legacy.remove("k"));
        assertNull(legacy.get("k"));
    }

    /**
     * A client that implements neither name in a compatibility pair is malformed, but the failure
     * must still be bounded and diagnosable. The old mutually recursive defaults overflowed the
     * thread stack before a caller could identify the missing method.
     */
    @SuppressWarnings("deprecation")
    @Test
    void missingCompatibilityPairImplementationsFailFast() {
        final MissingWriteNamesClient<String> incomplete = new MissingWriteNamesClient<>();

        final UnsupportedOperationException putFailure = assertThrows(UnsupportedOperationException.class, () -> incomplete.put("k", "v", 0));
        final UnsupportedOperationException setFailure = assertThrows(UnsupportedOperationException.class, () -> incomplete.set("k", "v", 0));
        final UnsupportedOperationException removeFailure = assertThrows(UnsupportedOperationException.class, () -> incomplete.remove("k"));
        final UnsupportedOperationException deleteFailure = assertThrows(UnsupportedOperationException.class, () -> incomplete.delete("k"));

        assertEquals("A DistributedCacheClient implementation must provide a non-recursive implementation of either put(...) or set(...)",
                putFailure.getMessage());
        assertEquals(putFailure.getMessage(), setFailure.getMessage());
        assertEquals("A DistributedCacheClient implementation must provide a non-recursive implementation of either remove(...) or delete(...)",
                removeFailure.getMessage());
        assertEquals(removeFailure.getMessage(), deleteFailure.getMessage());
    }

    // Expiration

    @Test
    void toSecondsOverflow() {
        // (Integer.MAX_VALUE + 1) * 1000 milliseconds -> seconds exceeds Integer.MAX_VALUE.
        assertThrows(IllegalArgumentException.class, () -> client.callToSeconds((Integer.MAX_VALUE + 1L) * 1000L));
    }

    @ParameterizedTest
    @CsvSource({ "0, 0", "1, 1", "999, 1", "1000, 1", "1500, 2", "2000, 2", "-1, 0", "-1000, 0", "-9223372036854775808, 0", "2147483647000, 2147483647" })
    void toSecondsWithSupportedDurationReturnsRoundedSeconds(final long millis, final int seconds) {
        assertEquals(seconds, client.callToSeconds(millis));
    }

    /** Concrete base-class fixture whose delegate never opens a connection. */
    private static final class TestClient<T> extends AbstractDistributedCacheClient<T> {
        private final DistributedCacheClient<T> delegate;

        TestClient(final String url) {
            // super(url) validates the URL and throws on null/empty/blank BEFORE any connection is opened.
            super(url);
            delegate = Mockito.mock(DistributedCacheClient.class);
        }

        @Override
        public T get(final String key) {
            return delegate.get(key);
        }

        @Override
        public boolean put(final String key, final T value, final long liveTime) {
            return delegate.put(key, value, liveTime);
        }

        @Override
        public boolean remove(final String key) {
            return delegate.remove(key);
        }

        @Override
        public long incr(final String key) {
            return delegate.incr(key);
        }

        @Override
        public long incr(final String key, final long delta) {
            return delegate.incr(key, delta);
        }

        @Override
        public long decr(final String key) {
            return delegate.decr(key);
        }

        @Override
        public long decr(final String key, final long delta) {
            return delegate.decr(key, delta);
        }

        @Override
        public void disconnect() {
            delegate.disconnect();
        }

        // Test-only access to the protected toSeconds helper.
        int callToSeconds(final long liveTime) {
            return toSeconds(liveTime);
        }
    }

    /** Client shaped like a binary/source implementation from 2.8.4: only legacy write names. */
    private static final class LegacyNamingClient<T> extends AbstractDistributedCacheClient<T> {
        private final Map<String, T> values = new HashMap<>();

        LegacyNamingClient() {
            super("legacy.test:1");
        }

        @Override
        public T get(final String key) {
            return values.get(key);
        }

        @Deprecated
        @Override
        public boolean set(final String key, final T value, final long liveTime) {
            values.put(key, value);
            return true;
        }

        @Deprecated
        @Override
        public boolean delete(final String key) {
            return values.remove(key) != null;
        }

        @Override
        public long incr(final String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long incr(final String key, final long delta) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long decr(final String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long decr(final String key, final long delta) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void disconnect() {
        }
    }

    /** Deliberately omits both members of each current/legacy compatibility pair. */
    private static final class MissingWriteNamesClient<T> extends AbstractDistributedCacheClient<T> {
        MissingWriteNamesClient() {
            super("missing.test:1");
        }

        @Override
        public T get(final String key) {
            return null;
        }

        @Override
        public long incr(final String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long incr(final String key, final long delta) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long decr(final String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long decr(final String key, final long delta) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void disconnect() {
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> DistributedCacheClient<T> newProxy(final InvocationHandler handler) {
        return (DistributedCacheClient<T>) Proxy.newProxyInstance(DistributedCacheClient.class.getClassLoader(),
                new Class<?>[] { DistributedCacheClient.class }, handler);
    }
}
