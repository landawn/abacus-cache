/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.TestBase;
import com.landawn.abacus.util.Strings;

/**
 * Covers {@link AbstractDistributedCacheClient} via a concrete subclass whose abstract operations are
 * backed by a <b>real</b> Memcached server reachable at {@code localhost:11211}
 * (e.g. {@code docker run --name memcached -p 11211:11211 -d memcached:latest}).
 *
 * <p>{@link MemcachedBackedClient} delegates the operational tests to a real {@link SpyMemcached}, so
 * they genuinely round-trip through the server. It deliberately leaves {@code getBulk}/{@code flushAll}
 * at their base-class defaults. A small in-memory {@link LegacyNamingClient} is used only to verify
 * source/binary compatibility between the pre-2.8.5 {@code set}/{@code delete} names and the current
 * {@code put}/{@code remove} names; it is not used to stand in for backend behavior.
 */
@Tag("2025")
public class AbstractDistributedCacheClientTest extends TestBase {

    private static final String SERVER_URL = "localhost:11211";

    /**
     * Minimal concrete client backed by the real server through a {@link SpyMemcached} delegate.
     * Leaves {@code getBulk}/{@code flushAll} at their abstract-base-class defaults so those defaults
     * stay under test.
     */
    private static final class MemcachedBackedClient<T> extends AbstractDistributedCacheClient<T> {
        private final SpyMemcached<T> delegate;

        MemcachedBackedClient(final String url) {
            // super(url) validates the URL and throws on null/empty/blank BEFORE any connection is opened.
            super(url);
            delegate = new SpyMemcached<>(url);
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

    private static MemcachedBackedClient<Object> client;

    @BeforeAll
    static void connect() {
        client = new MemcachedBackedClient<>(SERVER_URL);
    }

    @AfterAll
    static void disconnect() {
        if (client != null) {
            client.disconnect();
        }
    }

    // --- constructor validation (rejected before any connection is opened) ---------------------

    @Test
    public void testConstructor_EdgeCase_NullServerUrl() {
        assertThrows(IllegalArgumentException.class, () -> new MemcachedBackedClient<String>(null));
    }

    @Test
    public void testConstructor_EdgeCase_EmptyServerUrl() {
        assertThrows(IllegalArgumentException.class, () -> new MemcachedBackedClient<String>(""));
    }

    @Test
    public void testConstructor_EdgeCase_BlankServerUrl() {
        // A whitespace-only URL is semantically empty and must be rejected just like "" or null.
        assertThrows(IllegalArgumentException.class, () -> new MemcachedBackedClient<String>("   "));
        assertThrows(IllegalArgumentException.class, () -> new MemcachedBackedClient<String>("\t\n"));
    }

    @Test
    public void testServerUrl() {
        assertEquals(SERVER_URL, client.serverUrl());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testLegacyWriteNames_DelegateToCurrentImplementation() {
        final String key = "abstract-client-legacy:" + Strings.uuid();
        assertTrue(client.set(key, "v", 60_000));
        assertEquals("v", client.get(key));
        assertTrue(client.delete(key));
        assertNull(client.get(key));
    }

    @Test
    public void testCurrentWriteNames_DelegateToLegacyImplementation() {
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
    public void testMissingCompatibilityPairImplementations_FailFast() {
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

    /**
     * A generated proxy declares methods for every interface member, so reflection alone cannot
     * distinguish a real counterpart implementation from a handler that invokes both defaults.
     * Such a cycle must fail predictably instead of overflowing the thread stack.
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testCompatibilityDefaults_RecursiveProxyFailsFast() {
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

    /** A proxy handler that genuinely implements the legacy counterpart still uses the defaults. */
    @Test
    public void testCompatibilityDefaults_ProxyWithConcreteCounterpartsStillWorks() {
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

    @SuppressWarnings("unchecked")
    private static <T> DistributedCacheClient<T> newProxy(final InvocationHandler handler) {
        return (DistributedCacheClient<T>) Proxy.newProxyInstance(DistributedCacheClient.class.getClassLoader(),
                new Class<?>[] { DistributedCacheClient.class }, handler);
    }

    // --- base-class default operations (not overridden by the concrete client) -----------------

    @Test
    public void testGetBulk_Varargs_Unsupported() {
        assertThrows(UnsupportedOperationException.class, () -> client.getBulk("k1", "k2"));
    }

    @Test
    public void testGetBulk_Collection_Unsupported() {
        final Collection<String> keys = Arrays.asList("k1", "k2");
        assertThrows(UnsupportedOperationException.class, () -> client.getBulk(keys));
    }

    @Test
    public void testFlushAll_Unsupported() {
        assertThrows(UnsupportedOperationException.class, client::flushAll);
    }

    // --- real round-trip through the abstract operations ---------------------------------------

    /**
     * The concrete client's abstract operations actually reach the server: a value set through the
     * base-class-typed reference is read back and then deleted.
     */
    @Test
    public void testAbstractOperations_roundTripAgainstRealServer() {
        final String key = "abstract-client:" + Strings.uuid();
        assertTrue(client.put(key, "v", 60_000));
        assertEquals("v", client.get(key));
        assertTrue(client.remove(key));
        assertNull(client.get(key));
    }

    @Test
    public void testIncr_MissingKey_ReturnsMinusOne() {
        // Memcached's sentinel for incrementing a non-existent counter, surfaced through the abstract op.
        assertEquals(-1L, client.incr("abstract-client-missing:" + Strings.uuid()));
    }

    // --- toSeconds helper ----------------------------------------------------------------------

    @Test
    public void testToSeconds_ExactSecond() {
        assertEquals(2, client.callToSeconds(2000));
    }

    @Test
    public void testToSeconds_RoundsUpFractionalSecond() {
        // 1500ms -> 2s (rounded up)
        assertEquals(2, client.callToSeconds(1500));
        // 1ms -> 1s
        assertEquals(1, client.callToSeconds(1));
        // 999ms -> 1s
        assertEquals(1, client.callToSeconds(999));
    }

    @Test
    public void testToSeconds_EdgeCase_Zero() {
        assertEquals(0, client.callToSeconds(0));
    }

    @Test
    public void testToSeconds_EdgeCase_NegativeLiveTime() {
        // Per DistributedCacheClient.put(...) contract: 0 or negative means "no expiration".
        // toSeconds must therefore normalize any negative input to 0 rather than throwing.
        assertEquals(0, client.callToSeconds(-1));
        assertEquals(0, client.callToSeconds(-1000));
        assertEquals(0, client.callToSeconds(Long.MIN_VALUE));
    }

    @Test
    public void testToSeconds_EdgeCase_Overflow() {
        // (Integer.MAX_VALUE + 1) * 1000 milliseconds -> seconds exceeds Integer.MAX_VALUE.
        assertThrows(IllegalArgumentException.class, () -> client.callToSeconds(((long) Integer.MAX_VALUE + 1L) * 1000L));
    }

    @Test
    public void testToSeconds_MaxValidMillisRoundsDownAtBoundary() {
        // Exactly Integer.MAX_VALUE seconds in milliseconds — should NOT throw and should equal Integer.MAX_VALUE.
        assertEquals(Integer.MAX_VALUE, client.callToSeconds((long) Integer.MAX_VALUE * 1000L));
    }
}
