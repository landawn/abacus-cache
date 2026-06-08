/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.landawn.abacus.util.MemcachedLock;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

@Tag("2025")
public class MemcachedLockTest {
    final String url = "localhost:11211";

    /**
     * Mock-based replacement for the original {@code test_lock}, which was skipped whenever
     * a real memcached server was not running on localhost:11211. We intercept construction
     * of {@link MemcachedClient} so the {@link MemcachedLock} (and the {@code SpyMemcached}
     * it wraps internally) operate against a Mockito mock instead of opening a real socket.
     */
    @Test
    public void test_lock() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            final MemcachedLock<String, Long> lock = new MemcachedLock<>(url);
            final MemcachedClient mockMc = ctorIntercept.constructed().get(0);

            final String key = "mysql";

            // add() succeeds on first attempt (i.e. lock acquired).
            OperationFuture<Boolean> addOk = immediateBooleanFuture(true);
            when(mockMc.add(eq(key), anyInt(), any())).thenReturn(addOk);

            assertTrue(lock.lock(key, 10_000), "first lock() should acquire");
            verify(mockMc).add(eq(key), eq(10), any()); // 10_000 ms -> 10 s

            // isLocked() uses mc.get(...); return non-null to indicate "held".
            when(mockMc.get(key)).thenReturn(Long.valueOf(123));
            assertTrue(lock.isLocked(key));

            // get(target) returns the stored value.
            assertEquals(Long.valueOf(123), lock.get(key));
        }
    }

    /**
     * Second-acquire-fails scenario: the server returns false from add() when the key already exists.
     */
    @Test
    public void test_lock_returns_false_when_already_held() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            final MemcachedLock<String, Long> lock = new MemcachedLock<>(url);
            final MemcachedClient mockMc = ctorIntercept.constructed().get(0);

            OperationFuture<Boolean> alreadyHeld = immediateBooleanFuture(false);
            when(mockMc.add(any(String.class), anyInt(), any())).thenReturn(alreadyHeld);

            assertFalse(lock.lock("k", 5_000));
        }
    }

    /**
     * unlock() should issue a delete and propagate the boolean result.
     */
    @Test
    public void test_unlock_deletes_key() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            final MemcachedLock<String, Long> lock = new MemcachedLock<>(url);
            final MemcachedClient mockMc = ctorIntercept.constructed().get(0);

            OperationFuture<Boolean> deleted = immediateBooleanFuture(true);
            when(mockMc.delete("k")).thenReturn(deleted);

            assertTrue(lock.unlock("k"));
            verify(mockMc).delete("k");
        }
    }

    /**
     * isLocked() should return false when mc.get() returns null (i.e., no lock present).
     */
    @Test
    public void test_isLocked_false_when_not_held() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            final MemcachedLock<String, Long> lock = new MemcachedLock<>(url);
            final MemcachedClient mockMc = ctorIntercept.constructed().get(0);

            when(mockMc.get("k")).thenReturn(null);

            assertFalse(lock.isLocked("k"));
        }
    }

    /**
     * get() should return null when the stored value is an empty byte array — this is the
     * documented sentinel used by the no-value form of lock(target, ttl).
     */
    @Test
    public void test_get_returns_null_for_empty_byte_array_sentinel() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            final MemcachedLock<String, Long> lock = new MemcachedLock<>(url);
            final MemcachedClient mockMc = ctorIntercept.constructed().get(0);

            when(mockMc.get("k")).thenReturn(new byte[0]);

            assertNull(lock.get("k"));
        }
    }

    /**
     * Regression test for the MemcachedLock subclassability fix.
     *
     * <p>The Javadoc on {@link MemcachedLock#toKey(Object)} documents two subclass examples
     * (NamespacedLock, HashedLock), but the class itself was declared {@code final}, so any user
     * following the documentation got a compile error. The fix removes the {@code final} keyword.
     *
     * <p>This test simply verifies that subclassing the class — and overriding {@code toKey} —
     * compiles and behaves as expected. The construction of the subclass does require a memcached
     * server, so the test only exercises the static / reflective shape.
     */
    @Test
    public void test_class_is_subclassable() throws Exception {
        // Reflection — does NOT require a memcached server.
        assertFalse_isFinal(MemcachedLock.class);

        // The toKey override is reachable via a subclass at compile-time.
        final NamespacedDummyLock<String> subclass = NamespacedDummyLock.class.cast(NamespacedDummyLock.class.getDeclaredField("STUB").get(null));
        assertEquals("ns:my-resource", subclass.testToKey("my-resource"));
    }

    /**
     * Regression test for the MemcachedLock.toKey() null-check fix.
     *
     * <p>Before the fix, {@code toKey(null)} returned {@code "null"} via {@code N.stringOf(null)},
     * which would have silently created a global single "null" lock key shared across the whole
     * application — anyone passing a null target by accident would collide. After the fix,
     * {@code toKey(null)} throws {@code IllegalArgumentException} (matching its Javadoc contract).
     */
    @Test
    public void test_toKey_rejects_null_target() {
        final NamespacedDummyLock<String> stub;
        try {
            stub = (NamespacedDummyLock<String>) NamespacedDummyLock.class.getDeclaredField("STUB").get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        assertThrows(IllegalArgumentException.class, () -> stub.testToKey(null));
    }

    /**
     * Asserts that the given class is NOT declared {@code final}. This is a static / reflective
     * check and does not instantiate the class.
     */
    private static void assertFalse_isFinal(final Class<?> cls) {
        if (java.lang.reflect.Modifier.isFinal(cls.getModifiers())) {
            org.junit.jupiter.api.Assertions.fail(cls.getName() + " is declared final, but its Javadoc documents subclassing");
        }
    }

    /**
     * lock() with value parameter overload should also forward the same TTL conversion.
     */
    @Test
    public void testLock_WithValue() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            final MemcachedLock<String, Long> lock = new MemcachedLock<>(url);
            final MemcachedClient mockMc = ctorIntercept.constructed().get(0);

            OperationFuture<Boolean> addOk = immediateBooleanFuture(true);
            when(mockMc.add(any(String.class), anyInt(), any())).thenReturn(addOk);

            assertTrue(lock.lock("k", Long.valueOf(42), 5_000L));
            verify(mockMc).add(eq("k"), eq(5), any());
        }
    }

    @Test
    public void testLock_WithNullValue_usesValueLessSentinel() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            final MemcachedLock<String, Long> lock = new MemcachedLock<>(url);
            final MemcachedClient mockMc = ctorIntercept.constructed().get(0);

            OperationFuture<Boolean> addOk = immediateBooleanFuture(true);
            when(mockMc.add(any(String.class), anyInt(), any())).thenReturn(addOk);

            assertTrue(lock.lock("k", null, 5_000L));

            final ArgumentCaptor<Object> valueCaptor = ArgumentCaptor.forClass(Object.class);
            verify(mockMc).add(eq("k"), eq(5), valueCaptor.capture());
            assertTrue(valueCaptor.getValue() instanceof byte[]);
            assertEquals(0, ((byte[]) valueCaptor.getValue()).length);

            when(mockMc.get("k")).thenReturn(valueCaptor.getValue());
            assertTrue(lock.isLocked("k"));
            assertNull(lock.get("k"));
        }
    }

    @Test
    public void testLock_longTtl_usesMemcachedAbsoluteExpiration() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            final MemcachedLock<String, Long> lock = new MemcachedLock<>(url);
            final MemcachedClient mockMc = ctorIntercept.constructed().get(0);

            OperationFuture<Boolean> addOk = immediateBooleanFuture(true);
            when(mockMc.add(any(String.class), anyInt(), any())).thenReturn(addOk);

            final long liveTime = 31L * 24 * 60 * 60 * 1000;
            final long expectedEarliest = System.currentTimeMillis() / 1000L + (liveTime / 1000);

            assertTrue(lock.lock("k", Long.valueOf(42), liveTime));

            final long expectedLatest = System.currentTimeMillis() / 1000L + (liveTime / 1000);
            final ArgumentCaptor<Integer> expirationCaptor = ArgumentCaptor.forClass(Integer.class);
            verify(mockMc).add(eq("k"), expirationCaptor.capture(), any());

            assertTrue(expirationCaptor.getValue() > 30 * 24 * 60 * 60);
            assertTrue(expirationCaptor.getValue() >= expectedEarliest);
            assertTrue(expirationCaptor.getValue() <= expectedLatest);
        }
    }

    @Test
    public void testLock_preservesMemcachedIllegalArgumentException() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            final MemcachedLock<String, Long> lock = new MemcachedLock<>(url);
            final MemcachedClient mockMc = ctorIntercept.constructed().get(0);

            when(mockMc.add(any(String.class), anyInt(), any())).thenThrow(new IllegalArgumentException("bad memcached key"));

            assertThrows(IllegalArgumentException.class, () -> lock.lock("k", Long.valueOf(42), 5_000L));
        }
    }

    @Test
    public void testUnlock_preservesMemcachedIllegalArgumentException() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            final MemcachedLock<String, Long> lock = new MemcachedLock<>(url);
            final MemcachedClient mockMc = ctorIntercept.constructed().get(0);

            when(mockMc.delete("k")).thenThrow(new IllegalArgumentException("bad memcached key"));

            assertThrows(IllegalArgumentException.class, () -> lock.unlock("k"));
        }
    }

    @Test
    public void testLock_EdgeCase_NullTarget() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            final MemcachedLock<String, Long> lock = new MemcachedLock<>(url);
            assertThrows(IllegalArgumentException.class, () -> lock.lock(null, 1000L));
            assertThrows(IllegalArgumentException.class, () -> lock.lock(null, Long.valueOf(1), 1000L));
        }
    }

    @Test
    public void testLock_EdgeCase_NonPositiveLiveTime() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            final MemcachedLock<String, Long> lock = new MemcachedLock<>(url);
            assertThrows(IllegalArgumentException.class, () -> lock.lock("k", 0L));
            assertThrows(IllegalArgumentException.class, () -> lock.lock("k", -1L));
            assertThrows(IllegalArgumentException.class, () -> lock.lock("k", Long.valueOf(1), 0L));
        }
    }

    @Test
    public void testIsLocked_EdgeCase_NullTarget() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            final MemcachedLock<String, Long> lock = new MemcachedLock<>(url);
            assertThrows(IllegalArgumentException.class, () -> lock.isLocked(null));
        }
    }

    @Test
    public void testGet_EdgeCase_NullTarget() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            final MemcachedLock<String, Long> lock = new MemcachedLock<>(url);
            assertThrows(IllegalArgumentException.class, () -> lock.get(null));
        }
    }

    @Test
    public void testUnlock_EdgeCase_NullTarget() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            final MemcachedLock<String, Long> lock = new MemcachedLock<>(url);
            assertThrows(IllegalArgumentException.class, () -> lock.unlock(null));
        }
    }

    @Test
    public void testGet_ReturnsStoredValue() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            final MemcachedLock<String, String> lock = new MemcachedLock<>(url);
            final MemcachedClient mockMc = ctorIntercept.constructed().get(0);
            when(mockMc.get("k")).thenReturn("holder-1");
            assertEquals("holder-1", lock.get("k"));
        }
    }

    @Test
    public void testClient_ReturnsUnderlyingSpyMemcached() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            final MemcachedLock<String, Long> lock = new MemcachedLock<>(url);
            org.junit.jupiter.api.Assertions.assertNotNull(lock.client());
        }
    }

    @Test
    public void testClose_DisconnectsUnderlyingClient() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            final MemcachedLock<String, Long> lock = new MemcachedLock<>(url);
            final MemcachedClient mockMc = ctorIntercept.constructed().get(0);
            lock.close();
            verify(mockMc).shutdown();
        }
    }

    @Test
    public void testConstructor_EdgeCase_EmptyServerUrl() {
        assertThrows(IllegalArgumentException.class, () -> new MemcachedLock<String, Long>(""));
    }

    @Test
    public void testConstructor_EdgeCase_NullServerUrl() {
        assertThrows(IllegalArgumentException.class, () -> new MemcachedLock<String, Long>(null));
    }

    /**
     * A non-IllegalArgument communication error from the underlying client during lock() is wrapped
     * in a RuntimeException carrying the lock key (the IllegalArgument branch is covered separately).
     */
    @Test
    public void testLock_wrapsMemcachedCommunicationError() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            final MemcachedLock<String, Long> lock = new MemcachedLock<>(url);
            final MemcachedClient mockMc = ctorIntercept.constructed().get(0);

            when(mockMc.add(any(String.class), anyInt(), any())).thenThrow(new IllegalStateException("memcached down"));

            final RuntimeException ex = assertThrows(RuntimeException.class, () -> lock.lock("k", 5_000L));
            assertTrue(ex.getMessage().contains("Failed to acquire lock for key: k"));
        }
    }

    /**
     * A non-IllegalArgument communication error from the underlying client during unlock() is wrapped
     * in a RuntimeException carrying the lock key.
     */
    @Test
    public void testUnlock_wrapsMemcachedCommunicationError() {
        try (MockedConstruction<MemcachedClient> ctorIntercept = Mockito.mockConstruction(MemcachedClient.class)) {
            final MemcachedLock<String, Long> lock = new MemcachedLock<>(url);
            final MemcachedClient mockMc = ctorIntercept.constructed().get(0);

            when(mockMc.delete("k")).thenThrow(new IllegalStateException("memcached down"));

            final RuntimeException ex = assertThrows(RuntimeException.class, () -> lock.unlock("k"));
            assertTrue(ex.getMessage().contains("Failed to release lock for key: k"));
        }
    }

    /**
     * Returns an immediately-complete future yielding the supplied value. Used so that
     * spymemcached's resultOf() returns the canned value without blocking on the wire.
     */
    @SuppressWarnings("unchecked")
    private static <T> OperationFuture<T> immediateBooleanFuture(T value) {
        OperationFuture<T> f = mock(OperationFuture.class);
        try {
            when(f.get(org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.any())).thenReturn(value);
            when(f.get()).thenReturn(value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return f;
    }

    /**
     * A minimal subclass used to verify that MemcachedLock can be subclassed and that
     * the protected {@link MemcachedLock#toKey(Object)} method can be overridden.
     *
     * <p>The class never connects to memcached — instances are only ever used to call the
     * test-only {@link #testToKey(Object)} method, which delegates to the (possibly overridden)
     * {@code toKey}. A real connection would require a running memcached server, which we do
     * not want to depend on for a pure-Java unit test.
     */
    static final class NamespacedDummyLock<V> extends MemcachedLock<String, V> {
        /**
         * A pre-constructed STUB instance available via reflection so callers don't have to
         * connect to memcached just to test the toKey contract. The constructor itself does
         * still build a SpyMemcached client lazily; we tolerate that here because the test
         * only invokes {@link #testToKey(Object)} (a pure method that does not touch the wire).
         */
        static final NamespacedDummyLock<String> STUB;
        static {
            NamespacedDummyLock<String> tmp = null;
            try {
                tmp = new NamespacedDummyLock<>("localhost:11211");
            } catch (final Throwable t) {
                // If we can't construct (e.g., no memcached client present), fall back to a
                // null sentinel and let the dependent test fail gracefully. In practice
                // SpyMemcached's constructor does not actually connect (Jedis-style lazy),
                // so this rarely fires.
            }
            STUB = tmp;
        }

        NamespacedDummyLock(final String url) {
            super(url);
        }

        @Override
        protected String toKey(final String target) {
            // Force the base class's null check to run before applying our namespace.
            return "ns:" + super.toKey(target);
        }

        // Test-only shim that exposes the protected method.
        String testToKey(final String target) {
            return toKey(target);
        }

        @Override
        public void close() {
            // Suppress any error from closing without a real server.
            try {
                super.close();
            } catch (final Throwable ignore) {
                // OK in test
            }
        }
    }

}
