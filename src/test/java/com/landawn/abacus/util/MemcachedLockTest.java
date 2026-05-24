/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("2025")
public class MemcachedLockTest {
    final String url = "localhost:11211";
    final MemcachedLock<String, Long> memcachedLock = new MemcachedLock<>(url);

    @Test
    public void test_lock() {
        String key = "mysql";
        memcachedLock.lock(key, 10000);

        assertTrue(memcachedLock.isLocked(key));

        Object value = memcachedLock.get(key);

        N.println(value);
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
        final NamespacedDummyLock<String> subclass = NamespacedDummyLock.class.cast(
                NamespacedDummyLock.class.getDeclaredField("STUB").get(null));
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

    private static void assertFalse(final boolean condition) {
        org.junit.jupiter.api.Assertions.assertFalse(condition);
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
