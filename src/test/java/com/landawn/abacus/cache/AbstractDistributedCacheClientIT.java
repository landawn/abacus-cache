/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.util.Strings;

class AbstractDistributedCacheClientIT {

    private static final String SERVER_URL = com.landawn.abacus.cache.MemcachedTestSupport.serverUrl();

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

    // Writes

    @Test
    void incrMissingKeyReturnsMinusOne() {
        // Memcached's sentinel for incrementing a non-existent counter, surfaced through the abstract op.
        assertEquals(-1L, client.incr("abstract-client-missing:" + Strings.uuid()));
    }

    // Compatibility

    @SuppressWarnings("deprecation")
    @Test
    void legacyWriteNamesDelegateToCurrentImplementation() {
        final String key = "abstract-client-legacy:" + Strings.uuid();
        assertTrue(client.set(key, "v", 60_000));
        assertEquals("v", client.get(key));
        assertTrue(client.delete(key));
        assertNull(client.get(key));
    }

    // Other Behavior

    /**
     * The concrete client's abstract operations actually reach the server: a value set through the
     * base-class-typed reference is read back and then deleted.
     */
    @Test
    void abstractOperationsRoundTripAgainstRealServer() {
        final String key = "abstract-client:" + Strings.uuid();
        assertTrue(client.put(key, "v", 60_000));
        assertEquals("v", client.get(key));
        assertTrue(client.remove(key));
        assertNull(client.get(key));
    }

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
}
