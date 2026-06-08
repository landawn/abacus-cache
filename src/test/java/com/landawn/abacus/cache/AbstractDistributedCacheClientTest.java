/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collection;

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
 * <p>No mock client and no in-memory fake is used. {@link MemcachedBackedClient} delegates the abstract
 * operations ({@code get}/{@code set}/{@code delete}/{@code incr}/{@code decr}/{@code disconnect}) to an
 * internal {@link SpyMemcached}, so they genuinely round-trip through the server. Crucially it does
 * <em>not</em> override {@link AbstractDistributedCacheClient#getBulk getBulk} or
 * {@link AbstractDistributedCacheClient#flushAll flushAll}, so the base class's default behavior (both
 * throw {@link UnsupportedOperationException}) and its {@code toSeconds} helper remain the focus here.
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
        public boolean set(final String key, final T value, final long liveTime) {
            return delegate.set(key, value, liveTime);
        }

        @Override
        public boolean delete(final String key) {
            return delegate.delete(key);
        }

        @Override
        public long incr(final String key) {
            return delegate.incr(key);
        }

        @Override
        public long incr(final String key, final int delta) {
            return delegate.incr(key, delta);
        }

        @Override
        public long decr(final String key) {
            return delegate.decr(key);
        }

        @Override
        public long decr(final String key, final int delta) {
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
        assertTrue(client.set(key, "v", 60_000));
        assertEquals("v", client.get(key));
        assertTrue(client.delete(key));
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
        // Per DistributedCacheClient.set(...) contract: 0 or negative means "no expiration".
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
