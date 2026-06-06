/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.TestBase;

/**
 * Covers {@link AbstractDistributedCacheClient} via a minimal concrete subclass so the abstract
 * class's bookkeeping (server URL storage, default getBulk/flushAll throwing
 * UnsupportedOperationException, and the toSeconds rounding helper) can be exercised directly.
 */
@Tag("2025")
public class AbstractDistributedCacheClientTest extends TestBase {

    /**
     * Minimal concrete subclass that satisfies the abstract method requirements without doing
     * any I/O. Each abstract method either returns a stub value or throws — they are not
     * exercised by these tests, which target the base class's own behavior.
     */
    private static class DummyClient<T> extends AbstractDistributedCacheClient<T> {
        DummyClient(final String url) {
            super(url);
        }

        @Override
        public T get(final String key) {
            return null;
        }

        @Override
        public boolean set(final String key, final T value, final long liveTime) {
            return true;
        }

        @Override
        public boolean delete(final String key) {
            return true;
        }

        @Override
        public long incr(final String key) {
            return 0;
        }

        @Override
        public long incr(final String key, final int delta) {
            return 0;
        }

        @Override
        public long decr(final String key) {
            return 0;
        }

        @Override
        public long decr(final String key, final int delta) {
            return 0;
        }

        @Override
        public void disconnect() {
            // no-op
        }

        // Test-only access to the protected toSeconds helper.
        int callToSeconds(final long liveTime) {
            return toSeconds(liveTime);
        }
    }

    @Test
    public void testConstructor_EdgeCase_NullServerUrl() {
        assertThrows(IllegalArgumentException.class, () -> new DummyClient<String>(null));
    }

    @Test
    public void testConstructor_EdgeCase_EmptyServerUrl() {
        assertThrows(IllegalArgumentException.class, () -> new DummyClient<String>(""));
    }

    @Test
    public void testConstructor_EdgeCase_BlankServerUrl() {
        // A whitespace-only URL is semantically empty and must be rejected just like "" or null.
        assertThrows(IllegalArgumentException.class, () -> new DummyClient<String>("   "));
        assertThrows(IllegalArgumentException.class, () -> new DummyClient<String>("\t\n"));
    }

    @Test
    public void testServerUrl() {
        final DummyClient<String> client = new DummyClient<>("server:1234");
        assertEquals("server:1234", client.serverUrl());
    }

    @Test
    public void testGetBulk_Varargs_Unsupported() {
        final DummyClient<String> client = new DummyClient<>("s");
        assertThrows(UnsupportedOperationException.class, () -> client.getBulk("k1", "k2"));
    }

    @Test
    public void testGetBulk_Collection_Unsupported() {
        final DummyClient<String> client = new DummyClient<>("s");
        final Collection<String> keys = Arrays.asList("k1", "k2");
        assertThrows(UnsupportedOperationException.class, () -> {
            final Map<String, String> r = client.getBulk(keys);
            assertEquals(0, r.size()); // never reached
        });
    }

    @Test
    public void testFlushAll_Unsupported() {
        final DummyClient<String> client = new DummyClient<>("s");
        assertThrows(UnsupportedOperationException.class, client::flushAll);
    }

    @Test
    public void testToSeconds_ExactSecond() {
        final DummyClient<String> client = new DummyClient<>("s");
        assertEquals(2, client.callToSeconds(2000));
    }

    @Test
    public void testToSeconds_RoundsUpFractionalSecond() {
        final DummyClient<String> client = new DummyClient<>("s");
        // 1500ms -> 2s (rounded up)
        assertEquals(2, client.callToSeconds(1500));
        // 1ms -> 1s
        assertEquals(1, client.callToSeconds(1));
        // 999ms -> 1s
        assertEquals(1, client.callToSeconds(999));
    }

    @Test
    public void testToSeconds_EdgeCase_Zero() {
        final DummyClient<String> client = new DummyClient<>("s");
        assertEquals(0, client.callToSeconds(0));
    }

    @Test
    public void testToSeconds_EdgeCase_NegativeLiveTime() {
        // Per DistributedCacheClient.set(...) contract: 0 or negative means "no expiration".
        // toSeconds must therefore normalize any negative input to 0 rather than throwing.
        final DummyClient<String> client = new DummyClient<>("s");
        assertEquals(0, client.callToSeconds(-1));
        assertEquals(0, client.callToSeconds(-1000));
        assertEquals(0, client.callToSeconds(Long.MIN_VALUE));
    }

    @Test
    public void testToSeconds_EdgeCase_Overflow() {
        final DummyClient<String> client = new DummyClient<>("s");
        // (Integer.MAX_VALUE + 1) * 1000 milliseconds -> seconds exceeds Integer.MAX_VALUE.
        assertThrows(IllegalArgumentException.class, () -> client.callToSeconds(((long) Integer.MAX_VALUE + 1L) * 1000L));
    }

    @Test
    public void testToSeconds_MaxValidMillisRoundsDownAtBoundary() {
        // Exactly Integer.MAX_VALUE seconds in milliseconds — should NOT throw and should equal Integer.MAX_VALUE.
        final DummyClient<String> client = new DummyClient<>("s");
        assertEquals(Integer.MAX_VALUE, client.callToSeconds((long) Integer.MAX_VALUE * 1000L));
    }
}
