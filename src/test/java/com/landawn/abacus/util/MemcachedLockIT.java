/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MemcachedLockIT {

    private static final String SERVER_URL = com.landawn.abacus.cache.MemcachedTestSupport.serverUrl();

    private static MemcachedLock<String, Object> lock;

    @BeforeAll
    static void connect() {
        lock = new MemcachedLock<>(SERVER_URL);
    }

    @AfterAll
    static void close() {
        if (lock != null) {
            lock.close();
        }
    }

    @BeforeEach
    void flush() {
        lock.client().flushAll();
    }

    // Reads

    @Test
    void getReturnsStoredValue() {
        assertTrue(lock.tryLock("k", "holder-1", 5_000L));
        assertEquals("holder-1", lock.get("k"));
    }

    // Writes

    @Test
    void lockAcquiresWhenAbsent() {
        assertTrue(lock.tryLock("mysql", 10_000), "first lock() should acquire");
        assertTrue(lock.isLocked("mysql"));
        // A value-less lock(target, liveTime) stores the empty-byte sentinel, which get() maps to null.
        assertNull(lock.get("mysql"));
    }

    @Test
    void lockLongTtlIsStoredAndRetrievable() {
        // A TTL beyond 30 days is converted to an absolute Unix expiration timestamp; the lock value
        // must be immediately retrievable (not stored already-expired).
        final long liveTime = 31L * 24 * 60 * 60 * 1000; // 31 days
        assertTrue(lock.tryLock("k", Long.valueOf(42), liveTime));
        assertEquals(Long.valueOf(42), lock.get("k"));
    }

    @Test
    void lockReturnsFalseWhenAlreadyHeld() {
        assertTrue(lock.tryLock("k", 5_000));
        assertFalse(lock.tryLock("k", 5_000), "second acquire on a held lock must fail");
    }

    @Test
    void lockWithNullValueUsesValueLessSentinel() {
        assertTrue(lock.tryLock("k", null, 5_000L));
        assertTrue(lock.isLocked("k"));
        // A null value is stored as the same empty-byte sentinel as the no-value overload.
        assertNull(lock.get("k"));
    }

    @Test
    void lockWithValue() {
        assertTrue(lock.tryLock("k", Long.valueOf(42), 5_000L));
        assertTrue(lock.isLocked("k"));
        assertEquals(Long.valueOf(42), lock.get("k"));
    }

    // Removals

    @Test
    void tryUnlockQuietlyDeletesKeyAndReturnsTrue() {
        assertTrue(lock.tryLock("k", 10_000));
        assertTrue(lock.unlockQuietly("k"));
        assertFalse(lock.isLocked("k"));
        // Quietly releasing an already-released lock reports false (nothing was deleted).
        assertFalse(lock.unlockQuietly("k"));
    }

    @Test
    void unlockDeletesKeyAndReturnsTrue() {
        assertTrue(lock.tryLock("k", 10_000));
        assertTrue(lock.tryUnlock("k"));
        assertFalse(lock.isLocked("k"));
        // Releasing an already-released lock reports false (nothing was deleted).
        assertFalse(lock.tryUnlock("k"));
    }

    // Membership And Views

    @Test
    void isLockedFalseWhenNotHeld() {
        assertFalse(lock.isLocked("never-locked"));
    }

    // Compatibility

    /**
     * The deprecated {@code lock}/{@code unlock}/{@code tryUnlockQuietly} aliases must keep delegating
     * to their renamed counterparts ({@code tryLock}/{@code tryUnlock}/{@code unlockQuietly}) so
     * existing callers keep working until the aliases are removed.
     */
    @Test
    @SuppressWarnings("deprecation")
    void deprecatedAliasesDelegateToNewNames() {
        assertTrue(lock.lock("dep", 10_000), "deprecated lock() should still acquire");
        assertTrue(lock.isLocked("dep"));
        assertTrue(lock.unlock("dep"), "deprecated unlock() should still release");
        assertFalse(lock.isLocked("dep"));

        assertTrue(lock.lock("dep", "holder", 10_000L), "deprecated value lock() should still acquire");
        assertEquals("holder", lock.get("dep"));
        assertTrue(lock.tryUnlockQuietly("dep"), "deprecated tryUnlockQuietly() should still release");
        assertFalse(lock.isLocked("dep"));
    }
}
