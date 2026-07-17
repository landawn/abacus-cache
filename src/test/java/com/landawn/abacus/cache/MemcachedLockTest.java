/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.landawn.abacus.util.MemcachedLock;

/**
 * Integration tests for {@link MemcachedLock} that run against a real Memcached server reachable at
 * {@code localhost:11211} (e.g. {@code docker run --name memcached -p 11211:11211 -d memcached:latest}).
 *
 * <p>Lock/lease behavior round-trips through the real server. The lifecycle regression uses a
 * construction mock so it can pause inside disconnect and prove the closed state is published before
 * client teardown starts. A single real lock instance is shared across the class and the server is
 * flushed before each test so a fresh integration test always sees an unlocked world.
 */
@Tag("2025")
public class MemcachedLockTest {

    private static final String SERVER_URL = "localhost:11211";

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

    // --- acquire / release ---------------------------------------------------------------------

    @Test
    public void test_lock_acquires_when_absent() {
        assertTrue(lock.tryLock("mysql", 10_000), "first lock() should acquire");
        assertTrue(lock.isLocked("mysql"));
        // A value-less lock(target, liveTime) stores the empty-byte sentinel, which get() maps to null.
        assertNull(lock.get("mysql"));
    }

    @Test
    public void test_lock_returns_false_when_already_held() {
        assertTrue(lock.tryLock("k", 5_000));
        assertFalse(lock.tryLock("k", 5_000), "second acquire on a held lock must fail");
    }

    @Test
    public void test_unlock_deletes_key_and_returns_true() {
        assertTrue(lock.tryLock("k", 10_000));
        assertTrue(lock.tryUnlock("k"));
        assertFalse(lock.isLocked("k"));
        // Releasing an already-released lock reports false (nothing was deleted).
        assertFalse(lock.tryUnlock("k"));
    }

    @Test
    public void test_isLocked_false_when_not_held() {
        assertFalse(lock.isLocked("never-locked"));
    }

    @Test
    public void test_get_returns_null_for_value_less_lock() {
        assertTrue(lock.tryLock("k", 10_000));
        assertNull(lock.get("k"));
    }

    // --- value-bearing locks -------------------------------------------------------------------

    @Test
    public void testLock_WithValue() {
        assertTrue(lock.tryLock("k", Long.valueOf(42), 5_000L));
        assertTrue(lock.isLocked("k"));
        assertEquals(Long.valueOf(42), lock.get("k"));
    }

    @Test
    public void testLock_WithNullValue_usesValueLessSentinel() {
        assertTrue(lock.tryLock("k", null, 5_000L));
        assertTrue(lock.isLocked("k"));
        // A null value is stored as the same empty-byte sentinel as the no-value overload.
        assertNull(lock.get("k"));
    }

    @Test
    public void testGet_ReturnsStoredValue() {
        assertTrue(lock.tryLock("k", "holder-1", 5_000L));
        assertEquals("holder-1", lock.get("k"));
    }

    @Test
    public void testLock_longTtl_isStoredAndRetrievable() {
        // A TTL beyond 30 days is converted to an absolute Unix expiration timestamp; the lock value
        // must be immediately retrievable (not stored already-expired).
        final long liveTime = 31L * 24 * 60 * 60 * 1000; // 31 days
        assertTrue(lock.tryLock("k", Long.valueOf(42), liveTime));
        assertEquals(Long.valueOf(42), lock.get("k"));
    }

    // --- validation ----------------------------------------------------------------------------

    @Test
    public void testLock_EdgeCase_NullTarget() {
        assertThrows(IllegalArgumentException.class, () -> lock.tryLock(null, 1000L));
        assertThrows(IllegalArgumentException.class, () -> lock.tryLock(null, Long.valueOf(1), 1000L));
    }

    @Test
    public void testLock_EdgeCase_NonPositiveLiveTime() {
        assertThrows(IllegalArgumentException.class, () -> lock.tryLock("k", 0L));
        assertThrows(IllegalArgumentException.class, () -> lock.tryLock("k", -1L));
        assertThrows(IllegalArgumentException.class, () -> lock.tryLock("k", Long.valueOf(1), 0L));
    }

    @Test
    public void testIsLocked_EdgeCase_NullTarget() {
        assertThrows(IllegalArgumentException.class, () -> lock.isLocked(null));
    }

    @Test
    public void testGet_EdgeCase_NullTarget() {
        assertThrows(IllegalArgumentException.class, () -> lock.get(null));
    }

    @Test
    public void testUnlock_EdgeCase_NullTarget() {
        assertThrows(IllegalArgumentException.class, () -> lock.tryUnlock(null));
    }

    /**
     * An invalid Memcached key (longer than the 250-byte limit) makes the underlying client throw an
     * {@link IllegalArgumentException}, which {@code lock()} must propagate unchanged (its
     * dedicated catch branch rethrows {@code IllegalArgumentException} rather than wrapping it).
     */
    @Test
    public void testLock_preservesMemcachedIllegalArgumentException() {
        final String tooLongKey = "x".repeat(251);
        assertThrows(IllegalArgumentException.class, () -> lock.tryLock(tooLongKey, Long.valueOf(42), 5_000L));
    }

    @Test
    public void testUnlock_preservesMemcachedIllegalArgumentException() {
        final String tooLongKey = "x".repeat(251);
        assertThrows(IllegalArgumentException.class, () -> lock.tryUnlock(tooLongKey));
    }

    // --- tryUnlockQuietly ----------------------------------------------------------------------

    @Test
    public void test_tryUnlockQuietly_deletes_key_and_returns_true() {
        assertTrue(lock.tryLock("k", 10_000));
        assertTrue(lock.unlockQuietly("k"));
        assertFalse(lock.isLocked("k"));
        // Quietly releasing an already-released lock reports false (nothing was deleted).
        assertFalse(lock.unlockQuietly("k"));
    }

    @Test
    public void testTryUnlockQuietly_EdgeCase_NullTarget() {
        // A null target is a deterministic programming error, so it is still rejected (not swallowed).
        assertThrows(IllegalArgumentException.class, () -> lock.unlockQuietly(null));
    }

    /**
     * A key the memcached client rejects is a deterministic programming error, so
     * {@code tryUnlockQuietly} rethrows it exactly like {@link MemcachedLock#tryUnlock(Object)} —
     * swallowing it would disguise the bug as "lock already expired". If {@code lock()} succeeded
     * for a target, the same key cannot be rejected at unlock time, so this rethrow can never mask
     * an exception from the guarded critical section in the intended {@code finally}-block usage.
     */
    @Test
    public void testTryUnlockQuietly_EdgeCase_OverlongKey_throwsIAE() {
        final String tooLongKey = "x".repeat(251);
        assertThrows(IllegalArgumentException.class, () -> lock.unlockQuietly(tooLongKey));
    }

    /**
     * A genuine communication failure — here, the underlying client has been shut down, which makes
     * every operation throw {@code IllegalStateException("Shutting down")} — is swallowed and
     * reported as {@code false}, so releasing in a {@code finally} block cannot mask the exception
     * thrown by the guarded critical section.
     */
    @Test
    public void testTryUnlockQuietly_swallowsCommunicationError_returnsFalse() {
        final MemcachedLock<String, Long> local = new MemcachedLock<>(SERVER_URL);
        local.close();

        assertFalse(local.unlockQuietly("k"));
    }

    /**
     * The key is validated eagerly, before the closed check: an invalid key is a deterministic
     * programming error and must surface as the documented {@link IllegalArgumentException} even on
     * a closed instance, instead of being downgraded to the quiet {@code false} by the swallowed
     * closed-state {@code IllegalStateException}.
     */
    @Test
    public void testTryUnlockQuietly_EdgeCase_OverlongKeyOnClosedInstance_throwsIAE() {
        final MemcachedLock<String, Long> local = new MemcachedLock<>(SERVER_URL);
        local.close();

        assertThrows(IllegalArgumentException.class, () -> local.unlockQuietly("x".repeat(251)));
    }

    // --- construction / lifecycle --------------------------------------------------------------

    @Test
    public void testConstructor_EdgeCase_EmptyServerUrl() {
        assertThrows(IllegalArgumentException.class, () -> new MemcachedLock<String, Long>(""));
    }

    @Test
    public void testConstructor_EdgeCase_NullServerUrl() {
        assertThrows(IllegalArgumentException.class, () -> new MemcachedLock<String, Long>(null));
    }

    @Test
    public void testClient_ReturnsUnderlyingSpyMemcached() {
        assertNotNull(lock.client());
    }

    @Test
    public void testClose_disconnectsUnderlyingClientAndIsIdempotent() {
        try (MockedConstruction<SpyMemcached> construction = Mockito.mockConstruction(SpyMemcached.class)) {
            final MemcachedLock<String, Long> local = new MemcachedLock<>(SERVER_URL);
            final SpyMemcached<?> mockedClient = construction.constructed().get(0);
            final AtomicReference<MemcachedLock<String, Long>> holder = new AtomicReference<>(local);

            doAnswer(invocation -> {
                // close() must publish its state before entering a potentially slow disconnect.
                assertThrows(IllegalStateException.class, () -> holder.get().isLocked("during-close"));
                return null;
            }).when(mockedClient).disconnect();

            local.close();
            local.close(); // idempotent even though the underlying mock remains callable

            verify(mockedClient, times(1)).disconnect();
            verify(mockedClient, never()).get("during-close");
            assertThrows(IllegalStateException.class, () -> local.isLocked("k"));
            assertThrows(IllegalStateException.class, () -> local.tryLock("k", 1_000));
            assertThrows(IllegalStateException.class, () -> local.get("k"));
            assertThrows(IllegalStateException.class, () -> local.tryUnlock("k"));
            assertFalse(local.unlockQuietly("k"));
        }
    }

    // --- deprecated name aliases ---------------------------------------------------------------

    /**
     * The deprecated {@code lock}/{@code unlock}/{@code tryUnlockQuietly} aliases must keep delegating
     * to their renamed counterparts ({@code tryLock}/{@code tryUnlock}/{@code unlockQuietly}) so
     * existing callers keep working until the aliases are removed.
     */
    @Test
    @SuppressWarnings("deprecation")
    public void test_deprecated_aliases_delegate_to_new_names() {
        assertTrue(lock.lock("dep", 10_000), "deprecated lock() should still acquire");
        assertTrue(lock.isLocked("dep"));
        assertTrue(lock.unlock("dep"), "deprecated unlock() should still release");
        assertFalse(lock.isLocked("dep"));

        assertTrue(lock.lock("dep", "holder", 10_000L), "deprecated value lock() should still acquire");
        assertEquals("holder", lock.get("dep"));
        assertTrue(lock.tryUnlockQuietly("dep"), "deprecated tryUnlockQuietly() should still release");
        assertFalse(lock.isLocked("dep"));
    }

    // --- subclassability of toKey --------------------------------------------------------------

    /**
     * {@link MemcachedLock#toKey(Object)} is documented as overridable; the class must therefore not be
     * {@code final}, and an override must be reachable and used. This also covers the {@code toKey}
     * null-target rejection.
     */
    @Test
    public void test_toKey_is_subclassable_and_rejects_null_target() {
        assertFalse(java.lang.reflect.Modifier.isFinal(MemcachedLock.class.getModifiers()),
                "MemcachedLock must not be final - its Javadoc documents subclassing");

        try (NamespacedLock<String> nsLock = new NamespacedLock<>(SERVER_URL)) {
            assertEquals("ns:my-resource", nsLock.testToKey("my-resource"));
            assertThrows(IllegalArgumentException.class, () -> nsLock.testToKey(null));
        }
    }

    /**
     * A minimal subclass that namespaces keys, used to verify {@link MemcachedLock#toKey(Object)} can be
     * overridden. It connects to the real server (its constructor builds a {@code SpyMemcached}); the
     * test only exercises {@link #testToKey(Object)}, which is a pure transformation.
     */
    static final class NamespacedLock<V> extends MemcachedLock<String, V> {
        NamespacedLock(final String url) {
            super(url);
        }

        @Override
        protected String toKey(final String target) {
            // Delegate to super.toKey first so the base-class null check runs before namespacing.
            return "ns:" + super.toKey(target);
        }

        String testToKey(final String target) {
            return toKey(target);
        }
    }
}
