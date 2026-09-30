package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.landawn.abacus.util.MemcachedLock;

/** Service-free argument and lifecycle coverage for {@link MemcachedLock}. */
@Tag("2025")
public class MemcachedLockValidationUnitTest {

    private static final String SERVER_URL = "localhost:11211";

    @Test
    public void nullKeyFromOverrideIsRejectedBeforeAnyNetworkOperation() {
        try (NullKeyLock lock = new NullKeyLock()) {
            assertThrows(IllegalArgumentException.class, () -> lock.tryLock("target", 1_000L));
            assertThrows(IllegalArgumentException.class, () -> lock.isLocked("target"));
            assertThrows(IllegalArgumentException.class, () -> lock.get("target"));
            assertThrows(IllegalArgumentException.class, () -> lock.tryUnlock("target"));
            assertThrows(IllegalArgumentException.class, () -> lock.unlockQuietly("target"));
        }
    }

    /**
     * N.checkArg* treats a long message containing a space as the complete message, so the old
     * {@code "key returned by toKey"} argument produced an IAE whose whole message was that fragment,
     * without saying what was wrong with the key.
     */
    @Test
    public void nullKeyFromOverrideReportsACompleteMessage() {
        try (NullKeyLock lock = new NullKeyLock()) {
            for (final IllegalArgumentException e : List.of(assertThrows(IllegalArgumentException.class, () -> lock.tryLock("target", 1_000L)),
                    assertThrows(IllegalArgumentException.class, () -> lock.unlockQuietly("target")))) {
                assertEquals("The key returned by toKey must not be null", e.getMessage());
            }
        }
    }

    @Test
    public void ordinaryOperationsCheckClosedStateBeforeArguments() {
        final MemcachedLock<String, String> lock = new MemcachedLock<>(SERVER_URL);
        lock.close();

        assertThrows(IllegalStateException.class, () -> lock.tryLock(null, 1_000L));
        assertThrows(IllegalStateException.class, () -> lock.isLocked(null));
        assertThrows(IllegalStateException.class, () -> lock.get(null));
        assertThrows(IllegalStateException.class, () -> lock.tryUnlock(null));
        assertFalse(lock.unlockQuietly("valid-key"));
        assertThrows(IllegalArgumentException.class, () -> lock.unlockQuietly(null));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void malformedKeysCannotAcquireOrReleaseAnotherTargetsLease() throws Exception {
        final SpyMemcached<String> delegate = mock(SpyMemcached.class);
        final MemcachedLock<String, String> lock = lockWithDelegate(delegate);

        for (final String malformed : List.of("key" + (char) 0xD800, "key" + (char) 0xDC00, "key" + (char) 0xD800 + "x")) {
            assertThrows(IllegalArgumentException.class, () -> lock.tryLock(malformed, 1_000));
            assertThrows(IllegalArgumentException.class, () -> lock.tryLock(malformed, "holder", 1_000));
            assertThrows(IllegalArgumentException.class, () -> lock.isLocked(malformed));
            assertThrows(IllegalArgumentException.class, () -> lock.get(malformed));
            assertThrows(IllegalArgumentException.class, () -> lock.tryUnlock(malformed));
            assertThrows(IllegalArgumentException.class, () -> lock.unlockQuietly(malformed));
        }
        verifyNoInteractions(delegate);

        lock.close();
        assertThrows(IllegalArgumentException.class, () -> lock.unlockQuietly("key" + (char) 0xD800));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void malformedKeyReturnedByOverrideIsRejectedAndPairedSurrogatesAreAccepted() throws Exception {
        final SpyMemcached<String> delegate = mock(SpyMemcached.class);
        final NullKeyLock overridden = mock(NullKeyLock.class, CALLS_REAL_METHODS);
        setDelegate(overridden, delegate);
        doReturn("key" + (char) 0xDC00).when(overridden).toKey("target");
        assertThrows(IllegalArgumentException.class, () -> overridden.unlockQuietly("target"));
        verifyNoInteractions(delegate);

        final MemcachedLock<String, String> lock = lockWithDelegate(delegate);
        final String key = "key:" + new String(Character.toChars(0x1F600));
        when(delegate.add(key, "holder", 1_000L)).thenReturn(true);
        assertTrue(lock.tryLock(key, "holder", 1_000L));
        verify(delegate).add(key, "holder", 1_000L);
    }

    @Test
    public void nullOrBlankServerUrlIsRejectedWithIae() {
        assertThrows(IllegalArgumentException.class, () -> new MemcachedLock<String, String>(null));
        assertThrows(IllegalArgumentException.class, () -> new MemcachedLock<String, String>(""));
        assertThrows(IllegalArgumentException.class, () -> new MemcachedLock<String, String>("   "));
    }

    @Test
    @SuppressWarnings({ "unchecked", "removal" })
    public void nullTargetIsRejectedWithIaeBeforeAnyNetworkOperation() throws Exception {
        final SpyMemcached<String> delegate = mock(SpyMemcached.class);
        final MemcachedLock<String, String> lock = lockWithDelegate(delegate);

        assertThrows(IllegalArgumentException.class, () -> lock.tryLock(null, 1_000L));
        assertThrows(IllegalArgumentException.class, () -> lock.tryLock(null, "holder", 1_000L));
        assertThrows(IllegalArgumentException.class, () -> lock.isLocked(null));
        assertThrows(IllegalArgumentException.class, () -> lock.get(null));
        assertThrows(IllegalArgumentException.class, () -> lock.tryUnlock(null));
        assertThrows(IllegalArgumentException.class, () -> lock.unlockQuietly(null));
        assertThrows(IllegalArgumentException.class, () -> lock.lock(null, 1_000L));
        assertThrows(IllegalArgumentException.class, () -> lock.lock(null, "holder", 1_000L));
        assertThrows(IllegalArgumentException.class, () -> lock.unlock(null));
        assertThrows(IllegalArgumentException.class, () -> lock.tryUnlockQuietly(null));
        verifyNoInteractions(delegate);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void nullValueIsStoredAsTheValueLessMarker() throws Exception {
        final SpyMemcached<Object> delegate = mock(SpyMemcached.class);
        final MemcachedLock<String, Object> lock = mock(MemcachedLock.class, CALLS_REAL_METHODS);
        final Field clientField = MemcachedLock.class.getDeclaredField("mc");
        clientField.setAccessible(true);
        clientField.set(lock, delegate);

        final ArgumentCaptor<Object> stored = ArgumentCaptor.forClass(Object.class);
        when(delegate.add(eq("target"), stored.capture(), eq(1_000L))).thenReturn(true);

        assertTrue(lock.tryLock("target", null, 1_000L));
        assertTrue(stored.getValue() instanceof byte[]);
        assertEquals(0, ((byte[]) stored.getValue()).length);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void nonPositiveLiveTimeIsRejectedBeforeAnyNetworkOperation() throws Exception {
        final SpyMemcached<String> delegate = mock(SpyMemcached.class);
        final MemcachedLock<String, String> lock = lockWithDelegate(delegate);

        assertThrows(IllegalArgumentException.class, () -> lock.tryLock("target", 0L));
        assertThrows(IllegalArgumentException.class, () -> lock.tryLock("target", -1L));
        assertThrows(IllegalArgumentException.class, () -> lock.tryLock("target", "holder", 0L));
        assertThrows(IllegalArgumentException.class, () -> lock.tryLock("target", "holder", Long.MIN_VALUE));
        verifyNoInteractions(delegate);
    }

    /** Client failures: IAE propagates unchanged, others are wrapped (tryLock/tryUnlock) or swallowed (unlockQuietly). */
    @Test
    @SuppressWarnings("unchecked")
    public void clientFailuresArePropagatedWrappedOrSwallowedAsDocumented() throws Exception {
        final SpyMemcached<String> delegate = mock(SpyMemcached.class);
        final MemcachedLock<String, String> lock = lockWithDelegate(delegate);

        final IllegalArgumentException tooLarge = new IllegalArgumentException("Cannot cache data larger than 8 bytes");
        when(delegate.add("rejected", "holder", 1_000L)).thenThrow(tooLarge);
        assertSame(tooLarge, assertThrows(IllegalArgumentException.class, () -> lock.tryLock("rejected", "holder", 1_000L)));

        final RuntimeException timeout = new RuntimeException("timed out");
        when(delegate.add("slow", "holder", 1_000L)).thenThrow(timeout);
        final RuntimeException wrappedLock = assertThrows(RuntimeException.class, () -> lock.tryLock("slow", "holder", 1_000L));
        assertSame(timeout, wrappedLock.getCause());

        final IllegalStateException queueFull = new IllegalStateException("queue full");
        when(delegate.remove("slow")).thenThrow(queueFull);
        final RuntimeException wrappedUnlock = assertThrows(RuntimeException.class, () -> lock.tryUnlock("slow"));
        assertSame(queueFull, wrappedUnlock.getCause());
        assertFalse(lock.unlockQuietly("slow"));

        when(delegate.remove("held")).thenReturn(true);
        assertTrue(lock.tryUnlock("held"));
        assertTrue(lock.unlockQuietly("held"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getNormalizesTheValueLessMarkerAndIsLockedReportsPresence() throws Exception {
        final SpyMemcached<Object> delegate = mock(SpyMemcached.class);
        final MemcachedLock<String, Object> lock = mock(MemcachedLock.class, CALLS_REAL_METHODS);
        final Field clientField = MemcachedLock.class.getDeclaredField("mc");
        clientField.setAccessible(true);
        clientField.set(lock, delegate);

        when(delegate.get("valueless")).thenReturn(new byte[0]);
        when(delegate.get("valued")).thenReturn("holder");

        assertTrue(lock.isLocked("valueless"));
        assertNull(lock.get("valueless"));
        assertTrue(lock.isLocked("valued"));
        assertEquals("holder", lock.get("valued"));
        assertFalse(lock.isLocked("absent"));
        assertNull(lock.get("absent"));
    }

    @SuppressWarnings("unchecked")
    private static MemcachedLock<String, String> lockWithDelegate(final SpyMemcached<String> delegate) throws Exception {
        final MemcachedLock<String, String> lock = mock(MemcachedLock.class, CALLS_REAL_METHODS);
        setDelegate(lock, delegate);
        return lock;
    }

    private static void setDelegate(final MemcachedLock<String, String> lock, final SpyMemcached<String> delegate) throws Exception {
        final Field clientField = MemcachedLock.class.getDeclaredField("mc");
        clientField.setAccessible(true);
        clientField.set(lock, delegate);
    }

    private static final class NullKeyLock extends MemcachedLock<String, String> {

        NullKeyLock() {
            super(SERVER_URL);
        }

        @Override
        protected String toKey(final String target) {
            return null;
        }
    }
}
