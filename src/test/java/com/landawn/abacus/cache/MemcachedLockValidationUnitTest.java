package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
