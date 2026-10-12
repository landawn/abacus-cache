package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.TestBase;
import com.landawn.abacus.util.MemcachedLock;

/** Unicode regression coverage for allocation-free validation of lock targets. */
public class MemcachedLockUnicodeUnitTest extends TestBase {

    @Test
    @SuppressWarnings("unchecked")
    public void acceptsSupplementaryCodePointBoundariesAndTheExactUtf8KeyLimit() throws Exception {
        final SpyMemcached<String> delegate = mock(SpyMemcached.class);
        final MemcachedLock<String, String> lock = lockWithDelegate(delegate);
        final String firstSupplementary = new String(Character.toChars(Character.MIN_SUPPLEMENTARY_CODE_POINT));
        final String lastSupplementary = new String(Character.toChars(Character.MAX_CODE_POINT));
        final List<String> keys = List.of(firstSupplementary, lastSupplementary, firstSupplementary + lastSupplementary,
                "a".repeat(246) + lastSupplementary, "prefix" + firstSupplementary + "suffix");

        for (final String key : keys) {
            when(delegate.add(key, "holder", 1_000L)).thenReturn(true);
            when(delegate.remove(key)).thenReturn(true);
            assertTrue(lock.tryLock(key, "holder", 1_000L));
            assertTrue(lock.tryUnlock(key));
            verify(delegate).add(key, "holder", 1_000L);
            verify(delegate).remove(key);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void rejectsMalformedSurrogateSequencesBeforeEveryOperation() throws Exception {
        final SpyMemcached<String> delegate = mock(SpyMemcached.class);
        final MemcachedLock<String, String> lock = lockWithDelegate(delegate);
        final List<String> invalid = List.of("\uD800", "\uDBFF", "\uDC00", "\uDFFF", "\uDC00\uD800", "a\uD800x",
                "\uD800\uD800\uDC00", "\uD800\uDC00\uDC00", "\uD800\uDC00\uDBFF", "a".repeat(247) + "\uDBFF\uDFFF");

        for (final String key : invalid) {
            assertThrows(IllegalArgumentException.class, () -> lock.tryLock(key, "holder", 1_000L));
            assertThrows(IllegalArgumentException.class, () -> lock.isLocked(key));
            assertThrows(IllegalArgumentException.class, () -> lock.get(key));
            assertThrows(IllegalArgumentException.class, () -> lock.tryUnlock(key));
            assertThrows(IllegalArgumentException.class, () -> lock.unlockQuietly(key));
        }
        verifyNoInteractions(delegate);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void lockRejectsKeysExactlyLikeTheClient() throws Exception {
        final MemcachedLock<String, String> lock = lockWithDelegate(mock(SpyMemcached.class));

        for (final String key : List.of("a\uD800x", "\uDC00", "", "has space", "a".repeat(251))) {
            final String clientMessage = assertThrows(IllegalArgumentException.class, () -> SpyMemcached.checkKey(key)).getMessage();
            assertEquals(clientMessage, assertThrows(IllegalArgumentException.class, () -> lock.tryLock(key, "holder", 1_000L)).getMessage());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void quietReleaseStillValidatesMalformedKeysAfterClose() throws Exception {
        final SpyMemcached<String> delegate = mock(SpyMemcached.class);
        final MemcachedLock<String, String> lock = lockWithDelegate(delegate);
        lock.close();

        assertThrows(IllegalArgumentException.class, () -> lock.unlockQuietly("\uD800\uDC00\uDC00"));
        assertFalse(lock.unlockQuietly("\uD800\uDC00"));
        verify(delegate).disconnect();
    }

    @SuppressWarnings("unchecked")
    private static MemcachedLock<String, String> lockWithDelegate(final SpyMemcached<String> delegate) throws Exception {
        final MemcachedLock<String, String> lock = mock(MemcachedLock.class, CALLS_REAL_METHODS);
        final Field clientField = MemcachedLock.class.getDeclaredField("mc");
        clientField.setAccessible(true);
        clientField.set(lock, delegate);
        return lock;
    }
}
