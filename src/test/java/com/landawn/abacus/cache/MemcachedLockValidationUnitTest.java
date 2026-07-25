package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
