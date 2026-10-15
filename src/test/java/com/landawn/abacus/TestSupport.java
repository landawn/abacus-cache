package com.landawn.abacus;

import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

/** Bounded polling for externally scheduled work; conditions must not refresh the state being observed. */
public final class TestSupport {
    private TestSupport() {
    }

    public static void awaitCondition(final String description, final BooleanSupplier condition) {
        final long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!condition.getAsBoolean()) {
            if (Thread.currentThread().isInterrupted()) {
                fail("Interrupted while waiting for " + description);
            }
            if (System.nanoTime() - deadline >= 0) {
                fail("Timed out waiting for " + description);
            }
            LockSupport.parkNanos(Duration.ofMillis(1).toNanos());
        }
    }
}
