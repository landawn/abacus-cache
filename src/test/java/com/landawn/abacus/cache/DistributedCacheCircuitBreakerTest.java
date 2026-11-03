package com.landawn.abacus.cache;

import static com.landawn.abacus.cache.DistributedCacheBreakerAccess.booleanField;
import static com.landawn.abacus.cache.DistributedCacheBreakerAccess.breakerState;
import static com.landawn.abacus.cache.DistributedCacheBreakerAccess.intField;
import static com.landawn.abacus.cache.DistributedCacheBreakerAccess.longField;
import static com.landawn.abacus.cache.DistributedCacheBreakerAccess.setBreakerState;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.landawn.abacus.LogCapture;

/** Service-free regression coverage for {@link DistributedCache}'s circuit-breaker state. */

class DistributedCacheCircuitBreakerTest {

    // State and transitions

    /**
     * Regression guard for the torn-state race: the failure count and its timestamp must be
     * published through one atomic holder, not through independently writable atomics.
     */
    @Test
    void circuitBreakerCountAndTimestampUseOneAtomicState() throws Exception {
        final DistributedCache<String, String> cache = new DistributedCache<>(mock(DistributedCacheClient.class));
        try {
            final Field stateField = DistributedCache.class.getDeclaredField("circuitBreaker");
            stateField.setAccessible(true);

            assertTrue(AtomicReference.class.isAssignableFrom(stateField.getType()));

            final Object state = ((AtomicReference<?>) stateField.get(cache)).get();
            final Field failedCountField = state.getClass().getDeclaredField("failedCount");
            final Field lastFailedTimeField = state.getClass().getDeclaredField("lastFailedTime");
            final Field hasFailureField = state.getClass().getDeclaredField("hasFailure");
            failedCountField.setAccessible(true);
            lastFailedTimeField.setAccessible(true);
            hasFailureField.setAccessible(true);
            assertEquals(0, failedCountField.getInt(state));
            assertEquals(0L, lastFailedTimeField.getLong(state));
            assertFalse(hasFailureField.getBoolean(state));
            assertThrows(NoSuchFieldException.class, () -> DistributedCache.class.getDeclaredField("failedCounter"));
            assertThrows(NoSuchFieldException.class, () -> DistributedCache.class.getDeclaredField("lastFailedTime"));
        } finally {
            cache.close();
        }
    }

    @Test
    void failurePublishesMarkerAndSuccessResetsWholeState() throws Exception {
        @SuppressWarnings("unchecked")
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        when(client.get(anyString())).thenThrow(new IllegalStateException("backend unavailable"))
                .thenThrow(new IllegalStateException("backend unavailable"))
                .thenReturn("value");

        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 2, 0);

        try {
            assertNull(cache.getOrNull("key"));
            assertNull(cache.getOrNull("key"));

            final Object failedState = breakerState(cache);
            assertEquals(2, intField(failedState, "failedCount"));
            assertTrue(booleanField(failedState, "hasFailure"));

            // A zero retry delay lets the next read probe the backend immediately.
            assertEquals("value", cache.getOrNull("key"));

            final Object recoveredState = breakerState(cache);
            assertEquals(0, intField(recoveredState, "failedCount"));
            assertEquals(0L, longField(recoveredState, "lastFailedTime"));
            assertFalse(booleanField(recoveredState, "hasFailure"));
            verify(client, times(3)).get(anyString());
        } finally {
            cache.close();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    @ResourceLock(LogCapture.RESOURCE)
    void immediateRetryLogsOnlyTheFirstCircuitOpening(final int threshold) {
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        when(client.get("a2V5")).thenThrow(new IllegalStateException("unavailable"));
        try (LogCapture logs = new LogCapture(DistributedCache.class, Level.WARN)) {
            final DistributedCache<String, String> cache = new DistributedCache<>(client, "", threshold, 0);
            try {
                assertNull(cache.getOrNull("key"));
                assertNull(cache.getOrNull("key"));
                verify(client, times(2)).get("a2V5");
                assertEquals(1, logs.messages().size());
                assertTrue(logs.messages().get(0).contains("after 1 consecutive read failure;"));
            } finally {
                cache.close();
            }
        }
    }

    /** With a threshold of 1, the newer failure that survives a success still opens the circuit. */
    @Test
    void newerFailureSurvivingSuccessOpensCircuitAtThresholdOne() throws Exception {
        @SuppressWarnings("unchecked")
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 1, 60_000L);
        when(client.get(anyString())).thenAnswer(invocation -> {
            setBreakerState(cache, 1, System.nanoTime() + TimeUnit.SECONDS.toNanos(60));
            return "value";
        });

        try {
            assertEquals("value", cache.getOrNull("key"));
            assertNull(cache.getOrNull("key"), "the circuit opened by the newer failure must fail fast");
            verify(client, times(1)).get(anyString());
        } finally {
            cache.close();
        }
    }

    /**
     * Once the retry window has elapsed, reads probe the backend again. The count stays capped at the
     * threshold, so a single failed probe re-opens the circuit at once; that is not a closed-to-open
     * transition, so no second WARN is logged. A successful probe closes the circuit.
     */
    @Test
    @ResourceLock(LogCapture.RESOURCE)
    void expiredRetryWindowProbesBackendAndOneFailedProbeReopensWithoutNewWarning() throws Exception {
        @SuppressWarnings("unchecked")
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        when(client.get("a2V5")).thenThrow(new IllegalStateException("still unavailable")).thenReturn("value");

        try (LogCapture logs = new LogCapture(DistributedCache.class, Level.WARN)) {
            final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 3, 60_000L);
            try {
                final long expiredFailureTime = System.nanoTime() - TimeUnit.SECONDS.toNanos(61);
                setBreakerState(cache, 3, expiredFailureTime);

                assertNull(cache.getOrNull("key")); // probe reaches the backend and fails
                verify(client, times(1)).get("a2V5");
                final Object reopened = breakerState(cache);
                assertEquals(3, intField(reopened, "failedCount"));
                assertTrue(longField(reopened, "lastFailedTime") - expiredFailureTime > 0, "the failed probe must restart the retry window");

                assertNull(cache.getOrNull("key")); // short-circuited by the re-opened circuit
                verify(client, times(1)).get("a2V5");
                assertTrue(logs.messages().isEmpty(), "re-opening an already-open circuit is not a new transition");

                setBreakerState(cache, 3, expiredFailureTime);
                assertEquals("value", cache.getOrNull("key")); // successful probe
                assertFalse(booleanField(breakerState(cache), "hasFailure"));
                verify(client, times(2)).get("a2V5");
            } finally {
                cache.close();
            }
        }
    }

    @Test
    void zeroThresholdOpensAfterFirstRecordedFailure() {
        @SuppressWarnings("unchecked")
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        when(client.get(anyString())).thenThrow(new IllegalStateException("backend unavailable"));

        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 0, 60_000L);

        try {
            assertNull(cache.getOrNull("key")); // records the first failure
            assertNull(cache.getOrNull("key")); // short-circuited; no second backend call
            verify(client, times(1)).get(anyString());
        } finally {
            cache.close();
        }
    }

    // Concurrent recovery

    @Test
    @SuppressWarnings("unchecked")
    void delayedFailureCannotMoveCircuitRetryTimestampBackwards() throws Exception {
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        when(client.get(anyString())).thenThrow(new IllegalStateException("backend unavailable"));
        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 2, 60_000L);
        final AtomicReference<Object> state = new AtomicReference<>(breakerState(cache));
        final AtomicReference<Object> controlled = mock(AtomicReference.class);
        final AtomicBoolean firstUpdate = new AtomicBoolean(true);
        final CountDownLatch olderFailureReachedUpdate = new CountDownLatch(1);
        final CountDownLatch publishOlderFailure = new CountDownLatch(1);

        when(controlled.get()).thenAnswer(invocation -> state.get());
        when(controlled.getAndUpdate(any())).thenAnswer(invocation -> {
            final UnaryOperator<Object> update = invocation.getArgument(0);
            if (firstUpdate.compareAndSet(true, false)) {
                olderFailureReachedUpdate.countDown();
                assertTrue(publishOlderFailure.await(5, TimeUnit.SECONDS));
            }
            return state.getAndUpdate(update);
        });
        final Field field = DistributedCache.class.getDeclaredField("circuitBreaker");
        field.setAccessible(true);
        field.set(cache, controlled);

        final ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            final Future<?> olderFailure = executor.submit(() -> cache.getOrNull("older"));
            assertTrue(olderFailureReachedUpdate.await(5, TimeUnit.SECONDS));

            // Force a distinct clock tick after the older failure has reached its update boundary.
            final long reachedAt = System.nanoTime();
            while (System.nanoTime() == reachedAt) {
                Thread.onSpinWait();
            }
            assertNull(cache.getOrNull("newer"));
            final long newerTimestamp = longField(state.get(), "lastFailedTime");

            publishOlderFailure.countDown();
            olderFailure.get(5, TimeUnit.SECONDS);

            assertEquals(2, intField(state.get(), "failedCount"));
            assertTrue(longField(state.get(), "lastFailedTime") - newerTimestamp >= 0,
                    "Publishing a delayed failure must not shorten the newer failure's retry window");
        } finally {
            publishOlderFailure.countDown();
            executor.shutdownNow();
            cache.close();
        }
    }

    /**
     * {@link System#nanoTime()} values may wrap around, so the success reset must compare timestamps by
     * subtraction. The seeded stamp is ahead of the success in modular arithmetic (by almost 2^63 ns),
     * but numerically negative after the overflow, so a raw {@code <} comparison would call it older
     * and reset the circuit.
     */
    @Test
    void successComparesFailureTimestampWrapSafely() throws Exception {
        final long now = System.nanoTime();
        assumeTrue(now > 0, "the stamp below overflows, and so differs from a raw comparison, only for a positive clock reading");

        @SuppressWarnings("unchecked")
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        when(client.get(anyString())).thenReturn("value");
        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 5, 60_000L);

        try {
            final long wrappedNewerStamp = now + Long.MAX_VALUE;
            setBreakerState(cache, 3, wrappedNewerStamp);
            assertEquals("value", cache.getOrNull("key"));

            final Object state = breakerState(cache);
            assertTrue(booleanField(state, "hasFailure"), "a failure ahead of the success in modular time must survive it");
            assertEquals(1, intField(state, "failedCount"));
            assertEquals(wrappedNewerStamp, longField(state, "lastFailedTime"));
        } finally {
            cache.close();
        }
    }

    /**
     * A successful read must only clear failures recorded before its response arrived. If a concurrent
     * read records a failure after the response (success at t1, failure at t2 &gt; t1) but the success
     * reset runs later still (t3 &gt; t2), an unconditional reset would silently restart the
     * consecutive-failure count with stale availability information, delaying the circuit opening
     * during a developing outage.
     */
    @Test
    void successDoesNotEraseFailureRecordedAfterTheSuccessfulResponse() throws Exception {
        @SuppressWarnings("unchecked")
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        when(client.get(anyString())).thenReturn("value");
        // Threshold above the seeded count so the read still reaches the backend.
        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 5, 60_000L);

        try {
            // Stand in for a failure recorded after the successful response: a failure timestamp ahead
            // of the clock sample taken when client.get returns. It must survive the success reset.
            final long newerFailureTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            setBreakerState(cache, 1, newerFailureTime);
            assertEquals("value", cache.getOrNull("key"));

            final Object preservedState = breakerState(cache);
            assertEquals(1, intField(preservedState, "failedCount"), "a newer concurrent failure must survive a stale success reset");
            assertEquals(newerFailureTime, longField(preservedState, "lastFailedTime"));
            assertTrue(booleanField(preservedState, "hasFailure"));

            // A failure recorded before the successful response is superseded by it: the state resets.
            setBreakerState(cache, 1, System.nanoTime() - TimeUnit.SECONDS.toNanos(60));
            assertEquals("value", cache.getOrNull("key"));

            final Object resetState = breakerState(cache);
            assertEquals(0, intField(resetState, "failedCount"));
            assertFalse(booleanField(resetState, "hasFailure"));
            verify(client, times(2)).get(anyString());
        } finally {
            cache.close();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void successfulCasRetryPreservesAConcurrentRecovery() throws Exception {
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        when(client.get(anyString())).thenReturn("value");
        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 2, 0);
        final Object recovered = breakerState(cache);
        setBreakerState(cache, 1, System.nanoTime());
        final AtomicReference<Object> state = new AtomicReference<>(breakerState(cache));
        final AtomicReference<Object> controlled = mock(AtomicReference.class);
        when(controlled.get()).thenAnswer(invocation -> state.get());
        when(controlled.getAndUpdate(any())).thenAnswer(invocation -> {
            final UnaryOperator<Object> update = invocation.getArgument(0);
            update.apply(state.get());
            // Another successful reader wins the CAS; this updater must retry against its healthy state.
            state.set(recovered);
            return state.getAndUpdate(update);
        });
        final Field field = DistributedCache.class.getDeclaredField("circuitBreaker");
        field.setAccessible(true);
        field.set(cache, controlled);

        try {
            assertEquals("value", cache.getOrNull("key"));
            assertSame(recovered, state.get());
            assertFalse(booleanField(state.get(), "hasFailure"));
            assertEquals("value", cache.getOrNull("next"));
            verify(controlled, times(1)).getAndUpdate(any());
        } finally {
            cache.close();
        }
    }

    /**
     * A failure recorded after the successful response survives the success, but only as a run of one:
     * the failures counted before the success belong to a run that the success interrupted, and keeping
     * them would let that single newer failure open the circuit. Here the read passes the circuit check
     * with 4 of 5 failures counted; while it is in flight, a concurrent failure stamped after its
     * response brings the count to the threshold.
     */
    @Test
    void successKeepsOnlyTheNewerFailureNotTheRunItInterrupted() throws Exception {
        @SuppressWarnings("unchecked")
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 5, 60_000L);
        final long newerFailureTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        when(client.get(anyString())).thenAnswer(invocation -> {
            setBreakerState(cache, 5, newerFailureTime);
            return "value";
        }).thenReturn("next");

        try {
            setBreakerState(cache, 4, System.nanoTime() - TimeUnit.SECONDS.toNanos(60));
            assertEquals("value", cache.getOrNull("key"));

            final Object state = breakerState(cache);
            assertEquals(1, intField(state, "failedCount"), "only the failure recorded after the success may survive it");
            assertEquals(newerFailureTime, longField(state, "lastFailedTime"));
            assertTrue(booleanField(state, "hasFailure"));

            // One failure since the last success is below the threshold: the circuit must stay closed.
            assertEquals("next", cache.getOrNull("key"));
            verify(client, times(2)).get(anyString());
        } finally {
            cache.close();
        }
    }

    // Validation

    @Test
    void backendValidationFailureDoesNotChangeBreakerState() throws Exception {
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        final IllegalArgumentException failure = new IllegalArgumentException("encoded key too long");
        when(client.get("a2V5")).thenThrow(failure);
        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 2, 0);
        try {
            setBreakerState(cache, 1, System.nanoTime());
            final Object before = breakerState(cache);
            assertSame(failure, assertThrows(IllegalArgumentException.class, () -> cache.getOrNull("key")));
            assertSame(before, breakerState(cache));
        } finally {
            cache.close();
        }
    }

    /**
     * A read that fails because the calling thread is interrupted carries no backend-availability
     * information (SpyMemcached refuses every operation from such a thread before contacting the
     * server). Counting it would let one thread that keeps its interrupt flag set open the circuit
     * for every thread; here a zero threshold would open it on the first such read.
     */
    @ParameterizedTest
    @ValueSource(strings = { "tagged-enqueue-refusal", "wrapped-interrupted-exception", "closed-by-interrupt", "interrupted-io" })
    void readFailingFromTheCallersInterruptIsAMissThatDoesNotChangeBreakerState(final String failureKind) throws Exception {
        final RuntimeException failure = switch (failureKind) {
            // The shape SpyMemcached.tagInterruptedEnqueue produces (tagging itself needs the flag set;
            // see spyMemcachedInterruptedEnqueueRefusalDoesNotChangeBreakerState for the real path).
            case "tagged-enqueue-refusal" -> new IllegalStateException(SpyMemcached.INTERRUPTED_ENQUEUE_MESSAGE_PREFIX + " op",
                    new InterruptedException(SpyMemcached.INTERRUPTED_ENQUEUE_MESSAGE_PREFIX + " op"));
            case "wrapped-interrupted-exception" -> new RuntimeException(new InterruptedException("response wait"));
            case "closed-by-interrupt" -> new UncheckedIOException(new ClosedByInterruptException());
            default -> new UncheckedIOException(new InterruptedIOException("interrupted read"));
        };
        @SuppressWarnings("unchecked")
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        when(client.get("a2V5")).thenAnswer(invocation -> {
            if (Thread.currentThread().isInterrupted()) {
                throw failure;
            }

            return "value";
        });
        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 0, 60_000L);

        try {
            final Object closedState = breakerState(cache);
            Thread.currentThread().interrupt();

            try {
                assertNull(cache.getOrNull("key"));
                assertNull(cache.getOrNull("key"));
                assertTrue(Thread.currentThread().isInterrupted(), "the caller's interrupt status must be preserved");
            } finally {
                Thread.interrupted();
            }

            assertSame(closedState, breakerState(cache));
            assertEquals("value", cache.getOrNull("key"), "the circuit must still be closed for an uninterrupted reader");
            verify(client, times(3)).get("a2V5");
        } finally {
            cache.close();
        }
    }

    /**
     * The interrupt flag alone is not evidence: a genuine backend failure that happens to occur on an
     * interrupted thread (a Redis connection error, a socket timeout, which extends
     * InterruptedIOException, or an ISE with no interrupt cause) must still count toward the breaker.
     */
    @ParameterizedTest
    @ValueSource(strings = { "untagged-state", "connection-error", "socket-timeout" })
    void backendFailureOnAnInterruptedThreadIsStillCounted(final String failureKind) throws Exception {
        final RuntimeException failure = switch (failureKind) {
            case "untagged-state" -> new IllegalStateException("backend unavailable");
            case "connection-error" -> new RuntimeException("connection refused", new java.net.ConnectException("refused"));
            default -> new UncheckedIOException(new SocketTimeoutException("read timed out"));
        };
        @SuppressWarnings("unchecked")
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        when(client.get("a2V5")).thenThrow(failure);
        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 0, 60_000L);

        try {
            final Object closedState = breakerState(cache);
            Thread.currentThread().interrupt();

            try {
                assertNull(cache.getOrNull("key"));
            } finally {
                Thread.interrupted();
            }

            final Object state = breakerState(cache);
            assertFalse(closedState == state, "the failure must be recorded");
            assertTrue(booleanField(state, "hasFailure"));

            // The zero threshold opened the circuit: the next read fails fast without the backend.
            assertNull(cache.getOrNull("key"));
            verify(client, times(1)).get("a2V5");
        } finally {
            cache.close();
        }
    }

    /** End to end: spymemcached's real refusal shape, raised through the SpyMemcached wrapper. */
    @Test
    void spyMemcachedInterruptedEnqueueRefusalDoesNotChangeBreakerState() throws Exception {
        final net.spy.memcached.MemcachedClient delegate = mock(net.spy.memcached.MemcachedClient.class);
        when(delegate.asyncGet("a2V5")).thenAnswer(invocation -> {
            throw new IllegalStateException(SpyMemcached.INTERRUPTED_ENQUEUE_MESSAGE_PREFIX + " get op");
        });
        final DistributedCache<String, Object> cache = new DistributedCache<>(SpyMemcachedTestSupport.clientWithDelegate(delegate), "", 0, 60_000L);

        try {
            final Object closedState = breakerState(cache);
            Thread.currentThread().interrupt();

            try {
                assertNull(cache.getOrNull("key"));
                assertTrue(Thread.currentThread().isInterrupted());
            } finally {
                Thread.interrupted();
            }

            assertSame(closedState, breakerState(cache));
        } finally {
            cache.close();
        }
    }

    @Test
    void malformedUnicodeKeysAreRejectedWhileCircuitIsOpen() throws Exception {
        @SuppressWarnings("unchecked")
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        when(client.get(anyString())).thenThrow(new IllegalStateException("backend unavailable"));
        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 1, 60_000L);

        try {
            assertNull(cache.getOrNull("valid"));
            final Object failedState = breakerState(cache);

            assertThrows(IllegalArgumentException.class, () -> cache.getOrNull("\uD800"));
            assertThrows(IllegalArgumentException.class, () -> cache.containsKey("\uDC00"));
            assertSame(failedState, breakerState(cache));
            verify(client, times(1)).get(anyString());
        } finally {
            cache.close();
        }
    }

}
