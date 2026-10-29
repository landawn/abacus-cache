/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.landawn.abacus.LogCapture;
import com.landawn.abacus.cache.SpyMemcached;

class MemcachedLockTest {

    private static final String SERVER_URL = "localhost:11211";
    private MockedConstruction<SpyMemcached> construction;

    @BeforeEach
    void interceptClientConstruction() {
        construction = Mockito.mockConstruction(SpyMemcached.class);
    }

    @AfterEach
    void releaseConstructionMock() {
        construction.close();
    }

    // Construction

    @Test
    void clientReturnsUnderlyingSpyMemcached() {
        final MemcachedLock<String, String> local = new MemcachedLock<>(SERVER_URL);
        assertSame(construction.constructed().get(0), local.client());
        local.close();
    }

    @Test
    void nullOrBlankServerUrlIsRejectedWithIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> new MemcachedLock<String, String>(null));
        assertThrows(IllegalArgumentException.class, () -> new MemcachedLock<String, String>(""));
        assertThrows(IllegalArgumentException.class, () -> new MemcachedLock<String, String>("   "));
    }

    // Reads

    @Test
    @SuppressWarnings("unchecked")
    void getNormalizesTheValueLessMarkerAndIsLockedReportsPresence() throws Exception {
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

    @Test
    void nonEmptyByteArrayLockValuesAreReturnedUnchanged() throws Exception {
        final SpyMemcached<Object> delegate = mock(SpyMemcached.class);
        final byte[] value = { 1, 2 };
        when(delegate.get("key")).thenReturn(value);
        try (MemcachedLock<String, Object> lock = lockWithDelegate(delegate)) {
            assertSame(value, lock.get("key"));
        }
    }

    // Acquisition

    @Test
    @SuppressWarnings("unchecked")
    void nonPositiveLiveTimeIsRejectedBeforeAnyNetworkOperation() throws Exception {
        final SpyMemcached<String> delegate = mock(SpyMemcached.class);
        final MemcachedLock<String, String> lock = lockWithDelegate(delegate);

        assertThrows(IllegalArgumentException.class, () -> lock.tryLock("target", 0L));
        assertThrows(IllegalArgumentException.class, () -> lock.tryLock("target", -1L));
        assertThrows(IllegalArgumentException.class, () -> lock.tryLock("target", "holder", 0L));
        assertThrows(IllegalArgumentException.class, () -> lock.tryLock("target", "holder", Long.MIN_VALUE));
        verifyNoInteractions(delegate);
    }

    @Test
    @SuppressWarnings("unchecked")
    void nullValueIsStoredAsTheValueLessMarker() throws Exception {
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

    // Release

    @Test
    void releasePreservesClientValidationExceptions() throws Exception {
        final SpyMemcached<String> delegate = mock(SpyMemcached.class);
        final IllegalArgumentException failure = new IllegalArgumentException("invalid client key");
        when(delegate.remove("key")).thenThrow(failure);
        try (MemcachedLock<String, String> lock = lockWithDelegate(delegate)) {
            assertSame(failure, assertThrows(IllegalArgumentException.class, () -> lock.tryUnlock("key")));
            assertSame(failure, assertThrows(IllegalArgumentException.class, () -> lock.unlockQuietly("key")));
        }
    }

    /**
     * A genuine communication failure — here, the underlying client has been shut down, which makes
     * every operation throw {@code IllegalStateException("Shutting down")} — is swallowed and
     * reported as {@code false}, so releasing in a {@code finally} block cannot mask the exception
     * thrown by the guarded critical section.
     */
    @Test
    void tryUnlockQuietlySwallowsCommunicationErrorReturnsFalse() {
        final MemcachedLock<String, Long> local = new MemcachedLock<>(SERVER_URL);
        local.close();

        assertFalse(local.unlockQuietly("k"));
    }

    /**
     * A release in the {@code finally} block of a cancelled task runs with the interrupt status set.
     * spymemcached 2.12.3 cannot enqueue any operation on such a thread: {@code TCPMemcachedNodeImpl.addOp}
     * first calls {@code authLatch.await(...)}, which throws {@link InterruptedException} for an interrupted
     * thread even when the latch is already open, and rethrows it as
     * {@code IllegalStateException("Interrupted while waiting to add ...")}. The delegate below reproduces
     * that path, so without special handling the delete is never sent and the lease stays held until its TTL.
     */
    @Test
    @SuppressWarnings("removal")
    void releaseFromAnInterruptedThreadIsSentAndPreservesTheInterruptStatus() throws Exception {
        final SpyMemcached<String> delegate = mock(SpyMemcached.class);
        when(delegate.remove("key")).thenAnswer(invocation -> {
            try {
                new CountDownLatch(0).await(1, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting to add delete", e);
            }
            return true;
        });

        try (MemcachedLock<String, String> lock = lockWithDelegate(delegate)) {
            Thread.currentThread().interrupt();
            assertTrue(lock.unlockQuietly("key"));
            assertTrue(Thread.currentThread().isInterrupted());
            assertTrue(lock.tryUnlock("key"));
            assertTrue(Thread.currentThread().isInterrupted());
            assertTrue(lock.unlock("key"));
            assertTrue(lock.tryUnlockQuietly("key"));
            assertTrue(Thread.currentThread().isInterrupted());
            verify(delegate, times(4)).remove("key");
        } finally {
            Thread.interrupted();
        }
    }

    /**
     * The interrupt status is cleared only for the delete itself and restored when the release fails:
     * {@code tryUnlock} throws and {@code unlockQuietly} returns {@code false}, both with the status set.
     */
    @Test
    void failedReleaseFromAnInterruptedThreadRestoresTheInterruptStatus() throws Exception {
        final SpyMemcached<String> delegate = mock(SpyMemcached.class);
        final List<Boolean> interruptedDuringDelete = new ArrayList<>();
        when(delegate.remove("key")).thenAnswer(invocation -> {
            interruptedDuringDelete.add(Thread.currentThread().isInterrupted());
            throw new IllegalStateException("delete failed");
        });

        try (MemcachedLock<String, String> lock = lockWithDelegate(delegate)) {
            Thread.currentThread().interrupt();
            final RuntimeException failure = assertThrows(RuntimeException.class, () -> lock.tryUnlock("key"));
            assertEquals("delete failed", failure.getCause().getMessage());
            assertTrue(Thread.currentThread().isInterrupted(), "tryUnlock must restore the status when it throws");

            assertFalse(lock.unlockQuietly("key"));
            assertTrue(Thread.currentThread().isInterrupted(), "unlockQuietly must restore the status when it fails");
            assertEquals(List.of(false, false), interruptedDuringDelete, "the status is cleared only while the delete runs");
        } finally {
            Thread.interrupted();
        }
    }

    /**
     * An interrupt that arrives while waiting for the server's response ends the wait: SpyMemcached
     * restores the status and throws, and the lock reports a failed release (the status stays set).
     */
    @Test
    void interruptDuringTheResponseWaitIsReportedAsAReleaseFailure() throws Exception {
        final SpyMemcached<String> delegate = mock(SpyMemcached.class);
        when(delegate.remove("key")).thenAnswer(invocation -> {
            // Mirrors SpyMemcached.resultOf: restore the status, then throw an unchecked wrapper.
            Thread.currentThread().interrupt();
            throw new RuntimeException(new InterruptedException("response wait interrupted"));
        });

        try (MemcachedLock<String, String> lock = lockWithDelegate(delegate)) {
            assertFalse(Thread.currentThread().isInterrupted());
            final RuntimeException failure = assertThrows(RuntimeException.class, () -> lock.tryUnlock("key"));
            assertTrue(failure.getCause().getCause() instanceof InterruptedException);
            assertTrue(Thread.interrupted(), "the interrupt must stay visible to the caller");

            assertFalse(lock.unlockQuietly("key"));
            assertTrue(Thread.interrupted(), "the interrupt must stay visible to the caller");
        } finally {
            Thread.interrupted();
        }
    }

    // Lifecycle

    @Test
    void closeDisconnectsUnderlyingClientAndIsIdempotent() {
        {
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
            // Ordinary operations use the lifecycle failure consistently before argument checks.
            assertThrows(IllegalStateException.class, () -> local.tryLock(null, 1_000));
            assertThrows(IllegalStateException.class, () -> local.isLocked(null));
            assertThrows(IllegalStateException.class, () -> local.get(null));
            assertThrows(IllegalStateException.class, () -> local.tryUnlock(null));
            assertFalse(local.unlockQuietly("k"));
        }
    }

    @Test
    void ordinaryOperationsCheckClosedStateBeforeArguments() {
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
    void quietReleaseStillValidatesMalformedKeysAfterClose() throws Exception {
        final SpyMemcached<String> delegate = mock(SpyMemcached.class);
        final MemcachedLock<String, String> lock = lockWithDelegate(delegate);
        lock.close();

        assertThrows(IllegalArgumentException.class, () -> lock.unlockQuietly("\uD800\uDC00\uDC00"));
        assertFalse(lock.unlockQuietly("\uD800\uDC00"));
        verify(delegate).disconnect();
    }

    // Compatibility

    @Test
    @SuppressWarnings("removal")
    void legacyAliasesPreserveAcquisitionAndReleaseResults() throws Exception {
        final SpyMemcached<Object> delegate = mock(SpyMemcached.class);
        try (MemcachedLock<String, Object> lock = lockWithDelegate(delegate)) {
            when(delegate.add(eq("plain"), any(), eq(1000L))).thenReturn(true);
            when(delegate.add("valued", "holder", 2000L)).thenReturn(false);
            when(delegate.remove("plain")).thenReturn(true, false);
            assertTrue(lock.lock("plain", 1000));
            assertFalse(lock.lock("valued", "holder", 2000));
            assertTrue(lock.unlock("plain"));
            assertFalse(lock.tryUnlockQuietly("plain"));
            final ArgumentCaptor<Object> marker = ArgumentCaptor.forClass(Object.class);
            verify(delegate).add(eq("plain"), marker.capture(), eq(1000L));
            assertEquals(0, ((byte[]) marker.getValue()).length);
        }
    }

    // Key validation

    @Test
    @SuppressWarnings("unchecked")
    void acceptsSupplementaryCodePointBoundariesAndTheExactUtf8KeyLimit() throws Exception {
        final SpyMemcached<String> delegate = mock(SpyMemcached.class);
        final MemcachedLock<String, String> lock = lockWithDelegate(delegate);
        final String firstSupplementary = new String(Character.toChars(Character.MIN_SUPPLEMENTARY_CODE_POINT));
        final String lastSupplementary = new String(Character.toChars(Character.MAX_CODE_POINT));
        final List<String> keys = List.of(firstSupplementary, lastSupplementary, firstSupplementary + lastSupplementary, "a".repeat(246) + lastSupplementary,
                "prefix" + firstSupplementary + "suffix");

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
    void lockRejectsKeysExactlyLikeTheClient() throws Exception {
        final MemcachedLock<String, String> lock = lockWithDelegate(mock(SpyMemcached.class));

        for (final String key : List.of("a\uD800x", "\uDC00", "", "has space", "a".repeat(251))) {
            final String clientMessage = assertThrows(IllegalArgumentException.class, () -> SpyMemcached.checkKey(key)).getMessage();
            assertEquals(clientMessage, assertThrows(IllegalArgumentException.class, () -> lock.tryLock(key, "holder", 1_000L)).getMessage());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void malformedKeyReturnedByOverrideIsRejectedAndPairedSurrogatesAreAccepted() throws Exception {
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

    @ParameterizedTest
    @MethodSource("malformedKeys")
    void malformedKeysCannotAcquireOrReleaseAnotherTargetsLease(final String key) throws Exception {
        final SpyMemcached<String> delegate = mock(SpyMemcached.class);
        final MemcachedLock<String, String> lock = lockWithDelegate(delegate);
        assertThrows(IllegalArgumentException.class, () -> lock.tryLock(key, 1_000));
        assertThrows(IllegalArgumentException.class, () -> lock.tryLock(key, "holder", 1_000));
        assertThrows(IllegalArgumentException.class, () -> lock.isLocked(key));
        assertThrows(IllegalArgumentException.class, () -> lock.get(key));
        assertThrows(IllegalArgumentException.class, () -> lock.tryUnlock(key));
        assertThrows(IllegalArgumentException.class, () -> lock.unlockQuietly(key));
        verifyNoInteractions(delegate);
        lock.close();
        assertThrows(IllegalArgumentException.class, () -> lock.unlockQuietly(key));
    }

    @Test
    void nullKeyFromOverrideIsRejectedBeforeAnyNetworkOperation() {
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
    void nullKeyFromOverrideReportsACompleteMessage() {
        try (NullKeyLock lock = new NullKeyLock()) {
            for (final IllegalArgumentException e : List.of(assertThrows(IllegalArgumentException.class, () -> lock.tryLock("target", 1_000L)),
                    assertThrows(IllegalArgumentException.class, () -> lock.unlockQuietly("target")))) {
                assertEquals("The key returned by toKey must not be null", e.getMessage());
            }
        }
    }

    @Test
    @SuppressWarnings({ "unchecked", "removal" })
    void nullTargetIsRejectedWithIllegalArgumentExceptionBeforeAnyNetworkOperation() throws Exception {
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

    /**
     * {@link MemcachedLock#toKey(Object)} is documented as overridable; the class must therefore not be
     * {@code final}, and an override must be reachable and used. This also covers the {@code toKey}
     * null-target rejection.
     */
    @Test
    void toKeyIsSubclassableAndRejectsNullTarget() {
        assertFalse(java.lang.reflect.Modifier.isFinal(MemcachedLock.class.getModifiers()),
                "MemcachedLock must not be final - its Javadoc documents subclassing");

        try (NamespacedLock<String> nsLock = new NamespacedLock<>(SERVER_URL)) {
            assertEquals("ns:my-resource", nsLock.testToKey("my-resource"));
            assertThrows(IllegalArgumentException.class, () -> nsLock.testToKey(null));
        }
    }

    // Diagnostics and failures

    /** Client failures: IAE propagates unchanged, others are wrapped (tryLock/tryUnlock) or swallowed (unlockQuietly). */
    @Test
    @SuppressWarnings("unchecked")
    void clientFailuresArePropagatedWrappedOrSwallowedAsDocumented() throws Exception {
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

    @ParameterizedTest
    @ValueSource(strings = { "DEBUG", "OFF" })
    @ResourceLock(LogCapture.RESOURCE)
    void diagnosticsDescribeAcquisitionContentionAndCommunicationFailure(final String level) throws Exception {
        final SpyMemcached<String> delegate = mock(SpyMemcached.class);
        when(delegate.add("key", "holder", 1000)).thenReturn(true, false).thenThrow(new IllegalStateException("offline"));
        when(delegate.remove("key")).thenReturn(true, false, true, false).thenThrow(new IllegalStateException("offline"));
        try (LogCapture logs = new LogCapture(MemcachedLock.class, Level.valueOf(level));
             MemcachedLock<String, String> lock = lockWithDelegate(delegate)) {
            assertTrue(lock.tryLock("key", "holder", 1000));
            assertFalse(lock.tryLock("key", "holder", 1000));
            assertTrue(lock.tryUnlock("key"));
            assertFalse(lock.tryUnlock("key"));
            assertTrue(lock.unlockQuietly("key"));
            assertFalse(lock.unlockQuietly("key"));
            assertThrows(RuntimeException.class, () -> lock.tryLock("key", "holder", 1000));
            assertThrows(RuntimeException.class, () -> lock.tryUnlock("key"));
            assertFalse(lock.unlockQuietly("key"));
            if ("DEBUG".equals(level)) {
                for (final String expected : List.of("Acquired lock", "Lock already held", "Released lock", "No lock to release", "acquisition failed",
                        "release failed")) {
                    assertTrue(logs.messages().stream().anyMatch(message -> message.contains(expected)), expected);
                }
            } else {
                assertTrue(logs.messages().isEmpty());
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <V> MemcachedLock<String, V> lockWithDelegate(final SpyMemcached<V> delegate) throws Exception {
        final MemcachedLock<String, V> lock = mock(MemcachedLock.class, CALLS_REAL_METHODS);
        setDelegate(lock, delegate);
        return lock;
    }

    private static <V> void setDelegate(final MemcachedLock<String, V> lock, final SpyMemcached<V> delegate) throws Exception {
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

    private static List<String> malformedKeys() {
        return List.of("\uD800", "\uDBFF", "\uDC00", "\uDFFF", "\uDC00\uD800", "a\uD800x", "\uD800\uD800\uDC00", "\uD800\uDC00\uDC00", "\uD800\uDC00\uDBFF",
                "a".repeat(247) + "\uDBFF\uDFFF", "key\uD800", "key\uDC00", "key\uD800x", "x".repeat(251));
    }
}
