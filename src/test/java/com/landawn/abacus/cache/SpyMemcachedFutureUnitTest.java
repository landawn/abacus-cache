package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.GetFuture;

/** Service-free regression coverage for future handling and pre-dispatch validation. */
@Tag("2025")
public class SpyMemcachedFutureUnitTest {

    @Test
    @SuppressWarnings("unchecked")
    public void noArgumentGetIsBoundedWhileExplicitTimeoutPassesThrough() throws Exception {
        final Class<?> adapterType = Class.forName(SpyMemcached.class.getName() + "$DefaultTimeoutFuture");
        final Constructor<?> constructor = adapterType.getDeclaredConstructor(Future.class, long.class);
        constructor.setAccessible(true);

        final Future<String> timeoutDelegate = mock(Future.class);
        when(timeoutDelegate.get(25L, TimeUnit.MILLISECONDS)).thenThrow(new TimeoutException("test timeout"));
        final Future<String> bounded = (Future<String>) constructor.newInstance(timeoutDelegate, 25L);

        final ExecutionException error = assertThrows(ExecutionException.class, bounded::get);
        assertTrue(error.getCause() instanceof TimeoutException);
        verify(timeoutDelegate).get(25L, TimeUnit.MILLISECONDS);
        verify(timeoutDelegate).cancel(true);

        final Future<String> timedDelegate = mock(Future.class);
        when(timedDelegate.get(7L, TimeUnit.SECONDS)).thenReturn("value");
        final Future<String> explicitlyTimed = (Future<String>) constructor.newInstance(timedDelegate, 25L);
        assertEquals("value", explicitlyTimed.get(7L, TimeUnit.SECONDS));
        verify(timedDelegate).get(7L, TimeUnit.SECONDS);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void bulkTimeoutRetainsOriginalFailureWhenCancellationThrows() throws Exception {
        final Class<?> adapterType = Class.forName(SpyMemcached.class.getName() + "$DefaultTimeoutFuture");
        final Constructor<?> constructor = adapterType.getDeclaredConstructor(Future.class, long.class);
        constructor.setAccessible(true);

        final Future<String> delegate = mock(Future.class);
        final TimeoutException timeout = new TimeoutException("server did not respond");
        final RejectedExecutionException cancellationFailure = new RejectedExecutionException("listener executor has shut down");
        when(delegate.get(25L, TimeUnit.MILLISECONDS)).thenThrow(timeout);
        when(delegate.cancel(true)).thenThrow(cancellationFailure);
        final Future<String> bounded = (Future<String>) constructor.newInstance(delegate, 25L);

        final ExecutionException error = assertThrows(ExecutionException.class, bounded::get);
        assertSame(timeout, error.getCause());
        assertEquals(1, timeout.getSuppressed().length);
        assertSame(cancellationFailure, timeout.getSuppressed()[0]);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void synchronousTimeoutRetainsOriginalFailureWhenCancellationThrows() throws Exception {
        final SpyMemcached<Object> cache = clientWithDelegate(mock(MemcachedClient.class));
        final Future<String> delegate = mock(Future.class);
        final TimeoutException timeout = new TimeoutException("server did not respond");
        final RejectedExecutionException cancellationFailure = new RejectedExecutionException("listener executor has shut down");
        when(delegate.get(25L, TimeUnit.MILLISECONDS)).thenThrow(timeout);
        when(delegate.cancel(true)).thenThrow(cancellationFailure);

        final RuntimeException error = assertThrows(RuntimeException.class, () -> cache.resultOf(delegate));
        assertSame(timeout, error.getCause());
        assertEquals(1, timeout.getSuppressed().length);
        assertSame(cancellationFailure, timeout.getSuppressed()[0]);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void synchronousInterruptionSurvivesCancellationFailure() throws Exception {
        final SpyMemcached<Object> cache = clientWithDelegate(mock(MemcachedClient.class));
        final Future<String> delegate = mock(Future.class);
        final InterruptedException interruption = new InterruptedException("caller interrupted");
        final RejectedExecutionException cancellationFailure = new RejectedExecutionException("listener executor has shut down");
        when(delegate.get(25L, TimeUnit.MILLISECONDS)).thenThrow(interruption);
        when(delegate.cancel(true)).thenThrow(cancellationFailure);

        try {
            final RuntimeException error = assertThrows(RuntimeException.class, () -> cache.resultOf(delegate));
            assertSame(interruption, error.getCause());
            assertTrue(Thread.currentThread().isInterrupted());
            assertEquals(1, interruption.getSuppressed().length);
            assertSame(cancellationFailure, interruption.getSuppressed()[0]);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void executionFailureFromAnotherThreadDoesNotInterruptWaitingThread() throws Exception {
        final SpyMemcached<Object> cache = clientWithDelegate(mock(MemcachedClient.class));
        final Future<String> delegate = mock(Future.class);
        final InterruptedException workerInterruption = new InterruptedException("asynchronous worker interrupted");
        when(delegate.get(25L, TimeUnit.MILLISECONDS)).thenThrow(new ExecutionException(workerInterruption));

        try {
            final RuntimeException error = assertThrows(RuntimeException.class, () -> cache.resultOf(delegate));
            assertSame(workerInterruption, error.getCause());
            assertFalse(Thread.currentThread().isInterrupted(), "A worker failure must not cancel the waiting thread");
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void executionFailuresPreserveRuntimeIdentityAndWrapErrors() throws Exception {
        final SpyMemcached<Object> cache = clientWithDelegate(mock(MemcachedClient.class));

        for (final RuntimeException failure : List.of(new IllegalStateException("worker failed"), new CancellationException("operation cancelled"))) {
            final Future<String> delegate = mock(Future.class);
            when(delegate.get(25L, TimeUnit.MILLISECONDS)).thenThrow(new ExecutionException(failure));
            assertSame(failure, assertThrows(RuntimeException.class, () -> cache.resultOf(delegate)));
        }

        final Future<String> delegate = mock(Future.class);
        final AssertionError failure = new AssertionError("asynchronous worker error");
        when(delegate.get(25L, TimeUnit.MILLISECONDS)).thenThrow(new ExecutionException(failure));
        assertSame(failure, assertThrows(RuntimeException.class, () -> cache.resultOf(delegate)).getCause());
    }

    @Test
    @SuppressWarnings("deprecation")
    public void unpairedSurrogatesAreRejectedByEveryKeyedOperationBeforeDispatch() throws Exception {
        final MemcachedClient delegate = mock(MemcachedClient.class);
        final SpyMemcached<Object> cache = clientWithDelegate(delegate);
        final List<Consumer<String>> operations = List.of(cache::get, cache::asyncGet,
                key -> cache.getBulk("valid", key), key -> cache.getBulk(List.of("valid", key)),
                key -> cache.asyncGetBulk("valid", key), key -> cache.asyncGetBulk(List.of("valid", key)),
                key -> cache.put(key, "value", 1_000), key -> cache.asyncPut(key, "value", 1_000),
                key -> cache.asyncSet(key, "value", 1_000), key -> cache.add(key, "value", 1_000),
                key -> cache.asyncAdd(key, "value", 1_000), key -> cache.replace(key, "value", 1_000),
                key -> cache.asyncReplace(key, "value", 1_000), cache::remove, cache::asyncRemove, cache::asyncDelete,
                cache::incr, key -> cache.incr(key, 1), key -> cache.incr(key, 1, 0), key -> cache.incr(key, 1, 0, 1_000),
                cache::decr, key -> cache.decr(key, 1), key -> cache.decr(key, 1, 0), key -> cache.decr(key, 1, 0, 1_000));

        for (final String malformed : List.of("key" + (char) 0xD800, "key" + (char) 0xDC00, "key" + (char) 0xD800 + "x")) {
            for (final Consumer<String> operation : operations) {
                assertThrows(IllegalArgumentException.class, () -> operation.accept(malformed));
            }
        }
        verifyNoInteractions(delegate);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void pairedSurrogatesRemainValidKeys() throws Exception {
        final MemcachedClient delegate = mock(MemcachedClient.class);
        final SpyMemcached<Object> cache = clientWithDelegate(delegate);
        final String key = "key:" + new String(Character.toChars(0x1F600));
        final GetFuture<Object> future = mock(GetFuture.class);
        when(delegate.asyncGet(key)).thenReturn(future);
        when(future.get(25L, TimeUnit.MILLISECONDS)).thenReturn("value");

        assertEquals("value", cache.get(key));
        verify(delegate).asyncGet(key);
    }

    @SuppressWarnings("unchecked")
    private static SpyMemcached<Object> clientWithDelegate(final MemcachedClient delegate) throws Exception {
        final SpyMemcached<Object> cache = mock(SpyMemcached.class, CALLS_REAL_METHODS);
        final Field clientField = SpyMemcached.class.getDeclaredField("mc");
        clientField.setAccessible(true);
        clientField.set(cache, delegate);
        final Field timeoutField = SpyMemcached.class.getDeclaredField("operationTimeoutMillis");
        timeoutField.setAccessible(true);
        timeoutField.setLong(cache, 25L);
        return cache;
    }
}
