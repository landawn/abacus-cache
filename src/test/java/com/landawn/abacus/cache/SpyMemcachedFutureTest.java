package com.landawn.abacus.cache;

import static com.landawn.abacus.cache.SpyMemcachedTestSupport.clientWithDelegate;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.NodeLocator;

/** Service-free regression coverage for future handling and pre-dispatch validation. */

class SpyMemcachedFutureTest {

    // Bulk Reads

    @Test
    @SuppressWarnings("unchecked")
    void bulkTimeoutRetainsOriginalFailureWhenCancellationThrows() throws Exception {
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

    // Validation And Failures

    @Test
    @SuppressWarnings("unchecked")
    void executionFailureFromAnotherThreadDoesNotInterruptWaitingThread() throws Exception {
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
    void executionFailuresPreserveRuntimeIdentityAndWrapErrors() throws Exception {
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
    void resultOfRejectsNullFutureWithIae() throws Exception {
        final SpyMemcached<Object> cache = clientWithDelegate(mock(MemcachedClient.class));
        assertThrows(IllegalArgumentException.class, () -> cache.resultOf(null));
    }

    @Test
    @SuppressWarnings("unchecked")
    void synchronousInterruptionSurvivesCancellationFailure() throws Exception {
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
    void synchronousTimeoutRetainsOriginalFailureWhenCancellationThrows() throws Exception {
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
    void timedFutureGetPreservesNullTimeUnitContract() throws Exception {
        final Class<?> adapterType = Class.forName(SpyMemcached.class.getName() + "$DefaultTimeoutFuture");
        final Constructor<?> constructor = adapterType.getDeclaredConstructor(Future.class, long.class);
        constructor.setAccessible(true);
        final Future<String> future = (Future<String>) constructor.newInstance(new CompletableFuture<String>(), 25L);

        assertThrows(NullPointerException.class, () -> future.get(1, null));
    }

    // Other Behavior

    /** The delegate starts its timed response wait only after synchronous queue admission finishes. */
    @Test
    void gracefulDisconnectTimeoutDoesNotBoundQueueAdmission() throws Exception {
        final MemcachedConnection connection = mock(MemcachedConnection.class);
        final NodeLocator locator = mock(NodeLocator.class);
        when(connection.getLocator()).thenReturn(locator);
        when(locator.getAll()).thenReturn(List.of());

        final CountDownLatch broadcastStarted = new CountDownLatch(1);
        final CountDownLatch releaseBroadcast = new CountDownLatch(1);
        when(connection.broadcastOperation(any(), any())).thenAnswer(invocation -> {
            broadcastStarted.countDown();
            if (!releaseBroadcast.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("Test did not release the queue-admission gate");
            }
            return new CountDownLatch(0);
        });

        final DefaultConnectionFactory factory = new DefaultConnectionFactory() {
            @Override
            public MemcachedConnection createConnection(final List<InetSocketAddress> addresses) {
                return connection; // No selector, socket, or network thread is created.
            }
        };
        final MemcachedClient delegate = new MemcachedClient(factory, List.of(new InetSocketAddress("127.0.0.1", 11211)));
        final SpyMemcached<Object> cache = clientWithDelegate(delegate);
        final FutureTask<Void> shutdown = new FutureTask<>(() -> {
            cache.disconnect(1);
            return null;
        });
        final Thread worker = Thread.ofPlatform().daemon(true).unstarted(shutdown);

        try {
            worker.start();
            assertTrue(broadcastStarted.await(5, TimeUnit.SECONDS));
            assertThrows(TimeoutException.class, () -> shutdown.get(50, TimeUnit.MILLISECONDS),
                    "The 1 ms response timeout cannot expire while queue admission is still blocked");
            verify(connection, never()).shutdown();

            releaseBroadcast.countDown();
            shutdown.get(5, TimeUnit.SECONDS);
            verify(connection).shutdown();
        } finally {
            releaseBroadcast.countDown();
            worker.join(5_000);
            if (worker.isAlive()) {
                worker.interrupt();
                worker.join(5_000);
            }
            delegate.shutdown();
        }

        assertFalse(worker.isAlive(), "The shutdown probe must leave no worker running");
    }

    @Test
    @SuppressWarnings("unchecked")
    void noArgumentGetIsBoundedWhileExplicitTimeoutPassesThrough() throws Exception {
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
}
