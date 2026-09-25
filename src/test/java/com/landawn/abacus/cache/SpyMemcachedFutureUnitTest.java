package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.channels.UnresolvedAddressException;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import net.spy.memcached.CachedData;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;

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
    @SuppressWarnings("unchecked")
    public void seededCountersCanReturnAbsentWhenCompetingInitializationIsDeletedBeforeRetry() throws Exception {
        for (final boolean increment : new boolean[] { true, false }) {
            for (final boolean withExpiration : new boolean[] { false, true }) {
                final MemcachedClient delegate = mock(MemcachedClient.class);
                final OperationFuture<Long> initialMutation = mock(OperationFuture.class);
                final OperationFuture<Boolean> competingInsert = mock(OperationFuture.class);
                final OperationFuture<Long> retryMutation = mock(OperationFuture.class);
                when(initialMutation.get(25L, TimeUnit.MILLISECONDS)).thenReturn(-1L);
                // Another caller inserts after our first miss, so our conditional add fails.
                when(competingInsert.get(25L, TimeUnit.MILLISECONDS)).thenReturn(false);
                // That entry is deleted or expires before our sole mutation retry.
                when(retryMutation.get(25L, TimeUnit.MILLISECONDS)).thenReturn(-1L);
                when(delegate.add(eq("counter"), eq(withExpiration ? 1 : 0), eq("10"), any())).thenReturn(competingInsert);

                if (increment) {
                    when(delegate.asyncIncr("counter", 1L)).thenReturn(initialMutation, retryMutation);
                } else {
                    when(delegate.asyncDecr("counter", 1L)).thenReturn(initialMutation, retryMutation);
                }

                final SpyMemcached<Object> cache = clientWithDelegate(delegate);
                final long result;

                if (increment) {
                    result = withExpiration ? cache.incr("counter", 1, 10, 1_000) : cache.incr("counter", 1, 10);
                } else {
                    result = withExpiration ? cache.decr("counter", 1, 10, 1_000) : cache.decr("counter", 1, 10);
                }

                assertEquals(-1L, result);
                final InOrder sequence = inOrder(delegate, initialMutation, competingInsert, retryMutation);

                if (increment) {
                    sequence.verify(delegate).asyncIncr("counter", 1L);
                } else {
                    sequence.verify(delegate).asyncDecr("counter", 1L);
                }

                sequence.verify(initialMutation).get(25L, TimeUnit.MILLISECONDS);
                sequence.verify(delegate).add(eq("counter"), eq(withExpiration ? 1 : 0), eq("10"), any());
                sequence.verify(competingInsert).get(25L, TimeUnit.MILLISECONDS);

                if (increment) {
                    sequence.verify(delegate).asyncIncr("counter", 1L);
                } else {
                    sequence.verify(delegate).asyncDecr("counter", 1L);
                }

                sequence.verify(retryMutation).get(25L, TimeUnit.MILLISECONDS);
                sequence.verifyNoMoreInteractions();
            }
        }
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
    public void memcachedKeyValidationPrecedesLaterArgumentsAndDispatch() throws Exception {
        final MemcachedClient delegate = mock(MemcachedClient.class);
        final SpyMemcached<Object> cache = clientWithDelegate(delegate);

        for (final String key : List.of("", "bad key", "key\r", "key\n", "key\0", "x".repeat(251), "\u00e9".repeat(126))) {
            final IllegalArgumentException keyFailure = assertThrows(IllegalArgumentException.class, () -> cache.get(key));
            assertEquals(keyFailure.getMessage(), assertThrows(IllegalArgumentException.class, () -> cache.put(key, new Object(), Long.MAX_VALUE)).getMessage());
            assertEquals(keyFailure.getMessage(), assertThrows(IllegalArgumentException.class, () -> cache.asyncAdd(key, new Object(), Long.MAX_VALUE)).getMessage());
            assertEquals(keyFailure.getMessage(), assertThrows(IllegalArgumentException.class, () -> cache.incr(key, -1, -1, Long.MAX_VALUE)).getMessage());
            assertEquals(keyFailure.getMessage(), assertThrows(IllegalArgumentException.class, () -> cache.decr(key, -1, -1, Long.MAX_VALUE)).getMessage());
            assertThrows(IllegalArgumentException.class, () -> cache.getBulk("valid", key));
            assertThrows(IllegalArgumentException.class, () -> cache.asyncGetBulk(List.of("valid", key)));
        }

        verifyNoInteractions(delegate);
    }

    /** The protocol limit counts UTF-8 bytes, and an exactly 250-byte key remains valid. */
    @Test
    @SuppressWarnings("unchecked")
    public void memcachedKeyValidationAcceptsExactUtf8ByteLimit() throws Exception {
        final MemcachedClient delegate = mock(MemcachedClient.class);
        final SpyMemcached<Object> cache = clientWithDelegate(delegate);
        final String key = "\u00e9".repeat(125);
        final GetFuture<Object> future = mock(GetFuture.class);
        when(delegate.asyncGet(key)).thenReturn(future);
        when(future.get(25L, TimeUnit.MILLISECONDS)).thenReturn("value");

        assertEquals("value", cache.get(key));
        verify(delegate).asyncGet(key);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void shutdownCheckPrecedesInvalidMemcachedKeys() throws Exception {
        final SpyMemcached<Object> cache = clientWithDelegate(mock(MemcachedClient.class));
        cache.disconnect();

        assertThrows(IllegalStateException.class, () -> cache.put("bad key", new Object(), Long.MAX_VALUE));
        assertThrows(IllegalStateException.class, () -> cache.incr("", -1));
        assertThrows(IllegalStateException.class, () -> cache.asyncGetBulk("bad key"));
    }

    @Test
    @SuppressWarnings("deprecation")
    public void stockTranscoderRejectsNullAfterStateAndKeyChecksBeforeTtlAndDispatch() throws Exception {
        final MemcachedClient delegate = mock(MemcachedClient.class);
        when(delegate.getTranscoder()).thenReturn(new SerializingTranscoder());
        final SpyMemcached<Object> cache = clientWithDelegate(delegate);

        assertTrue(assertThrows(IllegalArgumentException.class, () -> cache.put(null, null, Long.MAX_VALUE)).getMessage().contains("key"));
        verifyNoInteractions(delegate);

        final List<Consumer<SpyMemcached<Object>>> writes = List.of(c -> c.put("key", null, Long.MAX_VALUE),
                c -> c.set("key", null, Long.MAX_VALUE), c -> c.asyncPut("key", null, Long.MAX_VALUE),
                c -> c.asyncSet("key", null, Long.MAX_VALUE), c -> c.add("key", null, Long.MAX_VALUE),
                c -> c.asyncAdd("key", null, Long.MAX_VALUE), c -> c.replace("key", null, Long.MAX_VALUE),
                c -> c.asyncReplace("key", null, Long.MAX_VALUE));

        for (final Consumer<SpyMemcached<Object>> write : writes) {
            assertTrue(assertThrows(IllegalArgumentException.class, () -> write.accept(cache)).getMessage().contains("value"));
        }

        cache.disconnect();
        assertThrows(IllegalStateException.class, () -> cache.put(null, null, Long.MAX_VALUE));
        verify(delegate, times(writes.size())).getTranscoder();
        verify(delegate).shutdown();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    @SuppressWarnings({ "unchecked", "deprecation" })
    public void kryoAndCustomTranscodersKeepTheirNullValueSupport() throws Exception {
        final Transcoder<Object> custom = new SerializingTranscoder() {
            @Override
            public CachedData encode(final Object value) {
                return new CachedData(0, new byte[0], getMaxSize());
            }
        };

        for (final Transcoder<Object> transcoder : List.of(new KryoTranscoder<Object>(), custom)) {
            final MemcachedClient delegate = mock(MemcachedClient.class);
            when(delegate.getTranscoder()).thenReturn(transcoder);
            final OperationFuture<Boolean> result = mock(OperationFuture.class);
            when(result.get(25L, TimeUnit.MILLISECONDS)).thenReturn(true);
            when(result.get()).thenReturn(true);
            when(delegate.set("key", 1, null)).thenAnswer(call -> {
                transcoder.encode(null);
                return result;
            });
            when(delegate.add("key", 1, null)).thenAnswer(call -> {
                transcoder.encode(null);
                return result;
            });
            when(delegate.replace("key", 1, null)).thenAnswer(call -> {
                transcoder.encode(null);
                return result;
            });
            final SpyMemcached<Object> cache = clientWithDelegate(delegate);

            assertTrue(cache.put("key", null, 1_000));
            assertTrue(cache.set("key", null, 1_000));
            assertTrue(cache.asyncPut("key", null, 1_000).get());
            assertTrue(cache.asyncSet("key", null, 1_000).get());
            assertTrue(cache.add("key", null, 1_000));
            assertTrue(cache.asyncAdd("key", null, 1_000).get());
            assertTrue(cache.replace("key", null, 1_000));
            assertTrue(cache.asyncReplace("key", null, 1_000).get());
        }
    }

    @Test
    public void customTranscoderNullPointerFailureIsNotTranslated() throws Exception {
        final NullPointerException failure = new NullPointerException("custom serializer failed");
        final Transcoder<Object> transcoder = new SerializingTranscoder() {
            @Override
            public CachedData encode(final Object value) {
                throw failure;
            }
        };
        final MemcachedClient delegate = mock(MemcachedClient.class);
        when(delegate.getTranscoder()).thenReturn(transcoder);
        when(delegate.set("key", 1, null)).thenAnswer(call -> transcoder.encode(null));
        final SpyMemcached<Object> cache = clientWithDelegate(delegate);

        assertSame(failure, assertThrows(NullPointerException.class, () -> cache.put("key", null, 1_000)));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void timedFutureGetPreservesNullTimeUnitContract() throws Exception {
        final Class<?> adapterType = Class.forName(SpyMemcached.class.getName() + "$DefaultTimeoutFuture");
        final Constructor<?> constructor = adapterType.getDeclaredConstructor(Future.class, long.class);
        constructor.setAccessible(true);
        final Future<String> future = (Future<String>) constructor.newInstance(new CompletableFuture<String>(), 25L);

        assertThrows(NullPointerException.class, () -> future.get(1, null));
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

    @Test
    public void createSpyMemcachedClientRejectsNullConnectionFactoryWithIae() {
        // Rejected before any MemcachedClient (and its IO thread) is created; previously
        // spymemcached threw NullPointerException("Connection factory required").
        assertThrows(IllegalArgumentException.class, () -> SpyMemcached.createSpyMemcachedClient("localhost:11211", null));
        // serverUrl is validated first (signature order), also with IllegalArgumentException.
        assertThrows(IllegalArgumentException.class, () -> SpyMemcached.createSpyMemcachedClient(null, null));
    }

    /**
     * An unresolvable host used to reach spymemcached, which opened its NIO selector and a socket
     * channel per address before the connect to the unresolved address threw
     * {@code UnresolvedAddressException}, leaking all of them. It must now be rejected before the
     * connection factory is asked to create any connection.
     */
    @Test
    public void unresolvableHostIsRejectedBeforeConnectionResourcesAreCreated() throws Exception {
        final DefaultConnectionFactory factory = spy(new DefaultConnectionFactory());
        final String serverUrl = "localhost:11211,spymemcached-unit-test-host.invalid:11211";

        final IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> SpyMemcached.createSpyMemcachedClient(serverUrl, factory));

        assertFalse(error instanceof UnresolvedAddressException, "rejected by the wrapper, not by a failed socket connect");
        verify(factory, never()).createConnection(anyList());
        assertThrows(IllegalArgumentException.class, () -> new SpyMemcached<>(serverUrl, 1_000L));
    }

    @Test
    public void resultOfRejectsNullFutureWithIae() throws Exception {
        final SpyMemcached<Object> cache = clientWithDelegate(mock(MemcachedClient.class));
        assertThrows(IllegalArgumentException.class, () -> cache.resultOf(null));
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
