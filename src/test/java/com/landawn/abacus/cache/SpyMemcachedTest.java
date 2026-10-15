/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static com.landawn.abacus.cache.SpyMemcachedTestSupport.clientWithDelegate;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.UnresolvedAddressException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.landawn.abacus.LogCapture;
import com.landawn.abacus.exception.UncheckedIOException;
import com.landawn.abacus.parser.ParserFactory;
import com.landawn.abacus.util.AddrUtil;
import com.landawn.abacus.util.ContinuableFuture;

import net.spy.memcached.CachedData;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedConnection;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.OperationErrorType;
import net.spy.memcached.ops.OperationException;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;

class SpyMemcachedTest {

    private static final String SERVER_URL = "localhost:11211";
    private org.mockito.MockedConstruction<MemcachedClient> construction;
    private SpyMemcached<Object> cache;

    @BeforeEach
    void setUp() {
        construction = org.mockito.Mockito.mockConstruction(MemcachedClient.class);
        cache = new SpyMemcached<>(SERVER_URL);
    }

    @AfterEach
    void tearDown() {
        try {
            cache.disconnect();
        } finally {
            construction.close();
        }
    }

    // Construction

    @Test
    void protectedClientFactoryReturnsTheConstructedDelegate() {
        final MemcachedClient created = SpyMemcached.createSpyMemcachedClient(SERVER_URL, new DefaultConnectionFactory());
        assertSame(construction.constructed().get(1), created);
        created.shutdown();
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void connectionFactoryUsesConfiguredTimeoutAndAvailableSerializer(final boolean kryoAvailable) {
        construction.close();
        final AtomicReference<ConnectionFactory> factory = new AtomicReference<>();
        construction = Mockito.mockConstruction(MemcachedClient.class, (client, context) -> factory.set((ConnectionFactory) context.arguments().get(0)));
        try (MockedStatic<ParserFactory> parsers = Mockito.mockStatic(ParserFactory.class, Mockito.CALLS_REAL_METHODS)) {
            parsers.when(ParserFactory::isKryoParserAvailable).thenReturn(kryoAvailable);
            final SpyMemcached<Object> local = new SpyMemcached<>("127.0.0.1:11211", 1234);
            try {
                assertEquals(1234, factory.get().getOperationTimeout());
                assertTrue(kryoAvailable ? factory.get().getDefaultTranscoder() instanceof KryoTranscoder
                        : factory.get().getDefaultTranscoder() instanceof SerializingTranscoder);
            } finally {
                local.disconnect();
            }
        }
    }

    @Test
    void emptyAddressListFromResolverFailsBeforeClientCreation() {
        try (MockedStatic<AddrUtil> addresses = Mockito.mockStatic(AddrUtil.class)) {
            addresses.when(() -> AddrUtil.getAddressList("127.0.0.1:11211")).thenReturn(List.of());
            assertTrue(assertThrows(IllegalArgumentException.class, () -> new SpyMemcached<>("127.0.0.1:11211")).getMessage()
                    .contains("No valid server addresses"));
            assertEquals(1, construction.constructed().size());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "DEBUG", "OFF" })
    @ResourceLock(LogCapture.RESOURCE)
    void constructorFailurePreservesTheCauseAndReportsTheEndpoint(final String level) {
        construction.close();
        final IllegalStateException failure = new IllegalStateException("connection setup failed");
        construction = Mockito.mockConstruction(MemcachedClient.class, (client, context) -> {
            throw failure;
        });
        try (LogCapture logs = new LogCapture(SpyMemcached.class, Level.valueOf(level))) {
            final RuntimeException thrown = assertThrows(RuntimeException.class, () -> new SpyMemcached<>("127.0.0.1:11211", 1234));
            assertSame(failure, thrown.getCause());
            if ("DEBUG".equals(level)) {
                assertTrue(logs.messages().stream().anyMatch(message -> message.contains("127.0.0.1:11211") && message.contains("1234")));
            } else {
                assertTrue(logs.messages().isEmpty());
            }
        }
    }

    @Test
    void connectionFactoryCreationWrapsIoFailureWithEndpointContext() {
        construction.close();
        final IOException failure = new IOException("selector unavailable");
        final DefaultConnectionFactory factory = new DefaultConnectionFactory() {
            @Override
            public MemcachedConnection createConnection(final List<java.net.InetSocketAddress> addresses) throws IOException {
                throw failure;
            }
        };
        try {
            final UncheckedIOException thrown = assertThrows(UncheckedIOException.class,
                    () -> SpyMemcached.createSpyMemcachedClient("127.0.0.1:11211", factory));
            assertSame(failure, thrown.getCause());
            assertTrue(thrown.getMessage().contains("127.0.0.1:11211"));
        } finally {
            construction = Mockito.mockConstruction(MemcachedClient.class);
        }
    }

    @Test
    void constructorRejectsBlankServerUrl() {
        assertThrows(IllegalArgumentException.class, () -> new SpyMemcached<>((String) null));
        assertThrows(IllegalArgumentException.class, () -> new SpyMemcached<>(""));
        assertThrows(IllegalArgumentException.class, () -> new SpyMemcached<>("   "));
    }

    @Test
    void constructorRejectsInvalidTimeout() {
        assertThrows(IllegalArgumentException.class, () -> new SpyMemcached<>(SERVER_URL, 0L));
        assertThrows(IllegalArgumentException.class, () -> new SpyMemcached<>(SERVER_URL, -1L));
    }

    @Test
    void createSpyMemcachedClientRejectsNullConnectionFactoryWithIae() {
        // Rejected before any MemcachedClient (and its IO thread) is created; previously
        // spymemcached threw NullPointerException("Connection factory required").
        assertThrows(IllegalArgumentException.class, () -> SpyMemcached.createSpyMemcachedClient("localhost:11211", null));
        // serverUrl is validated first (signature order), also with IllegalArgumentException.
        assertThrows(IllegalArgumentException.class, () -> SpyMemcached.createSpyMemcachedClient(null, null));
    }

    // Reads

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    void synchronousAndAsynchronousReadsReturnDelegateValuesAndSnapshotKeys() throws Exception {
        final MemcachedClient delegate = construction.constructed().get(0);
        final GetFuture<Object> get = mock(GetFuture.class);
        when(delegate.asyncGet("key")).thenReturn(get);
        when(get.get(anyLong(), any(TimeUnit.class))).thenReturn("value");
        assertEquals("value", cache.get("key"));
        assertEquals("value", cache.asyncGet("key").get(1, TimeUnit.SECONDS));

        final Map<String, Object> values = Map.of("a", 1, "b", 2);
        final BulkFuture<Map<String, Object>> bulk = mock(BulkFuture.class);
        when(bulk.get(anyLong(), any(TimeUnit.class))).thenReturn(values);
        when(delegate.asyncGetBulk(any(String[].class))).thenReturn((BulkFuture) bulk);
        when(delegate.asyncGetBulk(any(Collection.class))).thenReturn((BulkFuture) bulk);
        assertEquals(values, cache.getBulk("a", "b"));
        assertEquals(values, cache.getBulk(List.of("a", "b")));
        final java.util.ArrayList<String> keys = new java.util.ArrayList<>(List.of("a", "b"));
        final ContinuableFuture<Map<String, Object>> pending = cache.asyncGetBulk(keys);
        keys.clear();
        assertEquals(values, pending.get());
        final ArgumentCaptor<Collection<String>> captured = ArgumentCaptor.forClass(Collection.class);
        verify(delegate, times(2)).asyncGetBulk(captured.capture());
        assertEquals(List.of("a", "b"), captured.getValue());
        verify(bulk, times(3)).get(DistributedCacheClient.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
    }

    /**
     * spymemcached's bulk future implements no-argument {@code get()} by waiting for
     * {@link Long#MAX_VALUE} milliseconds. SpyMemcached must substitute its configured operation
     * timeout, surface the timeout through {@link ExecutionException}, and cancel the delegate so a
     * written request to an unresponsive server cannot pin the caller indefinitely.
     */
    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    void asyncGetBulkNoArgumentGetUsesConfiguredTimeout() throws Exception {
        final long timeoutMillis = 25L;
        final SpyMemcached<Object> local = new SpyMemcached<>(SERVER_URL, timeoutMillis);
        final Field clientField = SpyMemcached.class.getDeclaredField("mc");
        clientField.setAccessible(true);
        final MemcachedClient realClient = (MemcachedClient) clientField.get(local);
        final MemcachedClient mockedClient = mock(MemcachedClient.class);
        final BulkFuture<Map<String, Object>> bulkFuture = mock(BulkFuture.class);

        when(mockedClient.asyncGetBulk(any(String[].class))).thenReturn((BulkFuture) bulkFuture);
        when(bulkFuture.get(timeoutMillis, TimeUnit.MILLISECONDS)).thenThrow(new TimeoutException("test timeout"));
        clientField.set(local, mockedClient);

        try {
            final ContinuableFuture<Map<String, Object>> future = local.asyncGetBulk("never-completes");
            final ExecutionException error = assertThrows(ExecutionException.class, future::get);

            assertTrue(error.getCause() instanceof TimeoutException);
            verify(bulkFuture).get(timeoutMillis, TimeUnit.MILLISECONDS);
            verify(bulkFuture).cancel(true);
        } finally {
            realClient.shutdown();
        }
    }

    @Test
    void asyncGetRejectsNullKey() {
        assertThrows(IllegalArgumentException.class, () -> cache.asyncGet(null));
    }

    @Test
    void getRejectsNullKey() {
        assertThrows(IllegalArgumentException.class, () -> cache.get(null));
    }

    // Bulk Reads

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    void bulkFutureForwardsCompletionAndCancellation() throws Exception {
        final MemcachedClient delegate = construction.constructed().get(0);
        final BulkFuture<Map<String, Object>> bulk = mock(BulkFuture.class);
        when(delegate.asyncGetBulk(any(Collection.class))).thenReturn((BulkFuture) bulk);
        when(bulk.cancel(true)).thenReturn(true);
        final ContinuableFuture<Map<String, Object>> future = cache.asyncGetBulk(List.of());
        assertFalse(future.isDone());
        assertFalse(future.isCancelled());
        assertTrue(future.cancel(true));
        when(bulk.isDone()).thenReturn(true);
        when(bulk.isCancelled()).thenReturn(true);
        assertTrue(future.isDone());
        assertTrue(future.isCancelled());
        verify(bulk).cancel(true);
    }

    @Test
    void getBulkCollectionRejectsNullKeys() {
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk((java.util.Collection<String>) null));
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk(Arrays.asList("a", null)));
    }

    @Test
    void getBulkRejectsNullKeys() {
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk((String[]) null));
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk("a", null));
    }

    // Writes

    @Test
    @SuppressWarnings("unchecked")
    void unseededCountersPassTheirDeltaAndReturnTheServerResult() throws Exception {
        final MemcachedClient delegate = construction.constructed().get(0);
        final OperationFuture<Long> result = mock(OperationFuture.class);
        when(result.get(anyLong(), any(TimeUnit.class))).thenReturn(10L, -1L, 7L, 0L);
        when(delegate.asyncIncr("counter", 1)).thenReturn(result);
        when(delegate.asyncIncr("counter", 3L)).thenReturn(result);
        when(delegate.asyncDecr("counter", 1)).thenReturn(result);
        when(delegate.asyncDecr("counter", 3L)).thenReturn(result);
        assertEquals(10L, cache.incr("counter"));
        assertEquals(-1L, cache.incr("counter", 3L));
        assertEquals(7L, cache.decr("counter"));
        assertEquals(0L, cache.decr("counter", 3L));
        verify(delegate).asyncIncr("counter", 1);
        verify(delegate).asyncIncr("counter", 3L);
        verify(delegate).asyncDecr("counter", 1);
        verify(delegate).asyncDecr("counter", 3L);
    }

    @Test
    void addRejectsNullKey() {
        assertThrows(IllegalArgumentException.class, () -> cache.add(null, "v", 60_000));
    }

    @Test
    void asyncSetRejectsNullKey() {
        assertThrows(IllegalArgumentException.class, () -> cache.asyncPut(null, "v", 60_000));
    }

    @Test
    void decrRejectsNegativeDelta() {
        assertThrows(IllegalArgumentException.class, () -> cache.decr("counter", -1));
    }

    @Test
    void decrRejectsNullKey() {
        assertThrows(IllegalArgumentException.class, () -> cache.decr(null));
        assertThrows(IllegalArgumentException.class, () -> cache.decr(null, 1));
    }

    @Test
    void decrWithDefaultValueValidation() {
        assertThrows(IllegalArgumentException.class, () -> cache.decr(null, 1, 0L));
        assertThrows(IllegalArgumentException.class, () -> cache.decr("k", -1, 0L));
        assertThrows(IllegalArgumentException.class, () -> cache.decr(null, 1, 0L, 1000L));
        assertThrows(IllegalArgumentException.class, () -> cache.decr("k", -1, 0L, 1000L));
    }

    @Test
    void incrRejectsNegativeDelta() {
        assertThrows(IllegalArgumentException.class, () -> cache.incr("counter", -1));
    }

    @Test
    void incrRejectsNullKey() {
        assertThrows(IllegalArgumentException.class, () -> cache.incr(null));
        assertThrows(IllegalArgumentException.class, () -> cache.incr(null, 1));
    }

    @Test
    void incrWithDefaultValueValidation() {
        assertThrows(IllegalArgumentException.class, () -> cache.incr(null, 1, 0L));
        assertThrows(IllegalArgumentException.class, () -> cache.incr("k", -1, 0L));
        assertThrows(IllegalArgumentException.class, () -> cache.incr(null, 1, 0L, 1000L));
        assertThrows(IllegalArgumentException.class, () -> cache.incr("k", -1, 0L, 1000L));
    }

    @Test
    void replaceRejectsNullKey() {
        assertThrows(IllegalArgumentException.class, () -> cache.replace(null, "v", 60_000));
    }

    /**
     * A liveTime so large that the derived absolute Unix expiration second overflows {@code int}
     * (while the relative seconds still fit) is rejected before any network call.
     */
    @Test
    void setExpirationOverflowThrows() {
        assertThrows(IllegalArgumentException.class, () -> cache.put("k", "v", 2_000_000_000_000L));
    }

    @Test
    void setRejectsNullKey() {
        assertThrows(IllegalArgumentException.class, () -> cache.put(null, "v", 60_000));
    }

    // Removals

    @Test
    @SuppressWarnings("unchecked")
    void deleteAndFlushPreserveSuccessFailureAndAsyncResults() throws Exception {
        final MemcachedClient delegate = construction.constructed().get(0);
        final OperationFuture<Boolean> operation = mock(OperationFuture.class);
        when(operation.get(anyLong(), any(TimeUnit.class))).thenReturn(true, false, true, true, true, false, null, true);
        when(delegate.delete("key")).thenReturn(operation);
        when(delegate.flush()).thenReturn(operation);
        assertTrue(cache.remove("key"));
        assertFalse(cache.remove("key"));
        assertTrue(cache.asyncRemove("key").get(1, TimeUnit.SECONDS));
        assertTrue(cache.asyncDelete("key").get(1, TimeUnit.SECONDS));
        cache.flushAll();
        assertThrows(IllegalStateException.class, cache::flushAll);
        assertThrows(IllegalStateException.class, cache::flushAll);
        assertTrue(cache.asyncFlushAll().get(1, TimeUnit.SECONDS));
        cache.disconnect();
        assertThrows(IllegalStateException.class, cache::flushAll);
        assertThrows(IllegalStateException.class, cache::asyncFlushAll);
    }

    @Test
    void asyncDeleteRejectsNullKey() {
        assertThrows(IllegalArgumentException.class, () -> cache.asyncRemove(null));
    }

    @Test
    void deleteRejectsNullKey() {
        assertThrows(IllegalArgumentException.class, () -> cache.remove(null));
    }

    // Lifecycle

    @Test
    void gracefulShutdownFailureIsTerminalWithoutInterruptingTheCaller() {
        final MemcachedClient delegate = construction.constructed().get(0);
        final IllegalStateException failure = new IllegalStateException("shutdown queue full");
        doThrow(failure).when(delegate).shutdown(1000L, TimeUnit.MILLISECONDS);

        assertSame(failure, assertThrows(IllegalStateException.class, () -> cache.disconnect(1000)));
        assertFalse(Thread.currentThread().isInterrupted());
        cache.disconnect(1000);
        verify(delegate, times(1)).shutdown(1000L, TimeUnit.MILLISECONDS);
        assertThrows(IllegalStateException.class, () -> cache.get("after-close"));
    }

    @Test
    void disconnectIsIdempotent() {
        final SpyMemcached<Object> local = new SpyMemcached<>(SERVER_URL);
        local.disconnect();
        local.disconnect(); // safe to call again

        // After shutdown every operation fails in the wrapper, before argument validation or
        // dispatch into spymemcached. This gives all operation families one lifecycle contract.
        assertThrows(IllegalStateException.class, () -> local.get("k"));
        assertThrows(IllegalStateException.class, () -> local.get(null));
        assertThrows(IllegalStateException.class, () -> local.getBulk((String[]) null));
        assertThrows(IllegalStateException.class, () -> local.asyncPut("k", "v", 60_000));
        assertThrows(IllegalStateException.class, () -> local.incr("counter", -1));
        assertThrows(IllegalStateException.class, local::flushAll);
    }

    @Test
    void disconnectRemainsIdempotentOnFailure() throws Exception {
        final SpyMemcached<Object> local = new SpyMemcached<>(SERVER_URL);
        final Field clientField = SpyMemcached.class.getDeclaredField("mc");
        clientField.setAccessible(true);
        final MemcachedClient realClient = (MemcachedClient) clientField.get(local);
        final MemcachedClient failingClient = mock(MemcachedClient.class);
        clientField.set(local, failingClient);

        final RuntimeException failure = new RuntimeException("unexpected shutdown failure");
        doThrow(failure).when(failingClient).shutdown();

        try {
            assertEquals(failure, assertThrows(RuntimeException.class, local::disconnect));

            // The delegate has already entered its terminal shutdown sequence; retrying can only
            // repeat teardown against a partially released client.
            local.disconnect();
            verify(failingClient, times(1)).shutdown();
        } finally {
            realClient.shutdown();
        }
    }

    @Test
    void disconnectWithTimeout() {
        final SpyMemcached<Object> local = new SpyMemcached<>(SERVER_URL);
        local.disconnect(5_000);
        assertThrows(IllegalStateException.class, () -> local.get("k"));
    }

    /**
     * Regression coverage for graceful-shutdown interruption. SpyMemcached's delegate wraps the
     * {@link InterruptedException} raised by its queue wait in a plain RuntimeException and clears
     * the flag. The wrapper must restore the flag and remember that the delegate has nevertheless
     * entered its terminal shutdown sequence, so a second disconnect is a no-op.
     */
    @Test
    void disconnectWithTimeoutRestoresInterruptAndRemainsIdempotentOnFailure() throws Exception {
        final SpyMemcached<Object> local = new SpyMemcached<>(SERVER_URL);
        final Field clientField = SpyMemcached.class.getDeclaredField("mc");
        clientField.setAccessible(true);
        final MemcachedClient realClient = (MemcachedClient) clientField.get(local);
        final MemcachedClient failingClient = mock(MemcachedClient.class);
        clientField.set(local, failingClient);

        final RuntimeException interruption = new RuntimeException("interrupted graceful shutdown", new InterruptedException("test interrupt"));
        doThrow(interruption).when(failingClient).shutdown(anyLong(), eq(TimeUnit.MILLISECONDS));

        try {
            assertEquals(interruption, assertThrows(RuntimeException.class, () -> local.disconnect(1_000L)));
            assertTrue(Thread.currentThread().isInterrupted());

            // The failed call still terminally shut down the delegate; do not invoke it again.
            local.disconnect(1_000L);
            verify(failingClient, times(1)).shutdown(1_000L, TimeUnit.MILLISECONDS);
        } finally {
            Thread.interrupted(); // Do not leak the intentional interrupt into the JUnit worker.
            realClient.shutdown();
        }
    }

    @Test
    void shutdownCheckPrecedesInvalidMemcachedKeys() throws Exception {
        final SpyMemcached<Object> cache = clientWithDelegate(mock(MemcachedClient.class));
        cache.disconnect();

        assertThrows(IllegalStateException.class, () -> cache.put("bad key", new Object(), Long.MAX_VALUE));
        assertThrows(IllegalStateException.class, () -> cache.incr("", -1));
        assertThrows(IllegalStateException.class, () -> cache.asyncGetBulk("bad key"));
    }

    // Compatibility

    /**
     * An in-place return-type refinement changes a JVM method descriptor even when the new return
     * type is covariant. These bridge checks protect already-compiled callers that still link to
     * the original {@link Future}-returning descriptors.
     */
    @Test
    void existingAsyncMethodsRetainLegacyFutureBridges() {
        assertTrue(hasFutureBridge("asyncGet", String.class));
        assertTrue(hasFutureBridge("asyncGetBulk", String[].class));
        assertTrue(hasFutureBridge("asyncGetBulk", Collection.class));
        assertTrue(hasFutureBridge("asyncAdd", String.class, Object.class, long.class));
        assertTrue(hasFutureBridge("asyncReplace", String.class, Object.class, long.class));
        assertTrue(hasFutureBridge("asyncFlushAll"));
        assertTrue(hasFutureBridge("asyncFlushAll", long.class));
    }

    // Validation And Failures

    @Test
    void asyncAddRejectsNullKey() {
        assertThrows(IllegalArgumentException.class, () -> cache.asyncAdd(null, "v", 60_000));
    }

    @Test
    void asyncReplaceRejectsNullKey() {
        assertThrows(IllegalArgumentException.class, () -> cache.asyncReplace(null, "v", 60_000));
    }

    @Test
    void customTranscoderNullPointerFailureIsNotTranslated() throws Exception {
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
    @SuppressWarnings({ "unchecked", "deprecation" })
    void kryoAndCustomTranscodersKeepTheirNullValueSupport() throws Exception {
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
    @SuppressWarnings("unchecked")
    void pairedSurrogatesRemainValidKeys() throws Exception {
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
    @SuppressWarnings("deprecation")
    void stockTranscoderRejectsNullAfterStateAndKeyChecksBeforeTtlAndDispatch() throws Exception {
        final MemcachedClient delegate = construction.constructed().get(0);
        when(delegate.getTranscoder()).thenReturn(new SerializingTranscoder());
        final SpyMemcached<Object> cache = this.cache;

        assertTrue(assertThrows(IllegalArgumentException.class, () -> cache.put(null, null, Long.MAX_VALUE)).getMessage().contains("key"));
        verifyNoInteractions(delegate);

        final List<Consumer<SpyMemcached<Object>>> writes = List.of(c -> c.put("key", null, Long.MAX_VALUE), c -> c.set("key", null, Long.MAX_VALUE),
                c -> c.asyncPut("key", null, Long.MAX_VALUE), c -> c.asyncSet("key", null, Long.MAX_VALUE), c -> c.add("key", null, Long.MAX_VALUE),
                c -> c.asyncAdd("key", null, Long.MAX_VALUE), c -> c.replace("key", null, Long.MAX_VALUE), c -> c.asyncReplace("key", null, Long.MAX_VALUE));

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
    @SuppressWarnings("deprecation")
    void unpairedSurrogatesAreRejectedByEveryKeyedOperationBeforeDispatch() throws Exception {
        final MemcachedClient delegate = mock(MemcachedClient.class);
        final SpyMemcached<Object> cache = clientWithDelegate(delegate);
        final List<Consumer<String>> operations = List.of(cache::get, cache::asyncGet, key -> cache.getBulk("valid", key),
                key -> cache.getBulk(List.of("valid", key)), key -> cache.asyncGetBulk("valid", key), key -> cache.asyncGetBulk(List.of("valid", key)),
                key -> cache.put(key, "value", 1_000), key -> cache.asyncPut(key, "value", 1_000), key -> cache.asyncSet(key, "value", 1_000),
                key -> cache.add(key, "value", 1_000), key -> cache.asyncAdd(key, "value", 1_000), key -> cache.replace(key, "value", 1_000),
                key -> cache.asyncReplace(key, "value", 1_000), cache::remove, cache::asyncRemove, cache::asyncDelete, cache::incr, key -> cache.incr(key, 1),
                key -> cache.incr(key, 1, 0), key -> cache.incr(key, 1, 0, 1_000), cache::decr, key -> cache.decr(key, 1), key -> cache.decr(key, 1, 0),
                key -> cache.decr(key, 1, 0, 1_000));

        for (final String malformed : List.of("key" + (char) 0xD800, "key" + (char) 0xDC00, "key" + (char) 0xD800 + "x")) {
            for (final Consumer<String> operation : operations) {
                assertThrows(IllegalArgumentException.class, () -> operation.accept(malformed));
            }
        }
        verifyNoInteractions(delegate);
    }

    /**
     * An unresolvable host used to reach spymemcached, which opened its NIO selector and a socket
     * channel per address before the connect to the unresolved address threw
     * {@code UnresolvedAddressException}, leaking all of them. It must now be rejected before the
     * connection factory is asked to create any connection.
     */
    @Test
    void unresolvableHostIsRejectedBeforeConnectionResourcesAreCreated() throws Exception {
        final DefaultConnectionFactory factory = spy(new DefaultConnectionFactory());
        final String serverUrl = "localhost:11211,spymemcached-unit-test-host.invalid:11211";

        final IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> SpyMemcached.createSpyMemcachedClient(serverUrl, factory));

        assertFalse(error instanceof UnresolvedAddressException, "rejected by the wrapper, not by a failed socket connect");
        verify(factory, never()).createConnection(anyList());
        assertThrows(IllegalArgumentException.class, () -> new SpyMemcached<>(serverUrl, 1_000L));
    }

    // Other Behavior

    /**
     * Store and flush paths share one TTL conversion: a non-positive value means "no expiration"
     * ({@code 0}; a negative memcached expiration would mean "already expired"), positive
     * milliseconds round up to whole seconds, exactly 30 days stays relative, anything longer
     * becomes an absolute Unix timestamp, and a timestamp beyond 2^31-1 is rejected before dispatch.
     */
    @Test
    @SuppressWarnings("unchecked")
    void expirationConversionHonorsThirtyDayBoundaryForStoresAndFlushes() throws Exception {
        final MemcachedClient delegate = mock(MemcachedClient.class);
        final OperationFuture<Boolean> accepted = mock(OperationFuture.class);
        when(accepted.get(25L, TimeUnit.MILLISECONDS)).thenReturn(true);
        when(accepted.get()).thenReturn(true);
        when(delegate.set(anyString(), anyInt(), any())).thenReturn(accepted);
        when(delegate.flush(anyInt())).thenReturn(accepted);
        final SpyMemcached<Object> cache = clientWithDelegate(delegate);
        final long thirtyDaysMillis = 30L * 24 * 60 * 60 * 1000;

        for (final long liveTime : new long[] { Long.MIN_VALUE, -1, 0, 1, 1_000, 1_001, thirtyDaysMillis }) {
            assertTrue(cache.put("relative", "v", liveTime));
        }

        final ArgumentCaptor<Integer> relative = ArgumentCaptor.forClass(Integer.class);
        verify(delegate, times(7)).set(eq("relative"), relative.capture(), eq("v"));
        assertEquals(List.of(0, 0, 0, 1, 1, 2, 2_592_000), relative.getAllValues());

        final long before = System.currentTimeMillis() / 1000L;
        assertTrue(cache.put("absolute", "v", thirtyDaysMillis + 1));
        final long after = System.currentTimeMillis() / 1000L;
        final ArgumentCaptor<Integer> absolute = ArgumentCaptor.forClass(Integer.class);
        verify(delegate).set(eq("absolute"), absolute.capture(), eq("v"));
        assertTrue(absolute.getValue() >= before + 2_592_001 && absolute.getValue() <= after + 2_592_001, "absolute = " + absolute.getValue());

        assertTrue(cache.flushAll(-1));
        assertTrue(cache.flushAll(1_500));
        assertTrue(cache.asyncFlushAll(thirtyDaysMillis).get());
        final ArgumentCaptor<Integer> delays = ArgumentCaptor.forClass(Integer.class);
        verify(delegate, times(3)).flush(delays.capture());
        assertEquals(List.of(0, 2, 2_592_000), delays.getAllValues());

        final MemcachedClient untouched = mock(MemcachedClient.class);
        final SpyMemcached<Object> rejecting = clientWithDelegate(untouched);
        // Relative seconds overflow int, and an int-sized relative value whose absolute form passes 2038.
        for (final long tooLong : new long[] { Long.MAX_VALUE, 2_000_000_000_000L }) {
            assertThrows(IllegalArgumentException.class, () -> rejecting.put("k", "v", tooLong));
            assertThrows(IllegalArgumentException.class, () -> rejecting.asyncAdd("k", "v", tooLong));
            assertThrows(IllegalArgumentException.class, () -> rejecting.flushAll(tooLong));
            assertThrows(IllegalArgumentException.class, () -> rejecting.incr("k", 1, 0, tooLong));
            assertThrows(IllegalArgumentException.class, () -> rejecting.decr("k", 1, 0, tooLong));
        }
        verifyNoInteractions(untouched);
    }

    /**
     * A failed mutation (for example a CLIENT_ERROR on a non-numeric value) must surface as an
     * exception. spymemcached's own synchronous counters report every failure as {@code -1}, which
     * would be indistinguishable from "absent" and would make the seeding overloads try to seed.
     */
    @Test
    @SuppressWarnings("unchecked")
    void failedCounterMutationThrowsInsteadOfReturningMinusOneOrSeeding() throws Exception {
        final MemcachedClient delegate = mock(MemcachedClient.class);
        final OperationFuture<Long> failed = mock(OperationFuture.class);
        final OperationException failure = new OperationException(OperationErrorType.CLIENT, "CLIENT_ERROR cannot increment or decrement non-numeric value");
        when(failed.get(25L, TimeUnit.MILLISECONDS)).thenThrow(new ExecutionException(failure));
        when(delegate.asyncIncr("counter", 1)).thenReturn(failed);
        when(delegate.asyncIncr("counter", 1L)).thenReturn(failed);
        when(delegate.asyncDecr("counter", 1)).thenReturn(failed);
        when(delegate.asyncDecr("counter", 1L)).thenReturn(failed);
        final SpyMemcached<Object> cache = clientWithDelegate(delegate);

        final List<Consumer<SpyMemcached<Object>>> counters = List.of(c -> c.incr("counter"), c -> c.incr("counter", 1), c -> c.incr("counter", 1, 0),
                c -> c.incr("counter", 1, 0, 1_000), c -> c.decr("counter"), c -> c.decr("counter", 1), c -> c.decr("counter", 1, 0),
                c -> c.decr("counter", 1, 0, 1_000));

        for (final Consumer<SpyMemcached<Object>> counter : counters) {
            assertSame(failure, assertThrows(RuntimeException.class, () -> counter.accept(cache)).getCause());
        }

        verify(delegate, never()).add(anyString(), anyInt(), any(), any());
    }

    /** The protocol limit counts UTF-8 bytes, and an exactly 250-byte key remains valid. */
    @Test
    @SuppressWarnings("unchecked")
    void memcachedKeyValidationAcceptsExactUtf8ByteLimit() throws Exception {
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
    void memcachedKeyValidationPrecedesLaterArgumentsAndDispatch() throws Exception {
        final MemcachedClient delegate = mock(MemcachedClient.class);
        final SpyMemcached<Object> cache = clientWithDelegate(delegate);

        for (final String key : List.of("", "bad key", "key\r", "key\n", "key\0", "x".repeat(251), "\u00e9".repeat(126))) {
            final IllegalArgumentException keyFailure = assertThrows(IllegalArgumentException.class, () -> cache.get(key));
            assertEquals(keyFailure.getMessage(),
                    assertThrows(IllegalArgumentException.class, () -> cache.put(key, new Object(), Long.MAX_VALUE)).getMessage());
            assertEquals(keyFailure.getMessage(),
                    assertThrows(IllegalArgumentException.class, () -> cache.asyncAdd(key, new Object(), Long.MAX_VALUE)).getMessage());
            assertEquals(keyFailure.getMessage(), assertThrows(IllegalArgumentException.class, () -> cache.incr(key, -1, -1, Long.MAX_VALUE)).getMessage());
            assertEquals(keyFailure.getMessage(), assertThrows(IllegalArgumentException.class, () -> cache.decr(key, -1, -1, Long.MAX_VALUE)).getMessage());
            assertThrows(IllegalArgumentException.class, () -> cache.getBulk("valid", key));
            assertThrows(IllegalArgumentException.class, () -> cache.asyncGetBulk(List.of("valid", key)));
        }

        verifyNoInteractions(delegate);
    }

    /**
     * An absent counter is seeded through a raw-ASCII transcoder (never the client's default,
     * Kryo-backed one, whose bytes native incr/decr cannot mutate) and the default is returned
     * without applying the delta; an existing counter returns the mutation result without seeding.
     */
    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    void seededCounterMissStoresRawAsciiSeedAndExistingCounterSkipsSeeding() throws Exception {
        for (final boolean increment : new boolean[] { true, false }) {
            final MemcachedClient delegate = mock(MemcachedClient.class);
            final OperationFuture<Long> miss = mock(OperationFuture.class);
            final OperationFuture<Long> hit = mock(OperationFuture.class);
            final OperationFuture<Boolean> seeded = mock(OperationFuture.class);
            when(miss.get(25L, TimeUnit.MILLISECONDS)).thenReturn(-1L);
            when(hit.get(25L, TimeUnit.MILLISECONDS)).thenReturn(12L);
            when(seeded.get(25L, TimeUnit.MILLISECONDS)).thenReturn(true);
            when(delegate.add(eq("counter"), eq(0), eq("7"), any())).thenReturn(seeded);

            if (increment) {
                when(delegate.asyncIncr("counter", 5L)).thenReturn(miss, hit);
            } else {
                when(delegate.asyncDecr("counter", 5L)).thenReturn(miss, hit);
            }

            final SpyMemcached<Object> cache = clientWithDelegate(delegate);

            assertEquals(7L, increment ? cache.incr("counter", 5, 7) : cache.decr("counter", 5, 7));
            assertEquals(12L, increment ? cache.incr("counter", 5, 7) : cache.decr("counter", 5, 7));

            if (increment) {
                verify(delegate, times(2)).asyncIncr("counter", 5L);
            } else {
                verify(delegate, times(2)).asyncDecr("counter", 5L);
            }

            final ArgumentCaptor<Transcoder> seedTranscoder = ArgumentCaptor.forClass(Transcoder.class);
            verify(delegate).add(eq("counter"), eq(0), eq("7"), seedTranscoder.capture());
            verifyNoMoreInteractions(delegate);

            final CachedData encoded = seedTranscoder.getValue().encode("7");
            assertEquals(0, encoded.getFlags());
            assertFalse(seedTranscoder.getValue().asyncDecode(encoded));
            assertEquals("7", seedTranscoder.getValue().decode(encoded));
            assertArrayEquals("7".getBytes(StandardCharsets.US_ASCII), encoded.getData());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void seededCountersCanReturnAbsentWhenCompetingInitializationIsDeletedBeforeRetry() throws Exception {
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
    void serverUrlReturnsConstructorValue() {
        assertEquals(SERVER_URL, cache.serverUrl());
    }

    private static boolean hasFutureBridge(final String name, final Class<?>... parameterTypes) {
        return Arrays.stream(SpyMemcached.class.getDeclaredMethods())
                .anyMatch(method -> method.isBridge() && method.getName().equals(name) && method.getReturnType() == Future.class
                        && Arrays.equals(method.getParameterTypes(), parameterTypes));
    }
}
