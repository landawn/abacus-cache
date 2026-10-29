/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.landawn.abacus.parser.KryoParser;
import com.landawn.abacus.parser.ParserFactory;
import com.landawn.abacus.util.AddrUtil;

import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.SetParams;

/** Shared Redis wire-format, validation, and bulk contracts inherited by both client implementations. */
class AbstractJedisCacheClientTest {

    private static final KryoParser KRYO = ParserFactory.createKryoParser();

    private UnifiedJedis mockJedis;
    private AbstractJedisCacheClient<Object> cache;

    @BeforeEach
    void setUpClient() {
        mockJedis = Mockito.mock(UnifiedJedis.class);
        cache = new AbstractJedisCacheClient<>("unit.test:6379") {
            @Override
            protected UnifiedJedis clientFor(final byte[] keyBytes) {
                return mockJedis;
            }

            @Override
            protected void closeClients() {
                mockJedis.close();
            }

            @Override
            public void flushAll() {
                assertNotShutdown();
                mockJedis.flushAll();
            }
        };
    }

    @AfterEach
    void closeClient() {
        cache.disconnect();
    }

    // Construction

    @Test
    void emptyResolvedAddressListIsRejected() {
        try (MockedStatic<AddrUtil> addresses = Mockito.mockStatic(AddrUtil.class)) {
            addresses.when(() -> AddrUtil.getAddressList("127.0.0.1:6379")).thenReturn(List.of());
            assertTrue(assertThrows(IllegalArgumentException.class, () -> AbstractJedisCacheClient.resolveServerAddresses("127.0.0.1:6379")).getMessage()
                    .contains("No valid server addresses"));
        }
    }

    @Test
    void missingKryoIsReportedBeforeAnyClientIsCreated() {
        try (MockedStatic<ParserFactory> parsers = Mockito.mockStatic(ParserFactory.class)) {
            parsers.when(ParserFactory::isKryoParserAvailable).thenReturn(false);
            final IllegalStateException failure = assertThrows(IllegalStateException.class, () -> new JRedis<>("127.0.0.1:6379"));
            assertTrue(failure.getMessage().contains("Kryo is required"));
            Mockito.verifyNoInteractions(mockJedis);
        }
    }

    // Reads

    /**
     * Regression test: counter values (raw ASCII digits written by INCR/DECR) are not Kryo payloads.
     * About one in eight such values (every value starting with '6' or '9') made Kryo's generated
     * constructor accessor instantiate an abstract class, so {@code get}/{@code getBulk} leaked a
     * {@link InstantiationError} instead of the documented runtime (Kryo) exception — escaping
     * {@code catch (Exception)} handlers. Every counter-shaped payload must now fail with a
     * {@code KryoException}.
     */
    @ParameterizedTest
    @ValueSource(strings = { "6", "9", "64", "99", "9223372036854775807", "1", "-5", "42" })
    void getCounterPayloadFailsWithKryoExceptionNotError(final String counter) {

        when(mockJedis.get(utf8("jredis-review:counter"))).thenReturn(counter.getBytes(StandardCharsets.US_ASCII));

        assertThrows(com.esotericsoftware.kryo.KryoException.class, () -> cache.get("jredis-review:counter"), counter);
        assertThrows(com.esotericsoftware.kryo.KryoException.class, () -> cache.getBulk("jredis-review:counter"), counter);

    }

    /**
     * Regression test: plain text written by another Redis client is not a Kryo payload either.
     * Under abacus-common's built-in Kryo registrations these strings decode into collections of
     * {@code Multimap}s whose backing-map field holds a non-{@code Map} object; hashing one throws a
     * bare {@link IncompatibleClassChangeError} (the parent of the {@link InstantiationError} that
     * counter payloads raise), which escaped {@code get}/{@code getBulk} as an {@link Error}.
     */
    @ParameterizedTest
    @ValueSource(strings = { "{F1ZFa[8zgaGr[^+u FC[IWd1,Nay+", "-z*D50XyWEb1{E\"ZDm HfYx Y-N$N,ntYozv", "-*x^c3|X(vF FT#fy}Cibd" })
    void getForeignTextPayloadFailsWithKryoExceptionNotError(final String text) {
        when(mockJedis.get(utf8("foreign"))).thenReturn(text.getBytes(StandardCharsets.US_ASCII));

        assertThrows(com.esotericsoftware.kryo.KryoException.class, () -> cache.get("foreign"), text);
        assertThrows(com.esotericsoftware.kryo.KryoException.class, () -> cache.getBulk("foreign"), text);
    }

    /**
     * A NoSuchMethodError (or another IncompatibleClassChangeError subclass) raised while decoding means
     * the application's own classes are binary-incompatible, not that the payload is foreign: it must
     * propagate unchanged rather than be reported as "not written by this client's Kryo encoding".
     */
    @Test
    void getPropagatesABinaryIncompatibilityOfTheApplicationsOwnClasses() throws Exception {
        final Field holder = Class.forName(AbstractJedisCacheClient.class.getName() + "$KryoParserHolder").getDeclaredField("KRYO_PARSER");
        holder.setAccessible(true);
        final KryoParser shared = (KryoParser) holder.get(null);
        final NoSuchMethodError linkage = new NoSuchMethodError("'void LinkageFailingValue.migrate()'");
        shared.register(LinkageFailingValue.class, new Serializer<LinkageFailingValue>() {
            @Override
            public void write(final Kryo kryo, final Output output, final LinkageFailingValue object) {
                output.writeByte(1);
            }

            @Override
            public LinkageFailingValue read(final Kryo kryo, final Input input, final Class<? extends LinkageFailingValue> type) {
                input.readByte();
                throw linkage;
            }
        }, 50_101);

        when(mockJedis.get(utf8("linkage"))).thenReturn(shared.encode(new LinkageFailingValue()));

        assertSame(linkage, assertThrows(NoSuchMethodError.class, () -> cache.get("linkage")));
    }

    private static final class LinkageFailingValue {
    }

    @ParameterizedTest
    @MethodSource("encodedValues")
    void getDecodesStringsAndObjects(final Object expected) {
        when(mockJedis.get(utf8("key"))).thenReturn(KRYO.encode(expected));

        assertEquals(expected, cache.get("key"));
        verify(mockJedis).get(utf8("key"));
    }

    @ParameterizedTest
    @NullAndEmptySource
    void getTreatsMissingAndEmptyPayloadsAsAbsent(final byte[] payload) {
        when(mockJedis.get(utf8("key"))).thenReturn(payload);

        assertNull(cache.get("key"));
        verify(mockJedis).get(utf8("key"));
    }

    // Bulk reads

    @Test
    void collectionMutationDuringFirstGetCannotChangeLaterKeys() {
        final RecordingClient cache = new RecordingClient();
        final List<String> keys = new ArrayList<>(List.of("a", "a", "b"));
        cache.values.put("a", "value-a");
        cache.values.put("b", "value-b");
        cache.onRead = keys::clear;
        try {
            assertEquals(Map.of("a", "value-a", "b", "value-b"), cache.getBulk(keys));
            assertEquals(List.of("a", "b"), cache.reads);
        } finally {
            cache.disconnect();
        }
    }

    @Test
    void duplicateArrayDoesNotAllocateStorageForEveryOccurrence() {
        final RecordingClient cache = new RecordingClient();
        final String[] keys = new String[200_000];
        Arrays.fill(keys, "present");
        try {
            assertDuplicateAllocationBound(cache, null, keys);
        } finally {
            cache.disconnect();
        }
    }

    @Test
    void duplicateCollectionDoesNotAllocateStorageForEveryOccurrence() {
        final RecordingClient cache = new RecordingClient();
        try {
            assertDuplicateAllocationBound(cache, Collections.nCopies(200_000, "present"), null);
        } finally {
            cache.disconnect();
        }
    }

    @Test
    void duplicateKeysKeepFirstLookupOrderAndMissingEntriesStayAbsent() {
        final RecordingClient cache = new RecordingClient();
        cache.values.put("a", "value-a");
        cache.values.put("b", "value-b");
        try {
            assertEquals(Map.of("a", "value-a", "b", "value-b"), cache.getBulk("a", "missing", "a", "b", "missing", "b"));
            assertEquals(List.of("a", "missing", "b"), cache.reads);
        } finally {
            cache.disconnect();
        }
    }

    /**
     * A collection is copied and validated in one pass. The old validate-then-fetch implementation
     * iterated a live collection twice, so a changed second view could issue requests for keys that
     * had never been validated.
     */
    @Test
    void getBulkCollectionIsValidatedAndSnapshottedInOnePass() {
        final AtomicInteger iteratorCalls = new AtomicInteger();
        final Collection<String> changingView = new AbstractCollection<>() {
            @Override
            public Iterator<String> iterator() {
                return (iteratorCalls.getAndIncrement() == 0 ? List.of("a") : Arrays.asList("a", null)).iterator();
            }

            @Override
            public int size() {
                return 1;
            }
        };
        when(mockJedis.get(utf8("a"))).thenReturn(KRYO.encode("value-a"));

        assertEquals("value-a", cache.getBulk(changingView).get("a"));
        assertEquals(1, iteratorCalls.get(), "the caller's collection must be consumed exactly once");
    }

    /**
     * A bulk result is keyed by the requested key, so duplicate requests must not trigger repeated
     * network reads (which could observe different values or expiry states for the same map entry).
     */
    @Test
    void getBulkDuplicateKeysAreFetchedOnce() {
        when(mockJedis.get(utf8("same"))).thenReturn(KRYO.encode("value"));

        final Map<String, Object> result = cache.getBulk("same", "same", "same");

        assertEquals(Map.of("same", "value"), result);
        verify(mockJedis, times(1)).get(utf8("same"));
    }

    @ParameterizedTest(name = "collection overload: {0}")
    @ValueSource(booleans = { false, true })
    void getBulkEmptyRequestsDoNotContactTheBackend(final boolean collection) {
        assertTrue((collection ? cache.getBulk(List.of()) : cache.getBulk()).isEmpty());
        Mockito.verifyNoInteractions(mockJedis);
    }

    @Test
    void getBulkRejectsNullElement() {
        // Validation happens up-front, before any GET is issued.
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk("a", null, "c"));
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk(Arrays.asList("a", null)));
        verify(mockJedis, never()).get(any(byte[].class));
    }

    @Test
    void getBulkRejectsNullKeys() {
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk((String[]) null));
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk((List<String>) null));
    }

    @ParameterizedTest(name = "collection overload: {0}")
    @ValueSource(booleans = { false, true })
    void getBulkReturnsOnlyFoundKeys(final boolean collection) {
        final String[] keys = { "present", "missing", "empty" };
        when(mockJedis.get(utf8("present"))).thenReturn(KRYO.encode("value"));
        when(mockJedis.get(utf8("empty"))).thenReturn(new byte[0]);

        assertEquals(Map.of("present", "value"), collection ? cache.getBulk(List.of(keys)) : cache.getBulk(keys));
        for (final String key : keys) {
            verify(mockJedis).get(utf8(key));
        }
    }

    /**
     * The caller retains ownership of a varargs array. Mutating it after the first GET must not change
     * the already-validated operation or inject an unvalidated null into the remaining requests.
     */
    @Test
    void getBulkVarargsUsesPrivateSnapshot() {
        final String[] keys = { "a", "b" };
        when(mockJedis.get(utf8("a"))).thenAnswer(invocation -> {
            keys[1] = null;
            return null;
        });
        when(mockJedis.get(utf8("b"))).thenReturn(KRYO.encode("value-b"));

        final Map<String, Object> result = cache.getBulk(keys);

        assertEquals("value-b", result.get("b"));
        verify(mockJedis).get(utf8("b"));
    }

    @ParameterizedTest(name = "collection overload: {0}")
    @ValueSource(booleans = { false, true })
    void validSupplementaryUnicodeKeysPreserveUtf8Bytes(final boolean collection) {
        final String key = "user:\uD83D\uDE00:\uD800\uDC00";
        when(mockJedis.get(utf8(key))).thenReturn(KRYO.encode("value"));

        assertEquals("value", cache.get(key));
        assertEquals(Map.of(key, "value"), collection ? cache.getBulk(List.of("missing", key)) : cache.getBulk("missing", key));
        verify(mockJedis, times(2)).get(utf8(key));
    }

    // Writes

    @ParameterizedTest
    @MethodSource("encodedValues")
    void putEncodesStringsAndObjects(final Object expected) {
        when(mockJedis.set(any(byte[].class), any(byte[].class), any(SetParams.class))).thenReturn("OK");

        assertTrue(cache.put("key", expected, 30_000));

        final ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
        verify(mockJedis).set(eq(utf8("key")), payload.capture(), eq(SetParams.setParams().px(30_000L)));
        assertEquals(expected, KRYO.decode(payload.getValue()));
    }

    @Test
    void putNullValueEncodesAsEmptyByteArray() {
        when(mockJedis.set(any(byte[].class), any(byte[].class), any(SetParams.class))).thenReturn("OK");

        cache.put("k", null, 60_000);

        verify(mockJedis).set(eq(utf8("k")), eq(new byte[0]), any(SetParams.class));
    }

    /** Preserve millisecond precision, including sub-second and long-valued TTLs. */
    @ParameterizedTest
    @ValueSource(longs = { 300, 30_000, 60_000, Long.MAX_VALUE })
    void putPassesTheExactTtlInMilliseconds(final long liveTime) {
        when(mockJedis.set(any(byte[].class), any(byte[].class), any(SetParams.class))).thenReturn("OK");

        assertTrue(cache.put("key", "value", liveTime));
        verify(mockJedis).set(eq(utf8("key")), any(byte[].class), eq(SetParams.setParams().px(liveTime)));
        verify(mockJedis, never()).set(any(byte[].class), any(byte[].class));
    }

    @Test
    void putReturnsFalseWhenServerDoesNotSayOK() {
        when(mockJedis.set(any(byte[].class), any(byte[].class), any(SetParams.class))).thenReturn("NOT_OK");

        assertFalse(cache.put("k", "v", 60_000));
    }

    @ParameterizedTest
    @ValueSource(longs = { 0, -1, Long.MIN_VALUE })
    void putWithoutExpirationUsesPlainSet(final long liveTime) {
        when(mockJedis.set(any(byte[].class), any(byte[].class))).thenReturn("OK");

        assertTrue(cache.put("key", "value", liveTime));
        verify(mockJedis).set(eq(utf8("key")), any(byte[].class));
        verify(mockJedis, never()).set(any(byte[].class), any(byte[].class), any(SetParams.class));
    }

    // Counters

    @Test
    void decrReturnsNegativeWhenRedisDoes() {
        when(mockJedis.decr(utf8("counter"))).thenReturn(-1L);

        assertEquals(-1L, cache.decr("counter"));
    }

    @Test
    void decrWithDeltaCallsDecrBy() {
        when(mockJedis.decrBy(utf8("counter"), 3L)).thenReturn(-3L);

        assertEquals(-3L, cache.decr("counter", 3));
        verify(mockJedis).decrBy(utf8("counter"), 3L);
    }

    @Test
    void decrWithDeltaRejectsNegativeDelta() {
        assertThrows(IllegalArgumentException.class, () -> cache.decr("counter", -1));
        verify(mockJedis, never()).decrBy(any(byte[].class), eq(-1L));
    }

    @Test
    void incrReturnsTheServerValue() {
        when(mockJedis.incr(utf8("counter"))).thenReturn(12L);

        assertEquals(12L, cache.incr("counter"));
        verify(mockJedis).incr(utf8("counter"));
    }

    @Test
    void incrWithDeltaCallsIncrBy() {
        when(mockJedis.incrBy(utf8("counter"), 5L)).thenReturn(5L);

        assertEquals(5L, cache.incr("counter", 5));
        verify(mockJedis).incrBy(utf8("counter"), 5L);
    }

    @Test
    void incrWithDeltaRejectsNegativeDelta() {
        assertThrows(IllegalArgumentException.class, () -> cache.incr("counter", -1));
        verify(mockJedis, never()).incrBy(any(byte[].class), eq(-1L));
    }

    // Removals

    @ParameterizedTest
    @CsvSource({ "0,false", "1,true", "2,true" })
    void removeReportsWhetherRedisDeletedAnyKeys(final long deleted, final boolean expected) {
        when(mockJedis.del(utf8("key"))).thenReturn(deleted);

        assertEquals(expected, cache.remove("key"));
        verify(mockJedis).del(utf8("key"));
    }

    // Key validation

    @Test
    void invalidKeyAfterManyDuplicatesIsRejectedBeforeAnyBackendRead() {
        final RecordingClient cache = new RecordingClient();
        final List<String> keys = new ArrayList<>(Collections.nCopies(1_000, "valid"));
        keys.add("\uD800");
        try {
            assertThrows(IllegalArgumentException.class, () -> cache.getBulk(keys));
            assertThrows(IllegalArgumentException.class, () -> cache.getBulk(keys.toArray(String[]::new)));
            assertTrue(cache.reads.isEmpty(), "no GET may be issued before every key is validated: " + cache.reads);
        } finally {
            cache.disconnect();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "\uD800", "\uDC00", "prefix:\uD800suffix", "prefix:\uDC00suffix", "\uD800\uD800", "\uDC00\uD800" })
    void malformedUnicodeKeysAreRejectedBeforeAnyCommand(final String key) {

        assertThrows(IllegalArgumentException.class, () -> cache.get(key));
        assertThrows(IllegalArgumentException.class, () -> cache.put(key, "value", 0));
        assertThrows(IllegalArgumentException.class, () -> cache.remove(key));
        assertThrows(IllegalArgumentException.class, () -> cache.incr(key));
        assertThrows(IllegalArgumentException.class, () -> cache.incr(key, 1));
        assertThrows(IllegalArgumentException.class, () -> cache.decr(key));
        assertThrows(IllegalArgumentException.class, () -> cache.decr(key, 1));
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk("valid", key));
        assertThrows(IllegalArgumentException.class, () -> cache.getBulk(List.of("valid", key)));

        Mockito.verifyNoInteractions(mockJedis);
    }

    @Test
    void nullKeyIsRejectedWithIllegalArgumentExceptionBeforeAnyCommand() {
        assertThrows(IllegalArgumentException.class, () -> cache.getKeyBytes(null));
        assertThrows(IllegalArgumentException.class, () -> cache.get(null));
        assertThrows(IllegalArgumentException.class, () -> cache.put(null, "value", 0));
        assertThrows(IllegalArgumentException.class, () -> cache.put(null, "value", 60000));
        assertThrows(IllegalArgumentException.class, () -> cache.remove(null));
        assertThrows(IllegalArgumentException.class, () -> cache.incr(null));
        assertThrows(IllegalArgumentException.class, () -> cache.incr(null, 1));
        assertThrows(IllegalArgumentException.class, () -> cache.decr(null));
        assertThrows(IllegalArgumentException.class, () -> cache.decr(null, 1));

        Mockito.verifyNoInteractions(mockJedis);
    }

    private static void assertDuplicateAllocationBound(final RecordingClient cache, final Collection<String> collection, final String[] array) {
        final java.lang.management.ThreadMXBean platformBean = ManagementFactory.getThreadMXBean();
        assumeTrue(platformBean instanceof com.sun.management.ThreadMXBean);
        final com.sun.management.ThreadMXBean allocations = (com.sun.management.ThreadMXBean) platformBean;
        assumeTrue(allocations.isThreadAllocatedMemorySupported() && allocations.isThreadAllocatedMemoryEnabled());

        cache.values.put("present", "value");
        // Warm up the command, decoder, and collection paths before measuring task-local allocation.
        for (int i = 0; i < 50; i++) {
            bulkGet(cache, collection, array);
        }

        // Unrelated allocation on this thread (JIT activity, class loading, logging) can only add to
        // a sample, so the smallest of several samples is the stable measure. Retaining every
        // occurrence would cost at least 4 bytes per key (800,000 bytes for 200,000 keys) in every sample.
        final long threadId = Thread.currentThread().threadId();
        long allocated = Long.MAX_VALUE;

        for (int sample = 0; sample < 5; sample++) {
            cache.reads.clear();
            final long before = allocations.getThreadAllocatedBytes(threadId);
            final Map<String, String> result = bulkGet(cache, collection, array);
            allocated = Math.min(allocated, allocations.getThreadAllocatedBytes(threadId) - before);

            assertEquals(Map.of("present", "value"), result);
            assertEquals(List.of("present"), cache.reads);
        }

        assertTrue(allocated < 256_000L, "A bulk request containing one distinct key allocated " + allocated + " bytes");
    }

    private static Map<String, String> bulkGet(final RecordingClient cache, final Collection<String> collection, final String[] array) {
        return collection == null ? cache.getBulk(array) : cache.getBulk(collection);
    }

    private static final class RecordingClient extends AbstractJedisCacheClient<String> {
        // stubOnly: Mockito does not record invocations, so the mock neither allocates per call
        // inside the measured window nor grows across the warm-up calls. reads records the GETs.
        private final UnifiedJedis backend = mock(UnifiedJedis.class, withSettings().stubOnly());
        private final Map<String, String> values = new HashMap<>();
        private final List<String> reads = new ArrayList<>();
        private Runnable onRead = () -> {
        };

        private RecordingClient() {
            super("localhost:6379");
            when(backend.get(any(byte[].class))).thenAnswer(invocation -> {
                final String key = new String((byte[]) invocation.getArgument(0), StandardCharsets.UTF_8);
                reads.add(key);
                onRead.run();
                final String value = values.get(key);
                return value == null ? null : value.getBytes(StandardCharsets.UTF_8);
            });
        }

        @Override
        protected UnifiedJedis clientFor(final byte[] keyBytes) {
            return backend;
        }

        @Override
        protected String decode(final byte[] bytes) {
            return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
        }

        @Override
        public void flushAll() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void closeClients() {
            backend.close();
        }
    }

    private static Stream<Object> encodedValues() {
        return Stream.of("hello",
                Account.builder()
                        .id(7)
                        .gui("unit-test")
                        .firstName("Ada")
                        .lastName("Lovelace")
                        .emailAddress("ada@example.test")
                        .birthDate(Timestamp.valueOf("1815-12-10 00:00:00"))
                        .build());
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
