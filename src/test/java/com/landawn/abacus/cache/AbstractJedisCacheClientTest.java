/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import java.nio.charset.StandardCharsets;
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.landawn.abacus.parser.KryoParser;
import com.landawn.abacus.parser.ParserFactory;

import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.params.SetParams;

class AbstractJedisCacheClientTest {

    private static final KryoParser KRYO = ParserFactory.createKryoParser();

    private UnifiedJedis mockJedis;
    private AbstractJedisCacheClient<Object> cache;

    @BeforeEach
    void setUpClient() {
        mockJedis = org.mockito.Mockito.mock(UnifiedJedis.class);
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

    // Reads

    /**
     * Regression test: counter values (raw ASCII digits written by INCR/DECR) are not Kryo payloads.
     * About one in eight such values (every value starting with '6' or '9') made Kryo's generated
     * constructor accessor instantiate an abstract class, so {@code get}/{@code getBulk} leaked a
     * {@link InstantiationError} instead of the documented runtime (Kryo) exception — escaping
     * {@code catch (Exception)} handlers. Every counter-shaped payload must now fail with a
     * {@code KryoException}.
     */
    @Test
    void getCounterPayloadFailsWithKryoExceptionNotError() {
        for (final String counter : List.of("6", "9", "64", "99", "9223372036854775807", "1", "-5", "42")) {
            when(mockJedis.get(utf8("jredis-review:counter"))).thenReturn(counter.getBytes(StandardCharsets.US_ASCII));

            assertThrows(com.esotericsoftware.kryo.KryoException.class, () -> cache.get("jredis-review:counter"), counter);
            assertThrows(com.esotericsoftware.kryo.KryoException.class, () -> cache.getBulk("jredis-review:counter"), counter);
        }
    }

    @Test
    void getReturnsNullForEmptyByteArray() {
        when(mockJedis.get(any(byte[].class))).thenReturn(new byte[0]);

        assertNull(cache.get("empty"));
    }

    @Test
    void getReturnsNullWhenKeyMissing() {
        when(mockJedis.get(any(byte[].class))).thenReturn(null);

        assertNull(cache.get("missing"));
    }

    // Bulk Reads

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

    // Writes

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

    @Test
    void setLargeTtlIsNotNarrowedOrConverted() {
        when(mockJedis.set(any(byte[].class), any(byte[].class), any(SetParams.class))).thenReturn("OK");

        assertTrue(cache.put("large-ttl", "v", Long.MAX_VALUE));

        verify(mockJedis).set(eq(utf8("large-ttl")), any(byte[].class), eq(SetParams.setParams().px(Long.MAX_VALUE)));
    }

    /** A negative liveTime means "no expiration" too: plain SET, never SET ... PX with a negative value. */
    @Test
    void setNegativeTtlUsesPlainSet() {
        when(mockJedis.set(any(byte[].class), any(byte[].class))).thenReturn("OK");

        assertTrue(cache.put("forever", "value", -1));
        assertTrue(cache.put("forever", "value", Long.MIN_VALUE));

        verify(mockJedis, times(2)).set(eq(utf8("forever")), any(byte[].class));
        verify(mockJedis, never()).set(any(byte[].class), any(byte[].class), any(SetParams.class));
    }

    @Test
    void setNullValueEncodesAsEmptyByteArray() {
        when(mockJedis.set(any(byte[].class), any(byte[].class), any(SetParams.class))).thenReturn("OK");

        cache.put("k", null, 60_000);

        verify(mockJedis).set(eq(utf8("k")), eq(new byte[0]), any(SetParams.class));
    }

    @Test
    void setReturnsFalseWhenServerDoesNotSayOK() {
        when(mockJedis.set(any(byte[].class), any(byte[].class), any(SetParams.class))).thenReturn("NOT_OK");

        assertFalse(cache.put("k", "v", 60_000));
    }

    /**
     * Regression test for the TTL-coarsening bug: {@code liveTime} was previously converted to whole
     * seconds ({@code SET ... EX}, rounding up), so a 300 ms TTL silently became a full second —
     * over 3x longer than requested for sub-second entries. The TTL is now passed through with
     * millisecond precision ({@code SET ... PX}).
     */
    @Test
    void setSubSecondTtlHonoredWithMillisecondPrecision() {
        when(mockJedis.set(any(byte[].class), any(byte[].class), any(SetParams.class))).thenReturn("OK");

        assertTrue(cache.put("short-lived", "v", 300));

        verify(mockJedis).set(eq(utf8("short-lived")), any(byte[].class), eq(SetParams.setParams().px(300L)));
    }

    // Removals

    @Test
    void deleteCallsDelAndReturnsTrue() {
        when(mockJedis.del(any(byte[].class))).thenReturn(1L);

        assertTrue(cache.remove("k"));
        verify(mockJedis).del(utf8("k"));
    }

    // Validation And Failures

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

    @Test
    void malformedUnicodeKeysAreRejectedBeforeAnyCommand() {
        for (final String key : List.of("\uD800", "\uDC00", "prefix:\uD800suffix", "prefix:\uDC00suffix", "\uD800\uD800", "\uDC00\uD800")) {
            assertThrows(IllegalArgumentException.class, () -> cache.get(key));
            assertThrows(IllegalArgumentException.class, () -> cache.put(key, "value", 0));
            assertThrows(IllegalArgumentException.class, () -> cache.remove(key));
            assertThrows(IllegalArgumentException.class, () -> cache.incr(key));
            assertThrows(IllegalArgumentException.class, () -> cache.incr(key, 1));
            assertThrows(IllegalArgumentException.class, () -> cache.decr(key));
            assertThrows(IllegalArgumentException.class, () -> cache.decr(key, 1));
            assertThrows(IllegalArgumentException.class, () -> cache.getBulk("valid", key));
            assertThrows(IllegalArgumentException.class, () -> cache.getBulk(List.of("valid", key)));
        }

        Mockito.verifyNoInteractions(mockJedis);
    }

    @Test
    void nullKeyIsRejectedWithIllegalArgumentExceptionBeforeAnyCommand() {
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

    // Other Behavior

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

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
