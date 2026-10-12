package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import redis.clients.jedis.UnifiedJedis;

/** Service-free regression coverage for Redis bulk-read snapshots and their temporary memory use. */
@Tag("base-test")
public class JedisBulkMemoryUnitTest {

    @Test
    public void duplicateCollectionDoesNotAllocateStorageForEveryOccurrence() {
        final RecordingClient cache = new RecordingClient();
        try {
            assertDuplicateAllocationBound(cache, Collections.nCopies(200_000, "present"), null);
        } finally {
            cache.disconnect();
        }
    }

    @Test
    public void duplicateArrayDoesNotAllocateStorageForEveryOccurrence() {
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
    public void duplicateKeysKeepFirstLookupOrderAndMissingEntriesStayAbsent() {
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

    @Test
    public void collectionMutationDuringFirstGetCannotChangeLaterKeys() {
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
    public void invalidKeyAfterManyDuplicatesIsRejectedBeforeAnyBackendRead() {
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
        private Runnable onRead = () -> { };

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
}
