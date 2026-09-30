/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.builders.WriteBehindConfigurationBuilder;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoadingException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.cache.Ehcache;

@Tag("2025")
public class EhcacheTest {

    private static org.ehcache.Cache<String, String> newUnderlyingCache(final CacheManager cacheManager) {
        return cacheManager.createCache("c" + System.nanoTime(),
                CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class, ResourcePoolsBuilder.heap(100)));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMissingEntriesAreLoadedByDirectAndOptionalReads() throws Exception {
        final CacheLoaderWriter<String, String> loader = mock(CacheLoaderWriter.class);
        when(loader.load("direct")).thenReturn("loaded-direct");
        when(loader.load("optional")).thenReturn("loaded-optional");

        try (CacheManager manager = CacheManagerBuilder.newCacheManagerBuilder().build(true)) {
            final org.ehcache.Cache<String, String> underlying = manager.createCache("readThrough",
                    CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class, ResourcePoolsBuilder.heap(10))
                            .withLoaderWriter(loader));
            final Ehcache<String, String> wrapper = new Ehcache<>(underlying);
            try {
                assertFalse(underlying.containsKey("direct"));
                assertFalse(underlying.containsKey("optional"));

                assertEquals("loaded-direct", wrapper.getOrNull("direct"));
                assertEquals("loaded-optional", wrapper.get("optional").orElse(null));
                assertTrue(underlying.containsKey("direct"));
                assertTrue(underlying.containsKey("optional"));
                verify(loader).load("direct");
                verify(loader).load("optional");
            } finally {
                wrapper.close();
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLoaderFailuresPropagateThroughDirectAndOptionalReads() throws Exception {
        final CacheLoaderWriter<String, String> loader = mock(CacheLoaderWriter.class);
        final Exception failure = new Exception("loader unavailable");
        when(loader.load("direct")).thenThrow(failure);
        when(loader.load("optional")).thenThrow(failure);

        try (CacheManager manager = CacheManagerBuilder.newCacheManagerBuilder().build(true)) {
            final org.ehcache.Cache<String, String> underlying = manager.createCache("failedReadThrough",
                    CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class, ResourcePoolsBuilder.heap(10))
                            .withLoaderWriter(loader));
            final Ehcache<String, String> wrapper = new Ehcache<>(underlying);
            try {
                assertSame(failure, assertThrows(CacheLoadingException.class, () -> wrapper.getOrNull("direct")).getCause());
                assertSame(failure, assertThrows(CacheLoadingException.class, () -> wrapper.get("optional")).getCause());
                assertFalse(underlying.containsKey("direct"));
                assertFalse(underlying.containsKey("optional"));
            } finally {
                wrapper.close();
            }
        }
    }

    /**
     * Regression test for the Ehcache.close() bug.
     *
     * <p>The Javadoc of {@link Ehcache#close()} explicitly states: "This method only marks the
     * wrapper as closed; it does not close or dispose the underlying Ehcache instance. The
     * underlying cache manager is responsible for managing the lifecycle of Ehcache instances."
     *
     * <p>Before the fix, close() called {@code cacheImpl.clear()}, destroying all entries of the
     * underlying (externally-owned, possibly shared) Ehcache instance. This test verifies the
     * underlying cache data survives closing the wrapper.
     */
    @Test
    public void testCloseDoesNotClearUnderlyingCache() {
        final CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final org.ehcache.Cache<String, String> underlying = newUnderlyingCache(cacheManager);

            final Ehcache<String, String> wrapper = new Ehcache<>(underlying);
            wrapper.put("k1", "v1", 0, 0);
            wrapper.put("k2", "v2", 0, 0);

            assertEquals("v1", underlying.get("k1"));

            wrapper.close();
            assertTrue(wrapper.isClosed());

            // The underlying cache (owned by the CacheManager, not the wrapper) must retain its data.
            assertEquals("v1", underlying.get("k1"));
            assertEquals("v2", underlying.get("k2"));
        } finally {
            cacheManager.close();
        }
    }

    /**
     * Sanity check: a second close() is idempotent and still does not clear the underlying cache.
     */
    @Test
    public void testCloseIsIdempotentAndNonDestructive() {
        final CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final org.ehcache.Cache<String, String> underlying = newUnderlyingCache(cacheManager);

            final Ehcache<String, String> wrapper = new Ehcache<>(underlying);
            wrapper.put("a", "1", 0, 0);

            wrapper.close();
            wrapper.close(); // idempotent, no exception

            assertTrue(wrapper.isClosed());
            assertEquals("1", underlying.get("a"));
        } finally {
            cacheManager.close();
        }
    }

    /**
     * Confirms clear() still works (it must clear), distinguishing intended clear() from close().
     */
    @Test
    public void testClearStillClears() {
        final CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final org.ehcache.Cache<String, String> underlying = newUnderlyingCache(cacheManager);

            final Ehcache<String, String> wrapper = new Ehcache<>(underlying);
            wrapper.put("x", "y", 0, 0);
            assertEquals("y", underlying.get("x"));

            wrapper.clear();

            assertFalse(wrapper.isClosed());
            assertEquals(null, underlying.get("x"));
        } finally {
            cacheManager.close();
        }
    }

    @Test
    public void testConstructor_EdgeCase_NullCache() {
        assertThrows(IllegalArgumentException.class, () -> new Ehcache<String, String>(null));
    }

    @Test
    public void testGetOrNull() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            wrapper.put("k", "v", 0, 0);
            assertEquals("v", wrapper.getOrNull("k"));
            assertNull(wrapper.getOrNull("missing"));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testPut_EdgeCase_NullKey() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            assertThrows(IllegalArgumentException.class, () -> wrapper.put(null, "v", 0, 0));
        } finally {
            cm.close();
        }
    }

    /**
     * Null values are now rejected up-front with {@link IllegalArgumentException} (consistent with the
     * null-key contract), rather than surfacing as an unrelated {@code NullPointerException} from the
     * underlying Ehcache. Applies to both {@code put} and {@code putIfAbsent}.
     */
    @Test
    public void testPut_EdgeCase_NullValue() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            assertThrows(IllegalArgumentException.class, () -> wrapper.put("k", null, 0, 0));
            assertThrows(IllegalArgumentException.class, () -> wrapper.putIfAbsent("k", null));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testRemove() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            wrapper.put("k", "v", 0, 0);
            wrapper.remove("k");
            assertNull(wrapper.getOrNull("k"));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testContainsKey() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            wrapper.put("k", "v", 0, 0);
            assertTrue(wrapper.containsKey("k"));
            assertFalse(wrapper.containsKey("missing"));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testPutIfAbsent() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            assertNull(wrapper.putIfAbsent("k", "v1"));
            assertEquals("v1", wrapper.putIfAbsent("k", "v2"));
            assertEquals("v1", wrapper.getOrNull("k"));
        } finally {
            cm.close();
        }
    }

    /** A zero creation expiry makes putIfAbsent report "no previous mapping" without retaining the value. */
    @Test
    public void testPutIfAbsent_ZeroCreationExpiry_ReturnsNullWithoutStoring() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final org.ehcache.Cache<String, String> underlying = cm.createCache("zeroExpiry",
                    CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class, ResourcePoolsBuilder.heap(10))
                            .withExpiry(org.ehcache.config.builders.ExpiryPolicyBuilder.timeToLiveExpiration(java.time.Duration.ZERO)));
            final Ehcache<String, String> wrapper = new Ehcache<>(underlying);
            assertNull(wrapper.putIfAbsent("k", "v"));
            assertNull(wrapper.getOrNull("k"));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testPutIfAbsent_EdgeCase_NullKey() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            assertThrows(IllegalArgumentException.class, () -> wrapper.putIfAbsent(null, "v"));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testGetAll() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            wrapper.put("a", "1", 0, 0);
            wrapper.put("b", "2", 0, 0);

            final Set<String> keys = new HashSet<>();
            keys.add("a");
            keys.add("b");
            final Map<String, String> got = wrapper.getAll(keys);
            assertEquals("1", got.get("a"));
            assertEquals("2", got.get("b"));
        } finally {
            cm.close();
        }
    }

    /**
     * The heavily documented null-inclusion shape: an absent key appears in the result mapped to
     * {@code null} (so {@code containsKey} on the result is true) rather than being omitted.
     */
    @Test
    public void testGetAll_EdgeCase_AbsentKeyMappedToNull() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            wrapper.put("present", "1", 0, 0);

            final Set<String> keys = new HashSet<>();
            keys.add("present");
            keys.add("missing");
            final Map<String, String> got = wrapper.getAll(keys);

            assertEquals("1", got.get("present"));
            assertTrue(got.containsKey("missing"), "an absent key must appear in the result");
            assertNull(got.get("missing"), "an absent key must be mapped to null");
        } finally {
            cm.close();
        }
    }

    @Test
    public void testGetAll_EdgeCase_NullKeys() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            assertThrows(IllegalArgumentException.class, () -> wrapper.getAll(null));
        } finally {
            cm.close();
        }
    }

    /**
     * Regression test for the bulk-operation null-element validation gap.
     *
     * <p>Before the fix, a non-null key set containing a {@code null} element passed the wrapper's
     * validation and surfaced as Ehcache's internal {@code NullPointerException} — an undocumented
     * exception type inconsistent with the wrapper's IllegalArgumentException-based validation
     * everywhere else. The fix validates elements up front.
     */
    @Test
    public void testGetAll_EdgeCase_NullElementThrowsIAE() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            final Set<String> keys = new HashSet<>();
            keys.add("a");
            keys.add(null);
            assertThrows(IllegalArgumentException.class, () -> wrapper.getAll(keys));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testRemoveAll_EdgeCase_NullElementThrowsIAE() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            wrapper.put("a", "1", 0, 0);
            final Set<String> keys = new HashSet<>();
            keys.add("a");
            keys.add(null);
            assertThrows(IllegalArgumentException.class, () -> wrapper.removeAll(keys));
            // Validation happens up front: nothing was removed.
            assertEquals("1", wrapper.getOrNull("a"));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testPutAll() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            final Map<String, String> entries = new HashMap<>();
            entries.put("a", "1");
            entries.put("b", "2");
            wrapper.putAll(entries);
            assertEquals("1", wrapper.getOrNull("a"));
            assertEquals("2", wrapper.getOrNull("b"));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testPutAll_EdgeCase_NullEntries() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            assertThrows(IllegalArgumentException.class, () -> wrapper.putAll(null));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testPutAll_EdgeCase_NullKeyOrValueThrowsIAE() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));

            final Map<String, String> nullKeyEntries = new HashMap<>();
            nullKeyEntries.put("ok", "1");
            nullKeyEntries.put(null, "2");

            assertThrows(IllegalArgumentException.class, () -> wrapper.putAll(nullKeyEntries));
            assertNull(wrapper.getOrNull("ok"));

            final Map<String, String> nullValueEntries = new HashMap<>();
            nullValueEntries.put("ok", "1");
            nullValueEntries.put("bad", null);

            assertThrows(IllegalArgumentException.class, () -> wrapper.putAll(nullValueEntries));
            assertNull(wrapper.getOrNull("ok"));
        } finally {
            cm.close();
        }
    }

    /**
     * Regression test for the garbled putAll validation messages.
     *
     * <p>{@code N.checkElementNotNull} treats a message longer than 9 characters that contains a
     * space as the complete error message. The former arguments {@code "entries' keys"} /
     * {@code "entries' values"} therefore surfaced verbatim as the whole exception message, which
     * neither says that a {@code null} was found nor which part of the map was at fault.
     */
    @Test
    public void testPutAll_EdgeCase_NullKeyOrValueMessageIsDescriptive() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));

            final Map<String, String> nullKeyEntries = new HashMap<>();
            nullKeyEntries.put(null, "2");
            final IllegalArgumentException keyError = assertThrows(IllegalArgumentException.class, () -> wrapper.putAll(nullKeyEntries));
            assertEquals("null key is found in entries", keyError.getMessage());

            final Map<String, String> nullValueEntries = new HashMap<>();
            nullValueEntries.put("bad", null);
            final IllegalArgumentException valueError = assertThrows(IllegalArgumentException.class, () -> wrapper.putAll(nullValueEntries));
            assertEquals("null value is found in entries", valueError.getMessage());
        } finally {
            cm.close();
        }
    }

    @Test
    public void testRemoveAll() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            wrapper.put("a", "1", 0, 0);
            wrapper.put("b", "2", 0, 0);
            final Set<String> keys = new HashSet<>();
            keys.add("a");
            keys.add("b");
            wrapper.removeAll(keys);
            assertNull(wrapper.getOrNull("a"));
            assertNull(wrapper.getOrNull("b"));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testRemoveAll_EdgeCase_NullKeys() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            assertThrows(IllegalArgumentException.class, () -> wrapper.removeAll(null));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testKeySet_Unsupported() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            assertThrows(UnsupportedOperationException.class, wrapper::keySet);
        } finally {
            cm.close();
        }
    }

    @Test
    public void testSize_Unsupported() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            assertThrows(UnsupportedOperationException.class, wrapper::size);
        } finally {
            cm.close();
        }
    }

    /**
     * Regression coverage for the asymmetric null-key handling defect.
     *
     * <p>Before the fix {@link Ehcache#put(Object, Object, long, long)} and
     * {@code putIfAbsent} rejected null keys with {@code IllegalArgumentException}, but the
     * read-side methods ({@link Ehcache#getOrNull(Object)}, {@link Ehcache#remove(Object)},
     * {@link Ehcache#containsKey(Object)}) delegated straight to Ehcache 3, which raises an
     * unrelated {@code NullPointerException}. The fix harmonises the contract so every key-taking
     * operation rejects nulls with the same {@code IllegalArgumentException}.
     */
    @Test
    public void testGetOrNull_EdgeCase_NullKey() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            assertThrows(IllegalArgumentException.class, () -> wrapper.getOrNull(null));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testRemove_EdgeCase_NullKey() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            assertThrows(IllegalArgumentException.class, () -> wrapper.remove(null));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testContainsKey_EdgeCase_NullKey() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            assertThrows(IllegalArgumentException.class, () -> wrapper.containsKey(null));
        } finally {
            cm.close();
        }
    }

    @Test
    public void testOperations_AfterClose_Throw() {
        final CacheManager cm = CacheManagerBuilder.newCacheManagerBuilder().build(true);
        try {
            final Ehcache<String, String> wrapper = new Ehcache<>(newUnderlyingCache(cm));
            wrapper.close();
            assertThrows(IllegalStateException.class, () -> wrapper.getOrNull("k"));
            assertThrows(IllegalStateException.class, () -> wrapper.put("k", "v", 0, 0));
            assertThrows(IllegalStateException.class, () -> wrapper.remove("k"));
            assertThrows(IllegalStateException.class, () -> wrapper.containsKey("k"));
            assertThrows(IllegalStateException.class, wrapper::clear);
            assertThrows(IllegalStateException.class, () -> wrapper.putIfAbsent("k", "v"));
            assertThrows(IllegalStateException.class, () -> wrapper.getAll(new HashSet<>()));
            assertThrows(IllegalStateException.class, () -> wrapper.putAll(new HashMap<>()));
            assertThrows(IllegalStateException.class, () -> wrapper.removeAll(new HashSet<>()));
        } finally {
            cm.close();
        }
    }

    /**
     * Records each loader/writer callback as one string, e.g. {@code "load m"} or {@code "writeAll [a, b, c]"}.
     * Loads resolve every key to {@code "loaded-" + key}.
     */
    private static final class RecordingWriter implements CacheLoaderWriter<String, String> {
        final List<String> calls = new CopyOnWriteArrayList<>();
        private final CountDownLatch expectedCalls;

        RecordingWriter(final int expectedCallCount) {
            expectedCalls = new CountDownLatch(expectedCallCount);
        }

        private void record(final String call) {
            calls.add(call);
            expectedCalls.countDown();
        }

        @Override
        public String load(final String key) {
            record("load " + key);
            return "loaded-" + key;
        }

        @Override
        public Map<String, String> loadAll(final Iterable<? extends String> keys) {
            final List<String> sortedKeys = new ArrayList<>();
            keys.forEach(sortedKeys::add);
            Collections.sort(sortedKeys);
            record("loadAll " + sortedKeys);

            final Map<String, String> loaded = new HashMap<>();
            sortedKeys.forEach(k -> loaded.put(k, "loaded-" + k));
            return loaded;
        }

        @Override
        public void write(final String key, final String value) {
            record("write " + key);
        }

        @Override
        public void writeAll(final Iterable<? extends Map.Entry<? extends String, ? extends String>> entries) {
            final List<String> keys = new ArrayList<>();
            entries.forEach(e -> keys.add(e.getKey()));
            Collections.sort(keys);
            record("writeAll " + keys);
        }

        @Override
        public void delete(final String key) {
            record("delete " + key);
        }

        @Override
        public void deleteAll(final Iterable<? extends String> keys) {
            final List<String> sortedKeys = new ArrayList<>();
            keys.forEach(sortedKeys::add);
            Collections.sort(sortedKeys);
            record("deleteAll " + sortedKeys);
        }

        /** Waits for the expected callbacks (write-behind delivers them on Ehcache's own threads) and returns them sorted. */
        List<String> awaitSortedCalls() throws InterruptedException {
            assertTrue(expectedCalls.await(10, TimeUnit.SECONDS), () -> "writer callbacks so far: " + calls);
            return sortedCalls();
        }

        /** Returns the callbacks recorded so far, sorted. Loads run synchronously on the caller's thread. */
        List<String> sortedCalls() {
            final List<String> sorted = new ArrayList<>(calls);
            Collections.sort(sorted);
            return sorted;
        }
    }

    private static Map<String, String> entriesOf(final String... keys) {
        final Map<String, String> entries = new HashMap<>();

        for (final String key : keys) {
            entries.put(key, "v-" + key);
        }

        return entries;
    }

    private static List<String> runBulkWritesThroughWriter(final RecordingWriter writer,
            final CacheConfigurationBuilder<String, String> config) throws InterruptedException {
        try (CacheManager manager = CacheManagerBuilder.newCacheManagerBuilder().build(true)) {
            final Ehcache<String, String> wrapper = new Ehcache<>(manager.createCache("writer" + System.nanoTime(), config.withLoaderWriter(writer)));

            try {
                wrapper.putAll(entriesOf("a", "b", "c"));
                wrapper.removeAll(new HashSet<>(Arrays.asList("x", "y", "z"))); // absent keys still reach the writer

                return writer.awaitSortedCalls();
            } finally {
                wrapper.close();
            }
        }
    }

    private static CacheConfigurationBuilder<String, String> heapConfig() {
        return CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class, ResourcePoolsBuilder.heap(100));
    }

    @Test
    public void testBulkOps_WriteThroughWriter_ReceivesOneBulkCallPerKey() throws Exception {
        final RecordingWriter writer = new RecordingWriter(6);

        assertEquals(Arrays.asList("deleteAll [x]", "deleteAll [y]", "deleteAll [z]", "writeAll [a]", "writeAll [b]", "writeAll [c]"),
                runBulkWritesThroughWriter(writer, heapConfig()));
    }

    @Test
    public void testBulkOps_UnbatchedWriteBehindWriter_ReceivesIndividualWritesAndDeletes() throws Exception {
        final RecordingWriter writer = new RecordingWriter(6);

        assertEquals(Arrays.asList("delete x", "delete y", "delete z", "write a", "write b", "write c"),
                runBulkWritesThroughWriter(writer, heapConfig().withService(WriteBehindConfigurationBuilder.newUnBatchedWriteBehindConfiguration())));
    }

    @Test
    public void testBulkOps_BatchedWriteBehindWriter_ReceivesMultiKeyBatches() throws Exception {
        // One stripe and a batch size equal to each call's key count: each batch is submitted as soon as it
        // fills (the one-minute max delay never elapses), so every call's keys arrive in a single callback.
        final RecordingWriter writer = new RecordingWriter(2);

        assertEquals(Arrays.asList("deleteAll [x, y, z]", "writeAll [a, b, c]"),
                runBulkWritesThroughWriter(writer,
                        heapConfig().withService(WriteBehindConfigurationBuilder.newBatchedWriteBehindConfiguration(1, TimeUnit.MINUTES, 3).concurrencyLevel(1))));
    }

    private static Map<String, String> expectedLoadedValues(final String presentKey, final String... missingKeys) {
        final Map<String, String> expected = new HashMap<>();
        expected.put(presentKey, "v-" + presentKey);

        for (final String key : missingKeys) {
            expected.put(key, "loaded-" + key);
        }

        return expected;
    }

    @Test
    public void testGetAll_WithoutWriteBehind_LoaderReceivesOneLoadAllPerMissingKey() throws Exception {
        final RecordingWriter loader = new RecordingWriter(0);

        try (CacheManager manager = CacheManagerBuilder.newCacheManagerBuilder().build(true)) {
            final Ehcache<String, String> wrapper = new Ehcache<>(manager.createCache("readThroughAll", heapConfig().withLoaderWriter(loader)));

            try {
                wrapper.put("a", "v-a");
                loader.calls.clear(); // drop the write-through "write a"

                assertEquals(expectedLoadedValues("a", "m1", "m2"), wrapper.getAll(new HashSet<>(Arrays.asList("a", "m1", "m2"))));
                assertEquals(Arrays.asList("loadAll [m1]", "loadAll [m2]"), loader.sortedCalls());
            } finally {
                wrapper.close();
            }
        }
    }

    @Test
    public void testGetAll_UnbatchedWriteBehind_LoaderReceivesIndividualLoads() throws Exception {
        final RecordingWriter loader = new RecordingWriter(1);

        try (CacheManager manager = CacheManagerBuilder.newCacheManagerBuilder().build(true)) {
            final Ehcache<String, String> wrapper = new Ehcache<>(manager.createCache("writeBehindReads",
                    heapConfig().withLoaderWriter(loader).withService(WriteBehindConfigurationBuilder.newUnBatchedWriteBehindConfiguration())));

            try {
                wrapper.put("a", "v-a");
                assertEquals(Arrays.asList("write a"), loader.awaitSortedCalls()); // the queued write has been delivered
                loader.calls.clear();

                assertEquals(expectedLoadedValues("a", "m1", "m2"), wrapper.getAll(new HashSet<>(Arrays.asList("a", "m1", "m2"))));
                assertEquals(Arrays.asList("load m1", "load m2"), loader.sortedCalls());
            } finally {
                wrapper.close();
            }
        }
    }

    @Test
    public void testGetAll_BatchedWriteBehind_PendingDeleteAnsweredWithoutLoader() throws Exception {
        final RecordingWriter loader = new RecordingWriter(0);

        try (CacheManager manager = CacheManagerBuilder.newCacheManagerBuilder().build(true)) {
            // A batch size of 10 and a one-minute max delay keep the single delete queued for the whole test.
            final Ehcache<String, String> wrapper = new Ehcache<>(manager.createCache("pendingDeleteReads", heapConfig().withLoaderWriter(loader)
                    .withService(WriteBehindConfigurationBuilder.newBatchedWriteBehindConfiguration(1, TimeUnit.MINUTES, 10).concurrencyLevel(1))));

            try {
                wrapper.remove("p");

                final Map<String, String> result = wrapper.getAll(new HashSet<>(Arrays.asList("p", "m")));

                assertTrue(result.containsKey("p"));
                assertNull(result.get("p"));
                assertEquals("loaded-m", result.get("m"));
                assertEquals(Arrays.asList("load m"), loader.sortedCalls()); // no loader call for "p", and no loadAll
            } finally {
                wrapper.close();
            }
        }
    }
}
