/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class OffHeapAllocatorTest {

    private static final int SEGMENT_SIZE = AbstractOffHeapCache.SEGMENT_SIZE;

    // Membership And Views

    @Test
    void sizeClassQueuesAreCreatedOnlyWhenUsed() throws Exception {
        final AbstractOffHeapCache<String, byte[]> cache = newCache(false, 2, SEGMENT_SIZE, null);
        try {
            final Deque<AbstractOffHeapCache.Segment>[] queues = segmentQueues(cache);
            assertEquals(SEGMENT_SIZE / AbstractOffHeapCache.MIN_BLOCK_SIZE, queues.length);
            assertEquals(0, Arrays.stream(queues).filter(Objects::nonNull).count());

            assertTrue(cache.put("small", new byte[1]));
            assertTrue(cache.put("large", new byte[SEGMENT_SIZE]));
            // The single-slot class is full right away, so it still needs no queue.
            assertEquals(1, Arrays.stream(queues).filter(Objects::nonNull).count());
            assertNotNull(queues[0]);

            cache.remove("large"); // freeing the full single-slot segment queues it
            assertNotNull(queues[queues.length - 1]);
            cache.remove("small");
            cache.clear();
            assertTrue(cache.stats().occupiedSlots().isEmpty());
            assertTrue(cache.put("large", new byte[SEGMENT_SIZE]));
            assertTrue(cache.put("large-2", new byte[SEGMENT_SIZE]));
        } finally {
            cache.close();
        }
    }

    // Expiration And Capacity

    @Test
    void allocationRollbackReclaimsSegmentsFilledByEarlierChunks() {
        for (final boolean foreignMemory : new boolean[] { false, true }) {
            final Map<String, byte[]> disk = new ConcurrentHashMap<>();
            final AbstractOffHeapCache<String, byte[]> cache = newCache(foreignMemory, 2, SEGMENT_SIZE / 2, store(disk));
            try {
                assertTrue(cache.put("anchor", new byte[] { 23 }));
                final byte[] spilled = new byte[3 * SEGMENT_SIZE / 2];
                spilled[spilled.length - 1] = 31;
                assertTrue(cache.put("spill", spilled));
                assertEquals(1, cache.stats().sizeOnDisk());
                assertFalse(cache.stats().occupiedSlots().containsKey(SEGMENT_SIZE / 2));
                assertTrue(cache.put("different-class", new byte[128]));
                assertEquals(1, cache.stats().sizeOnDisk());
                assertArrayEquals(spilled, cache.getOrNull("spill"));
                assertArrayEquals(new byte[] { 23 }, cache.getOrNull("anchor"));
                assertEquals(192, cache.stats().occupiedMemory());
                assertEquals(0, cache.stats().evictionCount());
            } finally {
                cache.close();
            }
        }
    }

    @Test
    void allocationsDoNotScanPreviouslyFilledSegments() throws Exception {
        for (final boolean foreignMemory : new boolean[] { false, true }) {
            final AbstractOffHeapCache<String, byte[]> cache = newCache(foreignMemory, 8, SEGMENT_SIZE, null);
            try {
                final CountingQueue queue = installCountingQueue(cache, SEGMENT_SIZE);
                final byte[] value = new byte[SEGMENT_SIZE];

                for (int i = 0; i < 8; i++) {
                    assertTrue(cache.put("key-" + i, value));
                }

                // Counting visits gives a deterministic complexity check without a timing threshold:
                // scanning all previously filled segments would visit 0 + 1 + ... + 7 = 28.
                assertTrue(queue.visits <= 8, "allocation repeatedly scanned full segments: " + queue.visits);
                // Whatever the traversal, full segments must leave the vacancy queue or a later
                // allocation has to step over them.
                assertTrue(queue.isEmpty(), "full segments must not stay in the vacancy queue: " + queue.size());
                assertEquals(8, cache.stats().occupiedSlots().get(SEGMENT_SIZE).size());
                assertEquals(8L * SEGMENT_SIZE, cache.stats().occupiedMemory());
            } finally {
                cache.close();
            }
        }
    }

    @Test
    void freedSlotsInFullSegmentsAreReusedAndReported() {
        final int slotSize = SEGMENT_SIZE / 2;
        for (final boolean foreignMemory : new boolean[] { false, true }) {
            final AbstractOffHeapCache<String, byte[]> cache = newCache(foreignMemory, 2, slotSize, null);
            try {
                final byte[] value = new byte[slotSize];
                value[0] = 19;
                assertTrue(cache.put("a", value));
                assertTrue(cache.put("b", value));
                assertTrue(cache.put("c", value));
                assertTrue(cache.put("d", value));
                assertEquals(Map.of(0, 2, 1, 2), cache.stats().occupiedSlots().get(slotSize));

                cache.remove("a");
                assertTrue(cache.put("replacement", value), "a slot released from a full segment must be immediately reusable");
                assertEquals(Map.of(0, 2, 1, 2), cache.stats().occupiedSlots().get(slotSize));
                assertArrayEquals(value, cache.getOrNull("b"));
                assertArrayEquals(value, cache.getOrNull("replacement"));

                cache.remove("b");
                cache.remove("replacement");
                assertEquals(Map.of(0, 0, 1, 2), cache.stats().occupiedSlots().get(slotSize));
                assertTrue(cache.put("small", new byte[] { 37 }));
                assertEquals(Map.of(64, Map.of(0, 1), slotSize, Map.of(1, 2)), cache.stats().occupiedSlots());
                assertArrayEquals(value, cache.getOrNull("c"));
                assertArrayEquals(value, cache.getOrNull("d"));
                assertArrayEquals(new byte[] { 37 }, cache.getOrNull("small"));

                cache.clear();
                assertTrue(cache.stats().occupiedSlots().isEmpty());
                assertEquals(0, cache.stats().occupiedMemory());
                assertTrue(cache.put("after-clear", value));
                assertArrayEquals(value, cache.getOrNull("after-clear"));
                assertEquals(0, cache.stats().evictionCount());
            } finally {
                cache.close();
            }
        }
    }

    /**
     * A stale or duplicate slot handle must not corrupt the allocator, including after its segment
     * was reclaimed (slot size 0). With assertions enabled (the test suite) the release fails
     * loudly before touching any state; otherwise it is ignored and the rest of the handles are
     * still released.
     */
    @Test
    void staleSlotReleaseLeavesAllocatorConsistent() throws Exception {
        final AbstractOffHeapCache<String, byte[]> cache = newCache(false, 2, SEGMENT_SIZE / 2, null);
        try {
            assertTrue(cache.put("stale", new byte[100]));
            assertTrue(cache.put("live", new byte[100]));
            final long staleSlot = slotsOf(cache, "stale")[0];
            final long liveSlot = slotsOf(cache, "live")[0];
            cache.remove("stale");
            assertEquals(Map.of(128, Map.of(0, 1)), cache.stats().occupiedSlots());

            final boolean assertionsEnabled = AbstractOffHeapCache.class.desiredAssertionStatus();
            final long[] handles = { staleSlot, liveSlot };

            if (assertionsEnabled) {
                assertInstanceOf(AssertionError.class, assertThrows(InvocationTargetException.class, () -> releaseSlots(cache, handles)).getCause());
                assertEquals(Map.of(128, Map.of(0, 1)), cache.stats().occupiedSlots(), "a rejected stale release must not change any state");
                assertArrayEquals(new byte[100], cache.getOrNull("live"));
                cache.remove("live");
            } else {
                releaseSlots(cache, handles); // the stale handle is skipped; the live one is released
                assertEquals(Map.of(128, Map.of(0, 0)), cache.stats().occupiedSlots());
            }

            cache.clear(); // reclaims the now empty segment: slot size 0
            assertTrue(cache.stats().occupiedSlots().isEmpty());

            if (assertionsEnabled) {
                assertInstanceOf(AssertionError.class,
                        assertThrows(InvocationTargetException.class, () -> releaseSlots(cache, new long[] { staleSlot })).getCause());
            } else {
                releaseSlots(cache, new long[] { staleSlot });
            }

            assertTrue(cache.stats().occupiedSlots().isEmpty());
            assertTrue(cache.put("large", new byte[SEGMENT_SIZE / 2]));
            assertTrue(cache.put("large-2", new byte[SEGMENT_SIZE / 2]));
            assertTrue(cache.put("small", new byte[100]));
            assertEquals(Map.of(SEGMENT_SIZE / 2, Map.of(0, 2), 128, Map.of(1, 1)), cache.stats().occupiedSlots());
        } finally {
            cache.close();
        }
    }

    // Concurrency

    @Test
    @Timeout(30)
    void concurrentReleaseAndClassReassignmentPreserveLiveEntries() throws Exception {
        for (final boolean foreignMemory : new boolean[] { false, true }) {
            final AbstractOffHeapCache<String, byte[]> cache = newCache(foreignMemory, 2, SEGMENT_SIZE, null);
            try {
                final ExecutorService executor = Executors.newFixedThreadPool(2);
                final CountDownLatch start = new CountDownLatch(1);
                try {
                    final Future<?> first = executor.submit(() -> exerciseClasses(cache, "first", start));
                    final Future<?> second = executor.submit(() -> exerciseClasses(cache, "second", start));
                    start.countDown();
                    first.get(10, TimeUnit.SECONDS);
                    second.get(10, TimeUnit.SECONDS);
                } finally {
                    executor.shutdownNow();
                    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
                }

                cache.clear();
                assertEquals(0, cache.size());
                assertEquals(0, cache.stats().occupiedMemory());
                assertEquals(0, cache.stats().evictionCount());
                assertTrue(cache.stats().occupiedSlots().isEmpty());
            } finally {
                cache.close();
            }
        }
    }

    // Other Behavior

    @Test
    void singleSlotSegmentsCanBeReassignedAfterRemoval() {
        for (final boolean foreignMemory : new boolean[] { false, true }) {
            final AbstractOffHeapCache<String, byte[]> cache = newCache(foreignMemory, 1, SEGMENT_SIZE, null);
            try {
                final byte[] large = new byte[SEGMENT_SIZE];
                large[large.length - 1] = 43;
                assertTrue(cache.put("large", large));
                cache.remove("large");
                assertTrue(cache.put("small", new byte[] { 11 }));
                assertEquals(Map.of(64, Map.of(0, 1)), cache.stats().occupiedSlots());
                cache.remove("small");
                assertTrue(cache.put("large-again", large));
                assertArrayEquals(large, cache.getOrNull("large-again"));
                assertEquals(Map.of(SEGMENT_SIZE, Map.of(0, 1)), cache.stats().occupiedSlots());
                assertEquals(0, cache.stats().evictionCount());
            } finally {
                cache.close();
            }
        }
    }

    private static void exerciseClasses(final AbstractOffHeapCache<String, byte[]> cache, final String key, final CountDownLatch start) {
        try {
            assertTrue(start.await(5, TimeUnit.SECONDS));
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }

        final byte[][] values = { new byte[1], new byte[SEGMENT_SIZE / 2 + 64], new byte[128], new byte[SEGMENT_SIZE / 2 + 128] };
        for (int i = 0; i < values.length; i++) {
            values[i][0] = (byte) (key.hashCode() + i);
        }
        for (int i = 0; i < 100; i++) {
            final byte[] value = values[i % values.length];
            assertTrue(cache.put(key, value), "at most two concurrent values must fit in two segments");
            assertArrayEquals(value, cache.getOrNull(key));
            cache.remove(key);
        }
    }

    private static AbstractOffHeapCache<String, byte[]> newCache(final boolean foreignMemory, final int capacityInMb, final int maxBlockSize,
            final OffHeapStore<String> store) {
        return foreignMemory
                ? ForeignMemoryOffHeapCache.<String, byte[]> builder().capacityInMB(capacityInMb).maxBlockSizeInBytes(maxBlockSize).offHeapStore(store).build()
                : OffHeapCache.<String, byte[]> builder().capacityInMB(capacityInMb).maxBlockSizeInBytes(maxBlockSize).offHeapStore(store).build();
    }

    private static OffHeapStore<String> store(final Map<String, byte[]> data) {
        return new OffHeapStore<>() {
            @Override
            public byte[] get(final String key) {
                return data.get(key);
            }

            @Override
            public boolean put(final String key, final byte[] value) {
                data.put(key, value);
                return true;
            }

            @Override
            public boolean remove(final String key) {
                return data.remove(key) != null;
            }
        };
    }

    private static CountingQueue installCountingQueue(final AbstractOffHeapCache<?, ?> cache, final int slotSize) throws Exception {
        final CountingQueue queue = new CountingQueue();
        segmentQueues(cache)[slotSize / AbstractOffHeapCache.MIN_BLOCK_SIZE - 1] = queue;
        return queue;
    }

    @SuppressWarnings("unchecked")
    private static Deque<AbstractOffHeapCache.Segment>[] segmentQueues(final AbstractOffHeapCache<?, ?> cache) throws Exception {
        final Field field = AbstractOffHeapCache.class.getDeclaredField("segmentQueues");
        field.setAccessible(true);
        return (Deque<AbstractOffHeapCache.Segment>[]) field.get(cache);
    }

    private static long[] slotsOf(final AbstractOffHeapCache<String, ?> cache, final String key) throws Exception {
        final Field entriesField = AbstractOffHeapCache.class.getDeclaredField("entries");
        entriesField.setAccessible(true);
        final Object entry = ((Map<?, ?>) entriesField.get(cache)).get(key);
        final Field slotsField = entry.getClass().getDeclaredField("slots");
        slotsField.setAccessible(true);
        return ((long[]) slotsField.get(entry)).clone();
    }

    private static void releaseSlots(final AbstractOffHeapCache<?, ?> cache, final long[] slots) throws Exception {
        final Method method = AbstractOffHeapCache.class.getDeclaredMethod("releaseSlots", long[].class);
        method.setAccessible(true);
        method.invoke(cache, (Object) slots);
    }

    /**
     * Counts element visits through every ArrayDeque traversal route. ArrayDeque implements
     * forEach, removeIf, bulk removal, toArray, contains and the descending iterator without going
     * through iterator(), so each is counted separately. Searches count the whole queue as an
     * upper bound.
     */
    private static final class CountingQueue extends ArrayDeque<AbstractOffHeapCache.Segment> {
        int visits;

        @Override
        public Iterator<AbstractOffHeapCache.Segment> iterator() {
            return counting(super.iterator());
        }

        @Override
        public Iterator<AbstractOffHeapCache.Segment> descendingIterator() {
            return counting(super.descendingIterator());
        }

        @Override
        public Spliterator<AbstractOffHeapCache.Segment> spliterator() {
            visits += size();
            return super.spliterator();
        }

        @Override
        public void forEach(final Consumer<? super AbstractOffHeapCache.Segment> action) {
            super.forEach(segment -> {
                visits++;
                action.accept(segment);
            });
        }

        @Override
        public boolean removeIf(final Predicate<? super AbstractOffHeapCache.Segment> filter) {
            return super.removeIf(segment -> {
                visits++;
                return filter.test(segment);
            });
        }

        @Override
        public boolean removeAll(final Collection<?> c) {
            visits += size();
            return super.removeAll(c);
        }

        @Override
        public boolean retainAll(final Collection<?> c) {
            visits += size();
            return super.retainAll(c);
        }

        @Override
        public boolean contains(final Object o) {
            visits += size();
            return super.contains(o);
        }

        @Override
        public boolean removeFirstOccurrence(final Object o) {
            visits += size();
            return super.removeFirstOccurrence(o);
        }

        @Override
        public boolean removeLastOccurrence(final Object o) {
            visits += size();
            return super.removeLastOccurrence(o);
        }

        @Override
        public Object[] toArray() {
            visits += size();
            return super.toArray();
        }

        @Override
        public <T> T[] toArray(final T[] a) {
            visits += size();
            return super.toArray(a);
        }

        private Iterator<AbstractOffHeapCache.Segment> counting(final Iterator<AbstractOffHeapCache.Segment> delegate) {
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return delegate.hasNext();
                }

                @Override
                public AbstractOffHeapCache.Segment next() {
                    visits++;
                    return delegate.next();
                }

                @Override
                public void remove() {
                    delegate.remove();
                }
            };
        }
    }
}
