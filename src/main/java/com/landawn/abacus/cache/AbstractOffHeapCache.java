/*
 * Copyright (c) 2025, Haiyang Li.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.landawn.abacus.cache;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import com.landawn.abacus.cache.OffHeapCacheStats.MinMaxAvg;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.parser.Parser;
import com.landawn.abacus.parser.ParserFactory;
import com.landawn.abacus.pool.AbstractPoolable;
import com.landawn.abacus.pool.ActivityPrint;
import com.landawn.abacus.pool.EvictionPolicy;
import com.landawn.abacus.pool.KeyedObjectPool;
import com.landawn.abacus.pool.PoolFactory;
import com.landawn.abacus.pool.PoolStats;
import com.landawn.abacus.pool.Poolable.Caller;
import com.landawn.abacus.type.ByteBufferType;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ByteArrayOutputStream;
import com.landawn.abacus.util.Comparators;
import com.landawn.abacus.util.ExceptionUtil;
import com.landawn.abacus.util.Suppliers;
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.MoreExecutors;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Numbers;
import com.landawn.abacus.util.Objectory;
import com.landawn.abacus.util.Tuple;
import com.landawn.abacus.util.Tuple.Tuple3;
import com.landawn.abacus.util.function.TriFunction;
import com.landawn.abacus.util.function.TriPredicate;
import com.landawn.abacus.util.stream.Collectors;
import com.landawn.abacus.util.stream.Stream;

//--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED

/**
 * Abstract base class for off-heap cache implementations providing the core memory management logic.
 * This class handles memory segmentation, allocation, eviction, and optional disk spillover.
 * It serves as the foundation for both Unsafe-based and Foreign Memory API-based implementations.
 * 
 * <br><br>
 * Architecture overview:
 * <ul>
 * <li>Memory is divided into fixed-size segments (default 1MB each)</li>
 * <li>Each segment can be subdivided into slots of various sizes</li>
 * <li>Slot sizes are multiples of MIN_BLOCK_SIZE (64 bytes)</li>
 * <li>Large objects span multiple slots if needed</li>
 * <li>Automatic defragmentation when memory becomes fragmented</li>
 * <li>Optional disk spillover via OffHeapStore interface</li>
 * </ul>
 * 
 * <br>
 * Memory management strategy:
 * <ul>
 * <li>Best-fit allocation within size-based segment queues</li>
 * <li>Lazy segment allocation - segments allocated only when needed</li>
 * <li>Automatic eviction based on LRU when capacity reached</li>
 * <li>Vacating process triggers defragmentation</li>
 * <li>Statistics tracking for monitoring and optimization</li>
 * </ul>
 *
 * @param <K> the key type
 * @param <V> the value type
 * @see OffHeapCache
 * @see OffHeapCache25
 */
abstract class AbstractOffHeapCache<K, V> extends AbstractCache<K, V> {

    /**
     * Default factor (0.2) that triggers memory defragmentation when this fraction
     * of memory becomes fragmented.
     */
    static final float DEFAULT_VACATING_FACTOR = 0.2f;

    /**
     * Default parser for serialization - uses Kryo if available, otherwise JSON.
     */
    static final Parser<?, ?> parser = ParserFactory.isKryoAvailable() ? ParserFactory.createKryoParser() : ParserFactory.createJSONParser();

    /**
     * Default serializer using the configured parser.
     */
    static final BiConsumer<Object, ByteArrayOutputStream> SERIALIZER = parser::serialize;

    /**
     * Default deserializer with special handling for byte arrays and ByteBuffers.
     */
    static final BiFunction<byte[], Type<?>, Object> DESERIALIZER = (bytes, type) -> {
        // it's destroyed after read from memory. dirty data may be read.
        if (type.isPrimitiveByteArray()) {
            return bytes;
        } else if (type.isByteBuffer()) {
            return ByteBufferType.valueOf(bytes);
        } else {
            return parser.deserialize(new ByteArrayInputStream(bytes), type.clazz());
        }
    };

    /**
     * Size of each memory segment in bytes (1MB).
     */
    static final int SEGMENT_SIZE = 1024 * 1024; // (int) N.ONE_MB;

    /**
     * Minimum slot size in bytes. All slot sizes are multiples of this value.
     */
    static final int MIN_BLOCK_SIZE = 64;

    /**
     * Default maximum size for a single memory block (8KB).
     */
    static final int DEFAULT_MAX_BLOCK_SIZE = 8192; // 8K

    /**
     * Shared scheduled executor for eviction tasks across all off-heap cache instances.
     */
    static final ScheduledExecutorService scheduledExecutor;

    static {
        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(IOUtil.CPU_CORES);
        // executor.setRemoveOnCancelPolicy(true);
        scheduledExecutor = MoreExecutors.getExitingScheduledExecutorService(executor);
    }

    // Statistics tracking
    final AtomicLong totalOccupiedMemorySize = new AtomicLong();
    final AtomicLong totalDataSize = new AtomicLong();
    final AtomicLong dataSizeOnDisk = new AtomicLong();
    final AtomicLong sizeOnDisk = new AtomicLong();
    final AtomicLong writeCountToDisk = new AtomicLong();
    final AtomicLong readCountFromDisk = new AtomicLong();
    final AtomicLong evictionCountFromDisk = new AtomicLong();

    final Logger logger;

    // Core memory management fields
    final long _capacityInBytes;
    final long _startPtr;
    final int _arrayOffset;
    final int _maxBlockSize;

    private final Segment[] _segments;
    private final BitSet _segmentBitSet = new BitSet();
    private final Map<Integer, Deque<Segment>> _segmentQueueMap = new ConcurrentHashMap<>();
    private final Deque<Segment>[] _segmentQueues;

    private final AsyncExecutor _asyncExecutor = new AsyncExecutor();
    private final AtomicInteger _activeVacationTaskCount = new AtomicInteger();
    private final KeyedObjectPool<K, Wrapper<V>> _pool;

    private ScheduledFuture<?> scheduleFuture;
    private Thread shutdownHook;

    // Configuration and extension points
    final BiConsumer<? super V, ByteArrayOutputStream> serializer;
    final BiFunction<byte[], Type<V>, ? extends V> deserializer;
    final OffHeapStore<K> offHeapStore;
    final boolean statsTimeOnDisk;
    final TriPredicate<ActivityPrint, Integer, Long> testerForLoadingItemFromDiskToMemory;
    final TriFunction<K, V, Integer, Integer> storeSelector;
    final LongSummaryStatistics totalWriteToDiskTimeStats = new LongSummaryStatistics();
    final LongSummaryStatistics totalReadFromDiskTimeStats = new LongSummaryStatistics();

    /**
     * Constructs an AbstractOffHeapCache with the specified configuration.
     * This constructor initializes the memory segments, object pool, and eviction scheduling.
     *
     * @param capacityInMB total off-heap memory capacity in megabytes
     * @param maxBlockSize maximum size of a single memory block in bytes
     * @param evictDelay delay between eviction runs in milliseconds
     * @param defaultLiveTime default TTL for entries in milliseconds
     * @param defaultMaxIdleTime default max idle time for entries in milliseconds
     * @param vacatingFactor fraction of fragmented memory that triggers defragmentation
     * @param arrayOffset array base offset for memory operations
     * @param serializer custom serializer or null for default
     * @param deserializer custom deserializer or null for default
     * @param offHeapStore optional disk store for spillover
     * @param statsTimeOnDisk whether to collect disk I/O timing statistics
     * @param testerForLoadingItemFromDiskToMemory predicate for loading from disk to memory
     * @param storeSelector function to determine storage location (0=default, 1=memory only, 2=disk only)
     * @param logger logger instance for this cache
     */
    @SuppressWarnings("rawtypes")
    protected AbstractOffHeapCache(final int capacityInMB, final int maxBlockSize, final long evictDelay, final long defaultLiveTime,
            final long defaultMaxIdleTime, final float vacatingFactor, final int arrayOffset, final BiConsumer<? super V, ByteArrayOutputStream> serializer,
            final BiFunction<byte[], Type<V>, ? extends V> deserializer, final OffHeapStore<K> offHeapStore, final boolean statsTimeOnDisk,
            final TriPredicate<ActivityPrint, Integer, Long> testerForLoadingItemFromDiskToMemory, final TriFunction<K, V, Integer, Integer> storeSelector,
            final Logger logger) {
        super(defaultLiveTime, defaultMaxIdleTime);

        N.checkArgument(maxBlockSize >= 1024 && maxBlockSize <= SEGMENT_SIZE, "The Maximum Block size can't be: {}. It must be >= 1024 and <= {}", maxBlockSize,
                SEGMENT_SIZE);

        this.logger = logger;

        _arrayOffset = arrayOffset;

        _capacityInBytes = capacityInMB * (1024L * 1024L); // N.ONE_MB;
        _maxBlockSize = maxBlockSize % MIN_BLOCK_SIZE == 0 ? maxBlockSize : (maxBlockSize / MIN_BLOCK_SIZE + 1) * MIN_BLOCK_SIZE;
        _segmentQueues = new Deque[_maxBlockSize / MIN_BLOCK_SIZE];

        // ByteBuffer.allocateDirect((int) capacity);
        _startPtr = allocate(_capacityInBytes);

        _segments = new Segment[(int) (_capacityInBytes / SEGMENT_SIZE)];

        for (int i = 0, len = _segments.length; i < len; i++) {
            _segments[i] = new Segment(_startPtr + (long) i * SEGMENT_SIZE, i);
        }

        _pool = PoolFactory.createKeyedObjectPool((int) (_capacityInBytes / MIN_BLOCK_SIZE), evictDelay, EvictionPolicy.LAST_ACCESS_TIME, true,
                vacatingFactor == 0f ? DEFAULT_VACATING_FACTOR : vacatingFactor);

        this.serializer = serializer == null ? (BiConsumer<V, ByteArrayOutputStream>) SERIALIZER : serializer;
        this.deserializer = deserializer == null ? (BiFunction) DESERIALIZER : deserializer;
        this.offHeapStore = offHeapStore;
        this.statsTimeOnDisk = statsTimeOnDisk;
        this.testerForLoadingItemFromDiskToMemory = testerForLoadingItemFromDiskToMemory;
        this.storeSelector = storeSelector;

        if (evictDelay > 0) {
            final Runnable evictTask = () -> {
                // Evict from the pool
                try {
                    evict();
                } catch (final Exception e) {
                    // ignore

                    if (logger.isWarnEnabled()) {
                        logger.warn(ExceptionUtil.getErrorMessage(e));
                    }
                }
            };

            scheduleFuture = scheduledExecutor.scheduleWithFixedDelay(evictTask, evictDelay, evictDelay, TimeUnit.MILLISECONDS);
        }

        shutdownHook = new Thread(() -> {
            logger.warn("Starting to shutdown task in OffHeapCache");

            try {
                close();
            } finally {
                logger.warn("Completed to shutdown task in OffHeapCache");
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    /**
     * Allocates the specified amount of off-heap memory.
     * Implementation depends on the specific memory management approach
     * (Unsafe vs Foreign Memory API).
     *
     * @param capacityInBytes number of bytes to allocate
     * @return the base address of the allocated memory
     */
    protected abstract long allocate(long capacityInBytes);

    /**
     * Deallocates all off-heap memory used by this cache.
     * Called during shutdown to release native memory resources.
     */
    protected abstract void deallocate();

    /**
     * Copies data from a byte array to off-heap memory.
     *
     * @param bytes source byte array
     * @param srcOffset offset in the source array
     * @param startPtr destination memory address
     * @param len number of bytes to copy
     */
    protected abstract void copyToMemory(byte[] bytes, int srcOffset, long startPtr, int len);

    /**
     * Copies data from off-heap memory to a byte array.
     *
     * @param startPtr source memory address
     * @param bytes destination byte array
     * @param destOffset offset in the destination array
     * @param len number of bytes to copy
     */
    protected abstract void copyFromMemory(final long startPtr, final byte[] bytes, final int destOffset, final int len);

    /**
     * Retrieves a value from the cache by its key.
     * Handles both memory and disk storage, with automatic promotion from disk
     * to memory based on access patterns if configured.
     *
     * @param k the key
     * @return the cached value, or null if not found
     */
    @Override
    public V gett(final K k) {
        final Wrapper<V> w = _pool.get(k);

        // Due to error:  cannot be safely cast to StoreWrapper/MultiSlotsWrapper
        if (w != null && StoreWrapper.class.equals(w.getClass())) {
            final StoreWrapper storeWrapper = (StoreWrapper) w;

            readCountFromDisk.incrementAndGet();

            if (statsTimeOnDisk || testerForLoadingItemFromDiskToMemory != null) {
                final long startTime = System.currentTimeMillis();

                try {
                    return w.read();
                } finally {
                    final long elapsedTime = System.currentTimeMillis() - startTime;

                    if (statsTimeOnDisk) {
                        synchronized (totalReadFromDiskTimeStats) {
                            totalReadFromDiskTimeStats.accept(elapsedTime);
                        }
                    }

                    if (testerForLoadingItemFromDiskToMemory != null
                            && testerForLoadingItemFromDiskToMemory.test(storeWrapper.activityPrint(), storeWrapper.size, elapsedTime)) {

                        final ActivityPrint activityPrint = storeWrapper.activityPrint();
                        final long maxIdleTime = activityPrint.getMaxIdleTime();
                        final long liveTime = activityPrint.getLiveTime() - (System.currentTimeMillis() - activityPrint.getCreatedTime());

                        final int size = storeWrapper.size;
                        final byte[] bytes = liveTime > 0 ? offHeapStore.get(k) : null;

                        if (bytes != null && bytes.length == size) {
                            Slot slot = null;
                            List<Slot> slots = null;
                            int occupiedMemory = 0;
                            Wrapper<V> slotWrapper = null;

                            try {
                                if (size <= _maxBlockSize) {
                                    slot = getAvailableSlot(size);

                                    if (slot != null) {
                                        final long slotStartPtr = slot.segment.segmentStartPtr + (long) slot.indexOfSlot * slot.segment.sizeOfSlot;

                                        copyToMemory(bytes, _arrayOffset, slotStartPtr, size);

                                        occupiedMemory = slot.segment.sizeOfSlot;

                                        slotWrapper = new SlotWrapper(storeWrapper.type, liveTime, maxIdleTime, size, slot, slotStartPtr);
                                    }
                                } else {
                                    slots = new ArrayList<>(size / _maxBlockSize + (size % _maxBlockSize == 0 ? 0 : 1));
                                    int copiedSize = 0;
                                    int srcOffset = _arrayOffset;

                                    while (copiedSize < size) {
                                        final int sizeToCopy = Math.min(size - copiedSize, _maxBlockSize);
                                        slot = getAvailableSlot(sizeToCopy);

                                        if (slot == null) {
                                            break;
                                        }

                                        final long startPtr = slot.segment.segmentStartPtr + (long) slot.indexOfSlot * slot.segment.sizeOfSlot;

                                        copyToMemory(bytes, srcOffset, startPtr, sizeToCopy);

                                        srcOffset += sizeToCopy;
                                        copiedSize += sizeToCopy;

                                        occupiedMemory += slot.segment.sizeOfSlot;
                                        slots.add(slot);
                                    }

                                    if (copiedSize == size) {
                                        slotWrapper = new MultiSlotsWrapper(storeWrapper.type, liveTime, maxIdleTime, size, slots);
                                    }
                                }
                            } finally {
                                if (slotWrapper == null) {
                                    if (slot != null) {
                                        slot.release();
                                    }

                                    if (slots != null) {
                                        for (final Slot e : slots) {
                                            e.release();
                                        }
                                    }
                                }
                            }

                            if (slotWrapper != null) {
                                boolean result = false;

                                try {
                                    remove(k);

                                    totalOccupiedMemorySize.addAndGet(occupiedMemory);

                                    totalDataSize.addAndGet(size);

                                    result = _pool.put(k, slotWrapper);
                                } finally {
                                    if (!result) {
                                        slotWrapper.destroy(Caller.PUT_ADD_FAILURE);
                                    }

                                }
                            }
                        }
                    }
                }
            } else {
                return w.read();
            }
        } else {
            return w == null ? null : w.read();
        }
    }

    /**
     * Stores a key-value pair in the cache with specified expiration settings.
     * The value is serialized and stored either in memory, on disk, or both
     * based on size and configuration.
     *
     * @param k the key
     * @param v the value to cache
     * @param liveTime time-to-live in milliseconds
     * @param maxIdleTime maximum idle time in milliseconds
     * @return true if the operation was successful
     */
    @Override
    public boolean put(final K k, final V v, final long liveTime, final long maxIdleTime) {
        final Type<V> type = N.typeOf(v.getClass());
        Wrapper<V> w = null;

        // final byte[] bytes = parser.serialize(v).getBytes();
        final long startTime = statsTimeOnDisk ? System.currentTimeMillis() : 0;
        ByteArrayOutputStream os = null;
        byte[] bytes = null;
        int size = 0;
        long occupiedMemory = 0;

        if (type.isPrimitiveByteArray()) {
            bytes = (byte[]) v;
            size = bytes.length;
        } else if (type.isByteBuffer()) {
            bytes = ByteBufferType.byteArrayOf((ByteBuffer) v);
            size = bytes.length;
        } else {
            os = Objectory.createByteArrayOutputStream();
            serializer.accept(v, os);
            bytes = os.array();
            size = os.size();
        }

        final boolean canBeStoredInMemory = storeSelector == null || storeSelector.apply(k, v, size) < 2;
        final boolean canBeStoredToDisk = storeSelector == null || storeSelector.apply(k, v, size) != 1;

        Slot slot = null;
        List<Slot> slots = null;

        try {
            if (size <= _maxBlockSize) {
                slot = canBeStoredInMemory ? getAvailableSlot(size) : null;

                if (slot != null) {
                    final long slotStartPtr = slot.segment.segmentStartPtr + (long) slot.indexOfSlot * slot.segment.sizeOfSlot;

                    copyToMemory(bytes, _arrayOffset, slotStartPtr, size);

                    occupiedMemory = slot.segment.sizeOfSlot;

                    w = new SlotWrapper(type, liveTime, maxIdleTime, size, slot, slotStartPtr);
                } else if (canBeStoredToDisk && offHeapStore != null) {
                    w = putToDisk(k, liveTime, maxIdleTime, type, bytes, size);
                }
            } else {
                int copiedSize = 0;

                if (canBeStoredInMemory) {
                    slots = new ArrayList<>(size / _maxBlockSize + (size % _maxBlockSize == 0 ? 0 : 1));
                    int srcOffset = _arrayOffset;

                    while (copiedSize < size) {
                        final int sizeToCopy = Math.min(size - copiedSize, _maxBlockSize);
                        slot = getAvailableSlot(sizeToCopy);

                        if (slot == null) {
                            break;
                        }

                        final long startPtr = slot.segment.segmentStartPtr + (long) slot.indexOfSlot * slot.segment.sizeOfSlot;

                        copyToMemory(bytes, srcOffset, startPtr, sizeToCopy);

                        srcOffset += sizeToCopy;
                        copiedSize += sizeToCopy;

                        occupiedMemory += slot.segment.sizeOfSlot;
                        slots.add(slot);
                    }
                }

                if (copiedSize == size) {
                    w = new MultiSlotsWrapper(type, liveTime, maxIdleTime, size, slots);
                } else if (canBeStoredToDisk && offHeapStore != null) {
                    w = putToDisk(k, liveTime, maxIdleTime, type, bytes, size);
                }
            }
        } finally {
            Objectory.recycle(os);

            // Due to error:  cannot be safely cast to StoreWrapper/MultiSlotsWrapper
            if (w == null || StoreWrapper.class.equals(w.getClass())) {
                if (slot != null) {
                    slot.release();
                }

                if (slots != null) {
                    for (final Slot e : slots) {
                        e.release();
                    }
                }
            }

            if (w == null) {
                _pool.remove(k);

                vacate();
                return false;
            }
        }

        boolean result = false;
        final Class<?> wcls = w.getClass();

        try {
            // Due to error:  cannot be safely cast to StoreWrapper/MultiSlotsWrapper

            if (SlotWrapper.class.equals(wcls) || MultiSlotsWrapper.class.equals(wcls)) {
                totalOccupiedMemorySize.addAndGet(occupiedMemory);
            }

            totalDataSize.addAndGet(size);

            result = _pool.put(k, w);
        } finally {
            if (!result) {
                w.destroy(Caller.PUT_ADD_FAILURE);
            }

            // Due to error:  cannot be safely cast to StoreWrapper/MultiSlotsWrapper
            if (StoreWrapper.class.equals(wcls)) {
                writeCountToDisk.incrementAndGet();

                if (statsTimeOnDisk) {
                    synchronized (totalWriteToDiskTimeStats) {
                        totalWriteToDiskTimeStats.accept(System.currentTimeMillis() - startTime);
                    }
                }
            }
        }

        return result;
    }

    /**
     * Stores a value to disk when memory is unavailable or based on storage policy.
     *
     * @param k the key
     * @param liveTime time-to-live in milliseconds
     * @param maxIdleTime maximum idle time in milliseconds
     * @param type the value type information
     * @param bytes serialized value bytes
     * @param size size of the serialized data
     * @return a wrapper for the disk-stored value, or null if storage failed
     */
    Wrapper<V> putToDisk(final K k, final long liveTime, final long maxIdleTime, final Type<V> type, final byte[] bytes, final int size) {
        if (offHeapStore.put(k, bytes.length == size ? bytes : N.copyOfRange(bytes, 0, size))) {
            return new StoreWrapper(type, liveTime, maxIdleTime, size, k);
        }

        return null;
    }

    /**
     * Finds and allocates an available memory slot of the specified size.
     * Uses a best-fit strategy within size-segregated segment queues.
     *
     * @param size required slot size in bytes
     * @return allocated slot or null if no space available
     */
    private Slot getAvailableSlot(final int size) {
        int slotSize = 0;

        if (size >= _maxBlockSize) {
            slotSize = _maxBlockSize;
        } else {
            slotSize = size % MIN_BLOCK_SIZE == 0 ? size : (size / MIN_BLOCK_SIZE + 1) * MIN_BLOCK_SIZE;
        }

        final int idx = slotSize / MIN_BLOCK_SIZE - 1;
        Deque<Segment> queue = _segmentQueues[idx];

        if (queue == null) {
            synchronized (_segmentQueues) {
                queue = _segmentQueues[idx];

                if (queue == null) {
                    queue = (_segmentQueues[idx] = new LinkedList<>());
                }
            }
        }

        Segment segment = null;
        int indexOfAvailableSlot = -1;

        synchronized (queue) {
            final Iterator<Segment> iterator = queue.iterator();
            final Iterator<Segment> descendingIterator = queue.descendingIterator();
            int half = queue.size() / 2 + 1;
            int cnt = 0;
            while (iterator.hasNext() && half-- >= 0) {
                cnt++;
                segment = iterator.next();

                if ((indexOfAvailableSlot = segment.allocateSlot()) >= 0) {
                    if (cnt > 3) {
                        iterator.remove();
                        queue.addFirst(segment);
                    }

                    break;
                }

                segment = descendingIterator.next();

                if ((indexOfAvailableSlot = segment.allocateSlot()) >= 0) {
                    if (cnt > 3) {
                        descendingIterator.remove();
                        queue.addFirst(segment);
                    }

                    break;
                }
            }
        }

        if (indexOfAvailableSlot < 0) {
            synchronized (_segmentBitSet) {
                final int nextSegmentIndex = _segmentBitSet.nextClearBit(0);

                if (nextSegmentIndex >= _segments.length) {
                    return null; // No space available;
                }

                _segmentBitSet.set(nextSegmentIndex);
                _segmentQueueMap.put(nextSegmentIndex, queue);

                segment = _segments[nextSegmentIndex];
                segment.sizeOfSlot = slotSize;

                indexOfAvailableSlot = segment.allocateSlot();

                synchronized (queue) {
                    queue.addFirst(segment);
                }
            }
        }

        return new Slot(indexOfAvailableSlot, segment);
    }

    /**
     * Triggers asynchronous memory defragmentation when fragmentation exceeds threshold.
     * This process consolidates free memory by evicting least recently used entries.
     */
    private void vacate() {
        if (_activeVacationTaskCount.incrementAndGet() > 1) {
            _activeVacationTaskCount.decrementAndGet();
            return;
        }

        _asyncExecutor.execute(() -> {
            try {
                _pool.vacate();

                evict();

                // wait for a couple of seconds to avoid the second vacation which just arrives before the vacation is done.
                N.sleep(3000);
            } finally {
                _activeVacationTaskCount.decrementAndGet();
            }
        });
    }

    /**
     * Removes an entry from the cache.
     * Properly cleans up memory or disk resources associated with the entry.
     *
     * @param k the key
     */
    @Override
    public void remove(final K k) {
        final Wrapper<V> w = _pool.remove(k);

        if (w != null) {
            w.destroy(Caller.REMOVE_REPLACE_CLEAR);
        }
    }

    /**
     * Checks if the cache contains a specific key.
     *
     * @param k the key
     * @return true if the key exists in the cache
     */
    @Override
    public boolean containsKey(final K k) {
        return _pool.containsKey(k);
    }

    /**
     * Returns a set of all keys in the cache.
     *
     * @return unmodifiable set of cache keys
     */
    @Override
    public Set<K> keySet() {
        return _pool.keySet();
    }

    /**
     * Returns the number of entries in the cache.
     *
     * @return the number of cache entries
     */
    @Override
    public int size() {
        return _pool.size();
    }

    /**
     * Removes all entries from the cache.
     * Clears both memory and disk storage.
     */
    @Override
    public void clear() {
        _pool.clear();
    }

    /**
     * Returns comprehensive statistics about cache performance and resource usage.
     * Provides detailed insights into memory utilization, disk usage, hit/miss rates,
     * eviction counts, and I/O performance metrics. This is essential for monitoring
     * and optimizing off-heap cache behavior.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OffHeapCache<String, byte[]> cache = new OffHeapCache<>(100);
     * cache.put("key1", data1);
     * cache.put("key2", data2);
     *
     * OffHeapCacheStats stats = cache.stats();
     * System.out.println("Size in memory: " + stats.sizeInMemory());
     * System.out.println("Size on disk: " + stats.sizeOnDisk());
     * System.out.println("Hit rate: " + stats.hitCount() + "/" + stats.getCount());
     * }</pre>
     *
     * @return cache statistics snapshot containing memory, disk, and performance metrics
     */
    public OffHeapCacheStats stats() {
        final PoolStats poolStats = _pool.stats();

        final Map<Integer, Map<Integer, Integer>> occupiedSlots = Stream.of(new ArrayList<>(_segmentQueueMap.values())).distinct().flatmap(queue -> {
            synchronized (queue) {
                return N.map(queue, it -> Tuple.of(it.sizeOfSlot, it.index, it.slotBitSet.cardinality()));
            }
        })
                .sorted(Comparators.<Tuple3<Integer, Integer, Integer>> comparingInt(it -> it._1).thenComparingInt(it -> it._2))
                .groupTo(it -> it._1, Collectors.toMap(it -> it._2, it -> it._3, Suppliers.ofLinkedHashMap()), Suppliers.ofLinkedHashMap());

        // TODO check if there is duplicated segment size and index. for debug purpose and will be removed later.
        if (Stream.of(occupiedSlots.values()).flatmap(Map::keySet).hasDuplicates()) {
            throw new RuntimeException("Duplicated segment size and index found");
        }

        final long totalWriteToDiskCount = totalWriteToDiskTimeStats.getCount();
        final long totalReadFromDiskCount = totalReadFromDiskTimeStats.getCount();

        final MinMaxAvg writeToDiskTimeStats = new MinMaxAvg(totalWriteToDiskCount > 0 ? totalWriteToDiskTimeStats.getMin() : 0.0D,
                totalWriteToDiskCount > 0 ? totalWriteToDiskTimeStats.getMax() : 0.0D,
                totalWriteToDiskCount > 0 ? Numbers.round(totalWriteToDiskTimeStats.getAverage(), 2) : 0.0D);

        final MinMaxAvg readFromDiskTimeStats = new MinMaxAvg(totalReadFromDiskCount > 0 ? totalReadFromDiskTimeStats.getMin() : 0.0D,
                totalReadFromDiskCount > 0 ? totalReadFromDiskTimeStats.getMax() : 0.0D,
                totalReadFromDiskCount > 0 ? Numbers.round(totalReadFromDiskTimeStats.getAverage(), 2) : 0.0D);

        return new OffHeapCacheStats(poolStats.capacity(), poolStats.size(), sizeOnDisk.get(), poolStats.putCount(), writeCountToDisk.get(),
                poolStats.getCount(), poolStats.hitCount(), readCountFromDisk.get(), poolStats.missCount(), poolStats.evictionCount(),
                evictionCountFromDisk.get(), _capacityInBytes, totalOccupiedMemorySize.get(), totalDataSize.get(), dataSizeOnDisk.get(), writeToDiskTimeStats,
                readFromDiskTimeStats, SEGMENT_SIZE, occupiedSlots);
    }

    /**
     * Closes the cache and releases all resources.
     * Stops eviction scheduling, clears all entries, and deallocates memory.
     * This method is thread-safe and idempotent.
     */
    @Override
    public synchronized void close() {
        if (_pool.isClosed()) {
            return;
        }

        try {
            if (scheduleFuture != null) {
                scheduleFuture.cancel(true);
            }
        } finally {
            try {
                if (shutdownHook != null) {
                    try {
                        Runtime.getRuntime().removeShutdownHook(shutdownHook);
                        shutdownHook = null;
                    } catch (final IllegalStateException e) {
                        // Already shutting down, ignore
                    }
                }
            } finally {
                try {
                    _pool.close();
                } finally {
                    deallocate();
                }
            }
        }
    }

    /**
     * Checks if the cache has been closed.
     *
     * @return true if {@link #close()} has been called
     */
    @Override
    public boolean isClosed() {
        return _pool.isClosed();
    }

    /**
     * Performs periodic eviction of empty segments to defragment memory.
     * Called by the scheduled eviction task.
     */
    protected void evict() {
        synchronized (_segmentBitSet) {
            for (int i = 0, len = _segments.length; i < len; i++) {
                if (_segments[i].slotBitSet.isEmpty()) {
                    final Deque<Segment> queue = _segmentQueueMap.get(i);

                    if (queue != null) {
                        synchronized (queue) {
                            if (_segments[i].slotBitSet.isEmpty()) {
                                queue.remove(_segments[i]);

                                _segmentQueueMap.remove(i);
                                _segmentBitSet.clear(i);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Represents a memory segment that can be divided into slots.
     * Each segment has a fixed size and can be configured with different slot sizes.
     */
    static final class Segment {

        private final BitSet slotBitSet = new BitSet();
        private final long segmentStartPtr;
        private final int index;
        // It can be reset/reused by set sizeOfSlot to: 64, 128, 256, 512, 1024, 2048, 4096, 8192....
        private int sizeOfSlot;

        public Segment(final long segmentStartPtr, final int index) {
            this.segmentStartPtr = segmentStartPtr;
            this.index = index;
        }

        public int index() {
            return index;
        }

        public int allocateSlot() {
            synchronized (slotBitSet) {
                final int result = slotBitSet.nextClearBit(0);

                if (result >= SEGMENT_SIZE / sizeOfSlot) {
                    return -1;
                }

                slotBitSet.set(result);

                return result;
            }
        }

        public void releaseSlot(final int slotIndex) {
            synchronized (slotBitSet) {
                slotBitSet.clear(slotIndex);
            }
        }

        @Override
        public String toString() {
            return "Segment [segmentStartPtr=" + segmentStartPtr + ", sizeOfSlot=" + sizeOfSlot + "]";
        }
    }

    /**
     * Represents an allocated memory slot within a segment.
     */
    record Slot(int indexOfSlot, Segment segment) {

        void release() {
            segment.releaseSlot(indexOfSlot);
        }
    }

    /**
     * Base wrapper class for cached values with metadata.
     */
    abstract static class Wrapper<T> extends AbstractPoolable {
        final Type<T> type;
        final int size;

        Wrapper(final Type<T> type, final long liveTime, final long maxIdleTime, final int size) {
            super(liveTime, maxIdleTime);

            this.type = type;
            this.size = size;
        }

        abstract T read();
    }

    /**
     * Wrapper for values stored in a single memory slot.
     */
    final class SlotWrapper extends Wrapper<V> {

        private Slot slot;
        private final long slotStartPtr;

        SlotWrapper(final Type<V> type, final long liveTime, final long maxIdleTime, final int size, final Slot slot, final long slotStartPtr) {
            super(type, liveTime, maxIdleTime, size);

            this.slot = slot;
            this.slotStartPtr = slotStartPtr;
        }

        @Override
        V read() {
            synchronized (this) {
                final byte[] bytes = new byte[size];

                copyFromMemory(slotStartPtr, bytes, _arrayOffset, size);

                if (type.isPrimitiveByteArray()) {
                    return (V) bytes;
                } else if (type.isByteBuffer()) {
                    return (V) ByteBufferType.valueOf(bytes);
                } else {
                    return deserializer.apply(bytes, type);
                }
            }
        }

        @Override
        public void destroy(final Caller caller) {
            synchronized (this) {
                if (slot != null) {
                    totalOccupiedMemorySize.addAndGet(-slot.segment.sizeOfSlot);
                    totalDataSize.addAndGet(-size);
                    slot.release();
                    slot = null;
                }
            }
        }
    }

    /**
     * Wrapper for values stored across multiple memory slots.
     */
    final class MultiSlotsWrapper extends Wrapper<V> {

        private List<Slot> slots;

        MultiSlotsWrapper(final Type<V> type, final long liveTime, final long maxIdleTime, final int size, final List<Slot> segments) {
            super(type, liveTime, maxIdleTime, size);

            slots = segments;
        }

        @Override
        V read() {
            synchronized (this) {
                final byte[] bytes = new byte[size];
                int size = this.size;
                int destOffset = _arrayOffset;
                Segment segment = null;

                for (final Slot slot : slots) {
                    segment = slot.segment;
                    final long startPtr = segment.segmentStartPtr + (long) slot.indexOfSlot * segment.sizeOfSlot;
                    final int sizeToCopy = Math.min(size, segment.sizeOfSlot);

                    copyFromMemory(startPtr, bytes, destOffset, sizeToCopy);

                    destOffset += sizeToCopy;
                    size -= sizeToCopy;
                }

                // should never happen.
                if (size != 0) {
                    throw new RuntimeException(
                            "Unknown error happening when retrieve value. The remaining size is " + size + " after finishing fetch data from all segments");
                }

                // it's destroyed after read from memory. dirty data may be read.
                if (type.isPrimitiveByteArray()) {
                    return (V) bytes;
                } else if (type.isByteBuffer()) {
                    return (V) ByteBufferType.valueOf(bytes);
                } else {
                    return deserializer.apply(bytes, type);
                }
            }
        }

        @Override
        public void destroy(final Caller caller) {
            synchronized (this) {
                if (N.notEmpty(slots)) {
                    for (final Slot slot : slots) {
                        totalOccupiedMemorySize.addAndGet(-slot.segment.sizeOfSlot);
                        slot.release();
                    }

                    totalDataSize.addAndGet(-size);

                    slots = null;
                }
            }
        }
    }

    /**
     * Wrapper for values stored on disk via OffHeapStore.
     */
    final class StoreWrapper extends Wrapper<V> {
        private K permanentKey;

        StoreWrapper(final Type<V> type, final long liveTime, final long maxIdleTime, final int size, final K permanentKey) {
            super(type, liveTime, maxIdleTime, size);

            this.permanentKey = permanentKey;

            sizeOnDisk.incrementAndGet();
            dataSizeOnDisk.addAndGet(size);
            totalDataSize.addAndGet(size);
        }

        @Override
        V read() {
            synchronized (this) {
                final byte[] bytes = offHeapStore.get(permanentKey);

                if (bytes == null) {
                    return null;
                }

                // should never happen.
                if (bytes.length != size) {
                    throw new RuntimeException("Unknown error happening when retrieve value. The fetched byte array size: " + bytes.length
                            + " is not equal to the expected size: " + size);
                }

                if (type.isPrimitiveByteArray()) {
                    return (V) bytes;
                } else if (type.isByteBuffer()) {
                    return (V) ByteBufferType.valueOf(bytes);
                } else {
                    return deserializer.apply(bytes, type);
                }
            }
        }

        @Override
        public void destroy(final Caller caller) {
            synchronized (this) {
                if (permanentKey != null) {
                    if (caller == Caller.EVICT || caller == Caller.VACATE) {
                        evictionCountFromDisk.incrementAndGet();
                    }

                    sizeOnDisk.decrementAndGet();
                    dataSizeOnDisk.addAndGet(-size);

                    totalDataSize.addAndGet(-size);

                    offHeapStore.remove(permanentKey);
                    permanentKey = null;
                }
            }
        }
    }

}