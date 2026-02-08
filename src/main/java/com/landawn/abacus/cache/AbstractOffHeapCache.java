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
 * @param <K> the type of keys used to identify cache entries
 * @param <V> the type of values stored in the cache
 * @see OffHeapCache
 * @see OffHeapCache25
 * @see OffHeapCacheStats
 * @see OffHeapStore
 */
abstract class AbstractOffHeapCache<K, V> extends AbstractCache<K, V> {

    /**
     * Default vacating factor (0.2) used by the object pool to trigger eviction.
     * When the pool reaches this utilization threshold, eviction of least recently
     * used entries is triggered to free up space.
     */
    static final float DEFAULT_VACATING_FACTOR = 0.2f;

    /**
     * Default parser for serialization - uses Kryo if available, otherwise JSON.
     */
    static final Parser<?, ?> parser = ParserFactory.isAvroParserAvailable() ? ParserFactory.createKryoParser() : ParserFactory.createJsonParser();

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
     * The cache divides the total memory capacity into fixed-size segments (1MB each) and manages
     * them using a sophisticated allocation strategy with size-based queuing and best-fit placement.
     * <br><br>
     * The constructor performs the following initialization steps:
     * <ul>
     * <li>Allocates off-heap memory of the specified capacity</li>
     * <li>Creates memory segments and initializes the segment tracking structures</li>
     * <li>Sets up the keyed object pool for cache entry management</li>
     * <li>Configures serialization/deserialization handlers</li>
     * <li>Schedules periodic eviction tasks if evictDelay is positive</li>
     * <li>Registers a shutdown hook for graceful resource cleanup</li>
     * </ul>
     * <br>
     * This constructor is thread-safe and all initialized data structures support concurrent access.
     *
     * @param capacityInMB the total off-heap memory capacity in megabytes. This determines the total
     *                     number of segments (capacityInMB MB / 1MB per segment). Must be positive.
     *                     The actual capacity will be exactly capacityInMB * 1048576 bytes.
     * @param maxBlockSize the maximum size of a single memory block in bytes. Values that fit within
     *                     this size will be stored in a single slot; larger values will be split across
     *                     multiple slots. Must be between 1024 and SEGMENT_SIZE (1048576). The value
     *                     will be rounded up to the nearest multiple of MIN_BLOCK_SIZE (64 bytes).
     * @param evictDelay the delay between eviction runs in milliseconds. If positive, schedules a
     *                   background task to periodically evict expired entries and release empty segments.
     *                   Use 0 or negative to disable automatic eviction (manual eviction via vacate() still works).
     * @param defaultLiveTime the default time-to-live for cache entries in milliseconds. Entries older
     *                        than this will be considered expired. Use 0 or negative for no expiration.
     *                        This can be overridden per entry when calling put().
     * @param defaultMaxIdleTime the default maximum idle time for entries in milliseconds. Entries not
     *                           accessed within this time will be considered expired. Use 0 or negative
     *                           for no idle timeout. This can be overridden per entry when calling put().
     * @param vacatingFactor the utilization threshold (0.0 to 1.0) that triggers defragmentation when
     *                       memory becomes fragmented. When the pool reaches this level of fragmentation,
     *                       the least recently used entries are evicted to free up space. Use 0.0 to apply
     *                       the DEFAULT_VACATING_FACTOR (0.2). Typical values range from 0.1 to 0.3.
     * @param arrayOffset the array base offset for memory operations, used to calculate the correct
     *                    memory address when copying data to/from byte arrays. This is implementation-specific
     *                    (e.g., Unsafe.ARRAY_BYTE_BASE_OFFSET for Unsafe-based implementations).
     * @param serializer the custom serializer function for converting values to byte streams, or null to
     *                   use the default serializer (Kryo if available, otherwise JSON). The serializer
     *                   should write the complete serialized form to the provided ByteArrayOutputStream.
     * @param deserializer the custom deserializer function for converting byte arrays back to values,
     *                     or null to use the default deserializer. The function receives the byte array
     *                     and type information, and should return the deserialized value.
     * @param offHeapStore the optional disk-based store for spillover storage when memory is full, or
     *                     null to disable disk spillover. When configured, values that don't fit in memory
     *                     can be automatically stored to disk instead of being rejected.
     * @param statsTimeOnDisk whether to collect detailed timing statistics for disk I/O operations.
     *                        When true, tracks min/max/average read and write times to disk. This has
     *                        minimal performance overhead but provides valuable monitoring data.
     * @param testerForLoadingItemFromDiskToMemory the predicate function to determine whether a disk-stored
     *                                             value should be promoted to memory based on access patterns.
     *                                             Receives the activity print, size, and I/O elapsed time.
     *                                             Return true to promote the value to memory. Use null to
     *                                             disable automatic promotion.
     * @param storeSelector the function to determine storage location for each put operation. Receives the
     *                      key, value, and size, and should return: 0 for default (try memory, fallback to disk),
     *                      1 for memory only (never store to disk), or 2 for disk only (always store to disk).
     *                      Use null for default behavior (always try memory first).
     * @param logger the logger instance for this cache, used to log warnings and errors during cache
     *               operations. Should not be null.
     * @throws IllegalArgumentException if maxBlockSize is less than 1024 or greater than SEGMENT_SIZE (1048576)
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
                        logger.warn("Error during cache eviction: " + ExceptionUtil.getErrorMessage(e));
                    }
                }
            };

            scheduleFuture = scheduledExecutor.scheduleWithFixedDelay(evictTask, evictDelay, evictDelay, TimeUnit.MILLISECONDS);
        }

        shutdownHook = new Thread(() -> {
            logger.info("Initiating OffHeapCache shutdown");

            try {
                close();
            } finally {
                logger.info("OffHeapCache shutdown completed");
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    /**
     * Allocates the specified amount of off-heap memory.
     * This is an abstract method that must be implemented by concrete subclasses
     * based on their specific memory management approach (e.g., Unsafe API or Foreign Memory API).
     * The allocated memory must remain valid until deallocate() is called.
     * <br><br>
     * Thread safety: This method is called only once during constructor initialization,
     * so thread-safety is not required for the implementation.
     *
     * @param capacityInBytes the number of bytes to allocate in off-heap memory. Must be positive.
     *                        Typically this will be a multiple of SEGMENT_SIZE (1048576 bytes).
     * @return the base memory address (pointer) of the allocated off-heap memory region.
     *         This address will be used for all subsequent memory access operations.
     */
    protected abstract long allocate(long capacityInBytes);

    /**
     * Deallocates all off-heap memory used by this cache.
     * This is an abstract method that must be implemented by concrete subclasses
     * to properly release native memory resources allocated by allocate().
     * After this method is called, the memory address returned by allocate() becomes invalid
     * and must not be accessed.
     * <br><br>
     * This method is called during cache shutdown, specifically in the close() method's
     * finally block to ensure memory is always released even if errors occur during cleanup.
     * <br><br>
     * Thread safety: This method is called only during the close() operation which is
     * synchronized, so thread-safety is not required for the implementation.
     */
    protected abstract void deallocate();

    /**
     * Copies data from a byte array to off-heap memory.
     * This is an abstract method that must be implemented by concrete subclasses
     * to perform the low-level memory copy operation using the appropriate API
     * (e.g., Unsafe.copyMemory() or MemorySegment operations).
     * <br><br>
     * Thread safety: This method may be called concurrently from multiple threads
     * during put operations. The implementation must ensure that concurrent copies
     * to different memory regions are safe.
     *
     * @param bytes the source byte array containing the data to copy. Must not be null.
     * @param srcOffset the starting offset in the source byte array. Together with the
     *                  array base offset (arrayOffset), this determines the actual source address.
     * @param startPtr the destination memory address in off-heap memory. Must be a valid
     *                 address within the allocated memory region (between startPtr and startPtr + capacity).
     * @param len the number of bytes to copy. Must be positive and must not exceed the
     *            available space at the destination address.
     */
    protected abstract void copyToMemory(byte[] bytes, int srcOffset, long startPtr, int len);

    /**
     * Copies data from off-heap memory to a byte array.
     * This is an abstract method that must be implemented by concrete subclasses
     * to perform the low-level memory copy operation using the appropriate API
     * (e.g., Unsafe.copyMemory() or MemorySegment operations).
     * <br><br>
     * Thread safety: This method may be called concurrently from multiple threads
     * during get operations. The implementation must ensure that concurrent copies
     * from different memory regions are safe.
     *
     * @param startPtr the source memory address in off-heap memory. Must be a valid
     *                 address within the allocated memory region (between startPtr and startPtr + capacity).
     * @param bytes the destination byte array to copy data into. Must not be null and
     *              must have sufficient capacity to hold the copied data.
     * @param destOffset the starting offset in the destination byte array. Together with the
     *                   array base offset (arrayOffset), this determines the actual destination address.
     * @param len the number of bytes to copy. Must be positive and must not exceed the
     *            available space in the destination array starting from destOffset.
     */
    protected abstract void copyFromMemory(final long startPtr, final byte[] bytes, final int destOffset, final int len);

    @Override
    public V getOrNull(final K key) {
        final Wrapper<V> w = _pool.get(key);

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
                        final byte[] bytes = liveTime > 0 ? offHeapStore.get(key) : null;

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
                                    remove(key);

                                    totalOccupiedMemorySize.addAndGet(occupiedMemory);

                                    totalDataSize.addAndGet(size);

                                    result = _pool.put(key, slotWrapper);
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

    @Override
    public boolean put(final K key, final V value, final long liveTime, final long maxIdleTime) {
        final Type<V> type = N.typeOf(value.getClass());
        Wrapper<V> w = null;

        // final byte[] bytes = parser.serialize(value).getBytes();
        final long startTime = statsTimeOnDisk ? System.currentTimeMillis() : 0;
        ByteArrayOutputStream os = null;
        byte[] bytes = null;
        int size = 0;
        long occupiedMemory = 0;

        if (type.isPrimitiveByteArray()) {
            bytes = (byte[]) value;
            size = bytes.length;
        } else if (type.isByteBuffer()) {
            bytes = ByteBufferType.byteArrayOf((ByteBuffer) value);
            size = bytes.length;
        } else {
            os = Objectory.createByteArrayOutputStream();
            serializer.accept(value, os);
            bytes = os.array();
            size = os.size();
        }

        final boolean canBeStoredInMemory = storeSelector == null || storeSelector.apply(key, value, size) < 2;
        final boolean canBeStoredToDisk = storeSelector == null || storeSelector.apply(key, value, size) != 1;

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
                    w = putToDisk(key, liveTime, maxIdleTime, type, bytes, size);
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
                    w = putToDisk(key, liveTime, maxIdleTime, type, bytes, size);
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
                _pool.remove(key);

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

            result = _pool.put(key, w);
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
     * This is an internal method called by put() when memory allocation fails or when
     * the storeSelector determines the value should be stored on disk. The method delegates
     * to the offHeapStore for actual disk persistence and creates a StoreWrapper to track
     * the disk-stored value within the cache.
     * <br><br>
     * The StoreWrapper maintains metadata about the disk-stored value including its type,
     * size, expiration settings, and the key needed to retrieve it from the offHeapStore.
     * Statistics are automatically updated when creating the wrapper (sizeOnDisk, dataSizeOnDisk,
     * totalDataSize).
     * <br><br>
     * Thread safety: This method may be called concurrently from multiple threads during put
     * operations. The offHeapStore implementation must handle concurrent put operations safely.
     *
     * @param k the cache key to associate with the disk-stored value. Must not be null.
     *          This key is stored in the StoreWrapper for later retrieval and removal.
     * @param liveTime the time-to-live in milliseconds for the disk-stored entry
     * @param maxIdleTime the maximum idle time in milliseconds for the disk-stored entry
     * @param type the value type information needed for deserialization when reading from disk
     * @param bytes the serialized value bytes to store on disk. The array may be larger than
     *              the actual data size, so only the first 'size' bytes are stored.
     * @param size the actual size of the serialized data in bytes. Must be positive and
     *             must not exceed the length of the bytes array.
     * @return a StoreWrapper for the disk-stored value if storage was successful,
     *         or null if the offHeapStore.put() operation failed
     */
    Wrapper<V> putToDisk(final K k, final long liveTime, final long maxIdleTime, final Type<V> type, final byte[] bytes, final int size) {
        if (offHeapStore.put(k, bytes.length == size ? bytes : N.copyOfRange(bytes, 0, size))) {
            return new StoreWrapper(type, liveTime, maxIdleTime, size, k);
        }

        return null;
    }

    /**
     * Finds and allocates an available memory slot of the specified size.
     * This method implements a sophisticated allocation strategy using size-segregated segment queues
     * and a best-fit search pattern. The algorithm minimizes fragmentation while maintaining good
     * performance through bidirectional searching and queue reordering.
     * <br><br>
     * Allocation algorithm:
     * <ol>
     * <li>Rounds the requested size up to the nearest multiple of MIN_BLOCK_SIZE (64 bytes),
     *     with a maximum of maxBlockSize</li>
     * <li>Determines the segment queue index based on the rounded slot size</li>
     * <li>Searches the queue bidirectionally (from both ends toward the middle) for a segment
     *     with an available slot</li>
     * <li>If a slot is found in a segment that's not near the front of the queue (position > 3),
     *     moves that segment to the front for faster future access</li>
     * <li>If no slot is found in existing segments, allocates a new segment from the pool
     *     (if capacity allows) and adds it to the queue</li>
     * </ol>
     * <br>
     * Thread safety: This method is thread-safe and uses fine-grained locking on individual
     * segment queues (not global locking). Multiple threads can allocate slots from different
     * size queues concurrently.
     *
     * @param size the required slot size in bytes. Must be positive. Will be rounded up to the
     *             nearest multiple of MIN_BLOCK_SIZE (64 bytes) or capped at maxBlockSize.
     * @return the allocated Slot containing the slot index and parent segment reference,
     *         or null if no space is available in the cache (all segments are full)
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
     * Triggers asynchronous vacating to free up memory space.
     * This method is called when memory allocation fails during a put operation, indicating
     * that the cache has reached capacity. It initiates a background cleanup process to evict
     * least recently used entries and release empty memory segments.
     * <br><br>
     * Vacating process:
     * <ol>
     * <li>Checks if a vacating task is already running using an atomic counter</li>
     * <li>If already running, returns immediately to avoid redundant work</li>
     * <li>Otherwise, schedules an asynchronous task that:
     *     <ul>
     *     <li>Calls _pool.vacate() to evict LRU entries based on the vacatingFactor threshold</li>
     *     <li>Calls evict() to release now-empty segments back to the pool</li>
     *     <li>Sleeps for 3 seconds to allow memory to stabilize before allowing another vacation</li>
     *     </ul>
     * </li>
     * </ol>
     * <br>
     * The method returns immediately after scheduling the vacating task, allowing the calling
     * thread to continue. This non-blocking behavior ensures that put operations don't hang
     * waiting for cleanup to complete.
     * <br><br>
     * Thread safety: This method is thread-safe and uses an atomic counter to ensure only
     * one vacating task runs at a time, even when called concurrently from multiple threads.
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

    @Override
    public void remove(final K key) {
        final Wrapper<V> w = _pool.remove(key);

        if (w != null) {
            w.destroy(Caller.REMOVE_REPLACE_CLEAR);
        }
    }

    @Override
    public boolean containsKey(final K key) {
        return _pool.containsKey(key);
    }

    @Override
    public Set<K> keySet() {
        return _pool.keySet();
    }

    @Override
    public int size() {
        return _pool.size();
    }

    @Override
    public void clear() {
        _pool.clear();
    }

    /**
     * Returns a point-in-time snapshot of cache statistics.
     * The snapshot includes entry counts, hit/miss counters, eviction counters, memory and disk usage,
     * disk I/O timing aggregates, and segment-slot occupancy details.
     * <br><br>
     * This method collects statistics from the underlying pool and segment queues, then assembles
     * an immutable {@link OffHeapCacheStats} view. Segment queues are synchronized while being read,
     * so the returned values are internally consistent for the capture moment.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OffHeapCache<String, byte[]> cache = new OffHeapCache<>(100);
     * cache.put("key1", "value1".getBytes());
     * cache.put("key2", "value2".getBytes());
     *
     * OffHeapCacheStats stats = cache.stats();
     * System.out.println("Entries in memory: " + stats.size());
     * System.out.println("Entries on disk: " + stats.sizeOnDisk());
     * System.out.println("Hit rate: " + stats.hitCount() + "/" + stats.getCount());
     * System.out.println("Memory usage: " + stats.dataSize() + "/" + stats.allocatedMemory());
     * }</pre>
     *
     * @return the statistics snapshot for this cache instance; never {@code null}
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
            throw new IllegalStateException("Internal error: duplicate segment size and index detected");
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

    @Override
    public boolean isClosed() {
        return _pool.isClosed();
    }

    /**
     * Performs periodic eviction of empty segments to release them back to the pool.
     * This is an internal maintenance method that reclaims memory segments that have become
     * completely empty (all slots freed) and makes them available for reuse with different slot sizes.
     * This process is essential for memory management as it allows segments to be repurposed when
     * workload patterns change.
     * <br><br>
     * Eviction process:
     * <ol>
     * <li>Iterates through all segments in the segment array</li>
     * <li>For each segment with an empty slot BitSet (no allocated slots):
     *     <ul>
     *     <li>Locates the segment's queue in the segmentQueueMap</li>
     *     <li>Uses double-checked locking to verify the segment is still empty</li>
     *     <li>Removes the segment from its queue</li>
     *     <li>Removes the segment from the segmentQueueMap</li>
     *     <li>Clears the segment's bit in the segmentBitSet, marking it as available</li>
     *     </ul>
     * </li>
     * </ol>
     * <br>
     * This method is called:
     * <ul>
     * <li>By the scheduled eviction task at regular intervals (if evictDelay > 0)</li>
     * <li>By the vacate() method after evicting entries from the pool</li>
     * </ul>
     * <br>
     * The method uses double-checked locking to minimize lock contention while ensuring thread safety.
     * It first checks if a segment is empty without holding the queue lock, then re-checks after
     * acquiring the lock to ensure the segment hasn't been allocated in the meantime.
     * <br><br>
     * Thread safety: This method is thread-safe and uses fine-grained locking on the segmentBitSet
     * and individual segment queues. It can be called concurrently with allocation operations.
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
     * Represents a memory segment that can be divided into fixed-size slots.
     * Each segment is SEGMENT_SIZE bytes (1MB) and can be configured with a specific slot size.
     * A segment can be reset and reused with a different slot size when all its slots are freed.
     * Uses a BitSet to track which slots are allocated, providing efficient allocation and
     * deallocation operations.
     * <br><br>
     * This is an internal class used by AbstractOffHeapCache for memory management.
     * Segments are allocated from the fixed array of segments created during cache initialization.
     * The sizeOfSlot field can be reconfigured to different multiples of MIN_BLOCK_SIZE (64, 128,
     * 256, 512, 1024, 2048, 4096, 8192, etc.) when the segment becomes empty and is reused.
     * <br><br>
     * Thread safety: All public methods are synchronized on the slotBitSet to ensure thread-safe
     * slot allocation and deallocation.
     */
    static final class Segment {

        private final BitSet slotBitSet = new BitSet();
        private final long segmentStartPtr;
        private final int index;
        // It can be reset/reused by set sizeOfSlot to: 64, 128, 256, 512, 1024, 2048, 4096, 8192....
        private int sizeOfSlot;

        /**
         * Constructs a segment descriptor for the specified native-memory range.
         *
         * @param segmentStartPtr the starting address of this segment in off-heap memory
         * @param index the segment index in the cache segment array
         */
        public Segment(final long segmentStartPtr, final int index) {
            this.segmentStartPtr = segmentStartPtr;
            this.index = index;
        }

        /**
         * Returns this segment's index in the segment array.
         *
         * @return the segment index
         */
        public int index() {
            return index;
        }

        /**
         * Allocates one slot from this segment.
         * The first available slot index is selected, marked as occupied, and returned.
         * If no slot is available for the current slot size, {@code -1} is returned.
         *
         * @return the allocated slot index, or {@code -1} if the segment is full
         */
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

        /**
         * Releases an allocated slot so it can be reused.
         *
         * @param slotIndex the slot index to release
         */
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
     * This record encapsulates the slot index and its parent segment,
     * providing a convenient way to release the slot when no longer needed.
     * <br><br>
     * This is an internal record used by AbstractOffHeapCache for memory management.
     * The slot must be released explicitly by calling release() to free the memory for reuse.
     * If a slot is allocated but not properly released, it will result in a memory leak within
     * the segment (the slot remains marked as occupied in the segment's BitSet).
     * <br><br>
     * Thread safety: The release() method is thread-safe as it delegates to the thread-safe
     * Segment.releaseSlot() method.
     *
     * @param indexOfSlot the slot index within the parent segment (0 to SEGMENT_SIZE/sizeOfSlot-1)
     * @param segment the parent Segment that contains this slot
     */
    record Slot(int indexOfSlot, Segment segment) {

        /**
         * Releases this slot back to its parent segment, making it available for reuse.
         * This method delegates to the segment's releaseSlot() method to clear the
         * corresponding bit in the segment's allocation BitSet.
         * <br><br>
         * Thread safety: This method is thread-safe.
         */
        void release() {
            segment.releaseSlot(indexOfSlot);
        }
    }

    /**
     * Base wrapper class for cached values with metadata.
     * Extends AbstractPoolable to support TTL and idle time tracking through the object pool.
     * Each wrapper stores the value's type information and serialized size.
     * <br><br>
     * This is an internal abstract class with three concrete implementations for
     * different storage strategies:
     * <ul>
     * <li>SlotWrapper - Values stored in a single memory slot (size &lt;= maxBlockSize)</li>
     * <li>MultiSlotsWrapper - Values stored across multiple memory slots (size &gt; maxBlockSize)</li>
     * <li>StoreWrapper - Values stored on disk via OffHeapStore</li>
     * </ul>
     * <br>
     * All wrappers are managed by the KeyedObjectPool which handles expiration and eviction
     * based on liveTime and maxIdleTime settings. The destroy() method is called by the pool
     * when entries are evicted, removed, or during cache shutdown to release resources.
     * <br><br>
     * Thread safety: Concrete implementations must ensure thread-safe read() and destroy()
     * operations, typically through synchronization.
     *
     * @param <T> the value type
     */
    abstract static class Wrapper<T> extends AbstractPoolable {
        final Type<T> type;
        final int size;

        /**
         * Constructs a new Wrapper with the specified metadata.
         *
         * @param type the value type information for deserialization
         * @param liveTime the time-to-live in milliseconds (0 or negative for default)
         * @param maxIdleTime the maximum idle time in milliseconds (0 or negative for default)
         * @param size the serialized size of the value in bytes
         */
        Wrapper(final Type<T> type, final long liveTime, final long maxIdleTime, final int size) {
            super(liveTime, maxIdleTime);

            this.type = type;
            this.size = size;
        }

        /**
         * Reads and deserializes the cached value.
         * Concrete implementations retrieve the raw bytes from their storage location
         * (memory or disk) and deserialize them based on the value type.
         * <br><br>
         * Thread safety: Implementations must be thread-safe.
         *
         * @return the deserialized value, or null if the value cannot be retrieved
         */
        abstract T read();
    }

    /**
     * Wrapper for values stored in a single memory slot.
     * Used for values that fit within the maximum block size (maxBlockSize). The slot reference
     * is used to release the memory when the entry is evicted or removed.
     * <br><br>
     * This is an internal class used by AbstractOffHeapCache. The wrapper is thread-safe
     * with synchronized access to read and destroy operations.
     * <br><br>
     * Memory layout: The value's serialized bytes are stored at the memory address
     * calculated as slotStartPtr = segment.segmentStartPtr + slot.indexOfSlot * segment.sizeOfSlot.
     * The actual data size may be less than the slot size (segment.sizeOfSlot), with the
     * difference being wasted space (internal fragmentation).
     * <br><br>
     * Thread safety: Both read() and destroy() methods are synchronized on the wrapper instance
     * to prevent concurrent access issues.
     */
    final class SlotWrapper extends Wrapper<V> {

        private Slot slot;
        private final long slotStartPtr;

        /**
         * Constructs a new SlotWrapper for a memory-stored value.
         *
         * @param type the value type information for deserialization
         * @param liveTime the time-to-live in milliseconds
         * @param maxIdleTime the maximum idle time in milliseconds
         * @param size the actual serialized size of the value in bytes
         * @param slot the allocated memory slot
         * @param slotStartPtr the starting memory address where the value is stored
         */
        SlotWrapper(final Type<V> type, final long liveTime, final long maxIdleTime, final int size, final Slot slot, final long slotStartPtr) {
            super(type, liveTime, maxIdleTime, size);

            this.slot = slot;
            this.slotStartPtr = slotStartPtr;
        }

        @Override
        V read() {
            synchronized (this) {
                if (slot == null) {
                    return null; // Already destroyed by concurrent eviction
                }

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
     * Used for large values that exceed the maximum block size (maxBlockSize). The value is split
     * across multiple slots (potentially in different segments), and all slots are released together
     * when evicted or removed.
     * <br><br>
     * This is an internal class used by AbstractOffHeapCache. The wrapper is thread-safe
     * with synchronized access to read and destroy operations.
     * <br><br>
     * Memory layout: The value's serialized bytes are split into chunks of up to maxBlockSize each.
     * Each chunk is stored in a separate slot, which may be in a different segment. The slots list
     * maintains the order of chunks, so during read(), the bytes are reassembled in the correct order.
     * <br><br>
     * Thread safety: Both read() and destroy() methods are synchronized on the wrapper instance
     * to prevent concurrent access issues.
     */
    final class MultiSlotsWrapper extends Wrapper<V> {

        private List<Slot> slots;

        /**
         * Constructs a new MultiSlotsWrapper for a large value stored across multiple slots.
         *
         * @param type the value type information for deserialization
         * @param liveTime the time-to-live in milliseconds
         * @param maxIdleTime the maximum idle time in milliseconds
         * @param size the actual serialized size of the value in bytes
         * @param segments the list of allocated slots in order (note: parameter name is misleading, should be 'slots')
         */
        MultiSlotsWrapper(final Type<V> type, final long liveTime, final long maxIdleTime, final int size, final List<Slot> segments) {
            super(type, liveTime, maxIdleTime, size);

            slots = segments;
        }

        @Override
        V read() {
            synchronized (this) {
                if (slots == null) {
                    return null; // Already destroyed by concurrent eviction
                }

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
                    throw new IllegalStateException(
                            "Failed to retrieve value: " + size + " bytes remaining after reading all segments (data corruption detected)");
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
     * Used when memory is full or when the storeSelector determines the value
     * should be stored on disk. The permanentKey is used to retrieve and remove
     * the value from the disk store.
     * <br><br>
     * This is an internal class used by AbstractOffHeapCache. The wrapper is thread-safe
     * with synchronized access to read and destroy operations.
     * <br><br>
     * Statistics: Upon construction, this wrapper increments sizeOnDisk, dataSizeOnDisk, and
     * totalDataSize counters. Upon destruction, these counters are decremented accordingly.
     * <br><br>
     * Thread safety: Both read() and destroy() methods are synchronized on the wrapper instance
     * to prevent concurrent access issues.
     */
    final class StoreWrapper extends Wrapper<V> {
        private K permanentKey;

        /**
         * Constructs a new StoreWrapper for a disk-stored value.
         * Immediately updates disk-related statistics counters upon construction.
         *
         * @param type the value type information for deserialization
         * @param liveTime the time-to-live in milliseconds
         * @param maxIdleTime the maximum idle time in milliseconds
         * @param size the actual serialized size of the value in bytes
         * @param permanentKey the key used to store and retrieve the value from offHeapStore
         */
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
                if (permanentKey == null) {
                    return null; // Already destroyed by concurrent eviction
                }

                final byte[] bytes = offHeapStore.get(permanentKey);

                if (bytes == null) {
                    return null;
                }

                // should never happen.
                if (bytes.length != size) {
                    throw new IllegalStateException(
                            "Failed to retrieve value: fetched size (" + bytes.length + " bytes) does not match expected size (" + size + " bytes)");
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
