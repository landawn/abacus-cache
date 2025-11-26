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
     * Default vacating factor (0.2) used by the object pool to trigger eviction.
     * When the pool reaches this utilization threshold, eviction of least recently
     * used entries is triggered to free up space.
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
     * during gett operations. The implementation must ensure that concurrent copies
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

    /**
     * Retrieves a value from the cache by its key.
     * This method handles retrieval from both memory-based storage (SlotWrapper/MultiSlotsWrapper)
     * and disk-based storage (StoreWrapper). For values stored on disk, this method may automatically
     * promote them to memory based on access frequency and I/O timing, as determined by the
     * testerForLoadingItemFromDiskToMemory predicate if configured. This adaptive behavior helps
     * optimize performance for frequently accessed data.
     * <br><br>
     * Retrieval behavior:
     * <ul>
     * <li>For memory-stored values: Data is copied directly from off-heap memory and deserialized</li>
     * <li>For disk-stored values: Data is read from the offHeapStore and may be promoted to memory if:
     *     <ul>
     *     <li>The testerForLoadingItemFromDiskToMemory predicate is configured</li>
     *     <li>The predicate returns true based on access patterns and I/O timing</li>
     *     <li>Sufficient memory is available for the promotion</li>
     *     </ul>
     * </li>
     * <li>Returns null if the key is not found or the entry has expired</li>
     * </ul>
     * <br>
     * Thread safety: This method is thread-safe and can be called concurrently from multiple threads.
     * The underlying object pool handles concurrent access, and each wrapper type (SlotWrapper,
     * MultiSlotsWrapper, StoreWrapper) has synchronized read operations.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Using concrete implementation
     * OffHeapCache<String, byte[]> cache = new OffHeapCache<>(100);
     * cache.put("key1", "value1".getBytes());
     * byte[] value = cache.gett("key1");
     * if (value != null) {
     *     System.out.println("Found: " + new String(value));
     * } else {
     *     System.out.println("Key not found or expired");
     * }
     * }</pre>
     *
     * @param k the cache key to retrieve. Must not be null.
     * @return the cached value associated with the key, or null if the key is not found,
     *         the entry has expired, or an error occurred during retrieval
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
     * This method serializes the value and stores it either in off-heap memory, on disk, or both,
     * based on size constraints, memory availability, and the configured storeSelector policy.
     * <br><br>
     * Storage decision logic:
     * <ul>
     * <li>If storeSelector is configured: Consults the selector to determine storage location
     *     <ul>
     *     <li>Return value 0: Try memory first, fallback to disk if memory unavailable</li>
     *     <li>Return value 1: Memory only (fail if memory unavailable)</li>
     *     <li>Return value 2: Disk only (always store to disk via offHeapStore)</li>
     *     </ul>
     * </li>
     * <li>If value size <= maxBlockSize: Stores in a single memory slot (SlotWrapper)</li>
     * <li>If value size > maxBlockSize: Splits across multiple slots (MultiSlotsWrapper)</li>
     * <li>If memory allocation fails: Falls back to disk storage if offHeapStore is configured</li>
     * <li>If both memory and disk storage fail: Returns false and triggers vacate() for cleanup</li>
     * </ul>
     * <br>
     * Serialization handling:
     * <ul>
     * <li>byte[] values: Stored directly without serialization</li>
     * <li>ByteBuffer values: Converted to byte array without using custom serializer</li>
     * <li>Other types: Serialized using the configured serializer (Kryo or JSON by default)</li>
     * </ul>
     * <br>
     * Thread safety: This method is thread-safe and can be called concurrently from multiple threads.
     * Memory allocation uses fine-grained locking on segment queues, and the object pool handles
     * concurrent put operations safely.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Using concrete implementation
     * OffHeapCache<String, byte[]> cache = new OffHeapCache<>(100);
     * byte[] data = "large data".getBytes();
     *
     * // Store with 1 hour TTL and 30 minutes idle time
     * boolean success = cache.put("key1", data, 3600000, 1800000);
     * if (success) {
     *     System.out.println("Data cached successfully");
     * } else {
     *     System.out.println("Failed to cache - memory and disk full");
     * }
     *
     * // Store with default TTL and idle time
     * cache.put("key2", "small data".getBytes(), 0, 0);
     * }</pre>
     *
     * @param k the cache key to associate with the value. Must not be null.
     * @param v the value to cache. Must not be null. Can be byte[], ByteBuffer, or any serializable object.
     * @param liveTime the time-to-live in milliseconds. If 0 or negative, uses the default live time
     *                 specified in the constructor. Entries older than this will be considered expired.
     * @param maxIdleTime the maximum idle time in milliseconds. If 0 or negative, uses the default
     *                    max idle time specified in the constructor. Entries not accessed within this
     *                    time will be considered expired.
     * @return true if the operation was successful and the value was stored (in memory or on disk),
     *         false if storage failed due to insufficient space in both memory and disk
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

    /**
     * Removes an entry from the cache and releases all associated resources.
     * This method properly cleans up resources based on the entry's storage type:
     * <ul>
     * <li>SlotWrapper: Releases the memory slot back to the segment for reuse</li>
     * <li>MultiSlotsWrapper: Releases all allocated slots back to their segments</li>
     * <li>StoreWrapper: Removes the value from the offHeapStore and updates disk statistics</li>
     * </ul>
     * <br>
     * The entry is removed from the underlying object pool, and the wrapper's destroy() method
     * is called with Caller.REMOVE_REPLACE_CLEAR to perform cleanup. Statistics counters are
     * automatically updated to reflect the freed resources.
     * <br><br>
     * This operation is idempotent - removing a non-existent key has no effect and is safe.
     * <br><br>
     * Thread safety: This method is thread-safe and can be called concurrently from multiple threads.
     * The object pool handles concurrent remove operations safely, and each wrapper type has
     * synchronized destroy operations.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Using concrete implementation
     * OffHeapCache<String, byte[]> cache = new OffHeapCache<>(100);
     * cache.put("key1", "value1".getBytes());
     * cache.remove("key1");  // Removes entry and frees resources
     * cache.remove("key1");  // Safe to call again, no effect
     * }</pre>
     *
     * @param k the cache key to remove. Must not be null. If the key doesn't exist, no operation is performed.
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
     * This method delegates to the underlying object pool's containsKey() method and returns
     * true for entries stored in both memory (SlotWrapper/MultiSlotsWrapper) and on disk (StoreWrapper).
     * <br><br>
     * Important notes:
     * <ul>
     * <li>This method does not verify if the entry has expired. An expired entry will still
     *     return true until it is evicted or accessed via gett()</li>
     * <li>This is a lightweight check that does not access the actual value data</li>
     * <li>The result is a point-in-time snapshot; concurrent modifications may occur</li>
     * </ul>
     * <br>
     * Thread safety: This method is thread-safe and can be called concurrently from multiple threads.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Using concrete implementation
     * OffHeapCache<String, byte[]> cache = new OffHeapCache<>(100);
     * cache.put("key1", "value1".getBytes());
     * if (cache.containsKey("key1")) {
     *     System.out.println("Key exists");
     * }
     *
     * // Check before expensive operation
     * if (!cache.containsKey("expensive_key")) {
     *     // Compute expensive value and put it in cache
     * }
     * }</pre>
     *
     * @param k the cache key to check. Must not be null.
     * @return true if the key exists in the cache (regardless of expiration status), false otherwise
     */
    @Override
    public boolean containsKey(final K k) {
        return _pool.containsKey(k);
    }

    /**
     * Returns a set of all keys in the cache.
     * This method delegates to the underlying object pool's keySet() method and returns
     * a set view containing keys for entries stored in both memory (SlotWrapper/MultiSlotsWrapper)
     * and on disk (StoreWrapper).
     * <br><br>
     * Important notes:
     * <ul>
     * <li>The set includes keys for expired entries until they are evicted by the background
     *     eviction task or accessed via gett()</li>
     * <li>The returned set is a snapshot at the time of the call; subsequent modifications
     *     to the cache are not reflected in the returned set</li>
     * <li>Iterating over the set does not access the actual value data, so it's a lightweight operation</li>
     * </ul>
     * <br>
     * Thread safety: This method is thread-safe and can be called concurrently from multiple threads.
     * The returned set is a snapshot and can be safely iterated without external synchronization.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Using concrete implementation
     * OffHeapCache<String, byte[]> cache = new OffHeapCache<>(100);
     * cache.put("key1", "value1".getBytes());
     * cache.put("key2", "value2".getBytes());
     * Set<String> keys = cache.keySet();
     * System.out.println("Cache contains " + keys.size() + " keys");
     *
     * // Iterate over all keys
     * for (String key : cache.keySet()) {
     *     byte[] value = cache.gett(key);
     *     if (value != null) {
     *         System.out.println("Key: " + key + ", Size: " + value.length);
     *     }
     * }
     * }</pre>
     *
     * @return a set view of the keys currently in this cache (including expired but not yet evicted entries)
     */
    @Override
    public Set<K> keySet() {
        return _pool.keySet();
    }

    /**
     * Returns the number of entries in the cache.
     * This method delegates to the underlying object pool's size() method and returns
     * the total count of entries stored in both memory (SlotWrapper/MultiSlotsWrapper)
     * and on disk (StoreWrapper).
     * <br><br>
     * Important notes:
     * <ul>
     * <li>The count includes expired entries until they are evicted by the background
     *     eviction task or removed</li>
     * <li>The returned value is a point-in-time snapshot; concurrent modifications may occur</li>
     * <li>This is a lightweight operation that does not access actual entry data</li>
     * </ul>
     * <br>
     * Thread safety: This method is thread-safe and can be called concurrently from multiple threads.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Using concrete implementation
     * OffHeapCache<String, byte[]> cache = new OffHeapCache<>(100);
     * cache.put("key1", "value1".getBytes());
     * cache.put("key2", "value2".getBytes());
     * System.out.println("Cache size: " + cache.size());  // prints: Cache size: 2
     *
     * // Monitor cache growth
     * int sizeBefore = cache.size();
     * cache.put("key3", "value3".getBytes());
     * int sizeAfter = cache.size();
     * System.out.println("Added " + (sizeAfter - sizeBefore) + " entries");
     * }</pre>
     *
     * @return the number of entries currently in the cache (including expired but not yet evicted entries)
     */
    @Override
    public int size() {
        return _pool.size();
    }

    /**
     * Removes all entries from the cache and releases all associated resources.
     * This operation clears all entries stored in both memory (SlotWrapper/MultiSlotsWrapper)
     * and on disk (StoreWrapper via the offHeapStore if configured). The underlying object pool's
     * clear() method handles the cleanup and calls destroy() on all wrappers, which in turn:
     * <ul>
     * <li>For SlotWrapper: Releases the memory slot back to the segment</li>
     * <li>For MultiSlotsWrapper: Releases all allocated slots back to their segments</li>
     * <li>For StoreWrapper: Removes the value from the offHeapStore</li>
     * </ul>
     * <br>
     * All statistics counters (totalOccupiedMemorySize, totalDataSize, dataSizeOnDisk, sizeOnDisk)
     * are automatically updated to reflect the freed resources. After calling this method, the cache
     * will be empty but still usable for new entries.
     * <br><br>
     * Thread safety: This method is thread-safe and can be called concurrently with other operations.
     * The object pool handles concurrent clear operations safely.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Using concrete implementation
     * OffHeapCache<String, byte[]> cache = new OffHeapCache<>(100);
     * cache.put("key1", "value1".getBytes());
     * cache.put("key2", "value2".getBytes());
     * cache.clear();  // Removes all entries and frees resources
     * System.out.println("Size after clear: " + cache.size());  // prints: Size after clear: 0
     *
     * // Clear and reuse
     * cache.put("new_key", "new_value".getBytes());
     * System.out.println("Size after new entry: " + cache.size());  // prints: Size after new entry: 1
     * }</pre>
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
     * // Using a concrete implementation
     * OffHeapCache<String, byte[]> cache = new OffHeapCache<>(100);
     * byte[] data1 = "value1".getBytes();
     * byte[] data2 = "value2".getBytes();
     * cache.put("key1", data1);
     * cache.put("key2", data2);
     *
     * OffHeapCacheStats stats = cache.stats();
     * System.out.println("Size in memory: " + stats.sizeInMemory());
     * System.out.println("Size on disk: " + stats.sizeOnDisk());
     * System.out.println("Hit rate: " + stats.hitCount() + "/" + stats.getCount());
     * }</pre>
     *
     * @return the cache statistics snapshot containing memory, disk, and performance metrics
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
     * Closes the cache and releases all resources including off-heap memory and disk storage.
     * This method performs a complete shutdown of the cache in the following order:
     * <ol>
     * <li>Cancels the scheduled eviction task (if configured)</li>
     * <li>Removes the shutdown hook from the JVM runtime</li>
     * <li>Closes the underlying object pool, which:
     *     <ul>
     *     <li>Clears all entries by calling destroy() on each wrapper</li>
     *     <li>Releases memory slots and removes disk-stored values</li>
     *     <li>Updates all statistics counters</li>
     *     </ul>
     * </li>
     * <li>Deallocates the off-heap memory via the abstract deallocate() method</li>
     * </ol>
     * <br>
     * The method uses nested finally blocks to ensure that each cleanup step is attempted even
     * if previous steps fail, guaranteeing that native memory is always released.
     * <br><br>
     * Important notes:
     * <ul>
     * <li>This method is thread-safe and synchronized to prevent concurrent close operations</li>
     * <li>This method is idempotent - calling close() multiple times has no additional effect
     *     beyond the first call</li>
     * <li>After calling close(), the cache cannot be used again and any subsequent operations
     *     (put, gett, etc.) will fail</li>
     * <li>It is critical to call close() to release native off-heap memory, as it won't be
     *     automatically freed by the garbage collector</li>
     * </ul>
     * <br>
     * Thread safety: This method is synchronized and thread-safe. Only one thread will execute
     * the close logic, while other concurrent callers will wait.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Manual close
     * OffHeapCache<String, byte[]> cache = new OffHeapCache<>(100);
     * try {
     *     cache.put("key1", "value1".getBytes());
     *     // Use cache...
     * } finally {
     *     cache.close();  // Always close to free native memory
     * }
     *
     * // Or use try-with-resources (recommended):
     * try (OffHeapCache<String, byte[]> cache2 = new OffHeapCache<>(100)) {
     *     cache2.put("key1", "value1".getBytes());
     *     // Use cache...
     * }  // Automatically closed
     * }</pre>
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
     * This method delegates to the underlying object pool's isClosed() method to determine
     * whether the cache has been shut down via the close() method.
     * <br><br>
     * Important notes:
     * <ul>
     * <li>Once closed, the cache cannot be reopened and any operations (put, gett, etc.) will fail</li>
     * <li>The off-heap memory has been deallocated once the cache is closed</li>
     * <li>This is a lightweight check that simply returns a boolean flag</li>
     * </ul>
     * <br>
     * Thread safety: This method is thread-safe and can be called concurrently from multiple threads.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Using concrete implementation
     * OffHeapCache<String, byte[]> cache = new OffHeapCache<>(100);
     * System.out.println("Is closed: " + cache.isClosed());  // prints: Is closed: false
     * cache.close();
     * System.out.println("Is closed: " + cache.isClosed());  // prints: Is closed: true
     *
     * // Check before using
     * if (!cache.isClosed()) {
     *     cache.put("key1", "value1".getBytes());
     * }
     * }</pre>
     *
     * @return true if {@link #close()} has been called and the cache is shut down, false otherwise
     */
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
     * deallocation operations. This is an internal class used by AbstractOffHeapCache for
     * memory management.
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
     * This record encapsulates the slot index and its parent segment,
     * providing a convenient way to release the slot when no longer needed.
     * This is an internal record used by AbstractOffHeapCache for memory management.
     * The slot must be released explicitly to free the memory for reuse.
     */
    record Slot(int indexOfSlot, Segment segment) {

        void release() {
            segment.releaseSlot(indexOfSlot);
        }
    }

    /**
     * Base wrapper class for cached values with metadata.
     * Extends AbstractPoolable to support TTL and idle time tracking.
     * Each wrapper stores the value's type information and serialized size.
     * This is an internal abstract class with concrete implementations for
     * different storage strategies (single slot, multiple slots, or disk).
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
     * Used for values that fit within the maximum block size. The slot reference
     * is used to release the memory when the entry is evicted or removed. This is
     * an internal class used by AbstractOffHeapCache. The wrapper is thread-safe
     * with synchronized access to read and destroy operations.
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
     * Used for large values that exceed the maximum block size. The value is split
     * across multiple slots, and all slots are released together when evicted or removed.
     * This is an internal class used by AbstractOffHeapCache. The wrapper is thread-safe
     * with synchronized access to read and destroy operations.
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
     * Used when memory is full or when the storeSelector determines the value
     * should be stored on disk. The permanentKey is used to retrieve and remove
     * the value from the disk store. This is an internal class used by AbstractOffHeapCache.
     * The wrapper is thread-safe with synchronized access to read and destroy operations.
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