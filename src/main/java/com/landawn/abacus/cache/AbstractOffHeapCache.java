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
import java.util.concurrent.atomic.LongAdder;
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
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.MoreExecutors;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Numbers;
import com.landawn.abacus.util.Objectory;
import com.landawn.abacus.util.Suppliers;
import com.landawn.abacus.util.Tuple;
import com.landawn.abacus.util.Tuple.Tuple3;
import com.landawn.abacus.util.function.TriFunction;
import com.landawn.abacus.util.function.TriPredicate;
import com.landawn.abacus.util.stream.Collectors;
import com.landawn.abacus.util.stream.Stream;

//--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED

/**
 * Abstract base class for off-heap cache implementations providing the core
 * memory management logic. Handles memory segmentation, allocation, eviction,
 * and optional disk spillover. Serves as the foundation for both Unsafe-based
 * and Foreign Memory API-based implementations.
 *
 * <p>Architecture overview:
 * <ul>
 * <li>Memory is divided into fixed-size segments ({@value #SEGMENT_SIZE} bytes each).</li>
 * <li>Each segment can be subdivided into slots of various sizes.</li>
 * <li>Slot sizes are multiples of {@link #MIN_BLOCK_SIZE} (64 bytes).</li>
 * <li>Large objects span multiple slots if needed.</li>
 * <li>Empty segments are automatically reclaimed so they can be reused with a different slot size.</li>
 * <li>Optional disk spillover is supported via the {@link OffHeapStore} interface.</li>
 * </ul>
 *
 * <p>Memory management strategy:
 * <ul>
 * <li>Size-segregated segment queues with bidirectional best-fit slot search.</li>
 * <li>Lazy segment allocation - segments are allocated only when needed.</li>
 * <li>Automatic eviction based on last-access time when capacity is reached.</li>
 * <li>Vacating process evicts entries and reclaims now-empty segments.</li>
 * <li>Statistics tracking for monitoring and optimization.</li>
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
     * Default vacating factor (0.2) passed to the underlying object pool. Controls the
     * fraction of entries the pool removes when a vacating cycle is triggered.
     */
    static final float DEFAULT_VACATING_FACTOR = 0.2f;

    /**
     * Default parser used for serialization when no custom serializer is configured.
     * Uses Kryo if it is on the classpath, otherwise falls back to JSON.
     */
    static final Parser<?, ?> parser = ParserFactory.isKryoParserAvailable() ? ParserFactory.createKryoParser() : ParserFactory.createJsonParser();

    /**
     * Default serializer used when no custom serializer is configured.
     * Delegates to {@link #parser}.
     */
    static final BiConsumer<Object, ByteArrayOutputStream> SERIALIZER = parser::serialize;

    /**
     * Default deserializer used when no custom deserializer is configured.
     * Returns the raw bytes for {@code byte[]} types, wraps them in a {@link ByteBuffer}
     * for {@code ByteBuffer} types, and otherwise delegates to {@link #parser}.
     */
    static final BiFunction<byte[], Type<?>, Object> DESERIALIZER = (bytes, type) -> {
        if (type.isPrimitiveByteArray()) {
            return bytes;
        } else if (type.isByteBuffer()) {
            return ByteBufferType.valueOf(bytes);
        } else {
            return parser.deserialize(new ByteArrayInputStream(bytes), type.javaType());
        }
    };

    /**
     * Size of each memory segment in bytes (1 MB).
     */
    static final int SEGMENT_SIZE = 1024 * 1024; // (int) N.ONE_MB;

    /**
     * Minimum slot size in bytes. All slot sizes are rounded up to a multiple of this value.
     */
    static final int MIN_BLOCK_SIZE = 64;

    /**
     * Default maximum size for a single memory block (8 KB).
     */
    static final int DEFAULT_MAX_BLOCK_SIZE = 8192; // 8K

    /**
     * When {@link #getAvailableSlot(int)} locates a non-full segment only after scanning past this
     * many segments from the head of the size-class queue, that segment is moved to the front so
     * subsequent allocations of the same slot size find it immediately, keeping the common case fast.
     */
    private static final int SEGMENT_REORDER_THRESHOLD = 3;

    /**
     * Shared scheduled executor used for the background eviction task of all
     * off-heap cache instances created from this class.
     */
    static final ScheduledExecutorService scheduledExecutor;

    static {
        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(IOUtil.CPU_CORES);
        // Without this, a cancelled scheduleFuture sits in the executor's task queue until its next
        // scheduled run, holding a strong reference to the cache instance and preventing GC after
        // close(). Setting the policy makes cancel() purge the task from the queue immediately.
        executor.setRemoveOnCancelPolicy(true);
        scheduledExecutor = MoreExecutors.getExitingScheduledExecutorService(executor);
    }

    // Statistics tracking. LongAdder (rather than AtomicLong) is used because these
    // counters are write-heavy on the hot get/put/evict paths and only read in stats();
    // LongAdder spreads contention across cells, avoiding the single contended CAS.
    final LongAdder totalOccupiedMemorySize = new LongAdder();
    final LongAdder totalDataSize = new LongAdder();
    final LongAdder dataSizeOnDisk = new LongAdder();
    final LongAdder sizeOnDisk = new LongAdder();
    final LongAdder writeCountToDisk = new LongAdder();
    final LongAdder readCountFromDisk = new LongAdder();
    final LongAdder evictionCountFromDisk = new LongAdder();

    final Logger logger;

    // Core memory management fields
    final long _capacityInBytes;
    final long _startPtr;
    final int _arrayOffset;
    final int _maxBlockSize;

    private final Segment[] _segments;
    private final BitSet _segmentBitSet = new BitSet();
    // Lower bound for the next free segment scan. Only ever read/written while holding
    // the _segmentBitSet monitor, so a plain int suffices. Lets new-segment allocation
    // start nextClearBit() near the first free index instead of rescanning from 0.
    private int _nextSegmentIndexHint = 0;
    private final Map<Integer, Deque<Segment>> _segmentQueueMap = new ConcurrentHashMap<>();
    private final Deque<Segment>[] _segmentQueues;

    private final AsyncExecutor _asyncExecutor = new AsyncExecutor();
    private final AtomicInteger _activeVacationTaskCount = new AtomicInteger();
    // Wall-clock time the most recent vacate task finished. Used to debounce
    // repeated vacate requests without pinning a pool thread in a sleep.
    private volatile long _lastVacateFinishedTime = 0;
    private static final long VACATE_DEBOUNCE_MILLIS = 3000;
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
     * Constructs an {@code AbstractOffHeapCache} with the specified configuration.
     * Initializes the memory segments, object pool, and eviction scheduling.
     * The cache divides the total memory capacity into fixed-size segments (1 MB each)
     * and manages them using a size-based queuing strategy with best-fit placement.
     *
     * <p>The constructor performs the following initialization steps:
     * <ul>
     * <li>Allocates off-heap memory of the specified capacity.</li>
     * <li>Creates memory segments and initializes the segment tracking structures.</li>
     * <li>Sets up the keyed object pool for cache entry management.</li>
     * <li>Configures serialization/deserialization handlers.</li>
     * <li>Schedules periodic eviction tasks if {@code evictDelay} is positive.</li>
     * <li>Registers a shutdown hook for graceful resource cleanup.</li>
     * </ul>
     *
     * <p>The constructor is safe to invoke from a single initializing thread; all data
     * structures it sets up are designed for concurrent access thereafter.
     *
     * @param capacityInMB the total off-heap memory capacity in megabytes. Determines the total
     *                     number of segments ({@code capacityInMB} MB / 1 MB per segment). Must be positive.
     *                     The actual capacity will be exactly {@code capacityInMB * 1048576} bytes. For very
     *                     large caches ({@code capacityInMB} &ge; ~131072, i.e. 128 GB) the derived pool
     *                     entry-capacity is clamped to {@link Integer#MAX_VALUE} so the {@code int} cast
     *                     does not overflow; the off-heap memory itself is still allocated at full size.
     * @param maxBlockSize the maximum size of a single memory slot in bytes. Values that fit within
     *                     this size are stored in a single slot; larger values are split across
     *                     multiple slots. Must be between 1024 and {@link #SEGMENT_SIZE} (1048576). The
     *                     value is rounded up to the nearest multiple of {@link #MIN_BLOCK_SIZE} (64 bytes).
     * @param evictDelay the delay between eviction runs in milliseconds. If positive, schedules a
     *                   background task to periodically evict expired entries and release empty segments.
     *                   Use 0 or negative to disable automatic eviction (manual eviction via the internal
     *                   {@code vacate()} mechanism still works).
     * @param defaultLiveTime the default time-to-live for cache entries in milliseconds. Entries older
     *                        than this are considered expired. Use 0 or negative for no expiration.
     *                        Can be overridden per entry when calling {@link #put(Object, Object, long, long)}.
     * @param defaultMaxIdleTime the default maximum idle time for entries in milliseconds. Entries not
     *                           accessed within this interval are considered expired. Use 0 or negative
     *                           for no idle timeout. Can be overridden per entry when calling
     *                           {@link #put(Object, Object, long, long)}.
     * @param vacatingFactor the fraction (0.0 to 1.0) of entries the underlying object pool removes
     *                       during a vacate cycle. A value of {@code 0f} selects the
     *                       {@link #DEFAULT_VACATING_FACTOR} (0.2). Typical values range from 0.1 to 0.3.
     * @param arrayOffset the array base offset for memory operations, used to calculate the correct
     *                    memory address when copying data to/from byte arrays. Implementation-specific
     *                    (e.g., {@code Unsafe.ARRAY_BYTE_BASE_OFFSET} for Unsafe-based implementations).
     * @param serializer the custom serializer function for converting values to byte streams, or
     *                   {@code null} to use the default serializer (Kryo if available, otherwise JSON).
     *                   The serializer must write the complete serialized form to the provided
     *                   {@link ByteArrayOutputStream}.
     * @param deserializer the custom deserializer function for converting byte arrays back to values,
     *                     or {@code null} to use the default deserializer. The function receives the byte
     *                     array and type information and must return the deserialized value.
     * @param offHeapStore the optional disk-based store for spillover storage when memory is full, or
     *                     {@code null} to disable disk spillover. When configured, values that do not fit
     *                     in memory may be automatically stored to disk instead of being rejected.
     * @param statsTimeOnDisk whether to collect detailed timing statistics for disk I/O operations.
     *                        When {@code true}, tracks min/max/average read and write times to disk.
     *                        Has minimal performance overhead but provides valuable monitoring data.
     * @param testerForLoadingItemFromDiskToMemory the predicate that determines whether a disk-stored
     *                                             value should be promoted to memory based on access patterns.
     *                                             Receives the {@link ActivityPrint}, size, and I/O elapsed
     *                                             time, and returns {@code true} to promote the value to
     *                                             memory. Use {@code null} to disable automatic promotion.
     * @param storeSelector the function that determines the storage location for each put operation.
     *                      Receives the key, value, and serialized size and must return: {@code 0} for
     *                      default (memory with disk fallback when configured), {@code 1} for memory only
     *                      (never spill to disk), or {@code 2} for disk only (skip memory). Use {@code null}
     *                      for default behavior on every put.
     * @param logger the logger instance for this cache, used to log warnings and errors during cache
     *               operations. Must not be {@code null}.
     * @throws IllegalArgumentException if {@code capacityInMB} is not positive, or if {@code maxBlockSize}
     *                                  is less than 1024 or greater than {@link #SEGMENT_SIZE} (1048576), or if
     *                                  {@code vacatingFactor} is not in the range [0.0, 1.0]
     * @throws OutOfMemoryError if the underlying call to {@link #allocate(long)} fails because the host
     *                          cannot reserve {@code capacityInMB} MB of native memory
     */
    @SuppressWarnings("rawtypes")
    protected AbstractOffHeapCache(final int capacityInMB, final int maxBlockSize, final long evictDelay, final long defaultLiveTime,
            final long defaultMaxIdleTime, final float vacatingFactor, final int arrayOffset, final BiConsumer<? super V, ByteArrayOutputStream> serializer,
            final BiFunction<byte[], Type<V>, ? extends V> deserializer, final OffHeapStore<K> offHeapStore, final boolean statsTimeOnDisk,
            final TriPredicate<ActivityPrint, Integer, Long> testerForLoadingItemFromDiskToMemory, final TriFunction<K, V, Integer, Integer> storeSelector,
            final Logger logger) {
        super(defaultLiveTime, defaultMaxIdleTime);

        N.checkArgPositive(capacityInMB, "capacityInMB");
        N.checkArgument(maxBlockSize >= 1024 && maxBlockSize <= SEGMENT_SIZE, "maxBlockSize must be in the range [1024, {}]: {}", SEGMENT_SIZE, maxBlockSize);
        N.checkArgument(vacatingFactor >= 0f && vacatingFactor <= 1f, "vacatingFactor must be in the range [0.0, 1.0]: {}", vacatingFactor);

        this.logger = N.checkArgNotNull(logger, "logger");

        _arrayOffset = arrayOffset;

        _capacityInBytes = capacityInMB * (1024L * 1024L); // N.ONE_MB;
        _maxBlockSize = maxBlockSize % MIN_BLOCK_SIZE == 0 ? maxBlockSize : (maxBlockSize / MIN_BLOCK_SIZE + 1) * MIN_BLOCK_SIZE;
        _segmentQueues = new Deque[_maxBlockSize / MIN_BLOCK_SIZE];

        // Populate every size-class queue eagerly (the array is small and fixed) so the queue
        // references are safely published by the constructor. Lazily creating them with
        // double-checked locking on a non-volatile array element risked another thread reading a
        // non-null LinkedList reference whose internal (non-final) fields were not yet visible.
        for (int i = 0, len = _segmentQueues.length; i < len; i++) {
            _segmentQueues[i] = new LinkedList<>();
        }

        // ByteBuffer.allocateDirect((int) capacity);
        _startPtr = allocate(_capacityInBytes);

        _segments = new Segment[(int) (_capacityInBytes / SEGMENT_SIZE)];

        for (int i = 0, len = _segments.length; i < len; i++) {
            _segments[i] = new Segment(_startPtr + (long) i * SEGMENT_SIZE, i);
        }

        // Pool capacity is the maximum number of MIN_BLOCK_SIZE slots that fit in the cache. For
        // very large caches (capacityInMB >= ~131_072 i.e. 128 GB) the slot count can exceed
        // Integer.MAX_VALUE; cap at Integer.MAX_VALUE so the int cast does not wrap to a
        // negative or near-zero value and make every put fail with "pool full".
        final long maxSlots = _capacityInBytes / MIN_BLOCK_SIZE;
        final int poolCapacity = maxSlots > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) maxSlots;
        _pool = PoolFactory.createKeyedObjectPool(poolCapacity, evictDelay, EvictionPolicy.LAST_ACCESS_TIME, true,
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
                    // Swallowed so the scheduled task keeps running; eviction retries on the next cycle.
                    // Pass the exception (not just its message) so the stack trace is preserved.
                    if (logger.isWarnEnabled()) {
                        logger.warn("Error during background cache eviction; will retry on next scheduled run", e);
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

        // Adding a shutdown hook can fail with IllegalStateException if the JVM is already
        // shutting down. If it does, we still have a fully-initialised off-heap allocation that
        // would leak. Best-effort: log, ensure the scheduled eviction task is cancelled, and
        // release the off-heap memory before rethrowing so the constructor doesn't leave the
        // OS-allocated buffer dangling for the lifetime of the process.
        try {
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        } catch (final IllegalStateException shuttingDown) {
            if (logger.isWarnEnabled()) {
                logger.warn("Cannot register shutdown hook (JVM is shutting down); cleaning up off-heap allocation", shuttingDown);
            }
            try {
                if (scheduleFuture != null) {
                    scheduleFuture.cancel(true);
                }
            } finally {
                try {
                    deallocate();
                } catch (final RuntimeException deallocateFailure) {
                    if (logger.isErrorEnabled()) {
                        logger.error("Failed to release off-heap memory after shutdown-hook registration failure", deallocateFailure);
                    }
                }
            }
            throw shuttingDown;
        }
    }

    /**
     * Allocates the specified amount of off-heap memory.
     * Concrete subclasses implement this method using their specific memory management
     * approach (e.g., the Unsafe API or the Foreign Memory API). The allocated memory
     * must remain valid until {@link #deallocate()} is called.
     *
     * <p>Thread safety: this method is called only once during constructor initialization,
     * so thread-safety is not required for the implementation.
     *
     * @param capacityInBytes the number of bytes to allocate in off-heap memory. Must be positive.
     *                        Typically a multiple of {@link #SEGMENT_SIZE} (1048576 bytes).
     * @return the base memory address (pointer) of the allocated off-heap memory region.
     *         This address is used for all subsequent memory access operations.
     * @throws OutOfMemoryError if the implementation cannot reserve the requested amount of native memory
     * @throws IllegalArgumentException if {@code capacityInBytes} is not positive (some backends, such as
     *         the Foreign Memory API, reject a non-positive size this way). This is unreachable through
     *         normal construction because {@code capacityInMB} is validated to be positive first.
     */
    protected abstract long allocate(long capacityInBytes);

    /**
     * Deallocates all off-heap memory used by this cache.
     * Concrete subclasses implement this method to release the native memory resources
     * previously reserved by {@link #allocate(long)}. After this method is called, the
     * memory address returned by {@link #allocate(long)} becomes invalid and must not be
     * accessed.
     *
     * <p>This method is called during cache shutdown, specifically in the {@code finally}
     * block of {@link #close()}, to ensure that memory is always released even if errors
     * occur during cleanup.
     *
     * <p>Thread safety: this method is called only from the synchronized {@link #close()}
     * operation, so thread-safety is not required for the implementation.
     */
    protected abstract void deallocate();

    /**
     * Copies data from a byte array to off-heap memory.
     * Concrete subclasses implement this method to perform the low-level memory copy
     * operation using the appropriate API (e.g., {@code Unsafe.copyMemory()} or
     * {@code MemorySegment} operations).
     *
     * <p>Thread safety: this method may be called concurrently from multiple threads
     * during put operations. Implementations must ensure that concurrent copies
     * to different memory regions are safe.
     *
     * @param bytes the source byte array containing the data to copy. Must not be {@code null}.
     * @param srcOffset the offset into the source array, expressed in the convention required by the
     *                  concrete implementation. The interpretation is <b>implementation-defined</b>: an
     *                  {@code Unsafe}-based implementation expects an address-style offset that already
     *                  includes the array base offset ({@code arrayOffset}), whereas a
     *                  {@code MemorySegment}-based implementation expects a plain zero-based index and
     *                  applies no array header offset.
     * @param startPtr the destination memory address in off-heap memory. Must be a valid
     *                 address within the allocated off-heap memory region.
     * @param len the number of bytes to copy. Must be positive and must not exceed the
     *            available space at the destination address.
     */
    protected abstract void copyToMemory(byte[] bytes, int srcOffset, long startPtr, int len);

    /**
     * Copies data from off-heap memory to a byte array.
     * Concrete subclasses implement this method to perform the low-level memory copy
     * operation using the appropriate API (e.g., {@code Unsafe.copyMemory()} or
     * {@code MemorySegment} operations).
     *
     * <p>Thread safety: this method may be called concurrently from multiple threads
     * during get operations. Implementations must ensure that concurrent copies
     * from different memory regions are safe.
     *
     * @param startPtr the source memory address in off-heap memory. Must be a valid
     *                 address within the allocated off-heap memory region.
     * @param bytes the destination byte array to copy data into. Must not be {@code null} and
     *              must have sufficient capacity to hold the copied data.
     * @param destOffset the offset into the destination array, expressed in the convention required by the
     *                   concrete implementation. The interpretation is <b>implementation-defined</b>: an
     *                   {@code Unsafe}-based implementation expects an address-style offset that already
     *                   includes the array base offset ({@code arrayOffset}), whereas a
     *                   {@code MemorySegment}-based implementation expects a plain zero-based index and
     *                   applies no array header offset.
     * @param len the number of bytes to copy. Must be positive and must not exceed the
     *            available space in the destination array starting from {@code destOffset}.
     */
    protected abstract void copyFromMemory(final long startPtr, final byte[] bytes, final int destOffset, final int len);

    /**
     * Retrieves the value associated with the specified key, or {@code null} if no mapping exists.
     * The lookup first consults the in-memory object pool. If the entry is stored on disk (via a
     * {@code StoreWrapper}), the value is read back from the configured {@link OffHeapStore}, the
     * disk-read counter (and read-time statistics, if enabled) are updated, and the entry may be
     * promoted back into memory when {@code testerForLoadingItemFromDiskToMemory} is configured
     * and returns {@code true} for it.
     *
     * @param key the key whose associated value is to be returned; must not be {@code null}
     * @return the cached value, or {@code null} if the key is not present, the entry has expired,
     *         or the disk-backed entry has been removed from the store
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws IllegalStateException if a value cannot be reconstructed because the retrieved
     *                               size no longer matches the recorded size (data corruption detected)
     */
    @Override
    public V getOrNull(final K key) {
        N.checkArgNotNull(key, "key");

        final Wrapper<V> w = _pool.get(key);

        // Due to error:  cannot be safely cast to StoreWrapper/MultiSlotsWrapper
        if (w != null && w.kind == Wrapper.KIND_STORE) {
            final StoreWrapper storeWrapper = (StoreWrapper) w;

            if (statsTimeOnDisk || testerForLoadingItemFromDiskToMemory != null) {
                final long startTime = System.currentTimeMillis();

                // Read the serialized bytes from disk exactly once. The same bytes are
                // reused both to produce the return value and (when promotion is enabled)
                // to copy the value back into off-heap memory, avoiding a second disk read.
                final byte[] diskBytes;
                long elapsedTime = 0;

                try {
                    diskBytes = storeWrapper.readBytes();
                } finally {
                    // Clamp to >= 0: a backward wall-clock adjustment (NTP/manual) mid-read can make
                    // the elapsed time negative, which would both feed a nonsensical value to the
                    // promotion tester below and, via LongSummaryStatistics, make stats() throw from
                    // the non-negative MinMaxAvg constructor.
                    elapsedTime = Math.max(0L, System.currentTimeMillis() - startTime);

                    if (statsTimeOnDisk) {
                        synchronized (totalReadFromDiskTimeStats) {
                            totalReadFromDiskTimeStats.accept(elapsedTime);
                        }
                    }
                }

                if (diskBytes == null) {
                    removeStaleStoreWrapperIfCurrent(key, storeWrapper);
                    return null;
                }

                readCountFromDisk.increment();

                final V value = storeWrapper.deserialize(diskBytes);

                if (testerForLoadingItemFromDiskToMemory != null
                        && testerForLoadingItemFromDiskToMemory.test(storeWrapper.activityPrint(), storeWrapper.size, elapsedTime)) {

                    final ActivityPrint activityPrint = storeWrapper.activityPrint();
                    final long maxIdleTime = activityPrint.getMaxIdleTime();
                    final long liveTime = activityPrint.getLiveTime() - (System.currentTimeMillis() - activityPrint.getCreatedTime());

                    final int size = storeWrapper.size;
                    final byte[] bytes = liveTime > 0 ? diskBytes : null;

                    if (bytes != null && bytes.length == size) {
                        Slot slot = null;
                        List<Slot> slots = null;
                        // Must be long (not int): for a multi-slot promotion of a near-2GB value this
                        // accumulates ceil(size/maxBlockSize) slot sizes and would overflow int to a
                        // negative number, corrupting totalOccupiedMemorySize. Matches the put() path.
                        long occupiedMemory = 0;
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
                            // Only promote when the disk-backed entry we read is still the current
                            // mapping. A concurrent put()/remove() may have replaced it between the
                            // _pool.get(key) at the top of this method and now; an unconditional
                            // remove(key) here would destroy that newer entry and resurrect the
                            // (older) value we just read from disk - a lost update. This mirrors the
                            // "only if current" pattern in removeStaleStoreWrapperIfCurrent().
                            final Wrapper<V> removed = _pool.remove(key);

                            if (removed == storeWrapper) {
                                // Current entry is the disk wrapper we promoted from; retire it
                                // (this removes the now-redundant on-disk bytes) and install the
                                // in-memory copy.
                                removed.destroy(Caller.REMOVE_REPLACE_CLEAR);

                                boolean result = false;

                                try {
                                    totalOccupiedMemorySize.add(occupiedMemory);

                                    totalDataSize.add(size);

                                    result = _pool.put(key, slotWrapper);
                                } finally {
                                    if (!result) {
                                        slotWrapper.destroy(Caller.PUT_ADD_FAILURE);
                                    }
                                }
                            } else {
                                // The mapping changed concurrently. Restore whatever is now current
                                // and discard the promoted slots instead of clobbering the newer entry.
                                if (removed != null) {
                                    final boolean restored = _pool.put(key, removed);

                                    if (!restored) {
                                        removed.destroy(Caller.PUT_ADD_FAILURE);
                                    }
                                }

                                slotWrapper.destroy(Caller.PUT_ADD_FAILURE);
                            }
                        }
                    }
                }

                return value;
            } else {
                final V value = w.read();

                if (value == null) {
                    removeStaleStoreWrapperIfCurrent(key, storeWrapper);
                } else {
                    readCountFromDisk.increment();
                }

                return value;
            }
        } else {
            return w == null ? null : w.read();
        }
    }

    private void removeStaleStoreWrapperIfCurrent(final K key, final StoreWrapper storeWrapper) {
        final Wrapper<V> removed = _pool.remove(key);

        if (removed == null) {
            return;
        }

        if (removed == storeWrapper) {
            removed.destroy(Caller.REMOVE_REPLACE_CLEAR);
            return;
        }

        final boolean restored = _pool.put(key, removed);

        if (!restored) {
            removed.destroy(Caller.PUT_ADD_FAILURE);

            if (logger.isWarnEnabled()) {
                logger.warn("Failed to restore off-heap cache entry after removing stale disk wrapper for key: " + key);
            }
        }
    }

    /**
     * Stores a key-value pair in the cache with the specified expiration settings.
     * The value is serialized and copied into off-heap memory. Values that fit within
     * {@code maxBlockSize} are stored in a single slot; larger values are split across
     * multiple slots. When memory cannot be allocated (or when directed by the configured
     * {@code storeSelector}), the value may be written to the configured {@link OffHeapStore}
     * instead. If neither memory nor disk storage succeeds, an asynchronous vacating task is
     * scheduled and the method returns {@code false}.
     *
     * <p>Non-positive {@code liveTime} or {@code maxIdleTime} values are interpreted as
     * "no expiration" and internally translated to {@code Long.MAX_VALUE}.
     *
     * @param key the key with which the specified value is to be associated; must not be {@code null}
     * @param value the value to be cached; must not be {@code null}
     * @param liveTime the time-to-live for this entry in milliseconds; values {@code <= 0} mean no expiration
     * @param maxIdleTime the maximum idle time for this entry in milliseconds; values {@code <= 0} mean no idle timeout
     * @return {@code true} if the value was successfully cached (in memory or on disk);
     *         {@code false} otherwise
     * @throws IllegalArgumentException if {@code key} or {@code value} is {@code null}
     */
    @Override
    public boolean put(final K key, final V value, final long liveTime, final long maxIdleTime) {
        N.checkArgNotNull(key, "key");
        N.checkArgNotNull(value, "value");

        // A non-positive liveTime/maxIdleTime means "no expiration" per the documented contract
        // (see Cache#put and the OffHeapCache/OffHeapCache25 Builder javadoc). The underlying
        // ActivityPrint rejects values <= 0 with IllegalArgumentException, so translate them to
        // Long.MAX_VALUE here, which is the same value the library uses internally for "no expiration".
        final long effectiveLiveTime = liveTime > 0 ? liveTime : Long.MAX_VALUE;
        final long effectiveMaxIdleTime = maxIdleTime > 0 ? maxIdleTime : Long.MAX_VALUE;

        final Type<V> type = N.typeOf(value.getClass());
        final boolean replacingExisting = _pool.peek(key) != null;
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
            // Recycle the pooled buffer if serialization fails. os cannot be recycled in the main
            // finally only — it is created here, outside that try — so a throwing serializer would
            // otherwise leak the pooled ByteArrayOutputStream on every failure. Null it out on the
            // failure path so the main finally's Objectory.recycle(os) does not double-recycle it.
            boolean serialized = false;
            try {
                serializer.accept(value, os);
                bytes = os.array();
                size = os.size();
                serialized = true;
            } finally {
                if (!serialized) {
                    Objectory.recycle(os);
                    os = null;
                }
            }
        }

        final int storeSelection = storeSelector == null ? 0 : storeSelector.apply(key, value, size);
        final boolean canBeStoredInMemory = storeSelection < 2;
        final boolean canBeStoredToDisk = storeSelection != 1;

        Slot slot = null;
        List<Slot> slots = null;

        try {
            if (size <= _maxBlockSize) {
                slot = canBeStoredInMemory ? getAvailableSlot(size) : null;

                if (slot != null) {
                    final long slotStartPtr = slot.segment.segmentStartPtr + (long) slot.indexOfSlot * slot.segment.sizeOfSlot;

                    copyToMemory(bytes, _arrayOffset, slotStartPtr, size);

                    occupiedMemory = slot.segment.sizeOfSlot;

                    w = new SlotWrapper(type, effectiveLiveTime, effectiveMaxIdleTime, size, slot, slotStartPtr);
                } else if (canBeStoredToDisk && offHeapStore != null) {
                    w = putToDisk(key, effectiveLiveTime, effectiveMaxIdleTime, type, bytes, size);
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
                    w = new MultiSlotsWrapper(type, effectiveLiveTime, effectiveMaxIdleTime, size, slots);
                } else if (canBeStoredToDisk && offHeapStore != null) {
                    w = putToDisk(key, effectiveLiveTime, effectiveMaxIdleTime, type, bytes, size);
                }
            }
        } finally {
            Objectory.recycle(os);

            // Due to error:  cannot be safely cast to StoreWrapper/MultiSlotsWrapper
            if (w == null || w.kind == Wrapper.KIND_STORE) {
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

        // Handle the "could not store" outcome AFTER the try/finally. Doing this inside the finally
        // with a `return false` would silently discard any exception propagating out of the try
        // (e.g. an OutOfMemoryError while growing a slot, a copyToMemory failure, or a RuntimeException
        // thrown by a custom offHeapStore.put) — the classic "return in finally swallows the exception"
        // trap. The finally above still releases any allocated slots on every exit, including the
        // exceptional one, so cleanup is unchanged; only the silent swallowing is removed.
        if (w == null) {
            if (!replacingExisting) {
                vacate();
            }

            return false;
        }

        boolean result = false;
        final int wkind = w.kind;

        try {
            if (wkind == Wrapper.KIND_SLOT || wkind == Wrapper.KIND_MULTI_SLOTS) {
                totalOccupiedMemorySize.add(occupiedMemory);

                // StoreWrapper already accounts for totalDataSize in its constructor; only memory
                // wrappers need it added here (and their destroy() subtracts the matching amount).
                totalDataSize.add(size);
            }

            result = _pool.put(key, w);
        } finally {
            if (!result) {
                // The pool rejected the wrapper; for a StoreWrapper this also removes the disk bytes
                // that putToDisk just wrote. Since nothing was retained, do NOT count it toward the
                // disk write stats — that keeps putCountToDisk consistent with putCount, which by
                // contract excludes puts that fail before the wrapper is installed in the pool.
                w.destroy(Caller.PUT_ADD_FAILURE);
            } else if (wkind == Wrapper.KIND_STORE) {
                writeCountToDisk.increment();

                if (statsTimeOnDisk) {
                    synchronized (totalWriteToDiskTimeStats) {
                        // Clamp to >= 0: a backward wall-clock adjustment (NTP/manual) mid-write can
                        // make the elapsed time negative, which LongSummaryStatistics would carry into
                        // stats() and make the non-negative MinMaxAvg constructor throw.
                        totalWriteToDiskTimeStats.accept(Math.max(0L, System.currentTimeMillis() - startTime));
                    }
                }
            }
        }

        return result;
    }

    /**
     * Stores a value to disk when memory is unavailable or directed by the storage policy.
     * Internal helper called by {@link #put(Object, Object, long, long)} when memory
     * allocation fails or when the {@code storeSelector} determines the value should be
     * stored on disk. Delegates to {@code offHeapStore} for actual disk persistence and
     * creates a {@code StoreWrapper} to track the disk-stored value within the cache.
     *
     * <p>Before writing the new bytes, any existing wrapper at this key is removed from the
     * pool and destroyed. This is required so that a prior {@code StoreWrapper}'s
     * {@code destroy()} call invokes {@code offHeapStore.remove(k)} on the OLD bytes rather
     * than wiping the NEW bytes that are about to be written. For memory-backed prior
     * wrappers ({@code SlotWrapper} / {@code MultiSlotsWrapper}), this just releases their
     * slots a moment earlier than the pool's own replace would. The prior wrapper's
     * bookkeeping ({@code sizeOnDisk} / {@code dataSizeOnDisk} / {@code totalDataSize}) is
     * decremented as part of the destroy.
     *
     * <p>The new {@code StoreWrapper} maintains metadata about the disk-stored value including
     * its type, size, expiration settings, and the key needed to retrieve it from the
     * {@code offHeapStore}. Disk-related statistics are automatically updated when the wrapper
     * is constructed ({@code sizeOnDisk}, {@code dataSizeOnDisk}, {@code totalDataSize}).
     *
     * <p>Thread safety: this method may be called concurrently from multiple threads during put
     * operations. The {@code offHeapStore} implementation must handle concurrent put operations safely.
     *
     * @param k the cache key to associate with the disk-stored value. Must not be {@code null}.
     *          This key is stored in the {@code StoreWrapper} for later retrieval and removal.
     * @param liveTime the time-to-live in milliseconds for the disk-stored entry
     * @param maxIdleTime the maximum idle time in milliseconds for the disk-stored entry
     * @param type the value type information needed for deserialization when reading from disk
     * @param bytes the serialized value bytes to store on disk. The array may be larger than
     *              the actual data size, so only the first {@code size} bytes are stored.
     * @param size the actual size of the serialized data in bytes. Must be positive and
     *             must not exceed the length of the {@code bytes} array.
     * @return a {@code StoreWrapper} for the disk-stored value if storage was successful,
     *         or {@code null} if the {@code offHeapStore.put()} operation failed
     */
    Wrapper<V> putToDisk(final K k, final long liveTime, final long maxIdleTime, final Type<V> type, final byte[] bytes, final int size) {
        // Temporarily remove any prior wrapper at this key before writing disk bytes. On a
        // successful disk write, the prior wrapper is retired before the new wrapper is installed.
        // On a failed disk write, the prior wrapper is restored so a failed replacement does not
        // silently delete the existing cache entry.
        final Wrapper<V> prior = _pool.remove(k);

        final boolean stored;

        try {
            stored = offHeapStore.put(k, bytes.length == size ? bytes : N.copyOfRange(bytes, 0, size));
        } catch (final RuntimeException | Error e) {
            try {
                restorePriorAfterDiskWriteFailure(k, prior);
            } catch (final RuntimeException | Error restoreFailure) {
                e.addSuppressed(restoreFailure);
            }

            throw e;
        }

        if (stored) {
            if (prior != null) {
                if (prior.kind == Wrapper.KIND_STORE) {
                    ((StoreWrapper) prior).discardReplacedStoreMetadata();
                } else {
                    prior.destroy(Caller.REMOVE_REPLACE_CLEAR);
                }
            }

            return new StoreWrapper(type, liveTime, maxIdleTime, size, k);
        }

        restorePriorAfterDiskWriteFailure(k, prior);

        return null;
    }

    private void restorePriorAfterDiskWriteFailure(final K k, final Wrapper<V> prior) {
        if (prior != null) {
            final boolean restored = _pool.put(k, prior);

            if (!restored) {
                prior.destroy(Caller.PUT_ADD_FAILURE);

                if (logger.isWarnEnabled()) {
                    logger.warn("Failed to restore previous off-heap cache entry after disk write failure for key: " + k);
                }
            }
        }
    }

    /**
     * Finds and allocates an available memory slot of the specified size.
     * Uses size-segregated segment queues and a best-fit search pattern. The algorithm
     * minimizes fragmentation while maintaining good performance through bidirectional
     * searching and queue reordering.
     *
     * <p>Allocation algorithm:
     * <ol>
     * <li>Rounds the requested size up to the nearest multiple of {@link #MIN_BLOCK_SIZE}
     *     (64 bytes), capped at {@code maxBlockSize}.</li>
     * <li>Determines the segment queue index based on the rounded slot size.</li>
     * <li>Searches the queue bidirectionally (from both ends toward the middle) for a segment
     *     with an available slot.</li>
     * <li>If a slot is found after examining more than 3 segments from the front of the queue,
     *     moves that segment to the front for faster future access.</li>
     * <li>If no slot is found in existing segments, allocates a new segment from the pool
     *     (if capacity allows) and adds it to the queue.</li>
     * </ol>
     *
     * <p>Thread safety: this method is thread-safe. The fast path uses per-queue locks scoped to a
     * single slot size, so threads allocating slots of different sizes do not contend. When a new
     * segment must be allocated from the global pool, a global lock on {@code _segmentBitSet} is
     * briefly held to assign a fresh segment index.
     *
     * @param size the required slot size in bytes. Non-positive values are treated as
     *             {@link #MIN_BLOCK_SIZE}; otherwise the value is rounded up to the nearest
     *             multiple of {@link #MIN_BLOCK_SIZE} (64 bytes) and capped at {@code maxBlockSize}.
     * @return the allocated {@link Slot} containing the slot index and parent segment reference,
     *         or {@code null} if no space is available in the cache (all segments are full)
     */
    private Slot getAvailableSlot(final int size) {
        int slotSize = 0;

        if (size <= 0) {
            slotSize = MIN_BLOCK_SIZE;
        } else if (size >= _maxBlockSize) {
            slotSize = _maxBlockSize;
        } else {
            slotSize = size % MIN_BLOCK_SIZE == 0 ? size : (size / MIN_BLOCK_SIZE + 1) * MIN_BLOCK_SIZE;
        }

        final int idx = slotSize / MIN_BLOCK_SIZE - 1;
        // Queues are created eagerly in the constructor, so this element is always non-null and
        // safely published; no lazy initialization / double-checked locking is required here.
        final Deque<Segment> queue = _segmentQueues[idx];

        Segment segment = null;
        int indexOfAvailableSlot = -1;

        synchronized (queue) {
            final Iterator<Segment> iterator = queue.iterator();
            final Iterator<Segment> descendingIterator = queue.descendingIterator();
            // Scan from both ends toward the middle, so the two iterators together cover the whole
            // queue in at most half its length (the +1 covers the odd/middle element).
            int half = queue.size() / 2 + 1;
            int cnt = 0;
            while (iterator.hasNext() && half-- >= 0) {
                cnt++;
                segment = iterator.next();

                if ((indexOfAvailableSlot = segment.allocateSlot()) >= 0) {
                    if (cnt > SEGMENT_REORDER_THRESHOLD) {
                        iterator.remove();
                        queue.addFirst(segment);
                    }

                    break;
                }

                if (descendingIterator.hasNext()) {
                    segment = descendingIterator.next();

                    if ((indexOfAvailableSlot = segment.allocateSlot()) >= 0) {
                        if (cnt > SEGMENT_REORDER_THRESHOLD) {
                            descendingIterator.remove();
                            queue.addFirst(segment);
                        }

                        break;
                    }
                }
            }
        }

        if (indexOfAvailableSlot < 0) {
            synchronized (_segmentBitSet) {
                // Start the scan from the hint rather than rescanning the whole bitset
                // from index 0 on every new-segment allocation.
                final int nextSegmentIndex = _segmentBitSet.nextClearBit(_nextSegmentIndexHint);

                if (nextSegmentIndex >= _segments.length) {
                    return null; // No space available;
                }

                _segmentBitSet.set(nextSegmentIndex);
                _nextSegmentIndexHint = nextSegmentIndex + 1;
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
     * Called when memory allocation fails during a put operation, indicating
     * that the cache has reached capacity. Initiates a background cleanup process to evict
     * least-recently-used entries and release empty memory segments.
     *
     * <p>Vacating process:
     * <ol>
     * <li>Debounce gate: if a vacate completed within the last {@code VACATE_DEBOUNCE_MILLIS}
     *     (3000 ms), returns immediately without scheduling another one.</li>
     * <li>Otherwise, checks if a vacating task is already in flight using an atomic counter;
     *     if so, returns immediately to avoid redundant work.</li>
     * <li>Otherwise, schedules an asynchronous task that:
     *     <ul>
     *     <li>Calls {@code _pool.evict()} to evict LRU entries based on the {@code vacatingFactor} threshold.</li>
     *     <li>Calls {@link #evict()} to release now-empty segments back to the pool.</li>
     *     <li>Records its completion time so that the debounce window suppresses immediately
     *         re-triggered vacations (without pinning a pool thread in a sleep).</li>
     *     </ul>
     * </li>
     * </ol>
     * If the asynchronous executor rejects the scheduling, the in-flight counter is released so
     * subsequent {@code vacate()} calls are not permanently silenced.
     *
     * <p>This method returns immediately after scheduling the vacating task, allowing the calling
     * thread to continue. This non-blocking behavior ensures that put operations do not hang
     * waiting for cleanup to complete.
     *
     * <p>Thread safety: this method is thread-safe and uses an atomic counter to ensure only
     * one vacating task runs at a time, even when called concurrently from multiple threads.
     */
    private void vacate() {
        // Debounce: skip if a vacate finished very recently, so a burst of failing
        // puts doesn't schedule redundant vacations while memory is still settling.
        if (System.currentTimeMillis() - _lastVacateFinishedTime < VACATE_DEBOUNCE_MILLIS) {
            return;
        }

        if (_activeVacationTaskCount.incrementAndGet() > 1) {
            _activeVacationTaskCount.decrementAndGet();
            return;
        }

        boolean scheduled = false;
        try {
            _asyncExecutor.execute(() -> {
                try {
                    _pool.evict();

                    evict();
                } finally {
                    // Record completion and release the gate immediately. The debounce
                    // window above (not a thread-pinning sleep) prevents an immediate
                    // re-vacation, so a fresh vacate can start as soon as it is needed.
                    _lastVacateFinishedTime = System.currentTimeMillis();
                    _activeVacationTaskCount.decrementAndGet();
                }
            });
            scheduled = true;
        } finally {
            // If the executor rejects the task (e.g., it is shutting down), we must
            // release the gate here. Otherwise the counter stays > 0 forever and every
            // subsequent vacate() call sees > 1 and returns without doing any work.
            if (!scheduled) {
                _activeVacationTaskCount.decrementAndGet();
            }
        }
    }

    /**
     * Removes the cache entry associated with the specified key, if present.
     * The underlying wrapper is destroyed, releasing any off-heap memory slots or
     * disk storage held by the entry.
     *
     * @param key the key whose mapping is to be removed from the cache; must not be {@code null}
     * @throws IllegalArgumentException if {@code key} is {@code null}
     */
    @Override
    public void remove(final K key) {
        N.checkArgNotNull(key, "key");

        final Wrapper<V> w = _pool.remove(key);

        if (w != null) {
            w.destroy(Caller.REMOVE_REPLACE_CLEAR);
        }
    }

    /**
     * Returns whether the cache contains a mapping for the specified key.
     *
     * @param key the key whose presence in the cache is to be tested; must not be {@code null}
     * @return {@code true} if the cache contains a mapping for the specified key; {@code false} otherwise
     * @throws IllegalArgumentException if {@code key} is {@code null}
     */
    @Override
    public boolean containsKey(final K key) {
        N.checkArgNotNull(key, "key");

        return _pool.containsKey(key);
    }

    /**
     * Returns a set view of all keys currently held in the cache,
     * including the keys of entries that have been spilled to disk.
     *
     * @return the set of cache keys; never {@code null}
     */
    @Override
    public Set<K> keySet() {
        return _pool.keySet();
    }

    /**
     * Returns the number of entries currently held in the cache (including disk-stored entries).
     *
     * @return the current cache size
     */
    @Override
    public int size() {
        return _pool.size();
    }

    /**
     * Removes all entries from the cache, destroying their wrappers and releasing
     * any associated off-heap memory and disk storage.
     */
    @Override
    public void clear() {
        _pool.clear();
        evict();
    }

    /**
     * Returns a point-in-time snapshot of cache statistics.
     * The snapshot includes entry counts, hit/miss counters, eviction counters, memory and disk usage,
     * disk I/O timing aggregates, and segment-slot occupancy details.
     *
     * <p>This method collects statistics from the underlying pool and segment queues, then assembles
     * an immutable {@link OffHeapCacheStats} view. Segment queues and the disk I/O timing aggregates
     * are synchronized while being read (under the same monitors used by
     * {@link #put(Object, Object, long, long)} and {@link #getOrNull(Object)}), so the returned
     * values are internally consistent for the capture moment.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * OffHeapCache<String, byte[]> cache = new OffHeapCache<>(100);
     * cache.put("key1", "value1".getBytes());
     * cache.put("key2", "value2".getBytes());
     *
     * OffHeapCacheStats stats = cache.stats();
     * System.out.println("Entries in memory: " + stats.size());
     * System.out.println("Entries on disk: " + stats.sizeOnDisk());
     * System.out.println("Hits: " + stats.hitCount() + "/" + stats.getCount());
     * System.out.println("Memory usage: " + stats.dataSize() + "/" + stats.allocatedMemory());
     * }</pre>
     *
     * @return the statistics snapshot for this cache instance; never {@code null}
     */
    public OffHeapCacheStats stats() {
        final PoolStats poolStats = _pool.stats();

        final Map<Integer, Map<Integer, Integer>> occupiedSlots = Stream.of(new ArrayList<>(_segmentQueueMap.values())).distinct().flatmap(queue -> {
            synchronized (queue) {
                return N.map(queue, it -> Tuple.of(it.sizeOfSlot, it.index, it.cardinality()));
            }
        })
                .sorted(Comparators.<Tuple3<Integer, Integer, Integer>> comparingInt(it -> it._1).thenComparingInt(it -> it._2))
                .groupTo(it -> it._1, Collectors.toMap(it -> it._2, it -> it._3, Suppliers.ofLinkedHashMap()), Suppliers.ofLinkedHashMap());

        // Snapshot stats under the same monitors writers use (see put() and getOrNull()).
        // LongSummaryStatistics is not thread-safe; reading count/min/max/average from a
        // concurrently mutated instance can yield a torn state where, e.g., count > 0 but
        // min is still Long.MAX_VALUE, or where average's internal sum and count were sampled
        // at different moments and so represent no real point-in-time average.
        // Clamp each component to >= 0 so a stray negative observation (e.g. recorded during a
        // backward wall-clock jump) can never make the non-negative MinMaxAvg constructor throw and
        // turn a monitoring call into a failure. Clamping all three keeps the snapshot self-consistent.
        final double writeMin;
        final double writeMax;
        final double writeAvg;
        synchronized (totalWriteToDiskTimeStats) {
            final long count = totalWriteToDiskTimeStats.getCount();
            writeMin = count > 0 ? Math.max(0.0D, totalWriteToDiskTimeStats.getMin()) : 0.0D;
            writeMax = count > 0 ? Math.max(0.0D, totalWriteToDiskTimeStats.getMax()) : 0.0D;
            writeAvg = count > 0 ? Math.max(0.0D, Numbers.round(totalWriteToDiskTimeStats.getAverage(), 2)) : 0.0D;
        }

        final MinMaxAvg writeToDiskTimeStats = new MinMaxAvg(writeMin, writeMax, writeAvg);

        final double readMin;
        final double readMax;
        final double readAvg;
        synchronized (totalReadFromDiskTimeStats) {
            final long count = totalReadFromDiskTimeStats.getCount();
            readMin = count > 0 ? Math.max(0.0D, totalReadFromDiskTimeStats.getMin()) : 0.0D;
            readMax = count > 0 ? Math.max(0.0D, totalReadFromDiskTimeStats.getMax()) : 0.0D;
            readAvg = count > 0 ? Math.max(0.0D, Numbers.round(totalReadFromDiskTimeStats.getAverage(), 2)) : 0.0D;
        }

        final MinMaxAvg readFromDiskTimeStats = new MinMaxAvg(readMin, readMax, readAvg);

        return new OffHeapCacheStats(poolStats.capacity(), poolStats.size(), sizeOnDisk.sum(), poolStats.putCount(), writeCountToDisk.sum(),
                poolStats.getCount(), poolStats.hitCount(), readCountFromDisk.sum(), poolStats.missCount(), poolStats.evictionCount(),
                evictionCountFromDisk.sum(), _capacityInBytes, totalOccupiedMemorySize.sum(), totalDataSize.sum(), dataSizeOnDisk.sum(), writeToDiskTimeStats,
                readFromDiskTimeStats, SEGMENT_SIZE, occupiedSlots);
    }

    /**
     * Closes the cache and releases all resources.
     * Cancels the scheduled eviction task, removes the JVM shutdown hook, closes the
     * underlying object pool, and deallocates all off-heap memory. This method is
     * idempotent: invoking it on an already-closed cache returns immediately. It is
     * also invoked automatically by the registered shutdown hook during JVM shutdown.
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
                        // Expected when close() is invoked from the shutdown hook itself (JVM already
                        // shutting down); the hook will run anyway, so this is safe to ignore.
                        if (logger.isDebugEnabled()) {
                            logger.debug("Could not remove shutdown hook because the JVM is already shutting down; ignoring", e);
                        }
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
     * Returns whether this cache has been closed.
     *
     * @return {@code true} if {@link #close()} has been called; {@code false} if the cache is still operational
     */
    @Override
    public boolean isClosed() {
        return _pool.isClosed();
    }

    /**
     * Performs periodic eviction of empty segments to release them back to the pool.
     * Internal maintenance method that reclaims memory segments that have become completely
     * empty (all slots freed) and makes them available for reuse with different slot sizes.
     * This process is essential for memory management because it allows segments to be
     * repurposed when workload patterns change.
     *
     * <p>Eviction process:
     * <ol>
     * <li>Iterates through all segments in the segment array.</li>
     * <li>For each segment with an empty slot {@link BitSet} (no allocated slots):
     *     <ul>
     *     <li>Locates the segment's queue in {@code _segmentQueueMap}.</li>
     *     <li>Uses double-checked locking to verify the segment is still empty.</li>
     *     <li>Removes the segment from its queue.</li>
     *     <li>Removes the segment from {@code _segmentQueueMap}.</li>
     *     <li>Clears the segment's bit in {@code _segmentBitSet}, marking it as available.</li>
     *     </ul>
     * </li>
     * </ol>
     *
     * <p>This method is called:
     * <ul>
     * <li>By the scheduled eviction task at regular intervals (if {@code evictDelay} &gt; 0).</li>
     * <li>By the {@code vacate()} method after evicting entries from the pool.</li>
     * <li>By {@link #clear()} after the pool is cleared, to release the now-empty segments.</li>
     * </ul>
     *
     * <p>The method uses double-checked locking to minimize lock contention while ensuring thread safety.
     * It first checks if a segment is empty without holding the queue lock, then re-checks after
     * acquiring the lock to ensure the segment has not been allocated in the meantime.
     *
     * <p>Thread safety: this method is thread-safe and uses fine-grained locking on
     * {@code _segmentBitSet} and individual segment queues. It can be called concurrently
     * with allocation operations.
     */
    protected void evict() {
        synchronized (_segmentBitSet) {
            for (int i = 0, len = _segments.length; i < len; i++) {
                if (_segments[i].isEmpty()) {
                    final Deque<Segment> queue = _segmentQueueMap.get(i);

                    if (queue != null) {
                        synchronized (queue) {
                            if (_segments[i].isEmpty()) {
                                queue.remove(_segments[i]);

                                _segmentQueueMap.remove(i);
                                _segmentBitSet.clear(i);

                                // A lower index just became free; make sure the
                                // next-free-segment scan can find it again.
                                if (i < _nextSegmentIndexHint) {
                                    _nextSegmentIndexHint = i;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Represents a memory segment that can be divided into fixed-size slots.
     * Each segment is {@link #SEGMENT_SIZE} bytes (1 MB) and can be configured with a
     * specific slot size. A segment can be reset and reused with a different slot size
     * once all of its slots are freed. A {@link BitSet} tracks which slots are allocated,
     * providing efficient allocation and deallocation operations.
     *
     * <p>This is an internal class used by {@link AbstractOffHeapCache} for memory management.
     * Segments are allocated from the fixed array of segments created during cache initialization.
     * The {@code sizeOfSlot} field can be reconfigured to different multiples of
     * {@link #MIN_BLOCK_SIZE} (64, 128, 256, 512, 1024, 2048, 4096, 8192, etc.) when the
     * segment becomes empty and is reused.
     *
     * <p>Thread safety: the slot allocation and deallocation methods ({@link #allocateSlot()} and
     * {@link #releaseSlot(int)}) are synchronized on the internal slot {@link BitSet} to ensure
     * thread-safe slot management. {@link #index()} simply returns an immutable field and
     * requires no synchronization.
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
         * <p><b>Usage Examples:</b>
         * <pre>{@code
         * final long segmentStartPtr = 0L;
         * final Segment segment = new Segment(segmentStartPtr, 0);
         * }</pre>
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
         * <p><b>Usage Examples:</b>
         * <pre>{@code
         * int index = segment.index();
         * }</pre>
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
         * <p><b>Usage Examples:</b>
         * <pre>{@code
         * int slotIndex = segment.allocateSlot();
         * }</pre>
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
         * <p><b>Usage Examples:</b>
         * <pre>{@code
         * int slotIndex = 0;
         * segment.releaseSlot(slotIndex);
         * }</pre>
         *
         * @param slotIndex the slot index to release
         */
        public void releaseSlot(final int slotIndex) {
            synchronized (slotBitSet) {
                slotBitSet.clear(slotIndex);
            }
        }

        /**
         * Returns the number of slots currently allocated in this segment.
         *
         * <p>{@link BitSet} is not thread-safe, so this read is performed under the same
         * {@code slotBitSet} monitor that {@link #allocateSlot()} and {@link #releaseSlot(int)}
         * use for mutation. Reading {@code cardinality()} without this monitor can observe a torn
         * state while another thread is in {@code set()}/{@code clear()} (which may resize the
         * backing word array), yielding a garbage count.
         *
         * @return the number of allocated slots
         */
        public int cardinality() {
            synchronized (slotBitSet) {
                return slotBitSet.cardinality();
            }
        }

        /**
         * Returns whether this segment currently has no allocated slots.
         *
         * <p>Like {@link #cardinality()}, this read is performed under the {@code slotBitSet}
         * monitor because {@link BitSet} is not thread-safe; an unsynchronized {@code isEmpty()}
         * can race with a concurrent {@code releaseSlot(int)}/{@code allocateSlot()}.
         *
         * @return {@code true} if no slots are allocated; {@code false} otherwise
         */
        public boolean isEmpty() {
            synchronized (slotBitSet) {
                return slotBitSet.isEmpty();
            }
        }

        @Override
        public String toString() {
            return "Segment [segmentStartPtr=" + segmentStartPtr + ", sizeOfSlot=" + sizeOfSlot + "]";
        }
    }

    /**
     * Represents an allocated memory slot within a segment.
     * Encapsulates the slot index and its parent segment, providing a convenient way to
     * release the slot when it is no longer needed.
     *
     * <p>This is an internal record used by {@link AbstractOffHeapCache} for memory management.
     * The slot must be released explicitly by calling {@link #release()} to free the memory
     * for reuse. If a slot is allocated but not properly released, it will result in a memory
     * leak within the segment (the slot remains marked as occupied in the segment's
     * {@link BitSet}).
     *
     * <p>Thread safety: {@link #release()} is thread-safe because it delegates to the
     * thread-safe {@link Segment#releaseSlot(int)} method.
     *
     * @param indexOfSlot the slot index within the parent segment
     *                    (0 to {@code SEGMENT_SIZE/sizeOfSlot - 1})
     * @param segment the parent {@link Segment} that contains this slot
     */
    record Slot(int indexOfSlot, Segment segment) {

        /**
         * Releases this slot back to its parent segment, making it available for reuse.
         * Delegates to {@link Segment#releaseSlot(int)} to clear the corresponding bit in
         * the segment's allocation {@link BitSet}.
         *
         * <p>Thread safety: this method is thread-safe.
         */
        void release() {
            segment.releaseSlot(indexOfSlot);
        }
    }

    /**
     * Base wrapper class for cached values with metadata.
     * Extends {@link AbstractPoolable} to support TTL and idle-time tracking through the
     * object pool. Each wrapper stores the value's type information and serialized size.
     *
     * <p>This is an internal abstract class with three concrete implementations for the
     * different storage strategies:
     * <ul>
     * <li>{@link SlotWrapper} - values stored in a single memory slot ({@code size <= maxBlockSize}).</li>
     * <li>{@link MultiSlotsWrapper} - values stored across multiple memory slots ({@code size > maxBlockSize}).</li>
     * <li>{@link StoreWrapper} - values stored on disk via {@link OffHeapStore}.</li>
     * </ul>
     *
     * <p>All wrappers are managed by the {@link KeyedObjectPool}, which handles expiration and
     * eviction based on {@code liveTime} and {@code maxIdleTime} settings. The
     * {@link #destroy(Caller)} method is called by the pool when entries are evicted, removed,
     * or during cache shutdown, to release resources.
     *
     * <p>Thread safety: concrete implementations must ensure thread-safe {@link #read()} and
     * {@link #destroy(Caller)} operations, typically through synchronization.
     *
     * @param <T> the value type
     */
    abstract static class Wrapper<T> extends AbstractPoolable {
        /** Discriminator: value stored in a single off-heap slot. */
        static final int KIND_SLOT = 1;
        /** Discriminator: value stored across multiple off-heap slots. */
        static final int KIND_MULTI_SLOTS = 2;
        /** Discriminator: value stored on disk via {@link OffHeapStore}. */
        static final int KIND_STORE = 3;

        // Final discriminator set once at construction. Used instead of
        // getClass()/Class.equals(...) dispatch on the hot get/put paths (a
        // generic inner-class instanceof here triggers unchecked-cast warnings,
        // which is why an int tag is used rather than instanceof).
        final int kind;
        final Type<T> type;
        final int size;

        /**
         * Constructs a new Wrapper with the specified metadata.
         *
         * @param type the value type information for deserialization
         * @param liveTime the time-to-live in milliseconds; must be positive (callers translate
         *                 non-positive values to {@code Long.MAX_VALUE})
         * @param maxIdleTime the maximum idle time in milliseconds; must be positive (callers translate
         *                    non-positive values to {@code Long.MAX_VALUE})
         * @param size the serialized size of the value in bytes
         * @param kind the storage-strategy discriminator ({@link #KIND_SLOT}, {@link #KIND_MULTI_SLOTS} or {@link #KIND_STORE})
         */
        Wrapper(final Type<T> type, final long liveTime, final long maxIdleTime, final int size, final int kind) {
            super(liveTime, maxIdleTime);

            this.type = type;
            this.size = size;
            this.kind = kind;
        }

        /**
         * Reads and deserializes the cached value.
         * Concrete implementations retrieve the raw bytes from their storage location
         * (memory or disk) and deserialize them based on the value type.
         *
         * <p>Thread safety: implementations must be thread-safe.
         *
         * @return the deserialized value, or {@code null} if the value cannot be retrieved
         *         (for example, when the entry was already destroyed by concurrent eviction)
         * @throws IllegalStateException if the retrieved data size does not match the recorded
         *                               size (data corruption detected)
         */
        abstract T read();
    }

    /**
     * Wrapper for values stored in a single memory slot.
     * Used for values that fit within the configured maximum block size ({@code maxBlockSize}).
     * The slot reference is used to release the memory when the entry is evicted or removed.
     *
     * <p>This is an internal class used by {@link AbstractOffHeapCache}. The wrapper is
     * thread-safe via synchronized access to its read and destroy operations.
     *
     * <p>Memory layout: the value's serialized bytes are stored at the memory address
     * {@code slotStartPtr = segment.segmentStartPtr + slot.indexOfSlot * segment.sizeOfSlot}.
     * The actual data size may be less than the slot size ({@code segment.sizeOfSlot}), with
     * the difference being wasted space (internal fragmentation).
     *
     * <p>Thread safety: both {@link #read()} and {@link #destroy(Caller)} are synchronized
     * on the wrapper instance to prevent concurrent access issues.
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
            super(type, liveTime, maxIdleTime, size, KIND_SLOT);

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
                    totalOccupiedMemorySize.add(-slot.segment.sizeOfSlot);
                    totalDataSize.add(-size);
                    slot.release();
                    slot = null;
                }
            }
        }
    }

    /**
     * Wrapper for values stored across multiple memory slots.
     * Used for large values that exceed the configured maximum block size
     * ({@code maxBlockSize}). The value is split across multiple slots (potentially in
     * different segments), and all slots are released together when the entry is evicted
     * or removed.
     *
     * <p>This is an internal class used by {@link AbstractOffHeapCache}. The wrapper is
     * thread-safe via synchronized access to its read and destroy operations.
     *
     * <p>Memory layout: the value's serialized bytes are split into chunks of up to
     * {@code maxBlockSize} each. Each chunk is stored in a separate slot, which may be in
     * a different segment. The slots list maintains the order of chunks, so during
     * {@link #read()}, the bytes are reassembled in the correct order.
     *
     * <p>Thread safety: both {@link #read()} and {@link #destroy(Caller)} are synchronized
     * on the wrapper instance to prevent concurrent access issues.
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
         * @param slots the list of allocated slots holding the value chunks, in order
         */
        MultiSlotsWrapper(final Type<V> type, final long liveTime, final long maxIdleTime, final int size, final List<Slot> slots) {
            super(type, liveTime, maxIdleTime, size, KIND_MULTI_SLOTS);

            this.slots = slots;
        }

        @Override
        V read() {
            synchronized (this) {
                if (slots == null) {
                    return null; // Already destroyed by concurrent eviction
                }

                final byte[] bytes = new byte[size];
                int remaining = this.size;
                int destOffset = _arrayOffset;
                Segment segment = null;

                for (final Slot slot : slots) {
                    segment = slot.segment;
                    final long startPtr = segment.segmentStartPtr + (long) slot.indexOfSlot * segment.sizeOfSlot;
                    final int sizeToCopy = Math.min(remaining, segment.sizeOfSlot);

                    copyFromMemory(startPtr, bytes, destOffset, sizeToCopy);

                    destOffset += sizeToCopy;
                    remaining -= sizeToCopy;
                }

                // should never happen.
                if (remaining != 0) {
                    throw new IllegalStateException(
                            "Failed to retrieve value: " + remaining + " bytes remaining after reading all segments (data corruption detected)");
                }

                // `bytes` is a freshly read private copy of the value: expose it directly for raw
                // byte[]/ByteBuffer types, otherwise hand it to the deserializer.
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
                        totalOccupiedMemorySize.add(-slot.segment.sizeOfSlot);
                        slot.release();
                    }

                    totalDataSize.add(-size);

                    slots = null;
                }
            }
        }
    }

    /**
     * Wrapper for values stored on disk via {@link OffHeapStore}.
     * Used when memory is full or when the {@code storeSelector} determines the value
     * should be stored on disk. The {@code permanentKey} is used to retrieve and remove
     * the value from the disk store.
     *
     * <p>This is an internal class used by {@link AbstractOffHeapCache}. The wrapper is
     * thread-safe via synchronized access to its read and destroy operations.
     *
     * <p>Statistics: upon construction this wrapper increments the {@code sizeOnDisk},
     * {@code dataSizeOnDisk}, and {@code totalDataSize} counters. Upon destruction these
     * counters are decremented accordingly.
     *
     * <p>Thread safety: {@link #readBytes()} and {@link #destroy(Caller)} are synchronized
     * on the wrapper instance to prevent concurrent access issues. {@link #deserialize(byte[])}
     * is intentionally not synchronized so the (potentially expensive) deserialization step
     * can run outside the lock.
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
            super(type, liveTime, maxIdleTime, size, KIND_STORE);

            this.permanentKey = permanentKey;

            sizeOnDisk.increment();
            dataSizeOnDisk.add(size);
            totalDataSize.add(size);
        }

        /**
         * Reads the raw serialized bytes from disk exactly once. The destroyed-check
         * and the disk fetch are performed under the wrapper lock; the (potentially
         * expensive) deserialization is deliberately left to {@link #deserialize(byte[])}
         * so it can run outside the lock and so callers can reuse the bytes (e.g. to
         * promote the value back into memory without a second disk read).
         *
         * @return the serialized bytes, or {@code null} if the entry was already
         *         destroyed by concurrent eviction or is missing from the store
         * @throws IllegalStateException if the fetched size does not match the recorded size
         */
        byte[] readBytes() {
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

                return bytes;
            }
        }

        /**
         * Deserializes bytes previously obtained from {@link #readBytes()} into the value.
         * Operates only on the supplied local array (and the effectively-immutable type /
         * deserializer), so it is safe to invoke outside the wrapper lock.
         *
         * @param bytes the serialized bytes; must not be {@code null}
         * @return the deserialized value
         */
        V deserialize(final byte[] bytes) {
            if (type.isPrimitiveByteArray()) {
                return (V) bytes;
            } else if (type.isByteBuffer()) {
                return (V) ByteBufferType.valueOf(bytes);
            } else {
                return deserializer.apply(bytes, type);
            }
        }

        @Override
        V read() {
            final byte[] bytes = readBytes();

            return bytes == null ? null : deserialize(bytes);
        }

        void discardReplacedStoreMetadata() {
            synchronized (this) {
                if (permanentKey != null) {
                    sizeOnDisk.decrement();
                    dataSizeOnDisk.add(-size);
                    totalDataSize.add(-size);
                    permanentKey = null;
                }
            }
        }

        @Override
        public void destroy(final Caller caller) {
            synchronized (this) {
                if (permanentKey != null) {
                    if (caller == Caller.EVICT || caller == Caller.VACATE) {
                        evictionCountFromDisk.increment();
                    }

                    sizeOnDisk.decrement();
                    dataSizeOnDisk.add(-size);

                    totalDataSize.add(-size);

                    offHeapStore.remove(permanentKey);
                    permanentKey = null;
                }
            }
        }
    }

}
