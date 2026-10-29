/*
 * Copyright (C) 2025 HaiYang Li
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.landawn.abacus.cache;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import com.landawn.abacus.cache.OffHeapCacheStats.MinMaxAvg;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.parser.Parser;
import com.landawn.abacus.parser.ParserFactory;
import com.landawn.abacus.pool.ActivityPrint;
import com.landawn.abacus.type.ByteBufferType;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.ByteArrayOutputStream;
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.MoreExecutors;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Numbers;
import com.landawn.abacus.util.Objectory;
import com.landawn.abacus.util.function.TriFunction;
import com.landawn.abacus.util.function.TriPredicate;

/**
 * Abstract base class for off-heap caches that store serialized values in native memory outside
 * the JVM heap, with optional spill-over to a disk-backed {@link OffHeapStore}. Concrete
 * subclasses supply the native-memory primitives: {@link OffHeapCache} uses
 * {@code sun.misc.Unsafe} and {@link ForeignMemoryOffHeapCache} uses the Foreign Function &amp;
 * Memory API ({@code java.lang.foreign}).
 *
 * <p><b>Memory model.</b> The configured capacity is divided into fixed 1 MB segments
 * ({@link #SEGMENT_SIZE}). Each segment is dynamically dedicated to one slot size — any multiple
 * of the 64-byte {@link #MIN_BLOCK_SIZE} up to {@code maxBlockSize} — and carved into equal slots
 * of that size. A value whose serialized form fits in {@code maxBlockSize} occupies a single slot
 * of the smallest sufficient size class; a larger value is split into {@code maxBlockSize}-sized
 * chunks, each in its own slot (the final, possibly smaller chunk uses the smallest sufficient
 * class). A segment whose slots are all freed is reclaimed &mdash; by an allocation that finds no
 * free segment, the periodic maintenance pass, a vacate pass, or {@link #clear()} &mdash; and can
 * then be re-dedicated to a different slot size. A value that could not be placed even in an
 * otherwise empty region &mdash; because its serialized form, or the whole segments its
 * slot-rounded chunks require, exceed the capacity &mdash; never attempts in-memory placement.
 *
 * <p><b>Concurrency.</b> Four mechanisms, with the lock order map bin &rarr; entry monitor
 * &rarr; allocator lock (no code path acquires them in any other order):
 * <ul>
 * <li>Entries live in a {@link ConcurrentHashMap}; every same-key mutation (put, disk spill,
 *     promotion, removal) runs inside the map's atomic per-key {@code compute}, so no two
 *     mutations of one key can interleave. Only {@link #close()} empties the map outside
 *     {@code compute}, while it holds the lifecycle write lock.</li>
 * <li>Each entry's monitor guards its resources: readers copy native memory (or fetch store
 *     bytes) under it, and every path that frees or overwrites those resources holds it first, so
 *     a read can never observe freed memory or another entry's bytes.</li>
 * <li>A cache-wide read-write lock makes {@link #close()} (write side) mutually exclusive with
 *     all in-flight operations that touch native memory or the store (read side), so neither is
 *     ever released underneath one. (Heap-only probes such as {@code containsKey}, {@code size},
 *     {@code keySet}, and {@code stats} do not take it.)</li>
 * <li>A single allocator lock serializes all segment/slot bookkeeping.</li>
 * </ul>
 *
 * <p><b>Expiration.</b> Each entry carries an {@link ActivityPrint} with its TTL and idle timeout
 * ({@code <= 0} means "no limit" per the {@link Cache} contract). Expiry is enforced lazily on
 * access and, when {@code evictDelay > 0}, by a periodic maintenance pass that also reclaims
 * empty segments.
 *
 * <p><b>Memory pressure.</b> When slot allocation fails for a value that could fit, the put
 * falls back to the disk store when one is configured. If no store absorbs the value — because
 * none is configured, because the {@code storeSelector} restricted the value to memory, or
 * because the store write was rejected — the put returns {@code false} and
 * schedules an asynchronous, debounced vacate pass that evicts the least-recently-accessed
 * ~{@code vacatingFactor} of memory-resident entries and reclaims their now-empty segments.
 *
 * <p><b>Disk tier.</b> With an {@link OffHeapStore} configured, values that cannot be placed in
 * memory (or that the {@code storeSelector} routes to disk) are written to the store under the
 * cache key. The store write and the entry installation happen inside the same per-key atomic
 * operation; a failed or throwing write leaves the prior mapping and its bytes untouched (the
 * {@link OffHeapStore#put} contract guarantees the prior bytes are unchanged on failure). Reads
 * may optionally be promoted back into memory via the configured
 * {@code testerForLoadingItemFromDiskToMemory}. Promotion preserves the entry's original TTL
 * deadline, latest access time, idle limit, and access count. If either expiration limit is
 * exceeded while promotion is copying the bytes, the copy is discarded.
 *
 * <p><b>Value handling.</b> {@code byte[]} values are stored raw (exactly {@code array.length}
 * bytes); {@link ByteBuffer} values store the bytes from index 0 up to the current position
 * (position, limit, and mark are left untouched). All buffer inputs, including direct, read-only,
 * and file-mapped buffers, read back as independent writable, array-backed heap buffers. Directness,
 * read-only status, and the original file mapping are not preserved. Use {@code ByteBuffer} as the
 * cache's buffer value type; {@code V = MappedByteBuffer} is unsupported because the
 * {@link java.nio.MappedByteBuffer} subtype is not preserved. All other types go through the configured
 * serializer/deserializer, defaulting to Kryo when available, otherwise JSON. For disk reads that
 * may be promoted to memory, the deserializer receives a private copy so even an in-place decoder
 * cannot alter the bytes installed in the memory tier.
 *
 * @param <K> the key type
 * @param <V> the value type
 * @see OffHeapCache
 * @see ForeignMemoryOffHeapCache
 * @see OffHeapCacheStats
 * @see OffHeapStore
 */
abstract class AbstractOffHeapCache<K, V> extends AbstractCache<K, V> {

    /** Default fraction of entries evicted by a vacate pass triggered by memory pressure. */
    static final float DEFAULT_VACATING_FACTOR = 0.2f;

    /**
     * Default parser used for serialization when no custom serializer is configured.
     * Uses Kryo if it is on the classpath, otherwise falls back to JSON.
     */
    static final Parser<?, ?> PARSER = ParserFactory.isKryoParserAvailable() ? ParserFactory.createKryoParser() : ParserFactory.createJsonParser();

    /** Default serializer used when no custom serializer is configured. Delegates to {@link #PARSER}. */
    static final BiConsumer<Object, ByteArrayOutputStream> SERIALIZER = PARSER::serialize;

    /**
     * Default deserializer used when no custom deserializer is configured.
     * Returns the raw bytes for {@code byte[]} types, reconstructs a writable, array-backed heap
     * {@link ByteBuffer} for buffer types, and otherwise delegates to {@link #PARSER}.
     */
    static final BiFunction<byte[], Type<?>, Object> DESERIALIZER = (bytes, type) -> {
        if (type.isPrimitiveByteArray()) {
            return bytes;
        } else if (type.isByteBuffer()) {
            return ByteBufferType.valueOf(bytes);
        } else {
            return PARSER.deserialize(new ByteArrayInputStream(bytes), type.javaType());
        }
    };

    /** Size of each memory segment in bytes (1 MB). */
    static final int SEGMENT_SIZE = 1024 * 1024;

    /** Minimum slot size in bytes. All slot sizes are rounded up to a multiple of this value. */
    static final int MIN_BLOCK_SIZE = 64;

    /** Default maximum size for a single memory block (8 KB). */
    static final int DEFAULT_MAX_BLOCK_SIZE = 8192;

    /**
     * Bits used for the slot index inside a packed slot handle. A segment holds at most
     * {@code SEGMENT_SIZE / MIN_BLOCK_SIZE} = 16384 = 2^14 slots, so 14 bits always suffice.
     */
    private static final int SLOT_INDEX_BITS = 14;
    private static final int SLOT_INDEX_MASK = (1 << SLOT_INDEX_BITS) - 1;

    /** Suppresses a fresh vacate pass for this long after the previous one finished. */
    private static final long VACATE_DEBOUNCE_NANOS = TimeUnit.MILLISECONDS.toNanos(3000);

    /** {@code storeSelector} results: memory first with disk fallback, memory only, disk only. */
    private static final int STORE_DEFAULT = 0;
    private static final int STORE_MEMORY_ONLY = 1;
    private static final int STORE_DISK_ONLY = 2;

    /**
     * Number of keys with orphaned store bytes at which the first high-water warning is logged. Each
     * cache's warning mark starts here, doubles after every warning, and restarts here only once a
     * {@code clear()} has removed all of them.
     */
    static final int ORPHANED_STORE_KEYS_WARN_THRESHOLD = 1024;

    /**
     * Shared scheduled executor used for the periodic maintenance task of all off-heap cache
     * instances created from this class.
     */
    static final ScheduledExecutorService SCHEDULED_EXECUTOR;

    static {
        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(IOUtil.CPU_CORES);
        // Without this, a cancelled maintenance task sits in the executor's queue until its next
        // scheduled run, holding a strong reference to the closed cache and preventing GC.
        executor.setRemoveOnCancelPolicy(true);
        SCHEDULED_EXECUTOR = MoreExecutors.getExitingScheduledExecutorService(executor);
    }

    /** Why an entry is being freed; determines eviction counting. */
    private enum FreeCause {
        /** Superseded by a new same-key entry; never counted as an eviction. */
        REPLACED,
        /** Explicit remove()/clear()/close(), a stale mapping whose store bytes vanished, or a put's entry the map failed to link. */
        REMOVED,
        /** TTL/idle expiry (lazy on access, or the maintenance sweep). Counted as an eviction. */
        EXPIRED,
        /** Reclaimed by the memory-pressure vacate pass. Counted as an eviction. */
        EVICTED
    }

    // ------------------------------------------------------------------------------------- state

    /** The concrete subclass logger, used for all cache log output. */
    final Logger logger;

    /** Total size of the native off-heap region in bytes ({@code capacityInMB * 1048576}). */
    final long capacityInBytes;
    /** Base address of the native region returned by {@link #allocate(long)}. */
    final long baseAddress;
    /** Zero-based heap-array index adjustment supplied to the copy hooks; both built-in implementations pass 0. */
    final int arrayOffset;
    /** Maximum size of a single memory block in bytes, rounded up to a multiple of {@link #MIN_BLOCK_SIZE}. */
    final int maxBlockSize;

    /** All entries, both memory-backed and disk-backed. Same-key mutations use the map's atomic per-key compute. */
    private final ConcurrentHashMap<K, Entry<V>> entries = new ConcurrentHashMap<>();

    /**
     * Keys whose store bytes a failed disk put left behind and whose guarded removal also failed;
     * clear() retries them and close() removes them. Identity-based, because hashing the keys would
     * call the same failing methods. Unbounded: the store key-only API offers no other way to find
     * those bytes again, and each key reference is small next to the bytes it accounts for.
     * Guarded by its own monitor.
     */
    private final Set<K> orphanedStoreKeys = Collections.newSetFromMap(new IdentityHashMap<>());
    /** Size of {@link #orphanedStoreKeys} at which the next high-water warning is logged; guarded by that set's monitor. */
    private int orphanedStoreKeysWarnAt = ORPHANED_STORE_KEYS_WARN_THRESHOLD;
    /**
     * Keys drained by clear() retries that are still running, and so may yet be recorded again;
     * guarded by the set's monitor. Concurrent clear() calls (they share the lifecycle read lock)
     * must not take a momentarily empty set for "all removed".
     */
    private int orphanedStoreKeysInRetry;

    /**
     * Close() takes the write side while operations touching native memory or the store hold the
     * read side, so neither resource is released underneath an in-flight operation. Heap-only
     * probes do not acquire this lock.
     */
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();

    private volatile boolean closed;

    // --- allocator (all fields below are guarded by allocatorLock) ------------------------------

    /** The single allocator mutex: segment dedication, slot allocation/release, and reclamation. */
    private final Object allocatorLock = new Object();
    /** Marks which segments are currently dedicated to a slot-size class. */
    private final BitSet dedicatedSegments = new BitSet();
    private final Segment[] segments;
    /**
     * Per size class, only the dedicated segments with free slots; full segments stay indexed by
     * dedicatedSegments. A class's queue is created on its first use and is {@code null} until then.
     */
    private final Deque<Segment>[] segmentQueues;
    /** Lower bound for the next free-segment scan. */
    private int nextFreeSegmentHint = 0;
    /** Number of dedicated segments with no occupied slot; lets reclamation skip a futile scan. */
    private int emptyDedicatedSegmentCount = 0;

    // --- statistics ------------------------------------------------------------------------

    // LongAdder (rather than AtomicLong) because these counters are write-heavy on the hot
    // get/put/evict paths and only read in stats().
    private final LongAdder hitCount = new LongAdder();
    private final LongAdder missCount = new LongAdder();
    private final LongAdder putCount = new LongAdder();
    private final LongAdder evictionCount = new LongAdder();
    private final LongAdder evictionCountFromDisk = new LongAdder();
    private final LongAdder totalOccupiedMemorySize = new LongAdder();
    private final LongAdder totalDataSize = new LongAdder();
    private final LongAdder dataSizeOnDisk = new LongAdder();
    private final LongAdder sizeOnDisk = new LongAdder();
    /**
     * Live entries, maintained by install/free; read by size() and stats() instead of
     * entries.size(). ConcurrentHashMap updates its own count only after a compute has linked the
     * node and reorganized its bin, so a failure there (e.g. a key whose compareTo throws while an
     * overlong bin is treeified) leaves that count permanently off by the linked entry.
     */
    private final LongAdder entryCount = new LongAdder();
    // Named after the OffHeapCacheStats components they populate.
    private final LongAdder putCountToDisk = new LongAdder();
    private final LongAdder hitCountFromDisk = new LongAdder();

    private final TimingStats writeToDiskTimes = new TimingStats();
    private final TimingStats readFromDiskTimes = new TimingStats();

    // --- vacate ----------------------------------------------------------------------------

    /** Single-flight gate: at most one asynchronous vacate pass runs at a time. */
    private final AtomicBoolean vacating = new AtomicBoolean();
    /** Initialized one debounce interval in the past so the first pass is never suppressed. */
    private volatile long lastVacateFinishedNanos = System.nanoTime() - VACATE_DEBOUNCE_NANOS;

    // --- configuration -----------------------------------------------------------------------

    /** Fraction of entries evicted by a memory-pressure vacate pass; a configured {@code 0} is replaced with the default. */
    final float vacatingFactor;
    /** Serializer for non-raw values; defaults to Kryo (if available) or JSON when none is configured. */
    final BiConsumer<? super V, ByteArrayOutputStream> serializer;
    /** Deserializer for non-raw values; defaults to Kryo (if available) or JSON when none is configured. */
    final BiFunction<byte[], Type<V>, ? extends V> deserializer;
    /** Disk spill-over store, or {@code null} for a memory-only cache; owned and closed by this cache. */
    final OffHeapStore<K> offHeapStore;
    /** Whether disk I/O timing statistics are recorded. */
    final boolean statsTimeOnDisk;
    /** Optional predicate deciding when a disk read is promoted back into memory; {@code null} disables promotion. */
    final TriPredicate<ActivityPrint, Integer, Long> testerForLoadingItemFromDiskToMemory;
    /** Optional per-put routing function returning 0 (memory, disk fallback), 1 (memory only), or 2 (disk only). */
    final TriFunction<K, V, Integer, Integer> storeSelector;
    /** Store reads are timed for the timing statistics AND for the promotion tester's read-time argument. */
    private final boolean measureStoreReadTime;

    private ScheduledFuture<?> maintenanceFuture;
    private Thread shutdownHook;

    // ------------------------------------------------------------------------------ construction

    /**
     * Constructs an {@code AbstractOffHeapCache} with the specified configuration: allocates the
     * native region, prepares the segment allocator, schedules the periodic maintenance pass
     * (expiry sweep + empty-segment reclamation) when {@code evictDelay > 0}, and registers a JVM
     * shutdown hook that closes the cache. If any initialization step after the native allocation
     * fails, everything acquired so far (native memory, scheduled task, the supplied store) is
     * released before the failure propagates.
     *
     * @param capacityInMB the total off-heap capacity in megabytes; must be positive
     * @param maxBlockSize the maximum single-slot size in bytes, in {@code [1024, SEGMENT_SIZE]};
     *                     rounded up to a multiple of {@link #MIN_BLOCK_SIZE}
     * @param evictDelay the delay in milliseconds between maintenance passes; {@code 0} or a
     *                   negative value disables the periodic pass (lazy expiry still applies)
     * @param defaultLiveTime the default TTL in milliseconds for entries added without an explicit
     *                        one; {@code <= 0} means no expiration
     * @param defaultMaxIdleTime the default maximum idle time in milliseconds for entries added
     *                           without an explicit one; {@code <= 0} means no idle limit
     * @param vacatingFactor the fraction of entries evicted by a memory-pressure vacate pass, in
     *                       {@code [0.0, 1.0]}; {@code 0} selects the default (0.2)
     * @param arrayOffset the zero-based heap-array index adjustment supplied to the copy hooks;
     *                    both built-in implementations pass 0
     * @param serializer custom serializer, or {@code null} for the default (Kryo/JSON)
     * @param deserializer custom deserializer, or {@code null} for the default
     * @param offHeapStore optional disk store for spill-over; {@code null} for memory-only. The
     *                     cache owns it and closes it in {@link #close()}
     * @param statsTimeOnDisk whether to record disk I/O timing statistics
     * @param testerForLoadingItemFromDiskToMemory optional predicate deciding when a disk read is
     *                                             promoted back into memory; receives the entry's
     *                                             live {@link ActivityPrint}, its serialized size,
     *                                             and the store-read time in milliseconds
     * @param storeSelector optional per-put routing function returning 0 (memory, disk fallback),
     *                      1 (memory only), or 2 (disk only)
     * @param logger the concrete subclass logger
     * @throws IllegalArgumentException if {@code capacityInMB} is not positive, if {@code maxBlockSize}
     *                                  is outside {@code [1024, SEGMENT_SIZE]}, if {@code vacatingFactor}
     *                                  is outside {@code [0.0, 1.0]} (or is NaN), or if {@code logger}
     *                                  is {@code null}
     * @throws OutOfMemoryError if the Java heap cannot hold the per-segment bookkeeping (one
     *                          {@code Segment} per MB, created before any native memory is requested;
     *                          possible for a very large {@code capacityInMB}), or if the native
     *                          allocation cannot be reserved
     * @throws RejectedExecutionException if {@code evictDelay} is positive and the maintenance
     *                                    scheduler rejects its task (all cache-owned resources are
     *                                    released before this propagates)
     * @throws IllegalStateException if the JVM is already shutting down when the shutdown hook is
     *                               registered (all cache-owned resources are released before this
     *                               propagates)
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected AbstractOffHeapCache(final int capacityInMB, final int maxBlockSize, final long evictDelay, final long defaultLiveTime,
            final long defaultMaxIdleTime, final float vacatingFactor, final int arrayOffset, final BiConsumer<? super V, ByteArrayOutputStream> serializer,
            final BiFunction<byte[], Type<V>, ? extends V> deserializer, final OffHeapStore<K> offHeapStore, final boolean statsTimeOnDisk,
            final TriPredicate<ActivityPrint, Integer, Long> testerForLoadingItemFromDiskToMemory, final TriFunction<K, V, Integer, Integer> storeSelector,
            final Logger logger) throws IllegalArgumentException, OutOfMemoryError, RejectedExecutionException, IllegalStateException {
        super(defaultLiveTime, defaultMaxIdleTime);

        N.checkArgPositive(capacityInMB, cs.capacityInMB);
        N.checkArgument(maxBlockSize >= 1024 && maxBlockSize <= SEGMENT_SIZE, "maxBlockSize must be in the range [1024, {}]: {}", SEGMENT_SIZE, maxBlockSize);
        N.checkArgument(vacatingFactor >= 0f && vacatingFactor <= 1f, "vacatingFactor must be in the range [0.0, 1.0]: {}", vacatingFactor);

        this.logger = N.checkArgNotNull(logger, cs.logger);
        this.arrayOffset = arrayOffset;
        capacityInBytes = capacityInMB * (1024L * 1024L);
        this.maxBlockSize = roundUpToMinBlock(maxBlockSize);
        this.vacatingFactor = vacatingFactor == 0f ? DEFAULT_VACATING_FACTOR : vacatingFactor;
        this.serializer = serializer == null ? (BiConsumer<V, ByteArrayOutputStream>) SERIALIZER : serializer;
        this.deserializer = deserializer == null ? (BiFunction) DESERIALIZER : deserializer;
        this.offHeapStore = offHeapStore;
        this.statsTimeOnDisk = statsTimeOnDisk;
        this.testerForLoadingItemFromDiskToMemory = testerForLoadingItemFromDiskToMemory;
        this.storeSelector = storeSelector;
        measureStoreReadTime = statsTimeOnDisk || testerForLoadingItemFromDiskToMemory != null;

        segments = new Segment[(int) (capacityInBytes / SEGMENT_SIZE)];

        for (int i = 0, len = segments.length; i < len; i++) {
            segments[i] = new Segment(i);
        }

        // Queues are created on first use (see vacancyQueueOf): most of the possible size classes
        // (up to 16384 per cache) are never used.
        segmentQueues = new Deque[this.maxBlockSize / MIN_BLOCK_SIZE];

        baseAddress = allocate(capacityInBytes);

        // From this point on a failing initialization step would leak the native allocation
        // (close() is not reachable for a half-constructed instance), so release everything
        // acquired so far before propagating.
        try {
            if (evictDelay > 0) {
                maintenanceFuture = SCHEDULED_EXECUTOR.scheduleWithFixedDelay(this::runMaintenance, evictDelay, evictDelay, TimeUnit.MILLISECONDS);
            }

            shutdownHook = new Thread(() -> {
                logger.info("Closing off-heap cache on JVM shutdown");
                close();
            }, "abacus-offheap-cache-shutdown");

            Runtime.getRuntime().addShutdownHook(shutdownHook);
        } catch (final RuntimeException | Error initFailure) {
            Throwable failure = initFailure;
            failure = runCleanupStep(failure, () -> {
                if (maintenanceFuture != null) {
                    maintenanceFuture.cancel(true);
                }
            });
            failure = runCleanupStep(failure, this::deallocate);
            runCleanupStep(failure, () -> {
                if (offHeapStore != null) {
                    offHeapStore.close();
                }
            });

            throw initFailure;
        }
    }

    /** Periodic maintenance: removes expired entries and reclaims empty segments. */
    private void runMaintenance() {
        // Keep close() from deallocating while this pass is traversing cache structures. A pass
        // admitted just before cancellation observes the closed flag and becomes a no-op.
        lifecycleLock.readLock().lock();
        try {
            if (!closed) {
                sweepExpiredEntries();
                reclaimEmptySegments();
            }
        } catch (final Throwable e) {
            // Swallowed so the scheduled task keeps running; the pass retries on the next cycle.
            // scheduleWithFixedDelay silently suppresses every future run once any throwable
            // escapes. That includes an Error, and a checked exception thrown undeclared by a
            // user-supplied OffHeapStore or key (e.g. one written in Kotlin), so catch Throwable.
            logger.warn("Off-heap cache maintenance pass failed; will retry on the next scheduled run", e);
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    // ------------------------------------------------------------------------- subclass hooks

    /**
     * Allocates the specified amount of off-heap memory. Called exactly once, during construction.
     *
     * @param capacityInBytes the number of bytes to allocate; always positive and a multiple of
     *                        {@link #SEGMENT_SIZE}
     * @return the base address of the allocated region, used for all subsequent memory access
     * @throws IllegalArgumentException if {@code capacityInBytes} is negative (both built-in
     *         backends reject a negative size this way; unreachable through normal construction
     *         because {@code capacityInMB} is validated to be positive first)
     * @throws OutOfMemoryError if the implementation cannot reserve the requested amount of native memory
     */
    protected abstract long allocate(long capacityInBytes) throws IllegalArgumentException, OutOfMemoryError;

    /**
     * Releases the off-heap memory reserved by {@link #allocate(long)}. Called at most once, from
     * the lifecycle-write-locked {@link #close()} or from the constructor's failure-cleanup path
     * before the instance is published, so implementations need not be thread-safe.
     *
     * @throws RuntimeException if the native-memory backend fails to release its allocation
     */
    protected abstract void deallocate() throws RuntimeException;

    /**
     * Copies bytes from a heap array into off-heap memory. May be called concurrently for
     * different (disjoint) destination regions.
     *
     * @param startPtr the destination address in off-heap memory
     * @param bytes the source array; must not be {@code null}, including for a zero-length copy
     * @param srcOffset the zero-based source-array index, plus the {@code arrayOffset} supplied at
     *                  construction. A backend that needs an object-layout base offset must add it
     *                  using {@code long} arithmetic in this hook.
     * @param len the number of bytes to copy
     * @throws IllegalArgumentException if {@code bytes} is {@code null}
     * @throws RuntimeException if the backend rejects the source array, address, offset, or length,
     *         or if its native-memory region is no longer accessible
     */
    protected abstract void copyToMemory(long startPtr, byte[] bytes, int srcOffset, int len) throws IllegalArgumentException, RuntimeException;

    /**
     * Copies bytes from off-heap memory into a heap array. May be called concurrently for
     * different source regions.
     *
     * @param startPtr the source address in off-heap memory
     * @param bytes the destination array; must not be {@code null}, including for a zero-length copy
     * @param destOffset the zero-based destination-array index (plus the construction-time
     *                   {@code arrayOffset}), in the same convention as
     *                   {@link #copyToMemory(long, byte[], int, int)}'s {@code srcOffset}
     * @param len the number of bytes to copy
     * @throws IllegalArgumentException if {@code bytes} is {@code null}
     * @throws RuntimeException if the backend rejects the destination array, address, offset, or length,
     *         or if its native-memory region is no longer accessible
     */
    protected abstract void copyFromMemory(long startPtr, byte[] bytes, int destOffset, int len) throws IllegalArgumentException, RuntimeException;

    // ------------------------------------------------------------------------------------- get

    /**
     * Retrieves the value associated with the specified key, or {@code null} if no live mapping
     * exists. An expired entry encountered here is removed (and counted as an eviction). For a
     * disk-backed entry, the serialized bytes are fetched from the {@link OffHeapStore} and the
     * entry may be promoted back into memory when the configured promotion predicate accepts it.
     * Promotion preserves the original TTL and idle deadlines. A read that found live bytes can
     * still return its value if the entry expires while deserialization or promotion is running.
     * An unchecked exception thrown by the {@link OffHeapStore} read (its contract for an
     * operational read failure), by a custom deserializer, by the promotion predicate, or by the
     * promotion's native-memory copy is propagated; a failed promotion leaves the disk-backed
     * entry in place.
     *
     * @param key the key whose associated value is to be returned; must not be {@code null}
     * @return the cached value, or {@code null} if the key is not present, the entry has expired,
     *         the disk-backed entry's bytes are missing from the store, or the entry found by the
     *         lookup was removed or replaced (by a same-key put or a promotion back into memory)
     *         concurrently, even if the key stayed mapped throughout
     * @throws IllegalStateException if the cache has been closed, or if a value cannot be
     *                               reconstructed because the fetched size no longer matches the
     *                               recorded size (data corruption), or because a configured
     *                               deserializer returned {@code null}
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws RuntimeException if the backing-store read, value deserialization, promotion predicate,
     *         or native-memory copy throws an unchecked exception
     */
    @Override
    public V getOrNull(final K key) throws IllegalStateException, IllegalArgumentException, RuntimeException {
        lifecycleLock.readLock().lock();
        try {
            assertNotClosed();

            N.checkArgNotNull(key, cs.key);

            final Entry<V> entry = entries.get(key);

            if (entry == null) {
                missCount.increment();
                return null;
            }

            // Outcome flags resolved under the entry monitor; map updates that would acquire a
            // map bin lock are deferred until after the monitor is released (lock order: bin ->
            // entry monitor, never the reverse).
            boolean expired = false;
            boolean storeMiss = false;
            byte[] bytes = null;
            long storeReadMillis = -1L;

            synchronized (entry) {
                if (entry.freed) {
                    // Concurrently removed/replaced between the map lookup and here: a miss.
                } else if (entry.activityPrint.isExpired()) {
                    expired = true;
                } else if (entry.isMemory()) {
                    bytes = new byte[entry.size];
                    copyEntryFromMemory(entry, bytes);
                    entry.touch();
                } else {
                    // The store fetch must stay inside the monitor: every path that overwrites or
                    // removes this entry's bytes holds this monitor first, so bytes read here are
                    // guaranteed to be this entry's own.
                    final long startedAt = measureStoreReadTime ? System.nanoTime() : 0L;
                    final byte[] storeBytes = offHeapStore.get(key);

                    if (measureStoreReadTime) {
                        storeReadMillis = elapsedMillisSince(startedAt);
                    }

                    if (storeBytes == null) {
                        storeMiss = true;
                    } else if (storeBytes.length != entry.size) {
                        // Cannot be an internal race (see the ordering argument above); the store
                        // itself returned bytes inconsistent with what was written.
                        throw new IllegalStateException("Failed to retrieve value: fetched size (" + storeBytes.length
                                + " bytes) does not match expected size (" + entry.size + " bytes)");
                    } else {
                        // The OffHeapStore contract permits get() to return its own retained
                        // array; never expose it to the caller or a custom deserializer.
                        bytes = storeBytes.clone();
                        entry.touch();
                    }
                }
            }

            if (statsTimeOnDisk && storeReadMillis >= 0) {
                readFromDiskTimes.record(storeReadMillis);
            }

            if (bytes == null) {
                if (expired) {
                    removeIfCurrent(key, entry, FreeCause.EXPIRED);
                } else if (storeMiss) {
                    // A null store result is a confirmed miss by OffHeapStore contract. Drop the
                    // stale mapping; operational read failures must be reported by throwing and
                    // therefore never enter this cleanup path.
                    removeIfCurrent(key, entry, FreeCause.REMOVED);
                }

                missCount.increment();
                return null;
            }

            hitCount.increment();

            final boolean readFromDisk = !entry.isMemory();
            // A custom deserializer is allowed to decode in place. Preserve the pristine store
            // bytes when this read may subsequently install them in the memory tier.
            final byte[] bytesToDeserialize = readFromDisk && testerForLoadingItemFromDiskToMemory != null ? bytes.clone() : bytes;
            final V value = deserializeValue(bytesToDeserialize, entry.type);

            if (readFromDisk) {
                // Counted only once the value has actually been reconstructed; a throwing
                // deserializer is not a successful disk read.
                hitCountFromDisk.increment();
                maybePromoteToMemory(key, entry, bytes, storeReadMillis);
            }

            return value;
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    /**
     * Promotes a just-read disk entry back into memory when the configured predicate accepts it.
     * Best-effort: if no memory can be allocated or the mapping changed concurrently, the disk
     * entry simply stays in place. Promotion preserves the original creation/access timestamps,
     * expiration limits, and access count; copying the value must not restart either clock.
     *
     * @throws RuntimeException if the promotion predicate or native-memory copy throws an unchecked exception
     */
    private void maybePromoteToMemory(final K key, final Entry<V> diskEntry, final byte[] bytes, final long storeReadMillis) throws RuntimeException {
        if (testerForLoadingItemFromDiskToMemory == null
                || !testerForLoadingItemFromDiskToMemory.test(diskEntry.activityPrint, diskEntry.size, Math.max(0L, storeReadMillis))) {
            return;
        }

        // A value that can never be placed in the region would make the slot allocation attempt
        // doomed (and allocator-lock-heavy); skip it, mirroring doPut's guard.
        if (diskEntry.activityPrint.isExpired() || !canEverFitInMemory(diskEntry.size)) {
            return;
        }

        final boolean[] installed = { false };
        final long[] slots = allocateSlots(diskEntry.size);

        if (slots == null) {
            return; // No memory available; promotion is an optimization only.
        }

        try {
            copyToAllocatedSlots(diskEntry.size, slots, bytes);

            entries.compute(key, (k, current) -> {
                if (current != diskEntry) {
                    // The mapping changed concurrently; keep it and discard the copy after compute.
                    return current;
                }

                synchronized (diskEntry) {
                    // Allocation, copying, or waiting for this key can outlast either deadline.
                    // Recheck before installation and snapshot activity only after prior readers
                    // have finished, so promotion also retains their latest access information.
                    if (diskEntry.freed || diskEntry.activityPrint.isExpired()) {
                        return current;
                    }

                    final Entry<V> memoryEntry = new Entry<>(diskEntry.type, diskEntry.size, diskEntry.activityPrint.clone(), slots);
                    free(k, diskEntry, FreeCause.REPLACED, true);
                    install(memoryEntry);
                    installed[0] = true;
                    return memoryEntry;
                }
            });
        } catch (final Throwable e) {
            // Copy failures and a throwing key hashCode/equals must release unpublished slots.
            // Throwable rather than RuntimeException | Error so an undeclared checked exception
            // (Kotlin, sneaky throws) cannot strand the slots until close(); still rethrown.
            if (!installed[0]) {
                suppressCleanupFailure(e, () -> discardUninstalledSlots(slots));
            }
            throw e;
        }

        if (!installed[0]) {
            // Keep rejection cleanup outside the catch above: a failure while reclaiming an
            // already-released segment must never cause its slot handles to be released twice.
            discardUninstalledSlots(slots);
        }
    }

    // ------------------------------------------------------------------------------------- put

    /**
     * Stores a key-value pair with the specified expiration settings. The value is serialized and
     * placed in off-heap memory; when memory is unavailable (or the {@code storeSelector} directs
     * it), the value is written to the configured {@link OffHeapStore} instead.
     *
     * <p>Values that could not be placed even in an otherwise empty cache &mdash; because the
     * serialized form exceeds the entire off-heap capacity, or because its slot-rounded chunks need
     * more 1 MB segments than the region has &mdash; never attempt the in-memory placement (no
     * amount of vacating could make them fit) and go straight to the disk fallback, if any; such a
     * doomed put schedules no vacate pass and leaves the in-memory cache contents untouched.
     *
     * <p><b>Replacement failure:</b> a failed replacement never loses the prior entry. The new
     * entry is installed atomically with the retirement of the old one, and a failed or throwing
     * disk write leaves the prior mapping and its bytes unchanged per the {@link OffHeapStore#put}
     * contract. The one exception to "a throwing put changed nothing" is a failure of the key
     * methods described under <i>Key requirements</i> below, which can arrive after the replacement
     * has already been applied.
     *
     * <p>Non-positive {@code liveTime}/{@code maxIdleTime} values mean "no expiration"/"no idle
     * limit". For a {@link ByteBuffer} value, the bytes from index 0 up to the current position
     * are stored; the buffer's position, limit, and mark are left unchanged. An entry's expiration
     * clock starts only after its serialized bytes have been copied to native memory or, for a
     * disk-routed value, after the store write has completed; map installation follows.
     *
     * <p>An unchecked exception thrown by a custom serializer, by the {@code storeSelector}, or by
     * the {@link OffHeapStore} write is propagated, leaving any prior mapping unchanged.
     *
     * <p><b>Key requirements:</b> keys must implement {@code hashCode}, {@code equals}, and, for a
     * {@link Comparable} key, {@code compareTo} consistently and without throwing. The internal
     * index also calls them while serving other keys' operations ({@code compareTo} when a bin of
     * colliding keys is converted to a tree), so such a failure can surface from a put of another
     * key. That put then propagates the failure unchanged (an {@link Error} included), but its
     * value may already have been mapped (replacing any prior entry); such a value is counted in
     * {@code putCount} like any stored value and stays readable until it is replaced or removed,
     * possibly by a concurrent operation on the same key. Otherwise the value's memory slots and
     * counters are released and its store bytes removed. If that removal fails (the key's lookup
     * fails again, or the store's {@code remove} throws), the bytes, unreachable through the cache,
     * are removed by a later {@link #clear()} or by {@link #close()} (if the store's {@code remove}
     * fails during {@code close()}, that failure is logged and the bytes stay). Until then the cache
     * keeps a reference to each such key, and logs a warning when their number first reaches 1,024
     * and again each time it doubles; the count restarts once a {@code clear()} has removed them all.
     *
     * @param key the key; must not be {@code null}
     * @param value the value; must not be {@code null}
     * @param liveTime the TTL in milliseconds; {@code <= 0} means no expiration
     * @param maxIdleTime the maximum idle time in milliseconds; {@code <= 0} means no idle limit
     * @return {@code true} if the value was stored (in memory or on disk); {@code false} otherwise
     * @throws IllegalStateException if the cache has been closed, or memory pressure triggers
     *         submission of a vacate task after the shared async executor has been shut down
     * @throws IllegalArgumentException if {@code key} or {@code value} is {@code null}, or if
     *                                  {@code storeSelector} returns {@code null} or a value outside 0..2
     * @throws RuntimeException if serialization, the store selector, the native-memory copy, or the
     *         backing-store write throws an unchecked exception, or if the key's {@code hashCode},
     *         {@code equals}, or {@code compareTo} (or a colliding key's {@code compareTo}) throws one
     *         while the value is being mapped
     * @throws RejectedExecutionException if the put fails under memory pressure and the shared
     *         executor rejects the vacate task
     */
    @Override
    public boolean put(final K key, final V value, final long liveTime, final long maxIdleTime)
            throws IllegalStateException, IllegalArgumentException, RuntimeException, RejectedExecutionException {
        lifecycleLock.readLock().lock();
        try {
            assertNotClosed();

            N.checkArgNotNull(key, cs.key);
            N.checkArgNotNull(value, cs.value);

            return doPut(key, value, liveTime, maxIdleTime);
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    /**
     * Serializes and stores an already-validated key and value with the requested expiration limits.
     *
     * @throws RuntimeException if serialization, the store selector, the native-memory copy, or the
     *         backing-store write throws an unchecked exception
     * @throws IllegalArgumentException if the store selector returns {@code null} or a value outside 0..2
     * @throws IllegalStateException if memory pressure triggers a vacate task after the shared async executor has been shut down
     * @throws RejectedExecutionException if memory pressure triggers a vacate task that the executor rejects
     */
    private boolean doPut(final K key, final V value, final long liveTime, final long maxIdleTime)
            throws RuntimeException, IllegalArgumentException, IllegalStateException, RejectedExecutionException {
        // A non-positive liveTime/maxIdleTime means "no expiration" per the documented contract;
        // ActivityPrint rejects values <= 0, so translate to Long.MAX_VALUE.
        final long effectiveLiveTime = liveTime > 0 ? liveTime : Long.MAX_VALUE;
        final long effectiveMaxIdleTime = maxIdleTime > 0 ? maxIdleTime : Long.MAX_VALUE;

        @SuppressWarnings("unchecked")
        final Type<V> type = N.typeOf((Class<V>) value.getClass());

        ByteArrayOutputStream os = null;

        try {
            final byte[] bytes;
            final int size;

            if (type.isPrimitiveByteArray()) {
                bytes = (byte[]) value;
                size = bytes.length;
            } else if (type.isByteBuffer()) {
                // Moving a buffer's position to zero to read [0, position) would invalidate its
                // mark. Operate on a duplicate so put() is observationally read-only for the
                // caller's buffer, independent of ByteBufferType's own (currently duplicating)
                // implementation.
                bytes = ByteBufferType.byteArrayOf(((ByteBuffer) value).duplicate());
                size = bytes.length;
            } else {
                os = Objectory.createByteArrayOutputStream();
                serializer.accept(value, os);
                bytes = os.array();
                size = os.size();
            }

            final int selectedStore = selectedStoreOf(key, value, size);

            // A value that needs more segments than the entire off-heap region has can never fit,
            // no matter how many entries a vacate pass frees; skip the memory attempt entirely.
            final boolean canBeStoredInMemory = selectedStore != STORE_DISK_ONLY && canEverFitInMemory(size);
            final boolean canBeStoredToDisk = selectedStore != STORE_MEMORY_ONLY && offHeapStore != null;

            if (canBeStoredInMemory) {
                final long[] slots = allocateSlots(size);

                if (slots != null) {
                    final Entry<V> entry = createMemoryEntry(type, size, effectiveLiveTime, effectiveMaxIdleTime, slots, bytes);
                    final boolean[] installed = { false };

                    try {
                        entries.compute(key, (k, old) -> {
                            if (old != null) {
                                free(k, old, FreeCause.REPLACED, true);
                            }

                            install(entry);
                            installed[0] = true;
                            return entry;
                        });
                    } catch (final Throwable e) {
                        // Throwable, as in runMaintenance: code written in Kotlin (or using sneaky
                        // throws) can throw a checked exception undeclared, and it must not strand the
                        // slots either. Rethrown afterwards, so nothing is swallowed.
                        if (!installed[0]) {
                            // A throwing key hashCode/equals aborts the compute before the lambda
                            // installs the entry; release its freshly allocated slots.
                            discardUninstalledMemoryEntry(entry);
                        } else {
                            final boolean[] stored = { false };
                            suppressCleanupFailure(e, () -> stored[0] = discardIfUnlinked(key, entry));

                            if (stored[0]) {
                                // The value reached the map although the put throws: count it like any stored value.
                                putCount.increment();
                            }
                        }

                        throw e;
                    }

                    putCount.increment();
                    return true;
                }
            }

            if (canBeStoredToDisk && putToDisk(key, type, bytes, size, effectiveLiveTime, effectiveMaxIdleTime)) {
                return true;
            }

            if (canBeStoredInMemory) {
                // The put failed under genuine memory pressure (the value could fit an empty
                // cache, and no disk store absorbed it): reclaim space asynchronously so
                // subsequent puts can succeed.
                vacate();
            }

            return false;
        } finally {
            Objectory.recycle(os);
        }
    }

    /**
     * Validates and returns the storage routing for one put: 0 (default), 1 (memory only), or 2 (disk only).
     *
     * @throws RuntimeException if the configured store selector throws an unchecked exception
     * @throws IllegalArgumentException if the store selector returns {@code null} or a value outside 0..2
     */
    private int selectedStoreOf(final K key, final V value, final int size) throws RuntimeException, IllegalArgumentException {
        if (storeSelector == null) {
            return STORE_DEFAULT;
        }

        final Integer selected = storeSelector.apply(key, value, size);

        if (selected == null || selected < STORE_DEFAULT || selected > STORE_DISK_ONLY) {
            throw new IllegalArgumentException("storeSelector must return 0 (default), 1 (memory only), or 2 (disk only), but returned: " + selected);
        }

        return selected;
    }

    /**
     * Writes a value to the disk store and installs its entry, atomically with the retirement of
     * any prior same-key entry. The whole protocol runs inside the map's per-key {@code compute},
     * which serializes concurrent same-key spills. When the prior entry is disk-backed, its
     * monitor is held across the overwriting store write and its retirement, so no reader can
     * fetch the new bytes against the prior entry's metadata. A failed or throwing write leaves
     * the prior mapping (and, per the {@link OffHeapStore#put} contract, the prior bytes)
     * completely untouched. If the map fails after the entry was installed, an entry it did not
     * link is discarded together with its store bytes (see {@link #discardIfUnlinked}).
     *
     * @return {@code true} if the value was stored and installed
     * @throws RuntimeException if the backing-store write throws an unchecked exception
     */
    private boolean putToDisk(final K key, final Type<V> type, final byte[] bytes, final int size, final long liveTime, final long maxIdleTime)
            throws RuntimeException {
        // Always hand the store a private copy: `bytes` may alias a pooled serialization buffer
        // that is recycled (and reused by other threads) right after this put, or the caller's
        // own byte[]/ByteBuffer contents.
        final byte[] bytesToStore = N.copyOfRange(bytes, 0, size);

        final long[] writeMillis = { -1L };
        // The installed entry; created inside the compute, so its TTL starts after the store write.
        @SuppressWarnings("unchecked")
        final Entry<V>[] installed = new Entry[1];

        try {
            entries.compute(key, (k, old) -> {
                final boolean ok;

                if (old != null && !old.isMemory()) {
                    // The write overwrites the prior entry's bytes under the same store key: exclude
                    // the prior's readers for the write AND the retirement, so they either complete
                    // before the overwrite or observe the entry as freed.
                    synchronized (old) {
                        ok = timedStorePut(k, bytesToStore, writeMillis);

                        if (ok) {
                            // The bytes now belong to the new entry; do not remove them.
                            free(k, old, FreeCause.REPLACED, false);
                        }
                    }
                } else {
                    ok = timedStorePut(k, bytesToStore, writeMillis);

                    if (ok && old != null) {
                        free(k, old, FreeCause.REPLACED, true);
                    }
                }

                if (!ok) {
                    return old; // The prior mapping and its bytes are untouched.
                }

                final Entry<V> entry = new Entry<>(type, size, liveTime, maxIdleTime, null);
                install(entry);
                installed[0] = entry;
                return entry;
            });
        } catch (final Throwable e) {
            // Throwable for the same reason as doPut's memory path; rethrown afterwards.
            if (installed[0] != null) {
                final boolean[] stored = { false };
                suppressCleanupFailure(e, () -> stored[0] = discardIfUnlinked(key, installed[0]));

                if (stored[0]) {
                    // The value reached the map although the put throws: count it like any stored value.
                    recordDiskPut(writeMillis[0]);
                }
            }

            throw e;
        }

        if (installed[0] == null) {
            return false;
        }

        recordDiskPut(writeMillis[0]);
        return true;
    }

    private void recordDiskPut(final long writeMillis) {
        putCountToDisk.increment();

        if (writeMillis >= 0) {
            writeToDiskTimes.record(writeMillis);
        }

        putCount.increment();
    }

    /**
     * Performs one store write, recording the elapsed milliseconds of the successful call into
     * {@code elapsedOut[0]} when disk timing is enabled. Measures only {@link OffHeapStore#put},
     * excluding serialization and the unsuccessful in-memory placement work that preceded the
     * disk fallback.
     *
     * @throws RuntimeException if the backing-store write throws an unchecked exception
     */
    private boolean timedStorePut(final K key, final byte[] bytes, final long[] elapsedOut) throws RuntimeException {
        final long startedAt = statsTimeOnDisk ? System.nanoTime() : 0L;

        final boolean ok = offHeapStore.put(key, bytes);

        if (statsTimeOnDisk && ok) {
            elapsedOut[0] = elapsedMillisSince(startedAt);
        }

        return ok;
    }

    // -------------------------------------------------------------------------- other Cache ops

    /**
     * Removes the cache entry associated with the specified key, if present, releasing its memory
     * slots or disk bytes. The mapping is removed even if the {@link OffHeapStore} fails to remove
     * the disk bytes; such a failure is logged rather than propagated. An {@link Error} thrown by
     * the store propagates instead and leaves the entry in place, so the removal can be retried.
     *
     * @param key the key whose mapping is to be removed; must not be {@code null}
     * @throws IllegalStateException if the cache has been closed
     * @throws IllegalArgumentException if {@code key} is {@code null}
     */
    @Override
    public void remove(final K key) throws IllegalStateException, IllegalArgumentException {
        lifecycleLock.readLock().lock();
        try {
            assertNotClosed();

            N.checkArgNotNull(key, cs.key);

            // Freeing inside the per-key compute serializes this entry's store-byte removal with
            // any concurrent same-key disk write: detaching first and freeing outside would let
            // offHeapStore.remove(key) delete the bytes a newer entry just wrote under the key.
            entries.compute(key, (k, current) -> {
                if (current != null) {
                    free(k, current, FreeCause.REMOVED, true);
                }

                return null;
            });
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    /**
     * Returns whether the cache contains a live (non-expired) mapping for the specified key.
     * This is a read-only probe: an expired entry encountered here is reported as absent but is
     * left for the maintenance pass or the next {@code get} to remove.
     *
     * @param key the key to test; must not be {@code null}
     * @return {@code true} if a live mapping exists
     * @throws IllegalStateException if the cache has been closed
     * @throws IllegalArgumentException if {@code key} is {@code null}
     */
    @Override
    public boolean containsKey(final K key) throws IllegalStateException, IllegalArgumentException {
        assertNotClosed();

        N.checkArgNotNull(key, cs.key);

        final Entry<V> entry = entries.get(key);

        return entry != null && !entry.freed && !entry.activityPrint.isExpired();
    }

    /**
     * Returns a snapshot of all keys currently held in the cache, including keys of entries
     * spilled to disk. The returned set is a copy: subsequent cache changes are not reflected in
     * it, and it may include keys of entries that have expired but not yet been swept.
     *
     * @return a snapshot of the cache keys; never {@code null}
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public Set<K> keySet() throws IllegalStateException {
        assertNotClosed();

        return new HashSet<>(entries.keySet());
    }

    /**
     * Returns the number of entries currently held in the cache (including disk-backed entries,
     * and entries that have expired but not yet been swept).
     *
     * @return the current entry count
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public int size() throws IllegalStateException {
        assertNotClosed();

        return currentEntryCount();
    }

    /** The live-entry gauge, clamped to the int range; LongAdder.sum() is not an atomic snapshot and can transiently read below zero. */
    private int currentEntryCount() {
        return (int) Math.min(Math.max(0L, entryCount.sum()), Integer.MAX_VALUE);
    }

    /**
     * Removes all entries, releasing their memory slots and disk bytes, and reclaims the
     * now-empty segments for reuse by any slot size. As with {@link #remove(Object)}, a failure of
     * the {@link OffHeapStore} to remove an entry's disk bytes is logged rather than propagated,
     * while an {@link Error} thrown by the store propagates and leaves that entry, and any not yet
     * visited, in place. Entries put concurrently with this call may survive it. This call also
     * retries removing store bytes left behind by a failed disk put (see {@link #put(Object, Object, long, long)});
     * a key whose retry fails again, in its lookup or in the store's {@code remove}, stays recorded
     * for the next {@code clear()} or {@link #close()}. Such failures are reported by one summary
     * warning per call, except that the first {@link Error} among them is rethrown once every
     * recorded key has been tried. The retry runs even when removing the entries throws; that
     * failure is then rethrown, with any retry failure attached to it as suppressed.
     *
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public void clear() throws IllegalStateException {
        lifecycleLock.readLock().lock();
        try {
            assertNotClosed();

            try {
                removeAllEntries(FreeCause.REMOVED);
                reclaimEmptySegments();
            } catch (final Throwable e) {
                // Retry the orphaned store bytes even when a live entry cannot be removed: a clear()
                // that keeps failing on one entry would otherwise never reach them while the cache
                // stays open. A retry failure is attached to this one, which is rethrown.
                suppressCleanupFailure(e, this::retryOrphanedStoreKeys);
                throw e;
            }

            retryOrphanedStoreKeys();
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    /**
     * Returns whether this cache has been closed.
     *
     * @return {@code true} if the cache is closed
     */
    @Override
    public boolean isClosed() {
        return closed;
    }

    /**
     * Deliberately decommissions this application-scoped cache before JVM termination. It cancels
     * the maintenance task, removes the JVM shutdown hook, frees every entry (removing disk-backed
     * entries' bytes from the store, along with any bytes a failed disk put left behind),
     * deallocates the native region, and closes the configured
     * {@link OffHeapStore}, if any. If the cache remains active for the application lifetime, the
     * registered shutdown hook invokes this method automatically during JVM shutdown.
     * Every entry and cleanup step is attempted even when an earlier one fails. The entry index
     * is cleared even if an entry cannot be released. As with {@link #remove(Object)}, an
     * exception other than an {@link Error} from removing disk bytes is logged rather than propagated.
     * Other cleanup failures propagate, with the first failure retaining later distinct failures
     * as suppressed exceptions. This method is idempotent.
     *
     * <p><b>Reentrant callback restriction:</b> a serializer, store selector, or other callback
     * invoked from an operation holding this cache's lifecycle read lock must not close the cache
     * synchronously (a read-to-write lock upgrade is unsupported); such a call fails immediately
     * with {@link IllegalStateException}.
     *
     * @throws IllegalStateException if the current thread attempts to close the cache while it is
     *                               executing a cache operation or callback
     * @throws RuntimeException if canceling maintenance, removing the shutdown hook, releasing an
     *         entry, deallocating native memory, or closing the configured store throws an unchecked
     *         exception; later distinct cleanup failures are suppressed on the first failure
     */
    @Override
    public void close() throws IllegalStateException, RuntimeException {
        // ReentrantReadWriteLock does not support upgrading a held read lock to its write lock;
        // fail fast instead of deadlocking on ourselves.
        if (lifecycleLock.getReadHoldCount() > 0) {
            throw new IllegalStateException("Cannot close this off-heap cache reentrantly while the current thread is executing a cache operation or callback");
        }

        lifecycleLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }

            closed = true;

            Throwable failure = null;
            failure = runCleanupStep(failure, () -> {
                if (maintenanceFuture != null) {
                    maintenanceFuture.cancel(true);
                }
            });
            failure = runCleanupStep(failure, this::removeShutdownHook);
            failure = runCleanupStep(failure, this::removeAllEntriesOnClose);
            failure = runCleanupStep(failure, this::removeOrphanedStoreBytesOnClose);
            failure = runCleanupStep(failure, this::deallocate);
            failure = runCleanupStep(failure, () -> {
                if (offHeapStore != null) {
                    offHeapStore.close();
                }
            });

            if (failure instanceof final RuntimeException runtimeException) {
                throw runtimeException;
            } else if (failure instanceof final Error error) {
                throw error;
            }
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }

    private void removeShutdownHook() {
        if (shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (final IllegalStateException e) {
                // The JVM is already shutting down (we are likely running INSIDE the hook).
                if (logger.isDebugEnabled()) {
                    logger.debug("Could not remove shutdown hook because the JVM is already shutting down; ignoring", e);
                }
            } finally {
                shutdownHook = null;
            }
        }
    }

    /**
     * Runs a cleanup step from inside a failure handler, attaching any failure of its own to
     * {@code primary} as suppressed. Unlike {@link #runCleanupStep}, it also catches a checked
     * exception thrown undeclared (Kotlin, sneaky throws) by a key method or store, which would
     * otherwise escape and replace the failure being handled.
     */
    private static void suppressCleanupFailure(final Throwable primary, final Runnable step) {
        try {
            step.run();
        } catch (final Throwable e) {
            if (e != primary) {
                primary.addSuppressed(e);
            }
        }
    }

    /**
     * Runs one cleanup step, keeping the first failure as primary and attaching any later ones as
     * suppressed (guarding against self-suppression of a reused throwable instance).
     *
     * @return the primary failure so far, or {@code null} if none
     */
    private static Throwable runCleanupStep(final Throwable earlierFailure, final Runnable step) {
        try {
            step.run();
        } catch (final RuntimeException | Error e) {
            if (earlierFailure == null) {
                return e;
            }

            if (earlierFailure != e) {
                earlierFailure.addSuppressed(e);
            }
        }

        return earlierFailure;
    }

    // ------------------------------------------------------------------------------------ stats

    /**
     * Returns a sampled, point-in-time snapshot of cache statistics: entry counts, hit/miss and
     * eviction counters, memory and disk usage, disk I/O timing aggregates, and per-segment slot
     * occupancy.
     *
     * <p><b>&#9888;&#65039; Non-atomic snapshot:</b> counters are sampled independently, so
     * relationships between fields are conceptual invariants that may not hold exactly while the
     * cache is being updated concurrently.
     *
     * @return the statistics snapshot; never {@code null}
     * @throws IllegalStateException if the cache has been closed
     * @throws IllegalArgumentException if an accumulated counter has overflowed into a negative
     *         value, or an overflowed disk-timing accumulator produces invalid timing statistics
     */
    public OffHeapCacheStats stats() throws IllegalStateException, IllegalArgumentException {
        assertNotClosed();

        final long hits = hitCount.sum();
        final long misses = missCount.sum();

        // Capacity reports the maximum number of MIN_BLOCK_SIZE slots that fit in the region
        // (clamped to Integer.MAX_VALUE for very large caches) - an upper bound on entry count.
        final int capacity = (int) Math.min(capacityInBytes / MIN_BLOCK_SIZE, Integer.MAX_VALUE);

        // LongAdder.sum() is not an atomic snapshot; clamp the up-and-down gauges to >= 0 so a
        // transiently observed decrement-without-increment cannot make the stats record throw.
        final long gets = hits > Long.MAX_VALUE - misses ? Long.MAX_VALUE : hits + misses;

        return new OffHeapCacheStats(capacity, currentEntryCount(), Math.max(0L, sizeOnDisk.sum()), putCount.sum(), putCountToDisk.sum(), gets, hits,
                hitCountFromDisk.sum(), misses, evictionCount.sum(), Math.max(0L, evictionCountFromDisk.sum()), capacityInBytes,
                Math.max(0L, totalOccupiedMemorySize.sum()), Math.max(0L, totalDataSize.sum()), Math.max(0L, dataSizeOnDisk.sum()), writeToDiskTimes.snapshot(),
                readFromDiskTimes.snapshot(), SEGMENT_SIZE, snapshotOccupiedSlots());
    }

    // -------------------------------------------------------------------------- entry lifecycle

    /**
     * Single accounting site for an entry entering the cache. Called only inside the map's
     * per-key compute, immediately before the entry is returned as the new mapping.
     */
    private void install(final Entry<V> entry) {
        if (entry.isMemory()) {
            totalOccupiedMemorySize.add(occupiedMemoryOf(entry.size));
        } else {
            sizeOnDisk.increment();
            dataSizeOnDisk.add(entry.size);
        }

        totalDataSize.add(entry.size);
        entryCount.increment();
    }

    /**
     * Single accounting-and-release site for an entry leaving the cache. Marks the entry freed
     * under its monitor (excluding in-flight readers), releases its memory slots or disk bytes,
     * and reverses the counters that {@link #install(Entry)} updated.
     *
     * <p>An exception from the store's {@code remove} is logged and the entry is freed anyway (only
     * store space leaks). An {@link Error} from it propagates and leaves the entry live, with its
     * counters untouched. Callers free inside a map {@code compute}, which the Error aborts, so the
     * entry is still mapped and must stay readable and removable by a later attempt. (The
     * exceptions: {@link #close()} clears the map regardless, and {@link #discardIfUnlinked}
     * frees an entry the map failed to link without removing store bytes.)
     *
     * @param removeDiskBytes for a disk-backed entry, whether its store bytes are removed; a
     *                        caller that has just overwritten the bytes under the same key
     *                        passes {@code false}
     */
    private void free(final K key, final Entry<V> entry, final FreeCause cause, final boolean removeDiskBytes) {
        synchronized (entry) {
            if (entry.freed) {
                return;
            }

            if (entry.isMemory()) {
                // Marked before the release: the slots go back to the allocator here, so the entry
                // must never be read again, even if releasing them fails part-way.
                entry.freed = true;
                releaseSlots(entry);
            } else {
                if (removeDiskBytes) {
                    try {
                        offHeapStore.remove(key);
                    } catch (final Exception e) {
                        // The mapping is gone either way; a failed byte removal only leaks store space.
                        // Exception, not RuntimeException: a store written in Kotlin (or using sneaky
                        // throws) can raise a checked exception that its signature does not declare.
                        logger.warn("Failed to remove the backing bytes of an off-heap cache entry from the OffHeapStore", e);
                    }
                }

                // Marked only after the store call returns. An Error from it aborts the caller's
                // compute, which keeps this entry mapped; marking it first would strand a freed
                // entry in the map, invisible to reads and skipped by every later sweep.
                entry.freed = true;
            }
        }

        if (entry.isMemory()) {
            totalOccupiedMemorySize.add(-occupiedMemoryOf(entry.size));
        } else {
            sizeOnDisk.decrement();
            dataSizeOnDisk.add(-entry.size);
        }

        totalDataSize.add(-entry.size);
        entryCount.decrement();

        if (cause == FreeCause.EXPIRED || cause == FreeCause.EVICTED) {
            evictionCount.increment();

            if (!entry.isMemory()) {
                evictionCountFromDisk.increment();
            }
        }
    }

    /**
     * Atomically removes the mapping if it still points at {@code entry}, freeing it inside the
     * map's per-key compute so the entry's store-byte removal is serialized (by the bin lock)
     * with any concurrent same-key disk write. Expiration candidates are checked again under the
     * entry monitor, after any preceding read has had a chance to refresh its idle timestamp.
     */
    private void removeIfCurrent(final K key, final Entry<V> entry, final FreeCause cause) {
        entries.compute(key, (k, current) -> {
            if (current != entry) {
                return current;
            }

            synchronized (entry) {
                // An in-flight read may refresh idle time after a sweep selected this entry
                // but before the sweep acquired its monitor. Retire only a still-expired entry.
                if (cause == FreeCause.EXPIRED && !entry.activityPrint.isExpired()) {
                    return current;
                }

                free(k, entry, cause, true);
                return null;
            }
        });
    }

    /**
     * Cleans up after a put's {@code compute} failed once its remapping function had installed
     * {@code entry}. The map can fail after the function returns: treeifying an overlong bin
     * (which calls the keys' {@code compareTo}) or resizing runs once the entry is linked, while a
     * failed node allocation or tree insertion leaves it unlinked. A linked entry stays: releasing
     * its slots would let the next allocation overwrite them under a live mapping. An unlinked one
     * is unreachable, so it is freed here, outside any compute; the identity scan never touches
     * the key. An entry already marked freed was linked and has since been retired by a same-key
     * mutation, which also handled its slots or store bytes: only this method frees an unlinked entry.
     *
     * <p>A disk entry's store bytes were written under {@code key} before the failure and are
     * removed by {@link #removeStoreBytesIfUnowned}. That compute can fail the same way (a key
     * whose {@code compareTo} answers inconsistently, or a bin reshaped in between), and the
     * store's {@code remove} can throw; the caller then keeps the failure as suppressed, and the
     * key is recorded so that {@link #clear()} retries the removal and {@link #close()} completes
     * it. Until then the bytes are unreachable through this cache.
     *
     * @return {@code true} if the map linked the entry (it is still mapped, or was retired since),
     *         so the put stored its value despite the failure
     */
    private boolean discardIfUnlinked(final K key, final Entry<V> entry) {
        // A retirer sets freed before it unlinks the node, so a scan that misses a retired entry
        // still observes the flag.
        if (entries.containsValue(entry) || entry.freed) {
            return true;
        }

        free(key, entry, FreeCause.REMOVED, false);

        if (!entry.isMemory()) {
            try {
                removeStoreBytesIfUnowned(key);
            } catch (final Throwable e) {
                recordOrphanedStoreKey(key);
                throw e;
            }
        }

        return false;
    }

    /**
     * Removes the store bytes held under {@code key} unless a disk mapping owns them. Runs inside
     * the key's own compute, like every other byte removal, so it cannot clobber bytes that a
     * concurrent same-key disk put has written; a newer disk mapping owns whatever the store holds
     * for the key, so only an unmapped or memory-mapped key loses its bytes.
     */
    private void removeStoreBytesIfUnowned(final K key) {
        entries.compute(key, (k, current) -> {
            if (current == null || current.isMemory()) {
                // Unlike free(), a store failure propagates: the caller records the key, so the
                // removal is retried by the next clear() or close() instead of only leaking space.
                // The compute leaves the mapping unchanged either way.
                offHeapStore.remove(k);
            }

            return current;
        });
    }

    /**
     * Records a key for a later removal of its orphaned store bytes; identity-deduplicated. Logs a
     * warning each time the number of recorded keys doubles from {@link #ORPHANED_STORE_KEYS_WARN_THRESHOLD}.
     */
    private void recordOrphanedStoreKey(final K key) {
        final int recorded;

        synchronized (orphanedStoreKeys) {
            if (!orphanedStoreKeys.add(key) || orphanedStoreKeys.size() < orphanedStoreKeysWarnAt) {
                return;
            }

            recorded = orphanedStoreKeys.size();
            orphanedStoreKeysWarnAt = orphanedStoreKeysWarnAt > Integer.MAX_VALUE / 2 ? Integer.MAX_VALUE : orphanedStoreKeysWarnAt * 2;
        }

        logger.warn(recorded + " off-heap cache keys have orphaned OffHeapStore bytes awaiting removal by clear() or close(); a key type whose "
                + "hashCode, equals, or compareTo keeps failing, or a failing OffHeapStore.remove, is retaining them");
    }

    private List<K> drainOrphanedStoreKeys() {
        synchronized (orphanedStoreKeys) {
            if (orphanedStoreKeys.isEmpty()) {
                return List.of();
            }

            final List<K> keys = new ArrayList<>(orphanedStoreKeys);
            orphanedStoreKeys.clear();
            return keys;
        }
    }

    /**
     * Retries the guarded removal for every recorded key. A key that fails again stays recorded.
     * Failures other than the first {@link Error} are reported by one summary warning per call, so
     * a set of persistently failing keys does not log a line per key on every clear(); the first
     * Error is rethrown once every key has been tried. The high-water warning mark restarts only
     * once the set is empty: re-recording keys that keep failing must not repeat its warnings.
     */
    private void retryOrphanedStoreKeys() {
        final List<K> keys;

        synchronized (orphanedStoreKeys) {
            keys = drainOrphanedStoreKeys();
            orphanedStoreKeysInRetry += keys.size();
        }

        Error error = null;
        Throwable firstFailure = null;
        int failedCount = 0;

        try {
            for (final K key : keys) {
                try {
                    removeStoreBytesIfUnowned(key);
                } catch (final Throwable e) {
                    recordOrphanedStoreKey(key);
                    failedCount++;

                    if (e instanceof final Error err && error == null) {
                        error = err;
                    } else if (firstFailure == null) {
                        firstFailure = e;
                    }
                }
            }
        } finally {
            synchronized (orphanedStoreKeys) {
                orphanedStoreKeysInRetry -= keys.size();

                if (orphanedStoreKeys.isEmpty() && orphanedStoreKeysInRetry == 0) {
                    orphanedStoreKeysWarnAt = ORPHANED_STORE_KEYS_WARN_THRESHOLD;
                }
            }
        }

        if (firstFailure != null) {
            logger.warn(
                    failedCount + " of " + keys.size()
                            + " off-heap cache keys with orphaned OffHeapStore bytes could not be removed; retrying on the next clear() or close()",
                    firstFailure);
        }

        if (error != null) {
            throw error;
        }
    }

    /**
     * Removes every recorded key's store bytes directly. Safe without a per-key compute only here:
     * close() holds the lifecycle write lock, so no operation runs concurrently, and every mapping
     * has already been released, so no entry can own the bytes. Store failures other than an
     * {@link Error} are logged, as in {@code free}; the first Error is rethrown after all keys.
     */
    private void removeOrphanedStoreBytesOnClose() {
        Throwable failure = null;

        for (final K key : drainOrphanedStoreKeys()) {
            failure = runCleanupStep(failure, () -> {
                try {
                    offHeapStore.remove(key);
                } catch (final Exception e) {
                    logger.warn("Failed to remove orphaned backing bytes of an off-heap cache key from the OffHeapStore", e);
                }
            });
        }

        if (failure instanceof final Error error) {
            throw error;
        }
    }

    private void removeAllEntries(final FreeCause cause) {
        for (final Map.Entry<K, Entry<V>> mapEntry : entries.entrySet()) {
            removeIfCurrent(mapEntry.getKey(), mapEntry.getValue(), cause);
        }
    }

    /**
     * Releases each indexed entry while close holds the lifecycle write lock, then clears the
     * index even if a release fails. No per-key lookup is needed because cache operations have
     * stopped; avoiding one also prevents a key's hashCode or equals implementation from blocking
     * cleanup. The first failure is propagated after all releases have been attempted, with later
     * distinct failures suppressed.
     *
     * @throws RuntimeException if releasing an entry throws an unchecked exception
     */
    private void removeAllEntriesOnClose() throws RuntimeException {
        Throwable failure = null;

        try {
            for (final Map.Entry<K, Entry<V>> mapEntry : entries.entrySet()) {
                failure = runCleanupStep(failure, () -> free(mapEntry.getKey(), mapEntry.getValue(), FreeCause.REMOVED, true));
            }
        } finally {
            entries.clear();
        }

        if (failure instanceof final RuntimeException runtimeException) {
            throw runtimeException;
        } else if (failure instanceof final Error error) {
            throw error;
        }
    }

    /** Removes and frees every expired entry. Called by the periodic maintenance pass. */
    private void sweepExpiredEntries() {
        for (final Map.Entry<K, Entry<V>> mapEntry : entries.entrySet()) {
            final Entry<V> entry = mapEntry.getValue();

            if (!entry.freed && entry.activityPrint.isExpired()) {
                removeIfCurrent(mapEntry.getKey(), entry, FreeCause.EXPIRED);
            }
        }
    }

    // ---------------------------------------------------------------------------------- vacate

    /**
     * Schedules an asynchronous vacate pass that evicts the least-recently-accessed
     * ~{@code vacatingFactor} of memory-resident entries and reclaims their now-empty segments.
     * Debounced (a pass that finished within the last 3 s suppresses a new one) and single-flight
     * (at most one pass runs at a time). The pass holds the lifecycle read lock, so
     * {@link #close()} waits for it.
     *
     * @throws IllegalStateException if the shared async executor has been shut down before a vacate task is submitted
     * @throws RejectedExecutionException if the executor rejects submission of the vacate task
     */
    private void vacate() throws IllegalStateException, RejectedExecutionException {
        if (System.nanoTime() - lastVacateFinishedNanos < VACATE_DEBOUNCE_NANOS || !vacating.compareAndSet(false, true)) {
            return;
        }

        try {
            asyncExecutor.execute(() -> {
                lifecycleLock.readLock().lock();
                try {
                    if (!closed) {
                        evictLeastRecentlyAccessed();
                        reclaimEmptySegments();
                    }
                } catch (final Throwable e) {
                    // The task's future is never read; without this catch a failed pass would
                    // abort silently. Throwable, as in runMaintenance: an Error or an undeclared
                    // checked exception from a key must be logged too. The next put under
                    // pressure schedules a fresh pass.
                    logger.warn("Off-heap cache vacate pass failed; a later put under memory pressure will retry", e);
                } finally {
                    lifecycleLock.readLock().unlock();
                    lastVacateFinishedNanos = System.nanoTime();
                    vacating.set(false);
                }
            });
        } catch (final RuntimeException | Error e) {
            // The task could not be submitted; release the gate so a later put can retry.
            vacating.set(false);
            throw e;
        }
    }

    /**
     * Evicts the least-recently-accessed ~{@code vacatingFactor} of memory-resident entries.
     * Disk-backed entries are not candidates: evicting them frees no native memory, which is the
     * only pressure a vacate exists to relieve.
     */
    private void evictLeastRecentlyAccessed() {
        // Each candidate carries a frozen copy of its access time: sorting on the live (volatile,
        // concurrently touched) field would make comparisons inconsistent mid-sort, which TimSort
        // rejects with an IllegalArgumentException.
        final List<Map.Entry<Long, Map.Entry<K, Entry<V>>>> candidates = new ArrayList<>(entries.size());

        for (final Map.Entry<K, Entry<V>> mapEntry : entries.entrySet()) {
            final Entry<V> entry = mapEntry.getValue();

            if (entry.isMemory() && !entry.freed) {
                candidates.add(Map.entry(entry.activityPrint.getLastAccessTime(), mapEntry));
            }
        }

        if (candidates.isEmpty()) {
            return;
        }

        candidates.sort(Map.Entry.comparingByKey());

        final int evictTargetCount = evictTargetCountOf(candidates.size());

        for (int i = 0; i < evictTargetCount; i++) {
            final Map.Entry<K, Entry<V>> victim = candidates.get(i).getValue();
            removeIfCurrent(victim.getKey(), victim.getValue(), FreeCause.EVICTED);
        }
    }

    /**
     * The number of the given (positive) candidate count a vacate pass evicts: at least one, at
     * most all. The product is computed in {@code double}: in {@code float} arithmetic a count
     * above 2^24 can round UP (e.g. {@code (int) (16_777_219 * 1.0f) == 16_777_220}), which would
     * index past the candidate list.
     */
    private int evictTargetCountOf(final int candidateCount) {
        return Math.min(candidateCount, Math.max(1, (int) (candidateCount * (double) vacatingFactor)));
    }

    // -------------------------------------------------------------------------------- allocator

    /**
     * Allocates the slots needed for a value of the given serialized size: one slot of the
     * smallest sufficient class when {@code size <= maxBlockSize}, otherwise one
     * {@code maxBlockSize} slot per full chunk plus a smaller-class slot for the final partial
     * chunk. Returns {@code null} when the region cannot currently satisfy the request. Any partial
     * allocation is released, and segments made empty by that rollback are immediately returned to
     * the free pool so a disk fallback cannot strand them in the attempted size class.
     */
    private long[] allocateSlots(final int size) {
        final int chunkCount = chunkCountOf(size);
        final long[] slots = new long[chunkCount];

        synchronized (allocatorLock) {
            for (int i = 0; i < chunkCount; i++) {
                final long slot = allocateSlotLocked(slotSizeOfChunk(size, i));

                if (slot < 0) {
                    for (int j = 0; j < i; j++) {
                        releaseSlotLocked(slots[j]);
                    }

                    reclaimEmptySegmentsLocked();

                    return null;
                }

                slots[i] = slot;
            }
        }

        return slots;
    }

    /** Allocates one slot of the given class. Caller holds the allocator lock. */
    private long allocateSlotLocked(final int slotSize) {
        final Deque<Segment> queue = segmentQueues[slotSize / MIN_BLOCK_SIZE - 1];
        final Segment available = queue == null ? null : queue.peekFirst();

        if (available != null) {
            final int slotIndex = available.slotBits.nextClearBit(0);
            available.slotBits.set(slotIndex);

            if (available.used++ == 0) {
                emptyDedicatedSegmentCount--;
            }

            // Removing full segments keeps selection constant-time even when a class spans
            // many thousands of full segments. The first release puts the segment back.
            if (available.used == available.slotCount) {
                queue.removeFirst();
            }

            return packSlot(available.index, slotIndex);
        }

        // Every dedicated segment of this class is full; dedicate a fresh segment if one is free.
        int segmentIndex = dedicatedSegments.nextClearBit(nextFreeSegmentHint);

        if (segmentIndex >= segments.length && emptyDedicatedSegmentCount > 0) {
            // Segments emptied by remove/replace/expiry stay dedicated to their old class until
            // reclaimed. Reclaim them on demand here: otherwise this put would fail (spilling to
            // disk, or scheduling a vacate that evicts live entries) although memory is free.
            reclaimEmptySegmentsLocked();
            segmentIndex = dedicatedSegments.nextClearBit(nextFreeSegmentHint);
        }

        if (segmentIndex >= segments.length) {
            return -1;
        }

        dedicatedSegments.set(segmentIndex);
        nextFreeSegmentHint = segmentIndex + 1;

        final Segment segment = segments[segmentIndex];
        segment.slotSize = slotSize;
        segment.slotCount = SEGMENT_SIZE / slotSize;
        segment.slotBits.set(0);
        segment.used = 1;

        if (segment.slotCount > 1) {
            vacancyQueueOf(slotSize).addFirst(segment);
        }

        return packSlot(segmentIndex, 0);
    }

    /** Returns the vacancy queue of a slot-size class, creating it on first use. Caller holds the allocator lock. */
    private Deque<Segment> vacancyQueueOf(final int slotSize) {
        final int classIndex = slotSize / MIN_BLOCK_SIZE - 1;
        Deque<Segment> queue = segmentQueues[classIndex];

        if (queue == null) {
            queue = new ArrayDeque<>();
            segmentQueues[classIndex] = queue;
        }

        return queue;
    }

    /** Releases every slot held by a memory entry. Safe to call at most once per entry. */
    private void releaseSlots(final Entry<V> entry) {
        releaseSlots(entry.slots);
    }

    /**
     * Releases raw slot handles for a retired entry. Unlike {@link #discardUninstalledSlots(long[])},
     * segments emptied here are not reclaimed immediately (so same-class puts can reuse them);
     * that is left to the next allocation that finds no free segment, the maintenance pass, a
     * vacate pass, or {@link #clear()}.
     */
    private void releaseSlots(final long[] slots) {
        synchronized (allocatorLock) {
            for (final long slot : slots) {
                releaseSlotLocked(slot);
            }
        }
    }

    /** Releases an entry that never reached the map and immediately reclaims emptied segments. */
    private void discardUninstalledMemoryEntry(final Entry<V> entry) {
        discardUninstalledSlots(entry.slots);
    }

    /** Releases raw, unpublished slots and atomically returns newly empty segments to the free pool. */
    private void discardUninstalledSlots(final long[] slots) {
        synchronized (allocatorLock) {
            for (final long slot : slots) {
                releaseSlotLocked(slot);
            }

            reclaimEmptySegmentsLocked();
        }
    }

    private void releaseSlotLocked(final long slot) {
        final Segment segment = segments[segmentIndexOf(slot)];
        final int slotIndex = slotIndexOf(slot);

        // A stale or duplicate handle (its slot is already free, possibly in a since-reclaimed
        // segment) must not decrement the count: a segment whose count reached zero while slots
        // were still live would be reclaimed and re-dedicated underneath them. Ignoring it also
        // lets the caller go on to release the rest of an entry's slots. It is an internal bug,
        // so the assertion makes it fail loudly wherever assertions are enabled (the test suite).
        if (!segment.slotBits.get(slotIndex)) {
            assert false : "release of an unallocated off-heap slot (segment " + segment.index + ", slot " + slotIndex + ")";
            logger.warn("Ignoring release of an off-heap slot that is not allocated (segment " + segment.index + ", slot " + slotIndex + ")");
            return;
        }

        segment.slotBits.clear(slotIndex);

        // A full segment is absent from its vacancy queue; add it exactly once when its first
        // slot is freed. This also covers single-slot segments and partial-allocation rollback.
        if (segment.used == segment.slotCount) {
            vacancyQueueOf(segment.slotSize).addFirst(segment);
        }

        if (--segment.used == 0) {
            emptyDedicatedSegmentCount++;
        }
    }

    /**
     * Returns fully empty segments to the free pool so they can be re-dedicated to any slot size.
     * Called by the maintenance pass, the vacate pass, and {@link #clear()}; allocation calls
     * {@link #reclaimEmptySegmentsLocked()} directly.
     */
    private void reclaimEmptySegments() {
        synchronized (allocatorLock) {
            reclaimEmptySegmentsLocked();
        }
    }

    /** Returns all empty dedicated segments to the free pool. Caller holds the allocator lock. */
    private void reclaimEmptySegmentsLocked() {
        if (emptyDedicatedSegmentCount == 0) {
            return;
        }

        // Every empty segment has free slots and is therefore in its class's vacancy queue.
        for (final Deque<Segment> queue : segmentQueues) {
            if (queue == null || queue.isEmpty()) {
                continue;
            }

            for (final Iterator<Segment> it = queue.iterator(); it.hasNext();) {
                final Segment segment = it.next();

                if (segment.used == 0) {
                    it.remove();
                    dedicatedSegments.clear(segment.index);
                    segment.slotSize = 0;
                    segment.slotCount = 0;
                    emptyDedicatedSegmentCount--;

                    if (segment.index < nextFreeSegmentHint) {
                        nextFreeSegmentHint = segment.index;
                    }
                }
            }

            if (emptyDedicatedSegmentCount == 0) {
                return;
            }
        }
    }

    /** Builds the per-class, per-segment occupied-slot detail for {@link #stats()}. */
    @SuppressWarnings("unused")
    private Map<Integer, Map<Integer, Integer>> snapshotOccupiedSlots() {
        final Map<Integer, Map<Integer, Integer>> result = new TreeMap<>();

        synchronized (allocatorLock) {
            // Vacancy queues omit full segments. Traverse the dedication index instead so every
            // segment is reported, already ordered by index without per-class sorting buffers.
            for (int index = dedicatedSegments.nextSetBit(0); index >= 0; index = dedicatedSegments.nextSetBit(index + 1)) {
                final Segment segment = segments[index];
                result.computeIfAbsent(segment.slotSize, k -> new LinkedHashMap<>()).put(segment.index, segment.used);
            }
        }

        return result;
    }

    // ------------------------------------------------------------------------- copy and helpers

    /**
     * Whether a value of the given serialized size could be placed in the region in any allocator
     * state. Comparing the raw size with the capacity is not enough: every chunk occupies a slot
     * rounded up to its size class, each class needs whole segments of its own, and the final
     * partial chunk usually belongs to a different class than the full {@code maxBlockSize}
     * chunks. The value therefore needs at least
     * {@code ceil(maxBlockSizeSlots / slotsPerSegment)} segments for its {@code maxBlockSize}
     * slots, plus one more segment when the final chunk uses a smaller class; if that exceeds the
     * segment count, placement is impossible even in an empty cache.
     */
    private boolean canEverFitInMemory(final int size) {
        if (size > capacityInBytes) {
            return false;
        }

        final int chunkCount = chunkCountOf(size);
        final boolean finalChunkUsesMaxClass = slotSizeOfChunk(size, chunkCount - 1) == maxBlockSize;
        final long maxClassSlots = finalChunkUsesMaxClass ? chunkCount : chunkCount - 1L;
        final int slotsPerSegment = SEGMENT_SIZE / maxBlockSize;
        final long segmentsNeeded = (maxClassSlots + slotsPerSegment - 1) / slotsPerSegment + (finalChunkUsesMaxClass ? 0 : 1);

        return segmentsNeeded <= segments.length;
    }

    /** The number of slots a value of the given serialized size occupies. */
    private int chunkCountOf(final int size) {
        // Long arithmetic: for a size close to Integer.MAX_VALUE (legal when the capacity is
        // large enough), `size + maxBlockSize - 1` would overflow int and go negative.
        return size <= maxBlockSize ? 1 : (int) (((long) size + maxBlockSize - 1) / maxBlockSize);
    }

    /** The payload size of one chunk: {@code maxBlockSize} for all but the final, possibly smaller chunk. */
    private int chunkSizeOfChunk(final int size, final int chunkIndex) {
        final int count = chunkCountOf(size);
        return chunkIndex < count - 1 ? maxBlockSize : size - (count - 1) * maxBlockSize;
    }

    /** The slot-size class of a chunk: its size rounded up to a multiple of {@link #MIN_BLOCK_SIZE}. */
    private int slotSizeOfChunk(final int size, final int chunkIndex) {
        final int chunkSize = chunkSizeOfChunk(size, chunkIndex);

        return chunkSize <= 0 ? MIN_BLOCK_SIZE : roundUpToMinBlock(chunkSize);
    }

    /** The slot-rounded native memory a value of the given serialized size occupies. */
    private long occupiedMemoryOf(final int size) {
        final int chunkCount = chunkCountOf(size);

        // All full chunks use maxBlockSize slots (maxBlockSize is a MIN_BLOCK_SIZE multiple).
        return (long) (chunkCount - 1) * maxBlockSize + slotSizeOfChunk(size, chunkCount - 1);
    }

    private static int roundUpToMinBlock(final int size) {
        return size % MIN_BLOCK_SIZE == 0 ? size : (size / MIN_BLOCK_SIZE + 1) * MIN_BLOCK_SIZE;
    }

    private static long packSlot(final int segmentIndex, final int slotIndex) {
        return ((long) segmentIndex << SLOT_INDEX_BITS) | slotIndex;
    }

    private static int segmentIndexOf(final long slot) {
        return (int) (slot >>> SLOT_INDEX_BITS);
    }

    private static int slotIndexOf(final long slot) {
        return (int) (slot & SLOT_INDEX_MASK);
    }

    private long addressOf(final long slot, final int slotSize) {
        return baseAddress + (long) segmentIndexOf(slot) * SEGMENT_SIZE + (long) slotIndexOf(slot) * slotSize;
    }

    /**
     * Copies serialized bytes into freshly allocated, still-unpublished slots.
     *
     * @throws RuntimeException if a native-memory copy hook throws an unchecked exception
     */
    private void copyToAllocatedSlots(final int size, final long[] slots, final byte[] bytes) throws RuntimeException {
        int copied = 0;

        for (int i = 0; i < slots.length; i++) {
            final int chunkSize = chunkSizeOfChunk(size, i);
            copyToMemory(addressOf(slots[i], slotSizeOfChunk(size, i)), bytes, arrayOffset + copied, chunkSize);
            copied += chunkSize;
        }
    }

    /**
     * Creates and fills a memory entry, returning every allocated slot if construction or copying
     * fails before the entry can be offered to the map.
     *
     * @throws RuntimeException if a native-memory copy hook throws an unchecked exception
     */
    private Entry<V> createMemoryEntry(final Type<V> type, final int size, final long liveTime, final long maxIdleTime, final long[] slots, final byte[] bytes)
            throws RuntimeException {
        try {
            // Perform the potentially expensive native copy before starting the entry's TTL/idle
            // clocks. The resulting ActivityPrint is created before map installation begins,
            // matching the disk path, whose clock starts only after its store write completes.
            copyToAllocatedSlots(size, slots, bytes);
            return new Entry<>(type, size, liveTime, maxIdleTime, slots);
        } catch (final RuntimeException | Error e) {
            discardUninstalledSlots(slots);
            throw e;
        }
    }

    /**
     * Reassembles an entry's serialized bytes from its slots. Caller holds the entry monitor.
     *
     * @throws RuntimeException if a native-memory copy hook throws an unchecked exception
     */
    private void copyEntryFromMemory(final Entry<V> entry, final byte[] bytes) throws RuntimeException {
        int copied = 0;

        for (int i = 0; i < entry.slots.length; i++) {
            final int chunkSize = chunkSizeOfChunk(entry.size, i);
            copyFromMemory(addressOf(entry.slots[i], slotSizeOfChunk(entry.size, i)), bytes, arrayOffset + copied, chunkSize);
            copied += chunkSize;
        }
    }

    /**
     * Reconstructs a non-null cache value consistently for every storage tier.
     *
     * @throws RuntimeException if the configured deserializer throws an unchecked exception
     * @throws IllegalStateException if the configured deserializer returns {@code null}
     */
    @SuppressWarnings("unchecked")
    private V deserializeValue(final byte[] bytes, final Type<V> type) throws RuntimeException, IllegalStateException {
        final V value;

        if (type.isPrimitiveByteArray()) {
            value = (V) bytes;
        } else if (type.isByteBuffer()) {
            // Even ordinary allocateDirect() buffers extend MappedByteBuffer on this JDK.
            // Preserve heap reconstruction for all buffer inputs instead of allocating native
            // memory on every read based on that shared implementation superclass.
            value = (V) ByteBufferType.valueOf(bytes);
        } else {
            value = deserializer.apply(bytes, type);
        }

        if (value == null) {
            throw new IllegalStateException("Failed to reconstruct cache value: the configured deserializer returned null");
        }

        return value;
    }

    private static long elapsedMillisSince(final long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    /**
     * Checks that this cache is open.
     *
     * @throws IllegalStateException if this cache has been closed
     */
    private void assertNotClosed() throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("This off-heap cache has been closed");
        }
    }

    // -------------------------------------------------------------------------- nested types

    /**
     * A cache entry: immutable identity (type, size, storage) plus a mutable, monitor-guarded
     * lifecycle. Memory-backed entries hold packed slot handles; disk-backed entries hold
     * {@code null} slots and their bytes live in the {@link OffHeapStore} under the cache key.
     *
     * <p>Lifecycle protocol: {@code freed} is set exactly once, under {@code synchronized(this)},
     * by {@link AbstractOffHeapCache#free}; readers copy native memory / fetch store bytes under
     * the same monitor after checking the flag, so a read can never observe released memory or
     * bytes belonging to a newer same-key entry.
     */
    static final class Entry<T> {
        final Type<T> type;
        final int size;
        /** Packed slot handles ({@code segmentIndex << 14 | slotIndex}); {@code null} = disk-backed. */
        final long[] slots;
        /** Creation time, TTL/idle limits, last-access time, and access count. Mutated under the entry monitor. */
        final ActivityPrint activityPrint;
        /** One-way flag; written under the entry monitor, read either under it or as a racy hint. */
        volatile boolean freed;

        /**
         * Creates an entry with fresh expiration metadata.
         *
         * @throws IllegalArgumentException if {@code liveTime} or {@code maxIdleTime} is not positive
         */
        Entry(final Type<T> type, final int size, final long liveTime, final long maxIdleTime, final long[] slots) throws IllegalArgumentException {
            this(type, size, new ActivityPrint(liveTime, maxIdleTime), slots);
        }

        Entry(final Type<T> type, final int size, final ActivityPrint activityPrint, final long[] slots) {
            this.type = type;
            this.size = size;
            this.activityPrint = activityPrint;
            this.slots = slots;
        }

        boolean isMemory() {
            return slots != null;
        }

        /** Records an access. Caller holds the entry monitor. */
        void touch() {
            activityPrint.updateLastAccessTime();
            activityPrint.updateAccessCount();
        }
    }

    /**
     * A 1 MB region of the native allocation, dynamically dedicated to one slot-size class.
     * All fields except {@code index} are guarded by the allocator lock.
     */
    static final class Segment {
        final int index;
        /** Current slot size; meaningful only while the segment is dedicated. */
        int slotSize;
        /** Slots per segment at the current slot size ({@code SEGMENT_SIZE / slotSize}); 0 while not dedicated. */
        int slotCount;
        final BitSet slotBits = new BitSet();
        int used;

        Segment(final int index) {
            this.index = index;
        }
    }

    /**
     * Thread-safe min/max/average accumulator for disk I/O times in milliseconds. Observations
     * come from monotonic-clock differences and are therefore never negative.
     */
    private static final class TimingStats {
        private long count;
        private long min;
        private long max;
        private long sum;

        synchronized void record(final long millis) {
            count++;
            sum += millis;
            min = count == 1 ? millis : Math.min(min, millis);
            max = count == 1 ? millis : Math.max(max, millis);
        }

        /**
         * Captures the timing extrema and mean.
         *
         * @throws IllegalArgumentException if accumulator overflow produces a negative or non-finite
         *         value, or a mean outside the recorded minimum and maximum
         */
        synchronized MinMaxAvg snapshot() throws IllegalArgumentException {
            return count == 0 ? new MinMaxAvg(0.0D, 0.0D, 0.0D) : new MinMaxAvg(min, max, Numbers.round((double) sum / count, 2));
        }
    }
}
