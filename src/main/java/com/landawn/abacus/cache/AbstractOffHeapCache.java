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
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
 * class). A segment whose slots are all freed is reclaimed &mdash; by the periodic maintenance
 * pass, a vacate pass, or {@link #clear()} &mdash; and can then be re-dedicated to a different
 * slot size. A value whose serialized form exceeds the <i>entire</i> capacity never attempts
 * in-memory placement.
 *
 * <p><b>Concurrency.</b> Four mechanisms, with the lock order map bin &rarr; entry monitor
 * &rarr; allocator lock (no code path acquires them in any other order):
 * <ul>
 * <li>Entries live in a {@link ConcurrentHashMap}; every same-key mutation (put, disk spill,
 *     promotion, removal) runs inside the map's atomic per-key {@code compute}/{@code remove},
 *     so no two mutations of one key can interleave.</li>
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
 * none is configured or because its write was rejected — the put returns {@code false} and
 * schedules an asynchronous, debounced vacate pass that evicts the least-recently-accessed
 * ~{@code vacatingFactor} of memory-resident entries and reclaims their now-empty segments.
 *
 * <p><b>Disk tier.</b> With an {@link OffHeapStore} configured, values that cannot be placed in
 * memory (or that the {@code storeSelector} routes to disk) are written to the store under the
 * cache key. The store write and the entry installation happen inside the same per-key atomic
 * operation; a failed or throwing write leaves the prior mapping and its bytes untouched (the
 * {@link OffHeapStore#put} contract guarantees the prior bytes are unchanged on failure). Reads
 * may optionally be promoted back into memory via the configured
 * {@code testerForLoadingItemFromDiskToMemory}.
 *
 * <p><b>Value handling.</b> {@code byte[]} values are stored raw (exactly {@code array.length}
 * bytes); {@link ByteBuffer} values store the bytes from index 0 up to the current position
 * (position, limit, and mark are left untouched); all other types go through the configured
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
     * Returns the raw bytes for {@code byte[]} types, wraps them in a {@link ByteBuffer}
     * for {@code ByteBuffer} types, and otherwise delegates to {@link #PARSER}.
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
        /** Explicit remove()/clear()/close(), or a stale mapping whose store bytes vanished. */
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

    /** All entries, both memory-backed and disk-backed. Same-key mutations use atomic compute/remove. */
    private final ConcurrentHashMap<K, Entry<V>> entries = new ConcurrentHashMap<>();

    /**
     * Close() takes the write side while every cache operation holds the read side, so native
     * memory is never deallocated (and the store never closed) underneath an in-flight operation.
     */
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();

    private volatile boolean closed;

    // --- allocator (all fields below are guarded by allocatorLock) ------------------------------

    /** The single allocator mutex: segment dedication, slot allocation/release, and reclamation. */
    private final Object allocatorLock = new Object();
    /** Marks which segments are currently dedicated to a slot-size class. */
    private final BitSet dedicatedSegments = new BitSet();
    private final Segment[] segments;
    /** Per size class, the segments currently dedicated to it (segments with vacancies near the front). */
    private final Deque<Segment>[] segmentQueues;
    /** Lower bound for the next free-segment scan. */
    private int nextFreeSegmentHint = 0;

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
     * @param defaultLiveTime the default TTL in milliseconds for entries added without an explicit one
     * @param defaultMaxIdleTime the default maximum idle time in milliseconds for entries added without an explicit one
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
     * @throws IllegalArgumentException if a numeric argument is out of range or a required argument is {@code null}
     * @throws OutOfMemoryError if the native allocation cannot be reserved
     * @throws IllegalStateException if the JVM is already shutting down when the shutdown hook is registered
     * @throws SecurityException if runtime policy denies shutdown-hook registration
     * @throws java.util.concurrent.RejectedExecutionException if {@code evictDelay} is positive
     *                           and the maintenance scheduler rejects its task (all cache-owned
     *                           resources are released before this propagates)
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
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

        segmentQueues = new Deque[this.maxBlockSize / MIN_BLOCK_SIZE];

        for (int i = 0, len = segmentQueues.length; i < len; i++) {
            segmentQueues[i] = new ArrayDeque<>();
        }

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
        } catch (final Exception e) {
            // Swallowed so the scheduled task keeps running; the pass retries on the next cycle.
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
     * @throws OutOfMemoryError if the implementation cannot reserve the requested amount of native memory
     * @throws IllegalArgumentException if {@code capacityInBytes} is negative (both built-in
     *         backends reject a negative size this way; unreachable through normal construction
     *         because {@code capacityInMB} is validated to be positive first)
     */
    protected abstract long allocate(long capacityInBytes);

    /**
     * Releases the off-heap memory reserved by {@link #allocate(long)}. Called at most once, from
     * the lifecycle-write-locked {@link #close()} or from the constructor's failure-cleanup path
     * before the instance is published, so implementations need not be thread-safe.
     */
    protected abstract void deallocate();

    /**
     * Copies bytes from a heap array into off-heap memory. May be called concurrently for
     * different (disjoint) destination regions.
     *
     * @param startPtr the destination address in off-heap memory
     * @param bytes the source array
     * @param srcOffset the zero-based source-array index, plus the {@code arrayOffset} supplied at
     *                  construction. A backend that needs an object-layout base offset must add it
     *                  using {@code long} arithmetic in this hook.
     * @param len the number of bytes to copy
     */
    protected abstract void copyToMemory(long startPtr, byte[] bytes, int srcOffset, int len);

    /**
     * Copies bytes from off-heap memory into a heap array. May be called concurrently for
     * different source regions.
     *
     * @param startPtr the source address in off-heap memory
     * @param bytes the destination array
     * @param destOffset the zero-based destination-array index (plus the construction-time
     *                   {@code arrayOffset}), in the same convention as
     *                   {@link #copyToMemory(long, byte[], int, int)}'s {@code srcOffset}
     * @param len the number of bytes to copy
     */
    protected abstract void copyFromMemory(long startPtr, byte[] bytes, int destOffset, int len);

    // ------------------------------------------------------------------------------------- get

    /**
     * Retrieves the value associated with the specified key, or {@code null} if no live mapping
     * exists. An expired entry encountered here is removed (and counted as an eviction). For a
     * disk-backed entry, the serialized bytes are fetched from the {@link OffHeapStore} and the
     * entry may be promoted back into memory when the configured promotion predicate accepts it.
     *
     * @param key the key whose associated value is to be returned; must not be {@code null}
     * @return the cached value, or {@code null} if the key is not present, the entry has expired,
     *         the disk-backed entry's bytes are missing from the store, or the entry was removed
     *         concurrently
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws IllegalStateException if the cache has been closed, or if a value cannot be
     *                               reconstructed because the fetched size no longer matches the
     *                               recorded size (data corruption), or because a configured
     *                               deserializer returned {@code null}
     */
    @Override
    public V getOrNull(final K key) {
        N.checkArgNotNull(key, "key");

        lifecycleLock.readLock().lock();
        try {
            assertNotClosed();

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
     * entry simply stays in place.
     */
    private void maybePromoteToMemory(final K key, final Entry<V> diskEntry, final byte[] bytes, final long storeReadMillis) {
        if (testerForLoadingItemFromDiskToMemory == null
                || !testerForLoadingItemFromDiskToMemory.test(diskEntry.activityPrint, diskEntry.size, Math.max(0L, storeReadMillis))) {
            return;
        }

        final ActivityPrint print = diskEntry.activityPrint;
        // A wall-clock correction can make now precede the recorded creation time. Treat that as
        // zero elapsed lifetime; subtracting a negative age could otherwise overflow and suppress
        // promotion of a non-expiring entry.
        final long elapsedLiveTime = Math.max(0L, System.currentTimeMillis() - print.getCreatedTime());
        final long remainingLiveTime = print.getMaxLiveTime() == Long.MAX_VALUE ? Long.MAX_VALUE : print.getMaxLiveTime() - elapsedLiveTime;

        // A value larger than the entire region can never be placed; skip the doomed (and
        // allocator-lock-heavy) slot allocation attempt, mirroring doPut's guard.
        if (remainingLiveTime <= 0 || diskEntry.size > capacityInBytes) {
            return;
        }

        final long[] slots = allocateSlots(diskEntry.size);

        if (slots == null) {
            return; // No memory available; promotion is an optimization only.
        }

        final Entry<V> memoryEntry = createMemoryEntry(diskEntry.type, diskEntry.size, remainingLiveTime, print.getMaxIdleTime(), slots, bytes);

        try {
            entries.compute(key, (k, current) -> {
                if (current != diskEntry) {
                    // The mapping changed concurrently; discard the promoted copy, keep the current one.
                    discardUninstalledMemoryEntry(memoryEntry);
                    return current;
                }

                free(k, diskEntry, FreeCause.REPLACED, true);
                install(memoryEntry);
                return memoryEntry;
            });
        } catch (final RuntimeException | Error e) {
            // A throwing key hashCode/equals aborts the compute before the lambda installs the
            // promoted entry; release its freshly allocated slots instead of leaking them.
            discardUninstalledMemoryEntry(memoryEntry);
            throw e;
        }
    }

    // ------------------------------------------------------------------------------------- put

    /**
     * Stores a key-value pair with the specified expiration settings. The value is serialized and
     * placed in off-heap memory; when memory is unavailable (or the {@code storeSelector} directs
     * it), the value is written to the configured {@link OffHeapStore} instead.
     *
     * <p>Values whose serialized form exceeds the entire off-heap capacity never attempt the
     * in-memory placement (no amount of vacating could make them fit) and go straight to the disk
     * fallback, if any; such a doomed put schedules no vacate pass and leaves the in-memory cache
     * contents untouched.
     *
     * <p><b>Replacement failure:</b> a failed replacement never loses the prior entry. The new
     * entry is installed atomically with the retirement of the old one, and a failed or throwing
     * disk write leaves the prior mapping and its bytes unchanged per the {@link OffHeapStore#put}
     * contract.
     *
     * <p>Non-positive {@code liveTime}/{@code maxIdleTime} values mean "no expiration"/"no idle
     * limit". For a {@link ByteBuffer} value, the bytes from index 0 up to the current position
     * are stored; the buffer's position, limit, and mark are left unchanged. An entry's expiration
     * clock starts only after its serialized bytes have been copied to native memory or, for a
     * disk-routed value, after the store write has completed; map installation follows.
     *
     * @param key the key; must not be {@code null}
     * @param value the value; must not be {@code null}
     * @param liveTime the TTL in milliseconds; {@code <= 0} means no expiration
     * @param maxIdleTime the maximum idle time in milliseconds; {@code <= 0} means no idle limit
     * @return {@code true} if the value was stored (in memory or on disk); {@code false} otherwise
     * @throws IllegalArgumentException if {@code key} or {@code value} is {@code null}, or if
     *                                  {@code storeSelector} returns {@code null} or a value outside 0..2
     * @throws IllegalStateException if the cache has been closed
     * @throws java.util.concurrent.RejectedExecutionException if the put fails under memory
     *                                  pressure and the shared executor rejects the vacate task
     *                                  (possible only during JVM shutdown)
     */
    @Override
    public boolean put(final K key, final V value, final long liveTime, final long maxIdleTime) {
        N.checkArgNotNull(key, "key");
        N.checkArgNotNull(value, "value");

        lifecycleLock.readLock().lock();
        try {
            assertNotClosed();

            return doPut(key, value, liveTime, maxIdleTime);
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    private boolean doPut(final K key, final V value, final long liveTime, final long maxIdleTime) {
        // A non-positive liveTime/maxIdleTime means "no expiration" per the documented contract;
        // ActivityPrint rejects values <= 0, so translate to Long.MAX_VALUE.
        final long effectiveLiveTime = liveTime > 0 ? liveTime : Long.MAX_VALUE;
        final long effectiveMaxIdleTime = maxIdleTime > 0 ? maxIdleTime : Long.MAX_VALUE;

        final Type<V> type = N.typeOf(value.getClass());

        ByteArrayOutputStream os = null;

        try {
            final byte[] bytes;
            final int size;

            if (type.isPrimitiveByteArray()) {
                bytes = (byte[]) value;
                size = bytes.length;
            } else if (type.isByteBuffer()) {
                // ByteBufferType.byteArrayOf temporarily moves the supplied buffer's position to
                // zero, which invalidates its mark even though the position is restored. Operate
                // on a duplicate so put() is observationally read-only for the caller's buffer.
                bytes = ByteBufferType.byteArrayOf(((ByteBuffer) value).duplicate());
                size = bytes.length;
            } else {
                os = Objectory.createByteArrayOutputStream();
                serializer.accept(value, os);
                bytes = os.array();
                size = os.size();
            }

            final int selectedStore = selectedStoreOf(key, value, size);

            // A value larger than the entire off-heap region can never fit, no matter how many
            // entries a vacate pass frees; skip the memory attempt entirely for it.
            final boolean canBeStoredInMemory = selectedStore != STORE_DISK_ONLY && size <= capacityInBytes;
            final boolean canBeStoredToDisk = selectedStore != STORE_MEMORY_ONLY && offHeapStore != null;

            if (canBeStoredInMemory) {
                final long[] slots = allocateSlots(size);

                if (slots != null) {
                    final Entry<V> entry = createMemoryEntry(type, size, effectiveLiveTime, effectiveMaxIdleTime, slots, bytes);

                    try {
                        entries.compute(key, (k, old) -> {
                            if (old != null) {
                                free(k, old, FreeCause.REPLACED, true);
                            }

                            install(entry);
                            return entry;
                        });
                    } catch (final RuntimeException | Error e) {
                        // A throwing key hashCode/equals aborts the compute before the lambda
                        // installs the entry; release its freshly allocated slots.
                        discardUninstalledMemoryEntry(entry);
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

    /** Validates and returns the storage routing for one put: 0 (default), 1 (memory only), or 2 (disk only). */
    private int selectedStoreOf(final K key, final V value, final int size) {
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
     * completely untouched.
     *
     * @return {@code true} if the value was stored and installed
     */
    private boolean putToDisk(final K key, final Type<V> type, final byte[] bytes, final int size, final long liveTime, final long maxIdleTime) {
        // Always hand the store a private copy: `bytes` may alias a pooled serialization buffer
        // that is recycled (and reused by other threads) right after this put, or the caller's
        // own byte[]/ByteBuffer contents.
        final byte[] bytesToStore = N.copyOfRange(bytes, 0, size);

        final boolean[] stored = { false };
        final long[] writeMillis = { -1L };

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
            stored[0] = true;
            return entry;
        });

        if (!stored[0]) {
            return false;
        }

        putCountToDisk.increment();

        if (writeMillis[0] >= 0) {
            writeToDiskTimes.record(writeMillis[0]);
        }

        putCount.increment();
        return true;
    }

    /**
     * Performs one store write, recording the elapsed milliseconds of the successful call into
     * {@code elapsedOut[0]} when disk timing is enabled. Measures only {@link OffHeapStore#put},
     * excluding serialization and the unsuccessful in-memory placement work that preceded the
     * disk fallback.
     */
    private boolean timedStorePut(final K key, final byte[] bytes, final long[] elapsedOut) {
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
     * slots or disk bytes.
     *
     * @param key the key whose mapping is to be removed; must not be {@code null}
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public void remove(final K key) {
        N.checkArgNotNull(key, "key");

        lifecycleLock.readLock().lock();
        try {
            assertNotClosed();

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
     * @throws IllegalArgumentException if {@code key} is {@code null}
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public boolean containsKey(final K key) {
        N.checkArgNotNull(key, "key");

        assertNotClosed();

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
    public Set<K> keySet() {
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
    public int size() {
        assertNotClosed();

        return entries.size();
    }

    /**
     * Removes all entries, releasing their memory slots and disk bytes, and reclaims the
     * now-empty segments for reuse by any slot size.
     *
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public void clear() {
        lifecycleLock.readLock().lock();
        try {
            assertNotClosed();

            removeAllEntries(FreeCause.REMOVED);
            reclaimEmptySegments();
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    /**
     * Returns whether this cache has been closed.
     *
     * @return {@code true} if {@link #close()} has been called
     */
    @Override
    public boolean isClosed() {
        return closed;
    }

    /**
     * Deliberately decommissions this application-scoped cache before JVM termination. It cancels
     * the maintenance task, removes the JVM shutdown hook, frees every entry (removing disk-backed
     * entries' bytes from the store), deallocates the native region, and closes the configured
     * {@link OffHeapStore}, if any. If the cache remains active for the application lifetime, the
     * registered shutdown hook invokes this method automatically during JVM shutdown.
     * Every step runs even when an earlier one fails; the first failure propagates with any
     * later ones attached as suppressed exceptions. This method is idempotent.
     *
     * <p><b>Reentrant callback restriction:</b> a serializer, store selector, or other callback
     * invoked from an operation holding this cache's lifecycle read lock must not close the cache
     * synchronously (a read-to-write lock upgrade is unsupported); such a call fails immediately
     * with {@link IllegalStateException}.
     *
     * @throws IllegalStateException if the current thread attempts to close the cache while it is
     *                               executing a cache operation or callback
     * @throws SecurityException if the runtime denies removal of the registered shutdown hook;
     *                           native-memory and store cleanup is still attempted
     */
    @Override
    public void close() {
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
            failure = runCleanupStep(failure, () -> removeAllEntries(FreeCause.REMOVED));
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
     */
    public OffHeapCacheStats stats() {
        assertNotClosed();

        final long hits = hitCount.sum();
        final long misses = missCount.sum();

        // Capacity reports the maximum number of MIN_BLOCK_SIZE slots that fit in the region
        // (clamped to Integer.MAX_VALUE for very large caches) - an upper bound on entry count.
        final int capacity = (int) Math.min(capacityInBytes / MIN_BLOCK_SIZE, Integer.MAX_VALUE);

        // LongAdder.sum() is not an atomic snapshot; clamp the up-and-down gauges to >= 0 so a
        // transiently observed decrement-without-increment cannot make the stats record throw.
        final long gets = hits > Long.MAX_VALUE - misses ? Long.MAX_VALUE : hits + misses;

        return new OffHeapCacheStats(capacity, entries.size(), Math.max(0L, sizeOnDisk.sum()), putCount.sum(), putCountToDisk.sum(), gets, hits,
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
    }

    /**
     * Single accounting-and-release site for an entry leaving the cache. Marks the entry freed
     * under its monitor (excluding in-flight readers), releases its memory slots or disk bytes,
     * and reverses the counters that {@link #install(Entry)} updated.
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

            entry.freed = true;

            if (entry.isMemory()) {
                releaseSlots(entry);
            } else if (removeDiskBytes) {
                try {
                    offHeapStore.remove(key);
                } catch (final RuntimeException e) {
                    // The mapping is gone either way; a failed byte removal only leaks store space.
                    logger.warn("Failed to remove the backing bytes of an off-heap cache entry from the OffHeapStore", e);
                }
            }
        }

        if (entry.isMemory()) {
            totalOccupiedMemorySize.add(-occupiedMemoryOf(entry.size));
        } else {
            sizeOnDisk.decrement();
            dataSizeOnDisk.add(-entry.size);
        }

        totalDataSize.add(-entry.size);

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
     * with any concurrent same-key disk write.
     */
    private void removeIfCurrent(final K key, final Entry<V> entry, final FreeCause cause) {
        entries.compute(key, (k, current) -> {
            if (current != entry) {
                return current;
            }

            free(k, entry, cause, true);
            return null;
        });
    }

    private void removeAllEntries(final FreeCause cause) {
        for (final Map.Entry<K, Entry<V>> mapEntry : entries.entrySet()) {
            removeIfCurrent(mapEntry.getKey(), mapEntry.getValue(), cause);
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
     */
    private void vacate() {
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
                } catch (final Exception e) {
                    // The task's future is never read; without this catch a failed pass would
                    // abort silently. The next put under pressure schedules a fresh one.
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

        final int evictTargetCount = Math.max(1, (int) (candidates.size() * vacatingFactor));

        for (int i = 0; i < evictTargetCount; i++) {
            final Map.Entry<K, Entry<V>> victim = candidates.get(i).getValue();
            removeIfCurrent(victim.getKey(), victim.getValue(), FreeCause.EVICTED);
        }
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
        final int slotsPerSegment = SEGMENT_SIZE / slotSize;

        int scanned = 0;

        for (final Iterator<Segment> it = queue.iterator(); it.hasNext();) {
            final Segment segment = it.next();
            scanned++;

            if (segment.used < slotsPerSegment) {
                final int slotIndex = segment.slotBits.nextClearBit(0);
                segment.slotBits.set(slotIndex);
                segment.used++;

                // Keep segments with vacancies near the front so later allocations find them fast;
                // full segments naturally drift toward the back.
                if (scanned > 1) {
                    it.remove();
                    queue.addFirst(segment);
                }

                return packSlot(segment.index, slotIndex);
            }
        }

        // Every dedicated segment of this class is full; dedicate a fresh segment if one is free.
        final int segmentIndex = dedicatedSegments.nextClearBit(nextFreeSegmentHint);

        if (segmentIndex >= segments.length) {
            return -1;
        }

        dedicatedSegments.set(segmentIndex);
        nextFreeSegmentHint = segmentIndex + 1;

        final Segment segment = segments[segmentIndex];
        segment.slotSize = slotSize;
        segment.slotBits.set(0);
        segment.used = 1;
        queue.addFirst(segment);

        return packSlot(segmentIndex, 0);
    }

    /** Releases every slot held by a memory entry. Safe to call at most once per entry. */
    private void releaseSlots(final Entry<V> entry) {
        releaseSlots(entry.slots);
    }

    /**
     * Releases raw slot handles for a retired entry. Unlike {@link #discardUninstalledSlots(long[])},
     * segments emptied here are not reclaimed immediately; that is left to the maintenance pass,
     * a vacate pass, or {@link #clear()}.
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
        segment.slotBits.clear(slotIndexOf(slot));
        segment.used--;
    }

    /**
     * Returns fully empty segments to the free pool so they can be re-dedicated to any slot size.
     * Called by the maintenance pass, the vacate pass, and {@link #clear()}.
     */
    private void reclaimEmptySegments() {
        synchronized (allocatorLock) {
            reclaimEmptySegmentsLocked();
        }
    }

    /** Returns all empty dedicated segments to the free pool. Caller holds the allocator lock. */
    private void reclaimEmptySegmentsLocked() {
        for (final Deque<Segment> queue : segmentQueues) {
            for (final Iterator<Segment> it = queue.iterator(); it.hasNext();) {
                final Segment segment = it.next();

                if (segment.used == 0) {
                    it.remove();
                    dedicatedSegments.clear(segment.index);
                    segment.slotSize = 0;

                    if (segment.index < nextFreeSegmentHint) {
                        nextFreeSegmentHint = segment.index;
                    }
                }
            }
        }
    }

    /** Builds the per-class, per-segment occupied-slot detail for {@link #stats()}. */
    @SuppressWarnings("unused")
    private Map<Integer, Map<Integer, Integer>> snapshotOccupiedSlots() {
        final Map<Integer, Map<Integer, Integer>> result = new LinkedHashMap<>();

        synchronized (allocatorLock) {
            for (final Deque<Segment> queue : segmentQueues) {
                if (queue.isEmpty()) {
                    continue;
                }

                // Order segments by index within the class for a stable, readable snapshot.
                final List<Segment> ordered = new ArrayList<>(queue);
                ordered.sort((a, b) -> Integer.compare(a.index, b.index));

                for (final Segment segment : ordered) {
                    result.computeIfAbsent(segment.slotSize, k -> new LinkedHashMap<>()).put(segment.index, segment.used);
                }
            }
        }

        return result;
    }

    // ------------------------------------------------------------------------- copy and helpers

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

    /** Copies serialized bytes into freshly allocated, still-unpublished slots. */
    private void copyToAllocatedSlots(final int size, final long[] slots, final byte[] bytes) {
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
     */
    private Entry<V> createMemoryEntry(final Type<V> type, final int size, final long liveTime, final long maxIdleTime, final long[] slots,
            final byte[] bytes) {
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

    /** Reassembles an entry's serialized bytes from its slots. Caller holds the entry monitor. */
    private void copyEntryFromMemory(final Entry<V> entry, final byte[] bytes) {
        int copied = 0;

        for (int i = 0; i < entry.slots.length; i++) {
            final int chunkSize = chunkSizeOfChunk(entry.size, i);
            copyFromMemory(addressOf(entry.slots[i], slotSizeOfChunk(entry.size, i)), bytes, arrayOffset + copied, chunkSize);
            copied += chunkSize;
        }
    }

    /** Reconstructs a non-null cache value consistently for every storage tier. */
    @SuppressWarnings("unchecked")
    private V deserializeValue(final byte[] bytes, final Type<V> type) {
        final V value;

        if (type.isPrimitiveByteArray()) {
            value = (V) bytes;
        } else if (type.isByteBuffer()) {
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

    private void assertNotClosed() {
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

        Entry(final Type<T> type, final int size, final long liveTime, final long maxIdleTime, final long[] slots) {
            this.type = type;
            this.size = size;
            activityPrint = new ActivityPrint(liveTime, maxIdleTime);
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

        synchronized MinMaxAvg snapshot() {
            return count == 0 ? new MinMaxAvg(0.0D, 0.0D, 0.0D) : new MinMaxAvg(min, max, Numbers.round((double) sum / count, 2));
        }
    }
}
