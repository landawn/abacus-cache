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
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
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

import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.parser.Parser;
import com.landawn.abacus.parser.ParserFactory;
import com.landawn.abacus.pool.AbstractPoolable;
import com.landawn.abacus.pool.EvictionPolicy;
import com.landawn.abacus.pool.KeyedObjectPool;
import com.landawn.abacus.pool.PoolFactory;
import com.landawn.abacus.pool.PoolStats;
import com.landawn.abacus.type.ByteBufferType;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ByteArrayOutputStream;
import com.landawn.abacus.util.ExceptionUtil;
import com.landawn.abacus.util.Fn.Suppliers;
import com.landawn.abacus.util.Holder;
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.MoreExecutors;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Objectory;
import com.landawn.abacus.util.Tuple;
import com.landawn.abacus.util.stream.Stream;

//--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED

abstract class AbstractOffHeapCache<K, V> extends AbstractCache<K, V> {

    //    /**
    //     * Sets all bytes in a given block of memory to a copy of another
    //     * block.
    //     *
    //     * <p>This method determines each block's base address by means of two parameters,
    //     * and so it provides (in effect) a <em>double-register</em> addressing mode,
    //     * as discussed in {@link #getInt(Object,long)}.  When the object reference is null,
    //     * the offset supplies an absolute base address.
    //     *
    //     * <p>The transfers are in coherent (atomic) units of a size determined
    //     * by the address and length parameters.  If the effective addresses and
    //     * length are all even modulo 8, the transfer takes place in 'long' units.
    //     * If the effective addresses and length are (resp.) even modulo 4 or 2,
    //     * the transfer takes place in units of 'int' or 'short'.
    //     *
    //     */
    //    public native void copyMemory(Object srcBase, long srcOffset,
    //                                  Object destBase, long destOffset,
    //                                  long bytes);

    static final float DEFAULT_VACATING_FACTOR = 0.2f;

    static final Parser<?, ?> parser = ParserFactory.isKryoAvailable() ? ParserFactory.createKryoParser() : ParserFactory.createJSONParser();

    static final BiConsumer<Object, ByteArrayOutputStream> SERIALIZER = parser::serialize;

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

    static final int SEGMENT_SIZE = 1024 * 1024; // (int) N.ONE_MB;

    static final int MIN_BLOCK_SIZE = 64;

    static final int DEFAULT_MAX_BLOCK_SIZE = 8192; // 8K

    static final ScheduledExecutorService scheduledExecutor;

    static {
        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(IOUtil.CPU_CORES);
        // executor.setRemoveOnCancelPolicy(true);
        scheduledExecutor = MoreExecutors.getExitingScheduledExecutorService(executor);
    }

    final AtomicLong totalOccupiedMemorySize = new AtomicLong();
    final AtomicLong totalDataSize = new AtomicLong();
    final AtomicLong dataSizeOnDisk = new AtomicLong();
    final AtomicLong cachedCountOnDisk = new AtomicLong();
    final AtomicLong writeCountToDisk = new AtomicLong();
    final AtomicLong readCountFromDisk = new AtomicLong();
    final AtomicLong evictionCountFromDisk = new AtomicLong();

    final Logger logger;

    final long _capacityInBytes; //NOSONAR

    final long _startPtr; //NOSONAR

    final int _arrayOffset;

    final int _maxBlockSize;

    private final Segment[] _segments; //NOSONAR

    private final BitSet _segmentBitSet = new BitSet(); //NOSONAR

    private final Map<Integer, Deque<Segment>> _segmentQueueMap = new ConcurrentHashMap<>(); //NOSONAR

    private final Deque<Segment>[] _segmentQueues; //  = new Deque[_maxBlockSize / MIN_BLOCK_SIZE]; //

    //    {
    //        for (int i = 0, len = _segmentQueues.length; i < len; i++) {
    //            _segmentQueues[i] = new LinkedList<>();
    //        }
    //    }

    //    /** The queue 64. */
    //    private final Deque<Segment> _queue64 = new LinkedList<>(); //NOSONAR
    //
    //    /** The queue 128. */
    //    private final Deque<Segment> _queue128 = new LinkedList<>(); //NOSONAR
    //
    //    /** The queue 256. */
    //    private final Deque<Segment> _queue256 = new LinkedList<>(); //NOSONAR
    //
    //    /** The queue 384. */
    //    private final Deque<Segment> _queue384 = new LinkedList<>(); //NOSONAR
    //
    //    /** The queue 512. */
    //    private final Deque<Segment> _queue512 = new LinkedList<>(); //NOSONAR
    //
    //    /** The queue 640. */
    //    private final Deque<Segment> _queue640 = new LinkedList<>(); //NOSONAR
    //
    //    /** The queue 768. */
    //    private final Deque<Segment> _queue768 = new LinkedList<>(); //NOSONAR
    //
    //    /** The queue 896. */
    //    private final Deque<Segment> _queue896 = new LinkedList<>(); //NOSONAR
    //
    //    /** The queue 1024. */
    //    private final Deque<Segment> _queue1K = new LinkedList<>(); //NOSONAR
    //
    //    /** The queue 1280. */
    //    private final Deque<Segment> _queue1280 = new LinkedList<>(); //NOSONAR
    //
    //    /** The queue 1536. */
    //    private final Deque<Segment> _queue1536 = new LinkedList<>(); //NOSONAR
    //
    //    /** The queue 1792. */
    //    private final Deque<Segment> _queue1792 = new LinkedList<>(); //NOSONAR
    //
    //    /** The queue 2048. */
    //    private final Deque<Segment> _queue2K = new LinkedList<>(); //NOSONAR
    //
    //    /** The queue 2560. */
    //    private final Deque<Segment> _queue2_5K = new LinkedList<>(); //NOSONAR
    //
    //    /** The queue 3072. */
    //    private final Deque<Segment> _queue3K = new LinkedList<>(); //NOSONAR
    //
    //    /** The queue 3584. */
    //    private final Deque<Segment> _queue3_5K = new LinkedList<>(); //NOSONAR
    //
    //    /** The queue 4096. */
    //    private final Deque<Segment> _queue4K = new LinkedList<>(); //NOSONAR
    //
    //    /** The queue 5120. */
    //    private final Deque<Segment> _queue5K = new LinkedList<>(); //NOSONAR
    //
    //    /** The queue 6144. */
    //    private final Deque<Segment> _queue6K = new LinkedList<>(); //NOSONAR
    //
    //    /** The queue 7168. */
    //    private final Deque<Segment> _queue7K = new LinkedList<>(); //NOSONAR
    //
    //    /** The queue 8192. */
    //    private final Deque<Segment> _queue8K = new LinkedList<>(); //NOSONAR

    private final AsyncExecutor _asyncExecutor = new AsyncExecutor(); //NOSONAR

    private final AtomicInteger _activeVacationTaskCount = new AtomicInteger(); //NOSONAR

    private final KeyedObjectPool<K, Wrapper<V>> _pool; //NOSONAR

    private ScheduledFuture<?> scheduleFuture;
    final BiConsumer<? super V, ByteArrayOutputStream> serializer;
    final BiFunction<byte[], Type<V>, ? extends V> deserializer;
    final OffHeapStore<K> offHeapStore;
    final boolean statsTimeOnDisk;
    final Holder<BigInteger> totalWriteTimeToDiskHolder = Holder.of(BigInteger.ZERO);
    final Holder<BigInteger> totalReadTimeFromDiskHolder = Holder.of(BigInteger.ZERO);

    @SuppressWarnings("rawtypes")
    protected AbstractOffHeapCache(final int capacityInMB, final int maxBlockSize, final long evictDelay, final long defaultLiveTime,
            final long defaultMaxIdleTime, final float vacatingFactor, final int arrayOffset, final BiConsumer<? super V, ByteArrayOutputStream> serializer,
            final BiFunction<byte[], Type<V>, ? extends V> deserializer, final OffHeapStore<K> offHeapStore, final boolean statsTimeOnDisk,
            final Logger logger) {
        super(defaultLiveTime, defaultMaxIdleTime);

        N.checkArgument(maxBlockSize >= 1024 && maxBlockSize <= SEGMENT_SIZE, "The Maximum Block size can't be: {}. It must be >= 1024 and <= {}", maxBlockSize,
                SEGMENT_SIZE);

        this.logger = logger;

        _arrayOffset = arrayOffset;

        _capacityInBytes = capacityInMB * (1024L * 1024L); // N.ONE_MB;
        _maxBlockSize = maxBlockSize % MIN_BLOCK_SIZE == 0 ? maxBlockSize : (maxBlockSize / MIN_BLOCK_SIZE + 1) * MIN_BLOCK_SIZE;
        _segmentQueues = new Deque[_maxBlockSize / MIN_BLOCK_SIZE]; //

        // ByteBuffer.allocateDirect((int) capacity);
        _startPtr = allocate(_capacityInBytes);

        _segments = new Segment[(int) (_capacityInBytes / SEGMENT_SIZE)];

        for (int i = 0, len = _segments.length; i < len; i++) {
            _segments[i] = new Segment(_startPtr + (long) i * SEGMENT_SIZE);
        }

        _pool = PoolFactory.createKeyedObjectPool((int) (_capacityInBytes / MIN_BLOCK_SIZE), evictDelay, EvictionPolicy.LAST_ACCESS_TIME, true,
                vacatingFactor == 0f ? DEFAULT_VACATING_FACTOR : vacatingFactor);

        this.serializer = serializer == null ? (BiConsumer<V, ByteArrayOutputStream>) SERIALIZER : serializer;
        this.deserializer = deserializer == null ? (BiFunction) DESERIALIZER : deserializer;
        this.offHeapStore = offHeapStore;
        this.statsTimeOnDisk = statsTimeOnDisk;

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

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.warn("Starting to shutdown task in OffHeapCache");

            try {
                close();
            } finally {
                logger.warn("Completed to shutdown task in OffHeapCache");
            }
        }));
    }

    protected abstract long allocate(long capacityInBytes);

    protected abstract void deallocate();

    protected abstract void copyToMemory(byte[] bytes, int srcOffset, long startPtr, int len);

    protected abstract void copyFromMemory(final long startPtr, final byte[] bytes, final int destOffset, final int len);

    @Override
    public V gett(final K k) {
        final Wrapper<V> w = _pool.get(k);

        if (w instanceof StoreWrapper) {
            readCountFromDisk.incrementAndGet();

            if (statsTimeOnDisk) {
                final long startTime = System.currentTimeMillis();

                try {
                    return w.read();
                } finally {
                    synchronized (totalReadTimeFromDiskHolder) {
                        totalReadTimeFromDiskHolder
                                .setValue(totalReadTimeFromDiskHolder.value().add(BigInteger.valueOf(System.currentTimeMillis() - startTime)));
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
    public boolean put(final K k, final V v, final long liveTime, final long maxIdleTime) {
        final Type<V> type = N.typeOf(v.getClass());
        Wrapper<V> w = null;

        // final byte[] bytes = parser.serialize(v).getBytes();
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

        Slot slot = null;
        List<Slot> slots = null;

        try {
            if (size <= _maxBlockSize) {
                slot = getAvailableSlot(size);

                if (slot != null) {
                    final long slotStartPtr = slot.segment.segmentStartPtr + (long) slot.indexOfSlot * slot.segment.sizeOfSlot;

                    copyToMemory(bytes, _arrayOffset, slotStartPtr, size);

                    occupiedMemory = slot.segment.sizeOfSlot;

                    w = new SlotWrapper(type, liveTime, maxIdleTime, size, slot, slotStartPtr);
                } else if ((offHeapStore != null)) {
                    w = putToDisk(k, liveTime, maxIdleTime, type, bytes, size);
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
                    w = new MultiSlotsWrapper(type, liveTime, maxIdleTime, size, slots);
                } else if (offHeapStore != null) {
                    w = putToDisk(k, liveTime, maxIdleTime, type, bytes, size);
                }
            }
        } finally {
            Objectory.recycle(os);

            if (w == null) {
                _pool.remove(k);

                if (slot != null) {
                    slot.release();
                }

                if (slots != null) {
                    for (final Slot e : slots) {
                        e.release();
                    }
                }

                vacate();
                return false;
            }
        }

        boolean result = false;

        try {
            if (w instanceof SlotWrapper || w instanceof MultiSlotsWrapper) {
                totalOccupiedMemorySize.addAndGet(occupiedMemory);
            }

            totalDataSize.addAndGet(size);

            result = _pool.put(k, w);
        } finally {
            if (!result) {
                w.destroy();
            }
        }

        return result;
    }

    Wrapper<V> putToDisk(final K k, final long liveTime, final long maxIdleTime, final Type<V> type, final byte[] bytes, final int size) {
        writeCountToDisk.incrementAndGet();

        boolean putToDiskSuccessfully = false;

        if (statsTimeOnDisk) {
            final long startTime = System.currentTimeMillis();

            try {
                putToDiskSuccessfully = offHeapStore.put(k, bytes.length == size ? bytes : N.copyOfRange(bytes, 0, size));
            } finally {
                synchronized (totalWriteTimeToDiskHolder) {
                    totalWriteTimeToDiskHolder.setValue(totalWriteTimeToDiskHolder.value().add(BigInteger.valueOf(System.currentTimeMillis() - startTime)));
                }
            }
        } else {
            putToDiskSuccessfully = offHeapStore.put(k, bytes.length == size ? bytes : N.copyOfRange(bytes, 0, size));
        }

        if (putToDiskSuccessfully) {
            return new StoreWrapper(type, liveTime, maxIdleTime, size, k);
        }

        return null;
    }

    // TODO: performance tuning for concurrent put.
    private Slot getAvailableSlot(final int size) {
        int slotSize = 0;

        //        if (size <= 64) {
        //            queue = _queue64;
        //            slotSize = 64;
        //        } else if (size <= 128) {
        //            queue = _queue128;
        //            slotSize = 128;
        //        } else if (size <= 256) {
        //            queue = _queue256;
        //            slotSize = 256;
        //        } else if (size <= 384) {
        //            queue = _queue384;
        //            slotSize = 384;
        //        } else if (size <= 512) {
        //            queue = _queue512;
        //            slotSize = 512;
        //        } else if (size <= 640) {
        //            queue = _queue640;
        //            slotSize = 640;
        //        } else if (size <= 768) {
        //            queue = _queue768;
        //            slotSize = 768;
        //        } else if (size <= 896) {
        //            queue = _queue896;
        //            slotSize = 896;
        //        } else if (size <= 1024) {
        //            queue = _queue1K;
        //            slotSize = 1024;
        //        } else if (size <= 1280) {
        //            queue = _queue1280;
        //            slotSize = 1280;
        //        } else if (size <= 1536) {
        //            queue = _queue1536;
        //            slotSize = 1536;
        //        } else if (size <= 1792) {
        //            queue = _queue1792;
        //            slotSize = 1792;
        //        } else if (size <= 2048) {
        //            queue = _queue2K;
        //            slotSize = 2048;
        //        } else if (size <= 2560) {
        //            queue = _queue2_5K;
        //            slotSize = 2560;
        //        } else if (size <= 3072) {
        //            queue = _queue3K;
        //            slotSize = 3072;
        //        } else if (size <= 3584) {
        //            queue = _queue3_5K;
        //            slotSize = 3584;
        //        } else if (size <= 4096) {
        //            queue = _queue4K;
        //            slotSize = 4096;
        //        } else if (size <= 5120) {
        //            queue = _queue5K;
        //            slotSize = 5120;
        //        } else if (size <= 6144) {
        //            queue = _queue6K;
        //            slotSize = 6144;
        //        } else if (size <= 7168) {
        //            queue = _queue7K;
        //            slotSize = 7168;
        //        } else if (size <= 8192) {
        //            queue = _queue8K;
        //            slotSize = 8192;
        //        } else {
        //            throw new RuntimeException("Unsupported object size: " + size);
        //        }

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

            if (indexOfAvailableSlot < 0) {
                synchronized (_segmentBitSet) {
                    final int nextSegmentIndex = _segmentBitSet.nextClearBit(0);

                    if (nextSegmentIndex >= _segments.length) {
                        return null; // No space available;
                    }

                    segment = _segments[nextSegmentIndex];
                    segment.sizeOfSlot = slotSize;

                    _segmentBitSet.set(nextSegmentIndex);
                    _segmentQueueMap.put(nextSegmentIndex, queue);

                    queue.addFirst(segment);

                    indexOfAvailableSlot = segment.allocateSlot();
                }
            }
        }

        return new Slot(indexOfAvailableSlot, segment);
    }

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
    public void remove(final K k) {
        final Wrapper<V> w = _pool.remove(k);

        if (w != null) {
            w.destroy();
        }
    }

    @Override
    public boolean containsKey(final K k) {
        return _pool.containsKey(k);
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

    public OffHeapCacheStats stats() {
        final PoolStats poolStats = _pool.stats();

        final Map<Integer, List<Integer>> usedSlotCount = Stream.of(new ArrayList<>(_segmentQueueMap.values())).distinct().flatmap(queue -> {
            synchronized (queue) {
                return N.map(queue, it -> Tuple.of(it.sizeOfSlot, it.slotBitSet.cardinality()));
            }
        }).sortedBy(it -> it._1).groupTo(it -> it._1, it -> it._2, Suppliers.ofLinkedHashMap());

        final long currentWriteCountToDisk = writeCountToDisk.get();
        final long currentReadCountFromDisk = readCountFromDisk.get();
        final BigInteger totalWriteTimeToDisk = totalWriteTimeToDiskHolder.value();
        final BigInteger totalReadTimeFromDisk = totalReadTimeFromDiskHolder.value();

        return new OffHeapCacheStats(poolStats.capacity(), poolStats.size(), cachedCountOnDisk.get(), poolStats.putCount(), currentWriteCountToDisk,
                poolStats.getCount(), poolStats.hitCount(), currentReadCountFromDisk, poolStats.missCount(), poolStats.evictionCount(),
                evictionCountFromDisk.get(), _capacityInBytes, totalOccupiedMemorySize.get(), totalDataSize.get(), dataSizeOnDisk.get(), SEGMENT_SIZE,
                usedSlotCount, currentWriteCountToDisk == 0 ? 0D : totalWriteTimeToDisk.divide(BigInteger.valueOf(currentWriteCountToDisk)).doubleValue(),
                currentReadCountFromDisk == 0 ? 0D : totalReadTimeFromDisk.divide(BigInteger.valueOf(currentReadCountFromDisk)).doubleValue());
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
                _pool.close();
            } finally {
                deallocate();
            }
        }
    }

    @Override
    public boolean isClosed() {
        return _pool.isClosed();
    }

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

    static final class Segment {

        private final BitSet slotBitSet = new BitSet();
        private final long segmentStartPtr;
        // It can be reset/reused by set sizeOfSlot to: 64, 128, 256, 512, 1024, 2048, 4096, 8192....
        private int sizeOfSlot;

        public Segment(final long segmentStartPtr) {
            this.segmentStartPtr = segmentStartPtr;
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

    record Slot(int indexOfSlot, Segment segment) {

        void release() {
            segment.releaseSlot(indexOfSlot);
        }
    }

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
        public void destroy() {
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
        public void destroy() {
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

    final class StoreWrapper extends Wrapper<V> {
        private K permenantKey;

        StoreWrapper(final Type<V> type, final long liveTime, final long maxIdleTime, final int size, final K permenantKey) {
            super(type, liveTime, maxIdleTime, size);

            this.permenantKey = permenantKey;

            cachedCountOnDisk.incrementAndGet();
            dataSizeOnDisk.addAndGet(size);
        }

        @Override
        V read() {
            synchronized (this) {
                final byte[] bytes = offHeapStore.get(permenantKey);

                if (bytes == null) {
                    return null;
                }

                // should never happen.
                if (bytes.length != size) {
                    throw new RuntimeException("Unknown error happening when retrieve value. The fetched byte array size: " + size
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
        public void destroy() {
            synchronized (this) {
                if (permenantKey != null) {
                    cachedCountOnDisk.decrementAndGet();
                    dataSizeOnDisk.addAndGet(-size);

                    totalDataSize.addAndGet(-size);

                    evictionCountFromDisk.incrementAndGet();

                    offHeapStore.remove(permenantKey);
                    permenantKey = null;
                }
            }
        }
    }

}
