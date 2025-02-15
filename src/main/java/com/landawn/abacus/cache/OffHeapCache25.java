/*
 * Copyright (c) 2015, Haiyang Li.
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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
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

import com.landawn.abacus.annotation.SuppressFBWarnings;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.parser.Parser;
import com.landawn.abacus.parser.ParserFactory;
import com.landawn.abacus.pool.AbstractPoolable;
import com.landawn.abacus.pool.KeyedObjectPool;
import com.landawn.abacus.pool.PoolFactory;
import com.landawn.abacus.type.ByteBufferType;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ByteArrayOutputStream;
import com.landawn.abacus.util.ExceptionUtil;
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.MoreExecutors;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Objectory;

//--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED

/**
 * It's not designed for tiny objects(length of bytes < 128 after serialization).
 * Since it's off heap cache, modifying the objects from cache won't impact the objects in cache.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
@SuppressFBWarnings({ "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", "JLM_JSR166_UTILCONCURRENT_MONITORENTER" })
public class OffHeapCache25<K, V> extends AbstractCache<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(OffHeapCache25.class);
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

    private static final Parser<?, ?> parser = ParserFactory.isKryoAvailable() ? ParserFactory.createKryoParser() : ParserFactory.createJSONParser();

    private static final int SEGMENT_SIZE = 1024 * 1024; // (int) N.ONE_MB;

    private static final int MIN_BLOCK_SIZE = 256;

    private static final int MAX_BLOCK_SIZE = 8192; // 8K

    private static final ScheduledExecutorService scheduledExecutor;

    static {
        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(IOUtil.CPU_CORES);
        // executor.setRemoveOnCancelPolicy(true);
        scheduledExecutor = MoreExecutors.getExitingScheduledExecutorService(executor);
    }

    private final long _capacityB; //NOSONAR
    private final Arena arena;
    private final MemorySegment buffer;

    private final Segment[] _segments; //NOSONAR

    private final BitSet _segmentBitSet = new BitSet(); //NOSONAR

    private final Map<Integer, Deque<Segment>> _segmentQueueMap = new ConcurrentHashMap<>(); //NOSONAR

    /** The queue 256. */
    private final Deque<Segment> _queue256 = new LinkedList<>(); //NOSONAR

    /** The queue 384. */
    private final Deque<Segment> _queue384 = new LinkedList<>(); //NOSONAR

    /** The queue 512. */
    private final Deque<Segment> _queue512 = new LinkedList<>(); //NOSONAR

    /** The queue 640. */
    private final Deque<Segment> _queue640 = new LinkedList<>(); //NOSONAR

    /** The queue 768. */
    private final Deque<Segment> _queue768 = new LinkedList<>(); //NOSONAR

    /** The queue 896. */
    private final Deque<Segment> _queue896 = new LinkedList<>(); //NOSONAR

    /** The queue 1024. */
    private final Deque<Segment> _queue1K = new LinkedList<>(); //NOSONAR

    /** The queue 1280. */
    private final Deque<Segment> _queue1280 = new LinkedList<>(); //NOSONAR

    /** The queue 1536. */
    private final Deque<Segment> _queue1536 = new LinkedList<>(); //NOSONAR

    /** The queue 1792. */
    private final Deque<Segment> _queue1792 = new LinkedList<>(); //NOSONAR

    /** The queue 2048. */
    private final Deque<Segment> _queue2K = new LinkedList<>(); //NOSONAR

    /** The queue 2560. */
    private final Deque<Segment> _queue2_5K = new LinkedList<>(); //NOSONAR

    /** The queue 3072. */
    private final Deque<Segment> _queue3K = new LinkedList<>(); //NOSONAR

    /** The queue 3584. */
    private final Deque<Segment> _queue3_5K = new LinkedList<>(); //NOSONAR

    /** The queue 4096. */
    private final Deque<Segment> _queue4K = new LinkedList<>(); //NOSONAR

    /** The queue 5120. */
    private final Deque<Segment> _queue5K = new LinkedList<>(); //NOSONAR

    /** The queue 6144. */
    private final Deque<Segment> _queue6K = new LinkedList<>(); //NOSONAR

    /** The queue 7168. */
    private final Deque<Segment> _queue7K = new LinkedList<>(); //NOSONAR

    /** The queue 8192. */
    private final Deque<Segment> _queue8K = new LinkedList<>(); //NOSONAR

    private final AsyncExecutor _asyncExecutor = new AsyncExecutor(); //NOSONAR

    private final AtomicInteger _activeVacationTaskCount = new AtomicInteger(); //NOSONAR

    private final KeyedObjectPool<K, Wrapper<V>> _pool; //NOSONAR

    private ScheduledFuture<?> scheduleFuture;

    /**
     * The memory with the specified size of MB will be allocated at application start up.
     *
     * @param sizeMB
     */
    public OffHeapCache25(final int sizeMB) {
        this(sizeMB, 3000);
    }

    /**
     * The memory with the specified size of MB will be allocated at application start up.
     *
     * @param sizeMB
     * @param evictDelay unit is milliseconds
     */
    public OffHeapCache25(final int sizeMB, final long evictDelay) {
        this(sizeMB, evictDelay, DEFAULT_LIVE_TIME, DEFAULT_MAX_IDLE_TIME);
    }

    /**
     * The memory with the specified size of MB will be allocated at application start up.
     *
     * @param sizeMB
     * @param evictDelay unit is milliseconds
     * @param defaultLiveTime unit is milliseconds
     * @param defaultMaxIdleTime unit is milliseconds
     */
    public OffHeapCache25(final int sizeMB, final long evictDelay, final long defaultLiveTime, final long defaultMaxIdleTime) {
        super(defaultLiveTime, defaultMaxIdleTime);

        _capacityB = sizeMB * (1024L * 1024L); // N.ONE_MB;
        // ByteBuffer.allocateDirect((int) capacity);

        arena = Arena.ofShared();
        buffer = arena.allocate(ValueLayout.JAVA_BYTE, _capacityB);

        _segments = new Segment[(int) (_capacityB / SEGMENT_SIZE)];

        for (int i = 0, len = _segments.length; i < len; i++) {
            _segments[i] = new Segment((long) i * SEGMENT_SIZE);
        }

        _pool = PoolFactory.createKeyedObjectPool((int) (_capacityB / MIN_BLOCK_SIZE), evictDelay);

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

    private void freeMemory() {
        arena.close();
    }

    private static void copyFromMemory(final MemorySegment srcSegment, final long startPtr, final byte[] bytes, final int destOffset, final int len) {
        MemorySegment.copy(srcSegment, ValueLayout.JAVA_BYTE, startPtr, bytes, destOffset, len);
    }

    private static void copyToMemory(final byte[] srcBytes, final int srcOffset, final MemorySegment dstSegment, final long startPtr, final int len) {
        MemorySegment.copy(srcBytes, srcOffset, dstSegment, ValueLayout.JAVA_BYTE, startPtr, len);
    }

    /**
     * Gets the t.
     *
     * @param k
     * @return
     */
    @Override
    public V gett(final K k) {
        final Wrapper<V> w = _pool.get(k);

        return w == null ? null : w.read();
    }

    @Override
    public boolean put(final K k, final V v, final long liveTime, final long maxIdleTime) {
        final Type<V> type = N.typeOf(v.getClass());
        Wrapper<V> w = null;

        // final byte[] bytes = parser.serialize(v).getBytes();
        ByteArrayOutputStream os = null;
        byte[] bytes = null;
        int size = 0;

        if (type.isPrimitiveByteArray()) {
            bytes = (byte[]) v;
            size = bytes.length;
        } else if (type.isByteBuffer()) {
            bytes = ByteBufferType.byteArrayOf((ByteBuffer) v);
            size = bytes.length;
        } else {
            os = Objectory.createByteArrayOutputStream();
            parser.serialize(v, os);
            bytes = os.array();
            size = os.size();
        }

        if (size <= MAX_BLOCK_SIZE) {
            final AvailableSegment availableSegment = getAvailableSegment(size);

            if (availableSegment == null) {
                Objectory.recycle(os);

                vacate();
                return false;
            }

            final long startPtr = availableSegment.segment.startPtr + (long) availableSegment.availableBlockIndex * availableSegment.segment.sizeOfBlock;
            boolean isOK = false;

            try {
                copyToMemory(bytes, 0, buffer, startPtr, size);

                isOK = true;
            } catch (final Exception e) {
                isOK = false;

                e.printStackTrace();
            } finally {
                Objectory.recycle(os);

                if (!isOK) {
                    availableSegment.release();

                    //noinspection ReturnInsideFinallyBlock
                    return false; //NOSONAR
                }
            }

            w = new SWrapper<>(type, liveTime, maxIdleTime, size, availableSegment.segment, startPtr);
        } else {
            final List<SegmentEntry> segmentResult = new ArrayList<>(size / MAX_BLOCK_SIZE + (size % MAX_BLOCK_SIZE == 0 ? 0 : 1));
            int copiedSize = 0;
            int srcOffset = 0;

            try {
                while (copiedSize < size) {
                    final int sizeToCopy = Math.min(size - copiedSize, MAX_BLOCK_SIZE);
                    final AvailableSegment availableSegment = getAvailableSegment(sizeToCopy);

                    if (availableSegment == null) {

                        vacate();
                        return false;
                    }

                    final long startPtr = availableSegment.segment.startPtr
                            + (long) availableSegment.availableBlockIndex * availableSegment.segment.sizeOfBlock;
                    boolean isOK = false;

                    try {
                        copyToMemory(bytes, srcOffset, buffer, startPtr, sizeToCopy);

                        srcOffset += sizeToCopy;
                        copiedSize += sizeToCopy;

                        isOK = true;
                    } finally {
                        if (!isOK) {
                            availableSegment.release();

                            //noinspection ReturnInsideFinallyBlock
                            return false; //NOSONAR
                        }
                    }

                    segmentResult.add(new SegmentEntry(startPtr, availableSegment.segment));
                }

                w = new MWrapper<>(type, liveTime, maxIdleTime, size, segmentResult);
            } finally {
                Objectory.recycle(os);

                if (w == null) {
                    for (final SegmentEntry entry : segmentResult) {
                        final Segment segment = entry.segment;
                        segment.release((int) ((entry.startPtr - segment.startPtr) / segment.sizeOfBlock));
                    }
                }
            }
        }

        boolean result = false;

        try {
            result = _pool.put(k, w);
        } finally {
            if (!result && w != null) {
                w.destroy();
            }
        }

        return result;
    }

    // TODO: performance tuning for concurrent put.
    private AvailableSegment getAvailableSegment(final int size) {
        Deque<Segment> queue = null;
        int blockSize = 0;

        if (size <= 256) {
            queue = _queue256;
            blockSize = 256;
        } else if (size <= 384) {
            queue = _queue384;
            blockSize = 384;
        } else if (size <= 512) {
            queue = _queue512;
            blockSize = 512;
        } else if (size <= 640) {
            queue = _queue640;
            blockSize = 640;
        } else if (size <= 768) {
            queue = _queue768;
            blockSize = 768;
        } else if (size <= 896) {
            queue = _queue896;
            blockSize = 896;
        } else if (size <= 1024) {
            queue = _queue1K;
            blockSize = 1024;
        } else if (size <= 1280) {
            queue = _queue1280;
            blockSize = 1280;
        } else if (size <= 1536) {
            queue = _queue1536;
            blockSize = 1536;
        } else if (size <= 1792) {
            queue = _queue1792;
            blockSize = 1792;
        } else if (size <= 2048) {
            queue = _queue2K;
            blockSize = 2048;
        } else if (size <= 2560) {
            queue = _queue2_5K;
            blockSize = 2560;
        } else if (size <= 3072) {
            queue = _queue3K;
            blockSize = 3072;
        } else if (size <= 3584) {
            queue = _queue3_5K;
            blockSize = 3584;
        } else if (size <= 4096) {
            queue = _queue4K;
            blockSize = 4096;
        } else if (size <= 5120) {
            queue = _queue5K;
            blockSize = 5120;
        } else if (size <= 6144) {
            queue = _queue6K;
            blockSize = 6144;
        } else if (size <= 7168) {
            queue = _queue7K;
            blockSize = 7168;
        } else if (size <= 8192) {
            queue = _queue8K;
            blockSize = 8192;
        } else {
            throw new RuntimeException("Unsupported object size: " + size);
        }

        Segment segment = null;
        int availableBlockIndex = -1;

        synchronized (queue) {
            final Iterator<Segment> iterator = queue.iterator();
            final Iterator<Segment> descendingIterator = queue.descendingIterator();
            int half = queue.size() / 2 + 1;
            int cnt = 0;
            while (iterator.hasNext() && half-- > 0) {
                cnt++;
                segment = iterator.next();

                if ((availableBlockIndex = segment.allocate()) >= 0) {
                    if (cnt > 3) {
                        iterator.remove();
                        queue.addFirst(segment);
                    }

                    break;
                }

                segment = descendingIterator.next();

                if ((availableBlockIndex = segment.allocate()) >= 0) {
                    if (cnt > 3) {
                        descendingIterator.remove();
                        queue.addFirst(segment);
                    }

                    break;
                }
            }

            if (availableBlockIndex < 0) {
                synchronized (_segmentBitSet) {
                    final int nextSegmentIndex = _segmentBitSet.nextClearBit(0);

                    if (nextSegmentIndex >= _segments.length) {
                        return null; // No space available;
                    }

                    segment = _segments[nextSegmentIndex];
                    _segmentBitSet.set(nextSegmentIndex);
                    _segmentQueueMap.put(nextSegmentIndex, queue);

                    segment.sizeOfBlock = blockSize;
                    queue.addFirst(segment);

                    availableBlockIndex = segment.allocate();
                }
            }
        }

        return new AvailableSegment(segment, availableBlockIndex);
    }

    private void vacate() {
        if (_activeVacationTaskCount.get() > 0) {
            return;
        }

        synchronized (_activeVacationTaskCount) {
            if (_activeVacationTaskCount.get() > 0) {
                return;
            }

            _activeVacationTaskCount.incrementAndGet();

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
                freeMemory();
            }
        }
    }

    @Override
    public boolean isClosed() {
        return _pool.isClosed();
    }

    protected void evict() {
        for (int i = 0, len = _segments.length; i < len; i++) {
            if (_segments[i].blockBitSet.isEmpty()) {
                final Deque<Segment> queue = _segmentQueueMap.get(i);

                if (queue != null) {
                    synchronized (queue) {
                        if (_segments[i].blockBitSet.isEmpty()) {
                            synchronized (_segmentBitSet) {
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

    private abstract static class Wrapper<T> extends AbstractPoolable {

        final Type<T> type;

        final int size;

        Wrapper(final Type<T> type, final long liveTime, final long maxIdleTime, final int size) {
            super(liveTime, maxIdleTime);

            this.type = type;
            this.size = size;
        }

        abstract T read();
    }

    private final class SWrapper<T> extends Wrapper<T> {

        private Segment segment;

        private final long startPtr;

        SWrapper(final Type<T> type, final long liveTime, final long maxIdleTime, final int size, final Segment segment, final long startPtr) {
            super(type, liveTime, maxIdleTime, size);

            this.segment = segment;
            this.startPtr = startPtr;
        }

        @Override
        T read() {
            synchronized (this) {
                if (segment == null) {
                    return null;
                }

                final byte[] bytes = new byte[size];

                copyFromMemory(buffer, startPtr, bytes, 0, size);

                // it's destroyed after read from memory. dirty data may be read.
                if (type.isPrimitiveByteArray()) {
                    return (T) bytes;
                } else if (type.isByteBuffer()) {
                    return (T) ByteBufferType.valueOf(bytes);
                } else {
                    return parser.deserialize(new ByteArrayInputStream(bytes), type.clazz());
                }
            }
        }

        @Override
        public void destroy() {
            synchronized (this) {
                if (segment != null) {
                    segment.release((int) ((startPtr - segment.startPtr) / segment.sizeOfBlock));
                    segment = null;
                }
            }
        }
    }

    private final class MWrapper<T> extends Wrapper<T> {

        private List<SegmentEntry> segments;

        MWrapper(final Type<T> type, final long liveTime, final long maxIdleTime, final int size, final List<SegmentEntry> segments) {
            super(type, liveTime, maxIdleTime, size);

            this.segments = segments;
        }

        @Override
        T read() {
            synchronized (this) {
                final List<SegmentEntry> localSegments = segments;

                if (N.isEmpty(localSegments)) {
                    return null;
                }

                final byte[] bytes = new byte[size];
                int size = this.size;
                int destOffset = 0;

                for (final SegmentEntry entry : localSegments) {
                    final long startPtr = entry.startPtr;
                    final Segment segment = entry.segment;
                    final int sizeToCopy = Math.min(size, segment.sizeOfBlock);

                    copyFromMemory(buffer, startPtr, bytes, destOffset, sizeToCopy);

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
                    return segments == null ? null : (T) bytes;
                } else if (type.isByteBuffer()) {
                    return segments == null ? null : (T) ByteBufferType.valueOf(bytes);
                } else {
                    return segments == null ? null : parser.deserialize(new ByteArrayInputStream(bytes), type.clazz());
                }
            }
        }

        @Override
        public void destroy() {
            synchronized (this) {
                if (segments != null) {
                    for (final SegmentEntry entry : segments) {
                        final Segment segment = entry.segment;
                        segment.release((int) ((entry.startPtr - segment.startPtr) / segment.sizeOfBlock));
                    }

                    segments = null;
                }
            }
        }
    }

    private static final class Segment {

        private final BitSet blockBitSet = new BitSet();

        private final long startPtr;

        private int sizeOfBlock;

        public Segment(final long segmentStartPtr) {
            startPtr = segmentStartPtr;
        }

        public int allocate() {
            synchronized (blockBitSet) {
                final int result = blockBitSet.nextClearBit(0);

                if (result >= SEGMENT_SIZE / sizeOfBlock) {
                    return -1;
                }

                blockBitSet.set(result);

                return result;
            }
        }

        public void release(final int blockIndex) {
            synchronized (blockBitSet) {
                blockBitSet.clear(blockIndex);
            }
        }
        //
        //        public void clear() {
        //            synchronized (blockBitSet) {
        //                blockBitSet.clear();
        //                sizeOfBlock = 0;
        //            }
        //        }
    }

    private static final record AvailableSegment(Segment segment, int availableBlockIndex) {

        void release() {
            segment.release(availableBlockIndex);
        }
    }

    private static final record SegmentEntry(long startPtr, Segment segment) {
    }
}
