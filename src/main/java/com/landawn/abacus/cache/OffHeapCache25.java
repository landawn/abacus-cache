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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import com.landawn.abacus.annotation.SuppressFBWarnings;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.ByteArrayOutputStream;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

//--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED

/**
 * It's not designed for tiny objects(length of bytes < 128 after serialization).
 * Since it's off heap cache, modifying the objects from cache won't impact the objects in cache.
 *
 * @param <K> the key type
 * @param <V> the value type
 * @see <a href="https://openjdk.org/jeps/471">JEP 471: Foreign Function & Memory API (Incubator)</a>
 */
@SuppressFBWarnings({ "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", "JLM_JSR166_UTILCONCURRENT_MONITORENTER" })
public class OffHeapCache25<K, V> extends AbstractOffHeapCache<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(OffHeapCache25.class);

    private Arena arena;
    private MemorySegment buffer;

    /**
     * The memory with the specified size of MB will be allocated at application start up.
     *
     * @param sizeInMB
     */
    public OffHeapCache25(final int sizeInMB) {
        this(sizeInMB, 3000);
    }

    /**
     * The memory with the specified size of MB will be allocated at application start up.
     *
     * @param sizeInMB
     * @param evictDelay unit is milliseconds
     */
    public OffHeapCache25(final int sizeInMB, final long evictDelay) {
        this(sizeInMB, evictDelay, DEFAULT_LIVE_TIME, DEFAULT_MAX_IDLE_TIME);
    }

    /**
     * The memory with the specified size of MB will be allocated at application start up.
     *
     * @param sizeInMB
     * @param evictDelay unit is milliseconds
     * @param defaultLiveTime unit is milliseconds
     * @param defaultMaxIdleTime unit is milliseconds
     */
    public OffHeapCache25(final int sizeInMB, final long evictDelay, final long defaultLiveTime, final long defaultMaxIdleTime) {
        this(sizeInMB, evictDelay, defaultLiveTime, defaultMaxIdleTime, null, null);
    }

    OffHeapCache25(final int sizeInMB, final long evictDelay, final long defaultLiveTime, final long defaultMaxIdleTime,
            final BiConsumer<? super V, ByteArrayOutputStream> serializer, final BiFunction<byte[], Type<V>, ? extends V> deserializer) {
        super(sizeInMB, evictDelay, defaultLiveTime, defaultMaxIdleTime, 0, serializer, deserializer, logger);
    }

    @Override
    protected long allocate(final long capacityInBytes) {
        arena = Arena.ofShared();
        buffer = arena.allocate(capacityInBytes);

        return buffer.address();
    }

    @Override
    protected void deallocate() {
        // buffer.asSlice(_startPtr, _capacityB).fill((byte) 0); // Is it unnecessary?
        arena.close();
    }

    @Override
    protected void copyFromMemory(final long startPtr, final byte[] bytes, final int destOffset, final int len) {
        MemorySegment.copy(buffer, ValueLayout.JAVA_BYTE, startPtr - _startPtr, bytes, destOffset, len);
    }

    @Override
    protected void copyToMemory(final byte[] srcBytes, final int srcOffset, final long startPtr, final int len) {
        MemorySegment.copy(srcBytes, srcOffset, buffer, ValueLayout.JAVA_BYTE, startPtr - _startPtr, len);
    }

    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    @Data
    @NoArgsConstructor
    @Accessors(chain = true, fluent = true)
    public static class Builder<K, V> {
        private int sizeInMB;
        private long evictDelay;
        private long defaultLiveTime;
        private long defaultMaxIdleTime;
        private BiConsumer<? super V, ByteArrayOutputStream> serializer;
        private BiFunction<byte[], Type<V>, ? extends V> deserializer;

        public OffHeapCache25<K, V> build() {
            return new OffHeapCache25<>(sizeInMB, evictDelay, defaultLiveTime, defaultMaxIdleTime, serializer, deserializer);
        }
    }
}
