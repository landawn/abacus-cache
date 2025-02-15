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

import java.lang.reflect.Field;

import com.landawn.abacus.annotation.SuppressFBWarnings;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.util.ClassUtil;

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
public class OffHeapCache<K, V> extends AbstractOffHeapCache<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(OffHeapCache.class);

    private static final sun.misc.Unsafe UNSAFE;

    static {
        try {
            final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            ClassUtil.setAccessible(f, true);
            UNSAFE = (sun.misc.Unsafe) f.get(null);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to initialize Unsafe", e);
        }
    }

    private static final int BYTE_ARRAY_BASE = UNSAFE.arrayBaseOffset(byte[].class);

    /**
     * The memory with the specified size of MB will be allocated at application start up.
     *
     * @param sizeMB
     */
    public OffHeapCache(final int sizeMB) {
        this(sizeMB, 3000);
    }

    /**
     * The memory with the specified size of MB will be allocated at application start up.
     *
     * @param sizeMB
     * @param evictDelay unit is milliseconds
     */
    public OffHeapCache(final int sizeMB, final long evictDelay) {
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
    public OffHeapCache(final int sizeMB, final long evictDelay, final long defaultLiveTime, final long defaultMaxIdleTime) {
        super(sizeMB, evictDelay, defaultLiveTime, defaultMaxIdleTime, BYTE_ARRAY_BASE, logger);
    }

    @Override
    protected long allocate(final long capacityInBytes) {
        return UNSAFE.allocateMemory(capacityInBytes);
    }

    @Override
    protected void deallocate() {
        // UNSAFE.setMemory(_startPtr, _capacityB, 0); // Is it unnecessary?
        UNSAFE.freeMemory(_startPtr);
    }

    @Override
    protected void copyToMemory(final byte[] srcBytes, final int srcOffset, final long startPtr, final int len) {
        UNSAFE.copyMemory(srcBytes, srcOffset, null, startPtr, len);
    }

    @Override
    protected void copyFromMemory(final long startPtr, final byte[] bytes, final int destOffset, final int len) {
        UNSAFE.copyMemory(null, startPtr, bytes, destOffset, len);
    }

}
