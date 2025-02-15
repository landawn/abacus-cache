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

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.landawn.abacus.parser.Parser;
import com.landawn.abacus.parser.ParserFactory;
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.MoreExecutors;

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

    static final Parser<?, ?> parser = ParserFactory.isKryoAvailable() ? ParserFactory.createKryoParser() : ParserFactory.createJSONParser();

    static final int SEGMENT_SIZE = 1024 * 1024; // (int) N.ONE_MB;

    static final int MIN_BLOCK_SIZE = 256;

    static final int MAX_BLOCK_SIZE = 8192; // 8K

    static final ScheduledExecutorService scheduledExecutor;

    static {
        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(IOUtil.CPU_CORES);
        // executor.setRemoveOnCancelPolicy(true);
        scheduledExecutor = MoreExecutors.getExitingScheduledExecutorService(executor);
    }

    protected AbstractOffHeapCache(final long defaultLiveTime, final long defaultMaxIdleTime) {
        super(defaultLiveTime, defaultMaxIdleTime);
    }

}
