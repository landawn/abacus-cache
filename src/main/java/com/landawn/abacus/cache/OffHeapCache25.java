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
import com.landawn.abacus.pool.ActivityPrint;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.ByteArrayOutputStream;
import com.landawn.abacus.util.function.TriFunction;
import com.landawn.abacus.util.function.TriPredicate;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

//--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED

/**
 * A modern off-heap cache implementation using Java's Foreign Function &amp; Memory API.
 * This implementation leverages the new Foreign Memory API introduced in recent Java versions
 * as a safer alternative to sun.misc.Unsafe. It provides the same functionality as OffHeapCache
 * but with better safety guarantees and future compatibility.
 * 
 * <br><br>
 * Key features:
 * <ul>
 * <li>Uses Foreign Memory API instead of Unsafe</li>
 * <li>Automatic memory management with Arena</li>
 * <li>Type-safe memory access via MemorySegment</li>
 * <li>Same performance characteristics as Unsafe-based implementation</li>
 * <li>Better compatibility with future Java versions</li>
 * </ul>
 * 
 * <br>
 * Important notes:
 * <ul>
 * <li>Requires Java 21+ with Foreign Memory API</li>
 * <li>Not designed for tiny objects (&lt; 128 bytes after serialization)</li>
 * <li>Objects are copied, so modifications don't affect cached values</li>
 * <li>Memory is allocated at startup and held until shutdown</li>
 * </ul>
 * 
 * <br>
 * Example usage:
 * <pre>{@code
 * OffHeapCache25<String, byte[]> cache = OffHeapCache25.<String, byte[]>builder()
 *     .capacityInMB(100)
 *     .evictDelay(60000)
 *     .defaultLiveTime(3600000)
 *     .defaultMaxIdleTime(1800000)
 *     .build();
 * 
 * cache.put("key1", largeByteArray);
 * byte[] cached = cache.get("key1");
 *
 * OffHeapCacheStats stats = cache.stats();
 * System.out.println("Memory utilization: " + 
 *     (double) stats.occupiedMemory() / stats.allocatedMemory());
 * }</pre>
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of mapped values stored in this cache
 * @see AbstractOffHeapCache
 * @see OffHeapCacheStats
 * @see OffHeapStore
 * @see <a href="https://openjdk.org/jeps/471">JEP 471: Foreign Function &amp; Memory API</a>
 */
@SuppressFBWarnings({ "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", "JLM_JSR166_UTILCONCURRENT_MONITORENTER" })
public class OffHeapCache25<K, V> extends AbstractOffHeapCache<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(OffHeapCache25.class);

    private volatile Arena arena;
    private volatile MemorySegment buffer;

    /**
     * Creates an OffHeapCache25 with the specified capacity in megabytes.
     * Uses default eviction delay of 3000 milliseconds (3 seconds) and default expiration times
     * (3 hours for live time and 30 minutes for max idle time, as defined in AbstractOffHeapCache).
     * Memory is allocated at construction time and held until close().
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OffHeapCache25<String, byte[]> cache = new OffHeapCache25<>(100); // 100MB
     * byte[] largeData = new byte[1024];
     * cache.put("key1", largeData);
     * }</pre>
     *
     * @param capacityInMB the total off-heap memory to allocate in megabytes
     */
    OffHeapCache25(final int capacityInMB) {
        this(capacityInMB, 3000);
    }

    /**
     * Creates an OffHeapCache25 with specified capacity and eviction delay.
     * Uses default TTL of 3 hours and idle time of 30 minutes.
     * The eviction delay controls how frequently the cache scans for expired entries.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OffHeapCache25<Long, Data> cache = new OffHeapCache25<>(200, 60000); // 200MB, 60s eviction
     * Data data = new Data();
     * cache.put(123L, data, 7200000, 3600000); // 2h TTL, 1h idle
     * }</pre>
     *
     * @param capacityInMB the total off-heap memory to allocate in megabytes
     * @param evictDelay the delay between eviction runs in milliseconds
     */
    OffHeapCache25(final int capacityInMB, final long evictDelay) {
        this(capacityInMB, evictDelay, DEFAULT_LIVE_TIME, DEFAULT_MAX_IDLE_TIME);
    }

    /**
     * Creates an OffHeapCache25 with fully specified basic parameters.
     * Memory is allocated at construction time using a shared Arena and held until close().
     * This constructor provides complete control over cache timing behavior.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OffHeapCache25<String, byte[]> cache = new OffHeapCache25<>(500, 30000, 3600000, 1800000);
     * // 500MB, 30s eviction, 1h TTL, 30min idle
     * }</pre>
     *
     * @param capacityInMB the total off-heap memory to allocate in megabytes
     * @param evictDelay the delay between eviction runs in milliseconds
     * @param defaultLiveTime default time-to-live for entries in milliseconds
     * @param defaultMaxIdleTime default maximum idle time for entries in milliseconds
     */
    OffHeapCache25(final int capacityInMB, final long evictDelay, final long defaultLiveTime, final long defaultMaxIdleTime) {
        this(capacityInMB, DEFAULT_MAX_BLOCK_SIZE, evictDelay, defaultLiveTime, defaultMaxIdleTime, DEFAULT_VACATING_FACTOR, null, null, null, false, null,
                null);
    }

    OffHeapCache25(final int capacityInMB, final int maxBlockSize, final long evictDelay, final long defaultLiveTime, final long defaultMaxIdleTime,
            final float vacatingFactor, final BiConsumer<? super V, ByteArrayOutputStream> serializer,
            final BiFunction<byte[], Type<V>, ? extends V> deserializer, final OffHeapStore<K> offHeapStore, final boolean statsTimeOnDisk,
            final TriPredicate<ActivityPrint, Integer, Long> testerForLoadingItemFromDiskToMemory, final TriFunction<K, V, Integer, Integer> storeSelector) {
        super(capacityInMB, maxBlockSize, evictDelay, defaultLiveTime, defaultMaxIdleTime, vacatingFactor, 0, serializer, deserializer, offHeapStore,
                statsTimeOnDisk, testerForLoadingItemFromDiskToMemory, storeSelector, logger);
    }

    /**
     * Allocates off-heap memory using the Foreign Memory API.
     * Creates a shared Arena (allowing access from multiple threads) and allocates
     * a MemorySegment of the specified size. The shared Arena ensures thread-safe
     * access to the memory segment across the cache operations. This is an internal
     * method called automatically during cache construction.
     *
     * @param capacityInBytes the total number of bytes to allocate
     * @return the base address of the allocated memory segment
     * @throws OutOfMemoryError if the allocation fails due to insufficient native memory
     */
    @Override
    protected long allocate(final long capacityInBytes) {
        arena = Arena.ofShared();
        buffer = arena.allocate(capacityInBytes);

        return buffer.address();
    }

    /**
     * Deallocates the off-heap memory by closing the Arena.
     * This releases all memory associated with the Arena and makes the
     * MemorySegment inaccessible. Any subsequent attempts to access the
     * memory segment will result in an IllegalStateException. This is an
     * internal method called automatically during cache shutdown and should
     * not be called directly.
     */
    @Override
    protected void deallocate() {
        // buffer.asSlice(_startPtr, _capacityB).fill((byte) 0); // Is it unnecessary?
        arena.close();
    }

    /**
     * Copies bytes from off-heap memory to a Java array using MemorySegment API.
     * This provides type-safe memory access compared to Unsafe operations. The method
     * calculates the relative offset within the buffer by subtracting _startPtr from startPtr.
     * This is an internal method called automatically during cache get operations.
     *
     * @param startPtr the absolute source memory address from which to copy
     * @param bytes the destination byte array to which data is copied
     * @param destOffset the starting offset in the destination array (0-based index)
     * @param len the number of bytes to copy
     */
    @Override
    protected void copyFromMemory(final long startPtr, final byte[] bytes, final int destOffset, final int len) {
        MemorySegment.copy(buffer, ValueLayout.JAVA_BYTE, startPtr - _startPtr, bytes, destOffset, len);
    }

    /**
     * Copies bytes from a Java array to off-heap memory using MemorySegment API.
     * This provides type-safe memory access compared to Unsafe operations. The method
     * calculates the relative offset within the buffer by subtracting _startPtr from startPtr.
     * This is an internal method called automatically during cache put operations.
     *
     * @param srcBytes the source byte array from which to copy
     * @param srcOffset the starting offset in the source array (0-based index)
     * @param startPtr the absolute destination memory address to which data is copied
     * @param len the number of bytes to copy
     */
    @Override
    protected void copyToMemory(final byte[] srcBytes, final int srcOffset, final long startPtr, final int len) {
        MemorySegment.copy(srcBytes, srcOffset, buffer, ValueLayout.JAVA_BYTE, startPtr - _startPtr, len);
    }

    /**
     * Creates a new builder for constructing OffHeapCache25 instances.
     * The builder provides a fluent API for configuring all cache parameters.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OffHeapCache25<String, Data> cache = OffHeapCache25.<String, Data>builder()
     *     .capacityInMB(100)
     *     .evictDelay(60000)
     *     .build();
     * }</pre>
     *
     * @param <K> the type of keys maintained by the cache
     * @param <V> the type of values stored in the cache
     * @return a new Builder instance
     */
    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    /**
     * Builder class for creating OffHeapCache25 instances with custom configuration.
     * Provides a fluent API for setting all cache parameters including capacity,
     * eviction policies, serialization, and disk spillover options.
     *
     * <br><br>
     * Example usage:
     * <pre>{@code
     * OffHeapCache25<String, Data> cache = OffHeapCache25.<String, Data>builder()
     *     .capacityInMB(100)
     *     .maxBlockSizeInBytes(16384)
     *     .evictDelay(60000)
     *     .vacatingFactor(0.3f)
     *     .offHeapStore(myDiskStore)
     *     .statsTimeOnDisk(true)
     *     .build();
     * }</pre>
     *
     * @param <K> the type of keys maintained by the cache being built
     * @param <V> the type of values stored in the cache being built
     */
    @Data
    /**
     * Constructs a new Builder with default values.
     * All builder properties start with their default values and can be customized
     * using the fluent setter methods.
     */
    @NoArgsConstructor
    @Accessors(chain = true, fluent = true)
    public static class Builder<K, V> {
        /**
         * The total off-heap memory capacity in megabytes.
         */
        private int capacityInMB;

        /**
         * Maximum size of a single memory block in bytes (default: 8192).
         */
        private int maxBlockSizeInBytes = DEFAULT_MAX_BLOCK_SIZE;

        /**
         * Delay between eviction runs in milliseconds.
         */
        private long evictDelay;

        /**
         * Default time-to-live for cache entries in milliseconds.
         */
        private long defaultLiveTime;

        /**
         * Default maximum idle time for cache entries in milliseconds.
         */
        private long defaultMaxIdleTime;

        /**
         * Factor determining when to trigger memory defragmentation (default: 0.2).
         */
        private float vacatingFactor = DEFAULT_VACATING_FACTOR;

        /**
         * Custom serializer for converting values to bytes.
         */
        private BiConsumer<? super V, ByteArrayOutputStream> serializer;

        /**
         * Custom deserializer for converting bytes to values.
         */
        private BiFunction<byte[], Type<V>, ? extends V> deserializer;

        /**
         * Optional disk store for spillover when memory is full.
         */
        private OffHeapStore<K> offHeapStore;

        /**
         * Whether to collect disk I/O timing statistics.
         */
        private boolean statsTimeOnDisk;

        /**
         * Predicate to determine when to load items from disk back to memory.
         */
        private TriPredicate<ActivityPrint, Integer, Long> testerForLoadingItemFromDiskToMemory;

        /**
         * Function to determine storage location for entries.
         * Returns: 0 = default, 1 = memory only, 2 = disk only.
         */
        private TriFunction<K, V, Integer, Integer> storeSelector;

        /**
         * Builds and returns a new OffHeapCache25 instance with the configured parameters.
         * The cache will allocate off-heap memory immediately upon creation. If maxBlockSizeInBytes
         * is set to 0, it will be replaced with DEFAULT_MAX_BLOCK_SIZE (8192 bytes).
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * OffHeapCache25<String, Data> cache = OffHeapCache25.<String, Data>builder()
         *     .capacityInMB(100)
         *     .evictDelay(60000)
         *     .defaultLiveTime(3600000)
         *     .build();
         * }</pre>
         *
         * @return a new OffHeapCache25 instance with the configured settings
         */
        public OffHeapCache25<K, V> build() {
            return new OffHeapCache25<>(capacityInMB, maxBlockSizeInBytes == 0 ? DEFAULT_MAX_BLOCK_SIZE : maxBlockSizeInBytes, evictDelay, defaultLiveTime,
                    defaultMaxIdleTime, vacatingFactor, serializer, deserializer, offHeapStore, statsTimeOnDisk, testerForLoadingItemFromDiskToMemory,
                    storeSelector);
        }
    }
}