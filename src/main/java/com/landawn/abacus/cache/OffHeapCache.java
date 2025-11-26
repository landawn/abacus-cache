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
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import com.landawn.abacus.annotation.SuppressFBWarnings;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.pool.ActivityPrint;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.util.ByteArrayOutputStream;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.function.TriFunction;
import com.landawn.abacus.util.function.TriPredicate;

import lombok.Data;
import lombok.experimental.Accessors;

//--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED

/**
 * A high-performance off-heap cache implementation using sun.misc.Unsafe for direct memory management.
 * This cache stores objects outside the JVM heap to reduce garbage collection pressure and allow
 * for larger cache sizes. It includes support for automatic eviction, disk spillover, and
 * comprehensive statistics.
 * 
 * <br><br>
 * Key features:
 * <ul>
 * <li>Direct memory allocation outside JVM heap</li>
 * <li>Automatic memory defragmentation</li>
 * <li>Optional disk spillover when memory is full</li>
 * <li>Configurable serialization (defaults to Kryo)</li>
 * <li>Per-entry TTL and idle timeout</li>
 * <li>Comprehensive performance statistics</li>
 * </ul>
 * 
 * <br>
 * Important notes:
 * <ul>
 * <li>Not designed for tiny objects (&lt; 128 bytes after serialization)</li>
 * <li>Objects are copied, so modifications don't affect cached values</li>
 * <li>Requires JVM flags for Unsafe access (see class comment)</li>
 * <li>Memory is allocated at startup and held until shutdown</li>
 * </ul>
 * 
 * <br>
 * Example usage:
 * <pre>{@code
 * OffHeapCache<String, byte[]> cache = OffHeapCache.<String, byte[]>builder()
 *     .capacityInMB(100)
 *     .evictDelay(60000)
 *     .defaultLiveTime(3600000)
 *     .defaultMaxIdleTime(1800000)
 *     .build();
 *
 * byte[] largeByteArray = new byte[1024];
 * cache.put("key1", largeByteArray);
 * byte[] cached = cache.gett("key1");
 *
 * OffHeapCacheStats stats = cache.stats();
 * System.out.println("Memory utilization: " +
 *     (double) stats.occupiedMemory() / stats.allocatedMemory());
 * }</pre>
 *
 * @param <K> the type of keys used to identify cache entries
 * @param <V> the type of values stored in the cache
 * @see AbstractOffHeapCache
 * @see OffHeapCacheStats
 * @see OffHeapStore
 * @see <a href="https://openjdk.org/jeps/471">JEP 471: Foreign Function &amp; Memory API (Incubator)</a>
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

    @SuppressWarnings("removal")
    private static final int BYTE_ARRAY_BASE = UNSAFE.arrayBaseOffset(byte[].class);

    /**
     * Creates an OffHeapCache with the specified capacity in megabytes.
     * Uses default eviction delay of 3 seconds (3000 milliseconds) and default expiration times
     * (3 hours TTL and 30 minutes idle time). Memory is allocated at construction time and held until close().
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OffHeapCache<String, byte[]> cache = new OffHeapCache<>(100); // 100MB
     * byte[] largeData = new byte[1024];
     * cache.put("key1", largeData);
     * }</pre>
     *
     * @param capacityInMB the total off-heap memory to allocate in megabytes
     */
    OffHeapCache(final int capacityInMB) {
        this(capacityInMB, 3000);
    }

    /**
     * Creates an OffHeapCache with specified capacity and eviction delay.
     * Uses default TTL of 3 hours and idle time of 30 minutes.
     * The eviction delay controls how frequently the cache scans for expired entries.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OffHeapCache<Long, Data> cache = new OffHeapCache<>(200, 60000); // 200MB, 60s eviction
     * Data data = new Data();
     * cache.put(123L, data, 7200000, 3600000); // 2h TTL, 1h idle
     * }</pre>
     *
     * @param capacityInMB the total off-heap memory to allocate in megabytes
     * @param evictDelay the delay between eviction runs in milliseconds
     */
    OffHeapCache(final int capacityInMB, final long evictDelay) {
        this(capacityInMB, evictDelay, DEFAULT_LIVE_TIME, DEFAULT_MAX_IDLE_TIME);
    }

    /**
     * Creates an OffHeapCache with fully specified basic parameters.
     * Memory is allocated at construction time and held until close().
     * This constructor provides complete control over cache timing behavior.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OffHeapCache<String, byte[]> cache = new OffHeapCache<>(500, 30000, 3600000, 1800000);
     * // 500MB, 30s eviction, 1h TTL, 30min idle
     * }</pre>
     *
     * @param capacityInMB the total off-heap memory to allocate in megabytes
     * @param evictDelay the delay between eviction runs in milliseconds
     * @param defaultLiveTime default time-to-live for entries in milliseconds
     * @param defaultMaxIdleTime default maximum idle time for entries in milliseconds
     */
    OffHeapCache(final int capacityInMB, final long evictDelay, final long defaultLiveTime, final long defaultMaxIdleTime) {
        this(capacityInMB, DEFAULT_MAX_BLOCK_SIZE, evictDelay, defaultLiveTime, defaultMaxIdleTime, DEFAULT_VACATING_FACTOR, null, null, null, false, null,
                null);
    }

    /**
     * Package-private constructor with complete configuration for use by the Builder.
     * This constructor is called internally by the Builder's build() method and should not be
     * invoked directly. It provides access to all advanced configuration options including
     * custom serialization, disk spillover, and memory management tuning.
     *
     * @param capacityInMB the total off-heap memory to allocate in megabytes
     * @param maxBlockSize maximum size of a single memory block in bytes for memory allocation efficiency
     * @param evictDelay the delay between eviction runs in milliseconds
     * @param defaultLiveTime default time-to-live for entries in milliseconds
     * @param defaultMaxIdleTime default maximum idle time for entries in milliseconds
     * @param vacatingFactor factor (0.0-1.0) determining when to trigger memory defragmentation
     * @param serializer custom serializer for converting values to byte streams, or null for default Kryo serialization
     * @param deserializer custom deserializer for converting bytes to values, or null for default Kryo deserialization
     * @param offHeapStore optional disk store for spillover when memory is full, or null for memory-only cache
     * @param statsTimeOnDisk whether to collect detailed disk I/O timing statistics
     * @param testerForLoadingItemFromDiskToMemory predicate to determine when to load items from disk back to memory, or null for default behavior
     * @param storeSelector function to determine storage location (0=default, 1=memory only, 2=disk only), or null for default routing
     */
    OffHeapCache(final int capacityInMB, final int maxBlockSize, final long evictDelay, final long defaultLiveTime, final long defaultMaxIdleTime,
            final float vacatingFactor, final BiConsumer<? super V, ByteArrayOutputStream> serializer,
            final BiFunction<byte[], Type<V>, ? extends V> deserializer, final OffHeapStore<K> offHeapStore, final boolean statsTimeOnDisk,
            final TriPredicate<ActivityPrint, Integer, Long> testerForLoadingItemFromDiskToMemory, final TriFunction<K, V, Integer, Integer> storeSelector) {
        super(capacityInMB, maxBlockSize, evictDelay, defaultLiveTime, defaultMaxIdleTime, vacatingFactor, BYTE_ARRAY_BASE, serializer, deserializer,
                offHeapStore, statsTimeOnDisk, testerForLoadingItemFromDiskToMemory, storeSelector, logger);
    }

    /**
     * Allocates off-heap memory using sun.misc.Unsafe.
     * This memory is outside the JVM heap and must be explicitly freed via deallocate().
     * The allocated memory address is used for direct byte-level operations.
     * This is an internal method called automatically during cache construction.
     *
     * @param capacityInBytes the number of bytes to allocate
     * @return the memory address of the allocated memory block
     * @throws OutOfMemoryError if the allocation fails due to insufficient native memory
     */
    @SuppressWarnings("removal")
    @Override
    protected long allocate(final long capacityInBytes) {
        return UNSAFE.allocateMemory(capacityInBytes);
    }

    /**
     * Deallocates the off-heap memory allocated by allocate().
     * Called during cache shutdown to release native memory and prevent memory leaks.
     * This method is automatically invoked by close(). This is an internal method
     * and should not be called directly.
     */
    @SuppressWarnings("removal")
    @Override
    protected void deallocate() {
        // UNSAFE.setMemory(_startPtr, _capacityB, 0); // Is it unnecessary?
        UNSAFE.freeMemory(_startPtr);
    }

    /**
     * Copies bytes from a Java array to off-heap memory.
     * Uses unsafe operations for efficient memory transfer, bypassing standard
     * Java array access for maximum performance. This is an internal method
     * called automatically during cache put operations.
     *
     * @param srcBytes the source byte array from which to copy data
     * @param srcOffset the starting offset in the source array
     * @param startPtr the destination memory address in off-heap memory
     * @param len the number of bytes to copy
     */
    @SuppressWarnings("removal")
    @Override
    protected void copyToMemory(final byte[] srcBytes, final int srcOffset, final long startPtr, final int len) {
        UNSAFE.copyMemory(srcBytes, srcOffset, null, startPtr, len);
    }

    /**
     * Copies bytes from off-heap memory to a Java array.
     * Uses unsafe operations for efficient memory transfer, bypassing standard
     * Java array access for maximum performance. This is an internal method
     * called automatically during cache get operations.
     *
     * @param startPtr the source memory address in off-heap memory from which to copy data
     * @param bytes the destination byte array
     * @param destOffset the starting offset in the destination array
     * @param len the number of bytes to copy
     */
    @SuppressWarnings("removal")
    @Override
    protected void copyFromMemory(final long startPtr, final byte[] bytes, final int destOffset, final int len) {
        UNSAFE.copyMemory(null, startPtr, bytes, destOffset, len);
    }

    /**
     * Creates a new builder for constructing OffHeapCache instances.
     * The builder provides a fluent API for configuring all cache parameters
     * including capacity, eviction policies, serialization, and disk spillover.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OffHeapCache<String, Data> cache = OffHeapCache.<String, Data>builder()
     *     .capacityInMB(100)
     *     .evictDelay(60000)
     *     .defaultLiveTime(3600000)
     *     .defaultMaxIdleTime(1800000)
     *     .build();
     * }</pre>
     *
     * @param <K> the type of keys used to identify cache entries
     * @param <V> the type of values stored in the cache
     * @return a new Builder instance
     */
    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    /**
     * Builder class for creating OffHeapCache instances with custom configuration.
     * Provides a fluent API for setting all cache parameters including capacity,
     * eviction policies, serialization, and disk spillover options.
     * 
     * <br><br>
     * Example usage:
     * <pre>{@code
     * OffHeapStore<String> diskStore = new OffHeapStore<>(Paths.get("/tmp/cache"));
     * OffHeapCache<String, Data> cache = OffHeapCache.<String, Data>builder()
     *     .capacityInMB(100)
     *     .maxBlockSizeInBytes(16384)
     *     .evictDelay(60000)
     *     .vacatingFactor(0.3f)
     *     .offHeapStore(diskStore)
     *     .statsTimeOnDisk(true)
     *     .build();
     * }</pre>
     *
     * @param <K> the type of keys used to identify cache entries
     * @param <V> the type of values stored in the cache
     */
    @Data
    @Accessors(chain = true, fluent = true)
    public static class Builder<K, V> {

        /**
         * Creates a new Builder with default values.
         * All fields start with their default values and can be customized using the fluent setter methods.
         */
        public Builder() {
            // Default constructor with default values
        }

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
         * Builds and returns a new OffHeapCache instance with the configured parameters.
         * All builder properties are validated and used to construct the cache with
         * the specified configuration.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * OffHeapCache<String, Data> cache = OffHeapCache.<String, Data>builder()
         *     .capacityInMB(200)
         *     .evictDelay(60000)
         *     .vacatingFactor(0.3f)
         *     .build();
         * }</pre>
         *
         * @return a new OffHeapCache instance
         */
        public OffHeapCache<K, V> build() {
            return new OffHeapCache<>(capacityInMB, maxBlockSizeInBytes == 0 ? DEFAULT_MAX_BLOCK_SIZE : maxBlockSizeInBytes, evictDelay, defaultLiveTime,
                    defaultMaxIdleTime, vacatingFactor, serializer, deserializer, offHeapStore, statsTimeOnDisk, testerForLoadingItemFromDiskToMemory,
                    storeSelector);
        }
    }
}