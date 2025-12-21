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
            throw new RuntimeException("Failed to initialize sun.misc.Unsafe for off-heap memory allocation", e);
        }
    }

    @SuppressWarnings("removal")
    private static final int BYTE_ARRAY_BASE = UNSAFE.arrayBaseOffset(byte[].class);

    /**
     * Creates an OffHeapCache with the specified capacity in megabytes.
     * Uses default eviction delay of 3 seconds (3000 milliseconds) and default expiration times
     * (3 hours TTL and 30 minutes idle time). Memory is allocated at construction time and held until close().
     * The cache uses Kryo serialization by default if available, otherwise falls back to JSON serialization.
     *
     * <p><b>Memory Management:</b></p>
     * The memory is allocated immediately from native (off-heap) memory using sun.misc.Unsafe.
     * This memory remains allocated until close() is called. The actual allocation size is
     * capacityInMB * 1048576 bytes (1MB = 1048576 bytes).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create a 100MB off-heap cache
     * OffHeapCache<String, byte[]> cache = new OffHeapCache<>(100);
     *
     * byte[] largeData = new byte[1024];
     * cache.put("key1", largeData);
     * byte[] retrieved = cache.gett("key1");
     *
     * // Always close to free native memory
     * cache.close();
     * }</pre>
     *
     * @param capacityInMB the total off-heap memory to allocate in megabytes. Must be positive.
     *                     The actual capacity will be capacityInMB * 1048576 bytes.
     * @throws IllegalArgumentException if capacityInMB is not positive
     * @throws OutOfMemoryError if native memory allocation fails
     */
    OffHeapCache(final int capacityInMB) {
        this(capacityInMB, 3000);
    }

    /**
     * Creates an OffHeapCache with specified capacity and eviction delay.
     * Uses default TTL of 3 hours and idle time of 30 minutes.
     * The eviction delay controls how frequently the cache scans for expired entries and reclaims empty segments.
     * The cache uses Kryo serialization by default if available, otherwise falls back to JSON serialization.
     *
     * <p><b>Memory Management:</b></p>
     * Memory is allocated immediately from native (off-heap) memory. The eviction process runs
     * periodically at the specified interval to remove expired entries and reclaim memory from
     * empty segments. Setting evictDelay to 0 or negative disables automatic eviction, but
     * expired entries will still be removed lazily on access.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // 200MB cache with 60 second eviction interval
     * OffHeapCache<Long, Data> cache = new OffHeapCache<>(200, 60000);
     *
     * Data data = new Data();
     * cache.put(123L, data, 7200000, 3600000);   // 2h TTL, 1h idle
     * Data retrieved = cache.gett(123L);
     *
     * cache.close();
     * }</pre>
     *
     * @param capacityInMB the total off-heap memory to allocate in megabytes. Must be positive.
     *                     The actual capacity will be capacityInMB * 1048576 bytes.
     * @param evictDelay the delay between eviction runs in milliseconds. Use 0 or negative to disable automatic eviction.
     * @throws IllegalArgumentException if capacityInMB is not positive
     * @throws OutOfMemoryError if native memory allocation fails
     */
    OffHeapCache(final int capacityInMB, final long evictDelay) {
        this(capacityInMB, evictDelay, DEFAULT_LIVE_TIME, DEFAULT_MAX_IDLE_TIME);
    }

    /**
     * Creates an OffHeapCache with fully specified basic parameters.
     * Memory is allocated at construction time and held until close().
     * This constructor provides complete control over cache timing behavior.
     * The cache uses Kryo serialization by default if available, otherwise falls back to JSON serialization.
     *
     * <p><b>Memory Management:</b></p>
     * Native memory is allocated immediately. The default max block size (8192 bytes) is used,
     * and the default vacating factor (0.2) determines when LRU eviction triggers to free space.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // 500MB, 30s eviction, 1h TTL, 30min idle
     * OffHeapCache<String, byte[]> cache = new OffHeapCache<>(500, 30000, 3600000, 1800000);
     *
     * cache.put("key1", "data".getBytes());
     * byte[] data = cache.gett("key1");
     *
     * cache.close();
     * }</pre>
     *
     * @param capacityInMB the total off-heap memory to allocate in megabytes. Must be positive.
     *                     The actual capacity will be capacityInMB * 1048576 bytes.
     * @param evictDelay the delay between eviction runs in milliseconds. Use 0 or negative to disable automatic eviction.
     * @param defaultLiveTime default time-to-live for entries in milliseconds. Use 0 or negative for no TTL expiration.
     * @param defaultMaxIdleTime default maximum idle time for entries in milliseconds. Use 0 or negative for no idle timeout.
     * @throws IllegalArgumentException if capacityInMB is not positive
     * @throws OutOfMemoryError if native memory allocation fails
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
     * <br><br>
     * This is the most flexible constructor, delegating to the parent AbstractOffHeapCache with all
     * configuration parameters. For typical usage, prefer the public constructors or use the Builder
     * created via {@link #builder()}.
     *
     * <p><b>Memory Management:</b></p>
     * Memory is allocated immediately using sun.misc.Unsafe.allocateMemory(). The maxBlockSize parameter
     * controls how values are stored - values larger than maxBlockSize are split across multiple blocks.
     * The vacatingFactor determines when defragmentation is triggered to reclaim fragmented memory.
     *
     * @param capacityInMB the total off-heap memory to allocate in megabytes. Must be positive.
     *                     The actual capacity will be capacityInMB * 1048576 bytes.
     * @param maxBlockSize maximum size of a single memory block in bytes for memory allocation efficiency.
     *                     Must be between 1024 and SEGMENT_SIZE (1048576). Values larger than maxBlockSize
     *                     will be split across multiple blocks. Default is 8192 bytes.
     * @param evictDelay the delay between eviction runs in milliseconds. Use 0 or negative to disable automatic eviction.
     * @param defaultLiveTime default time-to-live for entries in milliseconds. Use 0 or negative for no TTL expiration.
     * @param defaultMaxIdleTime default maximum idle time for entries in milliseconds. Use 0 or negative for no idle timeout.
     * @param vacatingFactor factor (0.0-1.0) determining when to trigger memory defragmentation.
     *                       When the pool reaches this utilization threshold, LRU entries are evicted to free space.
     *                       Use 0.0 to apply the DEFAULT_VACATING_FACTOR (0.2). Typical values range from 0.1 to 0.3.
     * @param serializer custom serializer for converting values to byte streams, or null for default serialization
     *                   (Kryo if available, otherwise JSON). The serializer should write the complete serialized
     *                   form to the provided ByteArrayOutputStream.
     * @param deserializer custom deserializer for converting bytes to values, or null for default deserialization
     *                     (Kryo if available, otherwise JSON). The function receives the byte array and type
     *                     information, and should return the deserialized value.
     * @param offHeapStore optional disk store for spillover when memory is full, or null for memory-only cache.
     *                     When configured, values that don't fit in memory can be stored to disk instead of being rejected.
     * @param statsTimeOnDisk whether to collect detailed disk I/O timing statistics. When true, tracks min/max/average
     *                        read and write times to disk. This has minimal performance overhead.
     * @param testerForLoadingItemFromDiskToMemory predicate to determine when to load items from disk back to memory
     *                                             based on access patterns. Receives the activity print, size, and
     *                                             I/O elapsed time. Return true to promote the value to memory.
     *                                             Use null to disable automatic promotion.
     * @param storeSelector function to determine storage location for each put operation. Receives the key, value,
     *                      and size, and should return: 0 for default (try memory, fallback to disk), 1 for memory
     *                      only (never store to disk), or 2 for disk only (always store to disk). Use null for
     *                      default behavior (always try memory first).
     * @throws IllegalArgumentException if capacityInMB is not positive, or if maxBlockSize is outside valid range
     * @throws OutOfMemoryError if native memory allocation fails
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
     * This is an internal method called automatically during cache construction and should not be called directly.
     * <br><br>
     * The memory is allocated from the native heap, not managed by the Java garbage collector.
     * This reduces GC pressure but requires manual deallocation via {@link #deallocate()} to prevent memory leaks.
     * The allocated memory is uninitialized and may contain arbitrary data.
     *
     * <p><b>Memory Management:</b></p>
     * This method uses {@link sun.misc.Unsafe#allocateMemory(long)} which directly allocates native memory
     * from the operating system. The memory remains allocated until explicitly freed. Unlike heap memory,
     * this allocation does not trigger garbage collection or affect heap usage statistics.
     *
     * @param capacityInBytes the number of bytes to allocate. Must be positive. This is typically a multiple
     *                        of SEGMENT_SIZE (1048576 bytes).
     * @return the memory address (pointer) of the allocated memory block in native memory. This address is
     *         used for all subsequent memory access operations via copyToMemory and copyFromMemory.
     * @throws OutOfMemoryError if the allocation fails due to insufficient native memory available on the system
     * @see #deallocate()
     * @see #copyToMemory(byte[], int, long, int)
     * @see #copyFromMemory(long, byte[], int, int)
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
     * <br><br>
     * Once called, the memory address (_startPtr) becomes invalid and must not be accessed.
     * All cache operations should be stopped before calling this method. The method uses
     * sun.misc.Unsafe to free the native memory back to the operating system.
     *
     * <p><b>Memory Management:</b></p>
     * This method uses {@link sun.misc.Unsafe#freeMemory(long)} to return the allocated native memory
     * to the operating system. After deallocation, any attempt to access the memory address will result
     * in undefined behavior (likely a JVM crash). The parent class ensures this method is only called
     * once during shutdown.
     *
     * @see #close()
     * @see #allocate(long)
     */
    @SuppressWarnings("removal")
    @Override
    protected void deallocate() {
        // UNSAFE.setMemory(_startPtr, _capacityB, 0);   // Is it unnecessary?
        UNSAFE.freeMemory(_startPtr);
    }

    /**
     * Copies bytes from a Java array to off-heap memory.
     * Uses unsafe operations for efficient memory transfer, bypassing standard
     * Java array access for maximum performance. This is an internal method
     * called automatically during cache put operations and should not be called directly.
     * <br><br>
     * The method performs a low-level memory copy using sun.misc.Unsafe.copyMemory, which is
     * significantly faster than manual byte-by-byte copying. The srcOffset is combined with
     * the array base offset (BYTE_ARRAY_BASE) to determine the actual source memory address.
     *
     * <p><b>Memory Management:</b></p>
     * This method uses {@link sun.misc.Unsafe#copyMemory(Object, long, Object, long, long)} to perform
     * a fast, bulk memory copy. The first object parameter (srcBytes) indicates the source is a Java heap
     * object, while the second object parameter (null) indicates the destination is native memory.
     * No bounds checking is performed - invalid parameters will cause a JVM crash.
     *
     * @param srcBytes the source byte array from which to copy data. Must not be null.
     * @param srcOffset the starting offset in the source array. Together with BYTE_ARRAY_BASE,
     *                  this determines the actual source memory address. Must be non-negative.
     * @param startPtr the destination memory address in off-heap memory. Must be a valid address
     *                 within the allocated memory region (between _startPtr and _startPtr + capacity).
     * @param len the number of bytes to copy. Must be positive and must not exceed the available
     *            space at the destination address or the size of the source array from srcOffset.
     * @see #allocate(long)
     * @see #copyFromMemory(long, byte[], int, int)
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
     * called automatically during cache get operations and should not be called directly.
     * <br><br>
     * The method performs a low-level memory copy using sun.misc.Unsafe.copyMemory, which is
     * significantly faster than manual byte-by-byte copying. The destOffset is combined with
     * the array base offset (BYTE_ARRAY_BASE) to determine the actual destination memory address.
     *
     * <p><b>Memory Management:</b></p>
     * This method uses {@link sun.misc.Unsafe#copyMemory(Object, long, Object, long, long)} to perform
     * a fast, bulk memory copy. The first object parameter (null) indicates the source is native memory,
     * while the second object parameter (bytes) indicates the destination is a Java heap object.
     * No bounds checking is performed - invalid parameters will cause a JVM crash.
     *
     * @param startPtr the source memory address in off-heap memory from which to copy data. Must be a
     *                 valid address within the allocated memory region (between _startPtr and _startPtr + capacity).
     * @param bytes the destination byte array. Must not be null and must have sufficient capacity to
     *              hold the copied data (at least destOffset + len).
     * @param destOffset the starting offset in the destination array. Together with BYTE_ARRAY_BASE,
     *                   this determines the actual destination memory address. Must be non-negative.
     * @param len the number of bytes to copy. Must be positive and must not exceed the available
     *            space in the destination array starting from destOffset.
     * @see #allocate(long)
     * @see #copyToMemory(byte[], int, long, int)
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
     * This is the recommended way to create an OffHeapCache with custom configuration.
     *
     * <p><b>Builder Pattern:</b></p>
     * The builder uses Lombok's @Accessors(chain = true, fluent = true) to provide fluent setter methods.
     * All configuration methods return the builder instance for method chaining. Call build() to create
     * the configured cache instance.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Basic configuration
     * OffHeapCache<String, Data> cache = OffHeapCache.<String, Data>builder()
     *     .capacityInMB(100)
     *     .evictDelay(60000)
     *     .defaultLiveTime(3600000)
     *     .defaultMaxIdleTime(1800000)
     *     .build();
     *
     * // Advanced configuration with disk spillover
     * OffHeapStore<String> diskStore = new OffHeapStore<>(Paths.get("/tmp/cache"));
     * OffHeapCache<String, byte[]> advancedCache = OffHeapCache.<String, byte[]>builder()
     *     .capacityInMB(200)
     *     .maxBlockSizeInBytes(16384)
     *     .evictDelay(30000)
     *     .vacatingFactor(0.3f)
     *     .offHeapStore(diskStore)
     *     .statsTimeOnDisk(true)
     *     .build();
     * }</pre>
     *
     * @param <K> the type of keys used to identify cache entries
     * @param <V> the type of values stored in the cache
     * @return a new Builder instance with default values that can be customized via fluent setters
     * @see Builder
     */
    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    /**
     * Builder class for creating OffHeapCache instances with custom configuration.
     * Provides a fluent API for setting all cache parameters including capacity,
     * eviction policies, serialization, and disk spillover options. All setters
     * return the builder instance for method chaining.
     * <br><br>
     * The builder uses Lombok's @Data and @Accessors annotations to generate fluent
     * setter methods automatically. All fields have sensible defaults and can be
     * overridden as needed before calling build().
     *
     * <p><b>Default Values:</b></p>
     * <ul>
     * <li>capacityInMB: required (must be set)</li>
     * <li>maxBlockSizeInBytes: 8192 (8KB)</li>
     * <li>evictDelay: 0 (disabled)</li>
     * <li>defaultLiveTime: 0 (no expiration)</li>
     * <li>defaultMaxIdleTime: 0 (no idle timeout)</li>
     * <li>vacatingFactor: 0.2 (20%)</li>
     * <li>serializer: null (use default Kryo or JSON)</li>
     * <li>deserializer: null (use default Kryo or JSON)</li>
     * <li>offHeapStore: null (memory-only)</li>
     * <li>statsTimeOnDisk: false</li>
     * <li>testerForLoadingItemFromDiskToMemory: null (no auto-promotion)</li>
     * <li>storeSelector: null (default routing)</li>
     * </ul>
     *
     * <br>
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Simple cache with basic settings
     * OffHeapCache<String, Data> cache = OffHeapCache.<String, Data>builder()
     *     .capacityInMB(100)
     *     .evictDelay(60000)
     *     .defaultLiveTime(3600000)
     *     .build();
     *
     * // Advanced cache with disk spillover
     * OffHeapStore<String> diskStore = new OffHeapStore<>(Paths.get("/tmp/cache"));
     * OffHeapCache<String, Data> advancedCache = OffHeapCache.<String, Data>builder()
     *     .capacityInMB(100)
     *     .maxBlockSizeInBytes(16384)
     *     .evictDelay(60000)
     *     .vacatingFactor(0.3f)
     *     .offHeapStore(diskStore)
     *     .statsTimeOnDisk(true)
     *     .build();
     *
     * // Cache with custom serialization
     * OffHeapCache<String, User> customCache = OffHeapCache.<String, User>builder()
     *     .capacityInMB(50)
     *     .serializer((user, os) -> customSerializer.serialize(user, os))
     *     .deserializer((bytes, type) -> customDeserializer.deserialize(bytes))
     *     .build();
     * }</pre>
     *
     * @param <K> the type of keys used to identify cache entries
     * @param <V> the type of values stored in the cache
     * @see OffHeapCache#builder()
     */
    @Data
    @Accessors(chain = true, fluent = true)
    public static class Builder<K, V> {

        /**
         * Creates a new Builder with default values.
         * All fields start with their default values and can be customized using the fluent setter methods
         * generated by Lombok's @Accessors(chain = true, fluent = true). The capacityInMB field must be
         * explicitly set before calling build().
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Builder<String, Data> builder = new Builder<>();
         * builder.capacityInMB(100)
         *        .evictDelay(60000)
         *        .defaultLiveTime(3600000);
         * OffHeapCache<String, Data> cache = builder.build();
         * }</pre>
         */
        public Builder() {
            // Default constructor with default values
        }

        /**
         * The total off-heap memory capacity in megabytes.
         * This is a required field and must be set before calling build().
         * The actual capacity will be capacityInMB * 1048576 bytes.
         * <br>
         * Default: 0 (must be set to a positive value)
         */
        private int capacityInMB;

        /**
         * Maximum size of a single memory block in bytes.
         * Values larger than this will be split across multiple blocks.
         * Must be between 1024 and SEGMENT_SIZE (1048576).
         * Larger blocks reduce fragmentation but may waste space for small objects.
         * <br>
         * Default: 8192 bytes (8KB)
         */
        private int maxBlockSizeInBytes = DEFAULT_MAX_BLOCK_SIZE;

        /**
         * Delay between eviction runs in milliseconds.
         * Controls how frequently the cache scans for expired entries and reclaims empty segments.
         * Use 0 or negative to disable automatic eviction (expired entries still removed on access).
         * <br>
         * Default: 0 (disabled)
         */
        private long evictDelay;

        /**
         * Default time-to-live for cache entries in milliseconds.
         * Entries older than this will be considered expired and removed.
         * Use 0 or negative for no TTL expiration.
         * Can be overridden per entry in put() operations.
         * <br>
         * Default: 0 (no expiration)
         */
        private long defaultLiveTime;

        /**
         * Default maximum idle time for cache entries in milliseconds.
         * Entries not accessed within this time will be considered expired and removed.
         * Use 0 or negative for no idle timeout.
         * Can be overridden per entry in put() operations.
         * <br>
         * Default: 0 (no idle timeout)
         */
        private long defaultMaxIdleTime;

        /**
         * Factor (0.0-1.0) determining when to trigger memory defragmentation.
         * When the pool reaches this utilization threshold, LRU entries are evicted to free space.
         * Typical values range from 0.1 to 0.3. Higher values increase memory efficiency but may
         * cause more frequent evictions when approaching capacity.
         * <br>
         * Default: 0.2 (20%)
         */
        private float vacatingFactor = DEFAULT_VACATING_FACTOR;

        /**
         * Custom serializer for converting values to byte streams.
         * The serializer receives the value and a ByteArrayOutputStream, and should write
         * the complete serialized form to the stream.
         * <br>
         * Default: null (uses Kryo if available, otherwise JSON)
         */
        private BiConsumer<? super V, ByteArrayOutputStream> serializer;

        /**
         * Custom deserializer for converting byte arrays back to values.
         * The function receives the byte array and type information, and should return
         * the deserialized value instance.
         * <br>
         * Default: null (uses Kryo if available, otherwise JSON)
         */
        private BiFunction<byte[], Type<V>, ? extends V> deserializer;

        /**
         * Optional disk store for spillover when memory is full.
         * When configured, values that don't fit in memory can be stored to disk instead of being rejected.
         * The disk store manages persistence and retrieval of overflow entries.
         * <br>
         * Default: null (memory-only mode)
         */
        private OffHeapStore<K> offHeapStore;

        /**
         * Whether to collect detailed disk I/O timing statistics.
         * When true, tracks min/max/average read and write times to disk.
         * This has minimal performance overhead and is useful for monitoring disk performance.
         * Only applies when offHeapStore is configured.
         * <br>
         * Default: false
         */
        private boolean statsTimeOnDisk;

        /**
         * Predicate to determine when to load items from disk back to memory based on access patterns.
         * The predicate receives:
         * <ul>
         * <li>ActivityPrint - access pattern statistics for the entry</li>
         * <li>Integer - size of the entry in bytes</li>
         * <li>Long - I/O elapsed time in milliseconds</li>
         * </ul>
         * Return true to promote the value from disk to memory.
         * Only applies when offHeapStore is configured.
         * <br>
         * Default: null (automatic promotion disabled)
         */
        private TriPredicate<ActivityPrint, Integer, Long> testerForLoadingItemFromDiskToMemory;

        /**
         * Function to determine storage location for each put operation.
         * The function receives the key, value, and size (in bytes), and should return:
         * <ul>
         * <li>0 - default routing (try memory first, fallback to disk if memory unavailable)</li>
         * <li>1 - memory only (never store to disk, operation fails if memory unavailable)</li>
         * <li>2 - disk only (always store to disk via offHeapStore, skip memory)</li>
         * </ul>
         * Only applies when offHeapStore is configured.
         * <br>
         * Default: null (default routing behavior)
         */
        private TriFunction<K, V, Integer, Integer> storeSelector;

        /**
         * Builds and returns a new OffHeapCache instance with the configured parameters.
         * All builder properties are validated and used to construct the cache with
         * the specified configuration. This method applies defaults where needed (e.g.,
         * maxBlockSizeInBytes defaults to 8192 if set to 0) and delegates to the full
         * package-private constructor.
         * <br><br>
         * <b>Important:</b> The capacityInMB field must be set to a positive value before calling build(),
         * as it is required for cache initialization. All other fields are optional and have sensible defaults.
         *
         * <p><b>Memory Management:</b></p>
         * Calling build() immediately allocates the specified amount of native memory using sun.misc.Unsafe.
         * This memory remains allocated until close() is called on the returned cache instance. Failure to
         * close the cache will result in a native memory leak.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Basic cache with required fields
         * OffHeapCache<String, Data> cache = OffHeapCache.<String, Data>builder()
         *     .capacityInMB(200)
         *     .evictDelay(60000)
         *     .vacatingFactor(0.3f)
         *     .build();
         * try {
         *     cache.put("key", data);
         *     Data retrieved = cache.gett("key");
         * } finally {
         *     cache.close();   // Always close to free native memory
         * }
         *
         * // Advanced cache with all options
         * OffHeapStore<String> store = new OffHeapStore<>(Paths.get("/tmp/cache"));
         * OffHeapCache<String, Data> advancedCache = OffHeapCache.<String, Data>builder()
         *     .capacityInMB(100)
         *     .maxBlockSizeInBytes(16384)
         *     .evictDelay(30000)
         *     .defaultLiveTime(3600000)
         *     .defaultMaxIdleTime(1800000)
         *     .vacatingFactor(0.25f)
         *     .offHeapStore(store)
         *     .statsTimeOnDisk(true)
         *     .build();
         * }</pre>
         *
         * @return a new OffHeapCache instance configured with the builder's settings
         * @throws IllegalArgumentException if capacityInMB is not positive, or if maxBlockSizeInBytes is set
         *                                  but is less than 1024 or greater than SEGMENT_SIZE (1048576),
         *                                  or if other validation fails
         * @throws OutOfMemoryError if native memory allocation fails
         */
        public OffHeapCache<K, V> build() {
            return new OffHeapCache<>(capacityInMB, maxBlockSizeInBytes == 0 ? DEFAULT_MAX_BLOCK_SIZE : maxBlockSizeInBytes, evictDelay, defaultLiveTime,
                    defaultMaxIdleTime, vacatingFactor, serializer, deserializer, offHeapStore, statsTimeOnDisk, testerForLoadingItemFromDiskToMemory,
                    storeSelector);
        }
    }
}