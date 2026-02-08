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
 * byte[] largeByteArray = new byte[1024];
 * cache.put("key1", largeByteArray);
 * byte[] cached = cache.getOrNull("key1");
 *
 * OffHeapCacheStats stats = cache.stats();
 * System.out.println("Memory utilization: " +
 *     (double) stats.occupiedMemory() / stats.allocatedMemory());
 * }</pre>
 *
 * @param <K> the type of keys used to identify cache entries
 * @param <V> the type of values stored in the cache
 * @see AbstractOffHeapCache
 * @see OffHeapCache
 * @see OffHeapCacheStats
 * @see OffHeapStore
 * @see <a href="https://openjdk.org/jeps/454">JEP 454: Foreign Function &amp; Memory API</a>
 */
@SuppressFBWarnings({ "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE", "JLM_JSR166_UTILCONCURRENT_MONITORENTER" })
public class OffHeapCache25<K, V> extends AbstractOffHeapCache<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(OffHeapCache25.class);

    private volatile Arena arena;
    private volatile MemorySegment buffer;

    /**
     * Creates an OffHeapCache25 with the specified capacity in megabytes.
     * Uses default eviction delay of 3 seconds (3000 milliseconds) and default expiration times
     * ({@link Cache#DEFAULT_LIVE_TIME 3 hours TTL} and {@link Cache#DEFAULT_MAX_IDLE_TIME 30 minutes idle time}).
     * Memory is allocated at construction time and held until {@link #close()}.
     * The cache uses Kryo serialization by default if available, otherwise falls back to JSON serialization.
     *
     * <p><b>Memory Management:</b></p>
     * The memory is allocated immediately from native (off-heap) memory using the Foreign Memory API (Arena).
     * This memory remains allocated until {@link #close()} is called. The actual allocation size is
     * capacityInMB * 1048576 bytes (1MB = 1048576 bytes).
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * OffHeapCache25<String, byte[]> cache = new OffHeapCache25<>(100);   // 100MB
     * byte[] largeData = new byte[1024];
     * cache.put("key1", largeData);
     * byte[] retrieved = cache.getOrNull("key1");
     * cache.close();
     * }</pre>
     *
     * @param capacityInMB the total off-heap memory to allocate in megabytes. Must be positive.
     *                     The actual capacity will be capacityInMB * 1048576 bytes.
     * @throws IllegalArgumentException if capacityInMB is not positive
     * @throws OutOfMemoryError if native memory allocation fails
     */
    OffHeapCache25(final int capacityInMB) {
        this(capacityInMB, 3000);
    }

    /**
     * Creates an OffHeapCache25 with specified capacity and eviction delay.
     * Uses default {@link Cache#DEFAULT_LIVE_TIME TTL of 3 hours} and
     * {@link Cache#DEFAULT_MAX_IDLE_TIME idle time of 30 minutes}.
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
     * OffHeapCache25<Long, Data> cache = new OffHeapCache25<>(200, 60000);   // 200MB, 60s eviction
     * Data data = new Data();
     * cache.put(123L, data, 7200000, 3600000);   // 2h TTL, 1h idle
     * Data retrieved = cache.getOrNull(123L);
     * cache.close();
     * }</pre>
     *
     * @param capacityInMB the total off-heap memory to allocate in megabytes. Must be positive.
     *                     The actual capacity will be capacityInMB * 1048576 bytes.
     * @param evictDelay the delay between eviction runs in milliseconds. Use 0 or negative to disable automatic eviction.
     * @throws IllegalArgumentException if capacityInMB is not positive
     * @throws OutOfMemoryError if native memory allocation fails
     */
    OffHeapCache25(final int capacityInMB, final long evictDelay) {
        this(capacityInMB, evictDelay, DEFAULT_LIVE_TIME, DEFAULT_MAX_IDLE_TIME);
    }

    /**
     * Creates an OffHeapCache25 with fully specified basic parameters.
     * Memory is allocated at construction time and held until {@link #close()}.
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
     * OffHeapCache25<String, byte[]> cache = new OffHeapCache25<>(500, 30000, 3600000, 1800000);
     * cache.put("key1", "data".getBytes());
     * byte[] data = cache.getOrNull("key1");
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
    OffHeapCache25(final int capacityInMB, final long evictDelay, final long defaultLiveTime, final long defaultMaxIdleTime) {
        this(capacityInMB, DEFAULT_MAX_BLOCK_SIZE, evictDelay, defaultLiveTime, defaultMaxIdleTime, DEFAULT_VACATING_FACTOR, null, null, null, false, null,
                null);
    }

    /**
     * Package-private constructor with complete configuration for use by the Builder.
     * This constructor is called internally by the Builder's build() method and should not be
     * invoked directly. It provides access to all advanced configuration options including
     * custom serialization, disk spillover, and memory management tuning.
     * <br><br>
     * This is the most flexible constructor, delegating to the parent {@link AbstractOffHeapCache} with all
     * configuration parameters. For typical usage, prefer the simpler constructors or use the Builder
     * created via {@link #builder()}.
     *
     * <p><b>Memory Management:</b></p>
     * Memory is allocated immediately using the Foreign Memory API (Arena). The maxBlockSize parameter
     * controls how values are stored -- values larger than maxBlockSize are split across multiple blocks.
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
     * method called automatically during cache construction and should not be called directly.
     * <br><br>
     * The memory is allocated from the native heap, not managed by the Java garbage collector.
     * This reduces GC pressure but requires manual deallocation via {@link #deallocate()} to prevent memory leaks.
     *
     * @param capacityInBytes the number of bytes to allocate. Must be positive. This is typically a multiple
     *                        of SEGMENT_SIZE (1048576 bytes).
     * @return the memory address (pointer) of the allocated memory segment in native memory. This address is
     *         used for all subsequent memory access operations via copyToMemory and copyFromMemory.
     * @throws OutOfMemoryError if the allocation fails due to insufficient native memory available on the system
     */
    @Override
    protected long allocate(final long capacityInBytes) {
        arena = Arena.ofShared();
        buffer = arena.allocate(capacityInBytes);

        return buffer.address();
    }

    /**
     * Deallocates the off-heap memory by closing the Arena.
     * Called during cache shutdown to release native memory and prevent memory leaks.
     * This method is automatically invoked by close(). This is an internal method
     * and should not be called directly.
     * <br><br>
     * Once called, the memory address (_startPtr) becomes invalid and must not be accessed.
     * All cache operations should be stopped before calling this method. This releases all
     * memory associated with the Arena and makes the MemorySegment inaccessible. Any subsequent
     * attempts to access the memory segment will result in an IllegalStateException.
     *
     * @see #close()
     * @see #allocate(long)
     */
    @Override
    protected void deallocate() {
        // buffer.asSlice(_startPtr, _capacityB).fill((byte) 0);   // Is it unnecessary?
        arena.close();
    }

    /**
     * Copies bytes from off-heap memory to a Java array using MemorySegment API.
     * This provides type-safe memory access compared to Unsafe operations. This is an internal method
     * called automatically during cache get operations and should not be called directly.
     * <br><br>
     * The method performs a low-level memory copy using MemorySegment.copy, which is
     * type-safe and efficient. The method calculates the relative offset within the buffer
     * by subtracting _startPtr from startPtr to determine the correct position in the memory segment.
     *
     * @param startPtr the source memory address in off-heap memory from which to copy data. Must be a
     *                 valid address within the allocated memory region (between _startPtr and _startPtr + capacity).
     * @param bytes the destination byte array. Must not be null and must have sufficient capacity to
     *              hold the copied data.
     * @param destOffset the starting offset in the destination array. Together with the array structure,
     *                   this determines the actual destination memory address.
     * @param len the number of bytes to copy. Must be positive and must not exceed the available
     *            space in the destination array starting from destOffset.
     */
    @Override
    protected void copyFromMemory(final long startPtr, final byte[] bytes, final int destOffset, final int len) {
        MemorySegment.copy(buffer, ValueLayout.JAVA_BYTE, startPtr - _startPtr, bytes, destOffset, len);
    }

    /**
     * Copies bytes from a Java array to off-heap memory using MemorySegment API.
     * This provides type-safe memory access compared to Unsafe operations. This is an internal method
     * called automatically during cache put operations and should not be called directly.
     * <br><br>
     * The method performs a low-level memory copy using MemorySegment.copy, which is
     * type-safe and efficient. The method calculates the relative offset within the buffer
     * by subtracting _startPtr from startPtr to determine the correct position in the memory segment.
     *
     * @param srcBytes the source byte array from which to copy data. Must not be null.
     * @param srcOffset the starting offset in the source array. Together with the array structure,
     *                  this determines the actual source memory address.
     * @param startPtr the destination memory address in off-heap memory. Must be a valid address
     *                 within the allocated memory region (between _startPtr and _startPtr + capacity).
     * @param len the number of bytes to copy. Must be positive and must not exceed the available
     *            space at the destination address or the size of the source array.
     */
    @Override
    protected void copyToMemory(final byte[] srcBytes, final int srcOffset, final long startPtr, final int len) {
        MemorySegment.copy(srcBytes, srcOffset, buffer, ValueLayout.JAVA_BYTE, startPtr - _startPtr, len);
    }

    /**
     * Creates a new builder for constructing OffHeapCache25 instances.
     * The builder provides a fluent API for configuring all cache parameters
     * including capacity, eviction policies, serialization, and disk spillover.
     * This is the recommended way to create an OffHeapCache25 with custom configuration.
     *
     * <p><b>Builder Pattern:</b></p>
     * The builder uses Lombok's @Accessors(chain = true, fluent = true) to provide fluent setter methods.
     * All configuration methods return the builder instance for method chaining. Call build() to create
     * the configured cache instance.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Basic configuration
     * OffHeapCache25<String, Data> cache = OffHeapCache25.<String, Data>builder()
     *     .capacityInMB(100)
     *     .evictDelay(60000)
     *     .defaultLiveTime(3600000)
     *     .defaultMaxIdleTime(1800000)
     *     .build();
     *
     * // Advanced configuration with disk spillover
     * OffHeapStore<String> diskStore = new OffHeapStore<>(Paths.get("/tmp/cache"));
     * OffHeapCache25<String, byte[]> advancedCache = OffHeapCache25.<String, byte[]>builder()
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
     * @return a new {@link Builder} instance with default values that can be customized via fluent setters
     * @see Builder
     * @see Builder#build()
     */
    public static <K, V> Builder<K, V> builder() {
        return new Builder<>();
    }

    /**
     * Builder class for creating OffHeapCache25 instances with custom configuration.
     * Provides a fluent API for setting all cache parameters including capacity,
     * eviction policies, serialization, and disk spillover options. All setters
     * return the builder instance for method chaining.
     * <br><br>
     * The builder uses Lombok's {@code @Data} and {@code @Accessors} annotations to generate fluent
     * setter methods automatically. All fields have sensible defaults and can be
     * overridden as needed before calling {@link #build()}.
     *
     * <p><b>Default Values:</b></p>
     * <ul>
     * <li>capacityInMB: required (must be set to a positive value)</li>
     * <li>maxBlockSizeInBytes: 8192 (8KB)</li>
     * <li>evictDelay: 0 (automatic eviction disabled)</li>
     * <li>defaultLiveTime: 0 (no TTL expiration)</li>
     * <li>defaultMaxIdleTime: 0 (no idle timeout)</li>
     * <li>vacatingFactor: 0.2 (20%)</li>
     * <li>serializer: {@code null} (use default Kryo or JSON)</li>
     * <li>deserializer: {@code null} (use default Kryo or JSON)</li>
     * <li>offHeapStore: {@code null} (memory-only mode)</li>
     * <li>statsTimeOnDisk: {@code false}</li>
     * <li>testerForLoadingItemFromDiskToMemory: {@code null} (no auto-promotion)</li>
     * <li>storeSelector: {@code null} (default routing)</li>
     * </ul>
     *
     * <br>
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Simple cache with basic settings
     * OffHeapCache25<String, Data> cache = OffHeapCache25.<String, Data>builder()
     *     .capacityInMB(100)
     *     .evictDelay(60000)
     *     .defaultLiveTime(3600000)
     *     .build();
     *
     * // Advanced cache with disk spillover
     * OffHeapStore<String> diskStore = new OffHeapStore<>(Paths.get("/tmp/cache"));
     * OffHeapCache25<String, Data> advancedCache = OffHeapCache25.<String, Data>builder()
     *     .capacityInMB(100)
     *     .maxBlockSizeInBytes(16384)
     *     .evictDelay(60000)
     *     .vacatingFactor(0.3f)
     *     .offHeapStore(diskStore)
     *     .statsTimeOnDisk(true)
     *     .build();
     *
     * // Cache with custom serialization
     * OffHeapCache25<String, User> customCache = OffHeapCache25.<String, User>builder()
     *     .capacityInMB(50)
     *     .serializer((user, os) -> customSerializer.serialize(user, os))
     *     .deserializer((bytes, type) -> customDeserializer.deserialize(bytes))
     *     .build();
     * }</pre>
     *
     * @param <K> the type of keys used to identify cache entries
     * @param <V> the type of values stored in the cache
     * @see OffHeapCache25#builder()
     */
    @Data
    @Accessors(chain = true, fluent = true)
    public static class Builder<K, V> {

        /**
         * Creates a new Builder with default values.
         * All fields start with their default values and can be customized using the fluent setter methods
         * generated by Lombok's {@code @Accessors(chain = true, fluent = true)}. The {@code capacityInMB} field must be
         * explicitly set to a positive value before calling {@link #build()}.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * Builder<String, Data> builder = new Builder<>();
         * builder.capacityInMB(100)
         *        .evictDelay(60000)
         *        .defaultLiveTime(3600000);
         * OffHeapCache25<String, Data> cache = builder.build();
         * }</pre>
         */
        public Builder() {
            // Default constructor with default values
        }

        /**
         * The total off-heap memory capacity in megabytes.
         * This is a required field and must be set to a positive value before calling {@link #build()}.
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
         * Default: 0 (automatic eviction disabled)
         */
        private long evictDelay;

        /**
         * Default time-to-live for cache entries in milliseconds.
         * Entries older than this will be considered expired and removed.
         * Use 0 or negative for no TTL expiration.
         * Can be overridden per entry in put() operations.
         * <br>
         * Default: 0 (no TTL expiration)
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
         * Default: {@code null} (uses Kryo if available, otherwise JSON)
         */
        private BiConsumer<? super V, ByteArrayOutputStream> serializer;

        /**
         * Custom deserializer for converting byte arrays back to values.
         * The function receives the byte array and type information, and should return
         * the deserialized value instance.
         * <br>
         * Default: {@code null} (uses Kryo if available, otherwise JSON)
         */
        private BiFunction<byte[], Type<V>, ? extends V> deserializer;

        /**
         * Optional disk store for spillover when memory is full.
         * When configured, values that don't fit in memory can be stored to disk instead of being rejected.
         * The disk store manages persistence and retrieval of overflow entries.
         * <br>
         * Default: {@code null} (memory-only mode)
         */
        private OffHeapStore<K> offHeapStore;

        /**
         * Whether to collect detailed disk I/O timing statistics.
         * When {@code true}, tracks min/max/average read and write times to disk.
         * This has minimal performance overhead and is useful for monitoring disk performance.
         * Only applies when {@link #offHeapStore} is configured.
         * <br>
         * Default: {@code false}
         */
        private boolean statsTimeOnDisk;

        /**
         * Predicate to determine when to load items from disk back to memory based on access patterns.
         * The predicate receives:
         * <ul>
         * <li>{@link ActivityPrint} - access pattern statistics for the entry</li>
         * <li>{@link Integer} - size of the entry in bytes</li>
         * <li>{@link Long} - I/O elapsed time in milliseconds</li>
         * </ul>
         * Return {@code true} to promote the value from disk to memory.
         * Only applies when {@link #offHeapStore} is configured.
         * <br>
         * Default: {@code null} (automatic promotion disabled)
         */
        private TriPredicate<ActivityPrint, Integer, Long> testerForLoadingItemFromDiskToMemory;

        /**
         * Function to determine storage location for each put operation.
         * The function receives the key, value, and size (in bytes), and should return:
         * <ul>
         * <li>0 - default routing (try memory first, fallback to disk if memory unavailable)</li>
         * <li>1 - memory only (never store to disk, operation fails if memory unavailable)</li>
         * <li>2 - disk only (always store to disk via {@link #offHeapStore}, skip memory)</li>
         * </ul>
         * Only applies when {@link #offHeapStore} is configured.
         * <br>
         * Default: {@code null} (default routing behavior)
         */
        private TriFunction<K, V, Integer, Integer> storeSelector;

        /**
         * Builds and returns a new OffHeapCache25 instance with the configured parameters.
         * All builder properties are validated and used to construct the cache with
         * the specified configuration. This method applies defaults where needed (e.g.,
         * maxBlockSizeInBytes defaults to 8192 if set to 0) and delegates to the full
         * package-private constructor.
         * <br><br>
         * <b>Important:</b> The {@code capacityInMB} field must be set to a positive value before calling build(),
         * as it is required for cache initialization. All other fields are optional and have sensible defaults.
         *
         * <p><b>Memory Management:</b></p>
         * Calling build() immediately allocates the specified amount of native memory using the Foreign Memory API.
         * This memory remains allocated until {@link OffHeapCache25#close()} is called on the returned cache instance.
         * Failure to close the cache will result in a native memory leak.
         *
         * <p><b>Usage Examples:</b></p>
         * <pre>{@code
         * // Basic cache with required fields
         * OffHeapCache25<String, Data> cache = OffHeapCache25.<String, Data>builder()
         *     .capacityInMB(200)
         *     .evictDelay(60000)
         *     .vacatingFactor(0.3f)
         *     .build();
         *
         * // Advanced cache with all options
         * OffHeapStore<String> store = new OffHeapStore<>(Paths.get("/tmp/cache"));
         * OffHeapCache25<String, Data> advancedCache = OffHeapCache25.<String, Data>builder()
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
         * @return a new {@link OffHeapCache25} instance configured with the builder's settings
         * @throws IllegalArgumentException if capacityInMB is not positive, or if maxBlockSizeInBytes is set
         *                                  but is less than 1024 or greater than SEGMENT_SIZE (1048576),
         *                                  or if other validation fails
         * @throws OutOfMemoryError if native memory allocation fails
         */
        public OffHeapCache25<K, V> build() {
            return new OffHeapCache25<>(capacityInMB, maxBlockSizeInBytes == 0 ? DEFAULT_MAX_BLOCK_SIZE : maxBlockSizeInBytes, evictDelay, defaultLiveTime,
                    defaultMaxIdleTime, vacatingFactor, serializer, deserializer, offHeapStore, statsTimeOnDisk, testerForLoadingItemFromDiskToMemory,
                    storeSelector);
        }
    }
}