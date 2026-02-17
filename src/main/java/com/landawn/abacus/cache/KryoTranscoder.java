/*
 * Copyright (C) 2015 HaiYang Li
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.landawn.abacus.cache;

import com.landawn.abacus.parser.KryoParser;
import com.landawn.abacus.parser.ParserFactory;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

/**
 * A Memcached transcoder implementation that uses Kryo for serialization.
 * Kryo is a fast and efficient serialization framework that provides better
 * performance and smaller serialized sizes compared to Java's default serialization.
 * This transcoder integrates Kryo with SpyMemcached for optimal caching performance.
 *
 * <p>Benefits of using Kryo:
 * <ul>
 * <li>Faster serialization/deserialization</li>
 * <li>Smaller serialized data size</li>
 * <li>Support for circular references</li>
 * <li>No requirement for Serializable interface</li>
 * </ul>
 *
 * <p><b>Thread Safety:</b> This class is thread-safe. The underlying KryoParser
 * handles thread-safety internally using ThreadLocal for Kryo instances,
 * ensuring safe concurrent access from multiple threads.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Create a custom connection factory with Kryo transcoder
 * ConnectionFactory connFactory = new DefaultConnectionFactory() {
 *     @Override
 *     public Transcoder<Object> getDefaultTranscoder() {
 *         return new KryoTranscoder<>();
 *     }
 * };
 *
 * MemcachedClient client = new MemcachedClient(connFactory, addresses);
 * }</pre>
 *
 * @param <T> the type of objects to transcode
 * @see Transcoder
 * @see KryoParser
 */
public class KryoTranscoder<T> implements Transcoder<T> {

    private static final KryoParser kryoParser = ParserFactory.createKryoParser();

    private final int maxSize;

    /**
     * Creates a new KryoTranscoder with the default maximum size.
     * The default size is taken from {@code CachedData.MAX_SIZE}. Objects larger than
     * this size will fail to cache with an {@code IllegalArgumentException}.
     *
     * <p>This constructor is suitable for most use cases where the default size limits
     * are appropriate. For custom size requirements, use {@link #KryoTranscoder(int)}.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create transcoder with default max size
     * KryoTranscoder<User> transcoder = new KryoTranscoder<>();
     *
     * // Use with MemcachedClient
     * User user = new User("Alice", 25);
     * CachedData encoded = transcoder.encode(user);
     * User decoded = transcoder.decode(encoded);
     * }</pre>
     *
     * @see #KryoTranscoder(int)
     * @see CachedData#MAX_SIZE
     */
    public KryoTranscoder() {
        this(CachedData.MAX_SIZE);
    }

    /**
     * Creates a new KryoTranscoder with a specified maximum size.
     * Objects larger than this size cannot be cached and will throw an {@code IllegalArgumentException}
     * during the encode operation. This prevents oversized objects from being stored in the cache
     * and helps maintain predictable memory usage.
     *
     * <p>Use this constructor when you need to enforce custom size limits based on your
     * application's memory constraints or cache infrastructure requirements.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Create transcoder with 1MB max size
     * KryoTranscoder<User> transcoder = new KryoTranscoder<>(1024 * 1024);
     *
     * // Use with custom ConnectionFactory
     * ConnectionFactory connFactory = new DefaultConnectionFactory() {
     *     @Override
     *     public Transcoder<Object> getDefaultTranscoder() {
     *         return new KryoTranscoder<>(512 * 1024);   // 512KB limit
     *     }
     * };
     * }</pre>
     *
     * @param maxSize the maximum size in bytes for cached objects. Must be positive.
     * @throws IllegalArgumentException if maxSize is non-positive
     */
    public KryoTranscoder(final int maxSize) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("maxSize must be positive: " + maxSize);
        }
        this.maxSize = maxSize;
    }

    /**
     * Indicates whether this transcoder supports asynchronous decoding.
     * Kryo transcoding is fast enough that async decoding provides no benefit,
     * so this implementation always returns {@code false}.
     *
     * <p>This method is called by SpyMemcached to determine if the decode operation
     * should be performed asynchronously. Since Kryo deserialization is already highly
     * efficient, performing it synchronously avoids unnecessary threading overhead.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * KryoTranscoder<User> transcoder = new KryoTranscoder<>();
     * User user = new User("Bob", 30);
     * CachedData data = transcoder.encode(user);
     *
     * // Check if async decoding is supported
     * boolean supportsAsync = transcoder.asyncDecode(data);   // Returns false
     *
     * // Decoding will be performed synchronously
     * User decoded = transcoder.decode(data);
     * }</pre>
     *
     * @param d the cached data whose asynchronous decode capability is to be tested (must not be null)
     * @return {@code false}, always indicating synchronous decoding only
     */
    @Override
    public boolean asyncDecode(final CachedData d) {
        return false;
    }

    /**
     * Encodes an object to cached data using Kryo serialization.
     * The object is serialized to a byte array with no flags set (flags = 0). If the serialized
     * data exceeds the configured maximum size, an {@code IllegalArgumentException} is thrown.
     *
     * <p>This method is thread-safe. Multiple threads can safely encode objects concurrently
     * as the underlying {@code KryoParser} uses ThreadLocal instances to ensure thread safety.
     *
     * <p><b>Implementation Note:</b> The encoding process involves converting the object
     * graph to a compact binary representation using Kryo's optimized serialization protocol.
     * The resulting byte array is wrapped in a {@code CachedData} object along with metadata.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * KryoTranscoder<User> transcoder = new KryoTranscoder<>();
     *
     * // Encode a simple object
     * User user = new User("John", 30);
     * CachedData cached = transcoder.encode(user);
     * byte[] serialized = cached.getData();
     * System.out.println("Serialized size: " + serialized.length + " bytes");
     *
     * // Encode null is supported
     * CachedData nullData = transcoder.encode(null);
     *
     * // Complex objects with nested structures
     * Order order = new Order(123, Arrays.asList(item1, item2), customer);
     * CachedData orderData = transcoder.encode(order);
     * }</pre>
     *
     * @param o the object to encode and serialize (can be null)
     * @return the {@code CachedData} containing the serialized bytes and metadata, never null
     * @throws IllegalArgumentException if the serialized size exceeds maxSize
     * @throws RuntimeException if serialization fails due to Kryo-related errors
     * @see #decode(CachedData)
     * @see CachedData
     */
    @Override
    public CachedData encode(final T o) {
        final byte[] encoded = kryoParser.encode(o);
        if (encoded != null && encoded.length > maxSize) {
            throw new IllegalArgumentException("Encoded data size (" + encoded.length + " bytes) exceeds maxSize (" + maxSize + " bytes)");
        }
        return new CachedData(0, encoded, maxSize);
    }

    /**
     * Decodes cached data back to an object using Kryo deserialization.
     * This method reconstructs the original object from its serialized byte array representation.
     * It supports deserializing any object type that Kryo can handle, including complex object
     * graphs with nested structures and circular references.
     *
     * <p>This method is thread-safe. Multiple threads can safely decode data concurrently
     * as the underlying {@code KryoParser} uses ThreadLocal instances to ensure thread safety.
     *
     * <p><b>Important:</b> The class definitions of the objects being deserialized must be
     * available on the classpath. If the class structure has changed between encoding and decoding
     * (e.g., fields added/removed), deserialization may fail or produce unexpected results.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * KryoTranscoder<User> transcoder = new KryoTranscoder<>();
     *
     * // Basic encode-decode cycle
     * User originalUser = new User("Alice", 25);
     * CachedData cached = transcoder.encode(originalUser);
     * User decoded = transcoder.decode(cached);
     * assert originalUser.equals(decoded);
     *
     * // Handling null values
     * CachedData nullCached = transcoder.encode(null);
     * User nullDecoded = transcoder.decode(nullCached);   // Returns null
     *
     * // With MemcachedClient
     * MemcachedClient client = new MemcachedClient(connFactory, addresses);
     * CachedData data = (CachedData) client.get("user:123");
     * if (data != null) {
     *     User user = transcoder.decode(data);
     * }
     * }</pre>
     *
     * @param d the cached data to decode and deserialize (must not be null)
     * @return the deserialized object of type T (can be null if null was encoded)
     * @throws RuntimeException if the deserialization fails (e.g., corrupt data, class not found, incompatible class version)
     * @see #encode(Object)
     * @see CachedData#getData()
     */
    @Override
    public T decode(final CachedData d) {
        if (d == null) {
            return null;
        }
        final byte[] data = d.getData();
        if (data == null || data.length == 0) {
            return null;
        }
        return kryoParser.decode(data);
    }

    /**
     * Returns the maximum size for cached objects in bytes.
     * Objects that serialize to a size larger than this value will fail to cache
     * with an {@code IllegalArgumentException} during the {@link #encode(Object)} operation.
     *
     * <p>This value is set during construction and cannot be changed. It provides a
     * way for callers to query the size limit and make decisions about whether to
     * cache particular objects.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * KryoTranscoder<User> transcoder = new KryoTranscoder<>(1024 * 1024);
     *
     * // Query the max size
     * int maxSize = transcoder.getMaxSize();   // Returns 1048576 (1MB)
     *
     * // Check before encoding large objects
     * if (estimatedSize < transcoder.getMaxSize()) {
     *     CachedData data = transcoder.encode(largeObject);
     * } else {
     *     // Handle oversized object differently
     *     log.warn("Object too large to cache: {} bytes", estimatedSize);
     * }
     *
     * // Compare transcoders
     * KryoTranscoder<User> smallTranscoder = new KryoTranscoder<>(512 * 1024);
     * KryoTranscoder<User> largeTranscoder = new KryoTranscoder<>(2 * 1024 * 1024);
     * System.out.println("Size difference: " +
     *     (largeTranscoder.getMaxSize() - smallTranscoder.getMaxSize()) + " bytes");
     * }</pre>
     *
     * @return the maximum size in bytes for objects that can be cached
     * @see #encode(Object)
     * @see CachedData#MAX_SIZE
     */
    @Override
    public int getMaxSize() {
        return maxSize;
    }
}
