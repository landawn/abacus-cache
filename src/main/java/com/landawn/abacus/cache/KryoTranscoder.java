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
 * <br><br>
 * Benefits of using Kryo:
 * <ul>
 * <li>Faster serialization/deserialization</li>
 * <li>Smaller serialized data size</li>
 * <li>Support for circular references</li>
 * <li>No requirement for Serializable interface</li>
 * </ul>
 * 
 * <br>
 * Example usage:
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
 * @see SpyMemcached
 */
public class KryoTranscoder<T> implements Transcoder<T> {

    private static final KryoParser kryoParser = ParserFactory.createKryoParser();

    private final int maxSize;

    /**
     * Creates a new KryoTranscoder with the default maximum size.
     * The default size is taken from CachedData.MAX_SIZE. Objects larger than
     * this size will fail to cache.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * KryoTranscoder<User> transcoder = new KryoTranscoder<>();
     * User user = new User();
     * CachedData encoded = transcoder.encode(user);
     * }</pre>
     */
    public KryoTranscoder() {
        this(CachedData.MAX_SIZE);
    }

    /**
     * Creates a new KryoTranscoder with a specified maximum size.
     * Objects larger than this size cannot be cached and will throw an exception
     * during the encode operation. This prevents oversized objects from being stored.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * KryoTranscoder<User> transcoder = new KryoTranscoder<>(1024 * 1024); // 1MB max
     * }</pre>
     *
     * @param maxSize the maximum size in bytes for cached objects
     */
    public KryoTranscoder(final int maxSize) {
        this.maxSize = maxSize;
    }

    /**
     * Indicates whether this transcoder supports asynchronous decoding.
     * Kryo transcoding is fast enough that async decoding provides no benefit.
     *
     * @param d the cached data whose asynchronous decode capability is to be tested
     * @return false, indicating synchronous decoding only
     */
    @Override
    public boolean asyncDecode(final CachedData d) {
        return false;
    }

    /**
     * Encodes an object to cached data using Kryo serialization.
     * The object is serialized to bytes with no flags set. If the serialized
     * data exceeds the configured maximum size, an IllegalArgumentException is thrown.
     *
     * @param o the object to encode and serialize
     * @return the CachedData containing the serialized bytes
     * @throws IllegalArgumentException if the serialized size exceeds maxSize
     */
    @Override
    public CachedData encode(final T o) {
        return new CachedData(0, kryoParser.encode(o), maxSize);
    }

    /**
     * Decodes cached data back to an object using Kryo deserialization.
     * This method can deserialize any object type that Kryo supports. If the
     * deserialization fails due to incompatible data or other errors, a runtime
     * exception is thrown.
     *
     * @param d the cached data to decode and deserialize
     * @return the deserialized object
     * @throws RuntimeException if the deserialization fails (e.g., corrupt data, class not found)
     */
    @Override
    public T decode(final CachedData d) {
        return kryoParser.decode(d.getData());
    }

    /**
     * Returns the maximum size for cached objects.
     * Objects larger than this size will fail to cache.
     *
     * @return the maximum size in bytes
     */
    @Override
    public int getMaxSize() {
        return maxSize;
    }
}