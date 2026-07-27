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
import com.landawn.abacus.util.N;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

/**
 * A Memcached {@link Transcoder} implementation that uses Kryo for serialization.
 * Kryo is a fast and efficient serialization framework that typically delivers better
 * performance and smaller serialized payloads than Java's default serialization. This
 * transcoder integrates Kryo with SpyMemcached to improve caching performance.
 *
 * <p>Benefits of using Kryo:
 * <ul>
 * <li>Faster serialization/deserialization.</li>
 * <li>Smaller serialized data size.</li>
 * <li>No requirement for the {@link java.io.Serializable} interface.</li>
 * </ul>
 *
 * <p><b>&#9888;&#65039; Circular references are not supported by default:</b> the default {@link KryoParser}
 * creates its Kryo instances with reference tracking disabled (Kryo's default), so encoding an
 * object graph with a cycle (e.g., a bidirectional parent/child link) recurses until the stack
 * overflows (the {@link StackOverflowError} may surface wrapped inside a Kryo serialization
 * error), and even acyclic shared references are duplicated in the payload
 * (object identity is not preserved on decode). {@code KryoParser} does not expose Kryo's
 * reference-tracking option, so there is no configuration workaround: cache only value types
 * whose object graphs are trees.
 *
 * <p><b>Thread Safety:</b> This class is thread-safe. Concurrency safety is delegated to the
 * underlying shared {@link KryoParser}, which internally pools Kryo/Output/Input instances.
 * The pool is guarded by internal locks rather than thread-locals, so highly concurrent
 * encode/decode workloads may contend on the shared pool; correctness is unaffected. Treat parser
 * configuration as initialization-only: finish class registration and other configuration before
 * the first concurrent encode/decode, and do not mutate the parser while it is in use.
 *
 * <p><b>Usage Examples:</b>
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

    /**
     * Lazily initialized shared default Kryo parser used when no parser is supplied to the
     * constructor. Reused across instances to avoid the cost of building a parser per transcoder;
     * it pools Kryo/Output/Input instances internally under locks, so it is safe for concurrent
     * use. Held in a nested holder class so that constructing a {@code KryoTranscoder} with a
     * caller-supplied parser never touches it. When the optional Kryo dependency is absent, the
     * default-parser path fails with an actionable {@link IllegalStateException}; that failure must
     * not leak into code paths that never need the default.
     */
    private static final class DefaultParserHolder {
        /** The shared default parser, created on first access of this holder class. */
        static final KryoParser DEFAULT_KRYO_PARSER = ParserFactory.createKryoParser();
    }

    private static KryoParser defaultKryoParser() {
        if (!ParserFactory.isKryoParserAvailable()) {
            throw new IllegalStateException("Kryo is required by KryoTranscoder but is not on the classpath;"
                    + " add the optional com.esotericsoftware:kryo dependency or use another transcoder");
        }

        // Touch the lazy holder only after the normal-method availability check. Throwing from the
        // holder initializer would wrap the documented IllegalStateException in
        // ExceptionInInitializerError and permanently poison this class-init path.
        return DefaultParserHolder.DEFAULT_KRYO_PARSER;
    }

    private final KryoParser kryoParser;

    private final int maxSize;

    private static int checkMaxSize(final int maxSize) {
        return N.checkArgPositive(maxSize, "maxSize");
    }

    /**
     * Creates a new {@code KryoTranscoder} configured with the default maximum size.
     * The default value is {@link CachedData#MAX_SIZE}. Objects whose serialized form
     * exceeds this size will fail to cache and {@link #encode(Object)} will throw
     * {@link IllegalArgumentException}.
     *
     * <p>This constructor is suitable for most use cases where the default size limit
     * is appropriate. For custom size requirements, use {@link #KryoTranscoder(int)}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Create transcoder with the default max size
     * KryoTranscoder<String> transcoder = new KryoTranscoder<>();
     * int max = transcoder.getMaxSize();   // CachedData.MAX_SIZE
     *
     * // Encode then decode round-trips the value
     * CachedData encoded = transcoder.encode("Alice");
     * String decoded = transcoder.decode(encoded);   // "Alice" -- equal to the encoded value
     * }</pre>
     *
     * @throws IllegalStateException if the optional Kryo dependency is unavailable
     * @see #KryoTranscoder(int)
     * @see CachedData#MAX_SIZE
     */
    public KryoTranscoder() {
        this(CachedData.MAX_SIZE);
    }

    /**
     * Creates a new {@code KryoTranscoder} with the specified maximum size.
     * Objects whose serialized form exceeds {@code maxSize} cannot be cached, and
     * {@link #encode(Object)} will throw {@link IllegalArgumentException} when this limit is
     * exceeded. Enforcing a maximum size prevents oversized objects from being stored in the
     * cache and helps maintain predictable memory usage.
     *
     * <p>Use this constructor when you need to enforce custom size limits based on your
     * application's memory constraints or cache infrastructure requirements.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Create transcoder with a 1MB max size
     * KryoTranscoder<String> transcoder = new KryoTranscoder<>(1024 * 1024);
     * int max = transcoder.getMaxSize();   // 1048576 -- the value passed in
     *
     * // A non-positive maxSize is rejected
     * new KryoTranscoder<String>(0);    // throws IllegalArgumentException
     * new KryoTranscoder<String>(-1);   // throws IllegalArgumentException
     *
     * // Use with a custom ConnectionFactory
     * ConnectionFactory connFactory = new DefaultConnectionFactory() {
     *     @Override
     *     public Transcoder<Object> getDefaultTranscoder() {
     *         return new KryoTranscoder<>(512 * 1024);   // 512KB limit
     *     }
     * };
     * }</pre>
     *
     * @param maxSize the maximum size in bytes for cached objects; must be positive
     * @throws IllegalArgumentException if {@code maxSize} is not positive
     * @throws IllegalStateException if the optional Kryo dependency is unavailable
     * @see #KryoTranscoder(int, KryoParser)
     */
    public KryoTranscoder(final int maxSize) {
        // Constructor arguments are evaluated left-to-right. Validate before touching the lazy
        // default-parser holder so an invalid size always produces the documented argument error
        // (and does not initialize Kryo unnecessarily).
        this(checkMaxSize(maxSize), defaultKryoParser());
    }

    /**
     * Creates a new {@code KryoTranscoder} with the default maximum size ({@link CachedData#MAX_SIZE})
     * and a caller-supplied {@link KryoParser}.
     * Use this constructor to control Kryo configuration - most importantly pre-registering classes,
     * which can make the serialized form more compact. For wire-format stability, use explicit,
     * stable registration IDs; automatically assigned IDs are safe only when registration order is
     * identical in every process that reads the data.
     *
     * <p><b>Thread Safety:</b> The supplied {@link KryoParser} must be safe for concurrent use; the
     * bundled parsers pool Kryo/Output/Input instances internally. Complete all registrations and
     * configuration before passing this transcoder to concurrent clients.
     *
     * @param kryoParser the Kryo parser to use for serialization; must not be {@code null}
     * @throws IllegalArgumentException if {@code kryoParser} is {@code null}
     * @see #KryoTranscoder(int, KryoParser)
     */
    public KryoTranscoder(final KryoParser kryoParser) {
        this(CachedData.MAX_SIZE, kryoParser);
    }

    /**
     * Creates a new {@code KryoTranscoder} with the specified maximum size and a caller-supplied
     * {@link KryoParser}.
     * Objects whose serialized form exceeds {@code maxSize} cannot be cached, and
     * {@link #encode(Object)} will throw {@link IllegalArgumentException} when this limit is exceeded.
     * Supplying the parser lets callers control Kryo configuration (e.g. class pre-registration for
     * compactness, with explicit stable IDs when wire compatibility matters) rather than relying on
     * the shared default parser.
     *
     * <p><b>Thread Safety:</b> The supplied {@link KryoParser} must be safe for concurrent use; the
     * bundled parsers pool Kryo/Output/Input instances internally. Complete all registrations and
     * configuration before passing this transcoder to concurrent clients.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * KryoParser parser = ParserFactory.createKryoParser();   // configure / register classes as needed
     * KryoTranscoder<String> transcoder = new KryoTranscoder<>(512 * 1024, parser);
     * }</pre>
     *
     * @param maxSize the maximum size in bytes for cached objects; must be positive
     * @param kryoParser the Kryo parser to use for serialization; must not be {@code null}
     * @throws IllegalArgumentException if {@code maxSize} is not positive or {@code kryoParser} is {@code null}
     */
    public KryoTranscoder(final int maxSize, final KryoParser kryoParser) {
        this.maxSize = checkMaxSize(maxSize);
        this.kryoParser = N.checkArgNotNull(kryoParser, "kryoParser");
    }

    /**
     * Indicates whether this transcoder supports asynchronous decoding.
     * Kryo decoding is fast enough that asynchronous decoding offers no benefit, so this
     * implementation always returns {@code false}.
     *
     * <p>This method is called by SpyMemcached to determine whether the decode operation should
     * be deferred to a worker thread. Returning {@code false} keeps decoding on the calling
     * thread, avoiding unnecessary threading overhead.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * KryoTranscoder<String> transcoder = new KryoTranscoder<>();
     * CachedData data = transcoder.encode("Bob");
     *
     * // Async decoding is never supported, regardless of the argument
     * boolean supportsAsync = transcoder.asyncDecode(data);   // false
     * boolean forNull = transcoder.asyncDecode(null);         // false (the argument is ignored)
     *
     * // Decoding is therefore performed synchronously
     * String decoded = transcoder.decode(data);   // "Bob"
     * }</pre>
     *
     * @param d the cached data whose asynchronous-decode capability is being queried (ignored;
     *          may be {@code null})
     * @return {@code false} always, indicating synchronous decoding only
     */
    @Override
    public boolean asyncDecode(final CachedData d) {
        return false;
    }

    /**
     * Encodes an object to cached data using Kryo serialization.
     * The object is serialized to a byte array with no flags set (flags = 0). If the serialized
     * payload exceeds the configured maximum size, an {@link IllegalArgumentException} is thrown.
     *
     * <p>This method is thread-safe. The underlying {@link KryoParser} maintains internal pools of
     * Kryo/Output/Input instances guarded by internal locks rather than {@code ThreadLocal}; it is
     * safe for concurrent use, though highly concurrent workloads may contend on the shared pool.
     *
     * <p><b>Implementation Note:</b> The encoding step converts the object graph into a compact
     * binary representation using Kryo's optimized serialization protocol. The resulting byte
     * array is wrapped in a {@link CachedData} with flag value {@code 0} and the configured
     * maximum size as its declared maximum.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * KryoTranscoder<String> transcoder = new KryoTranscoder<>();
     *
     * // Encode a simple object
     * CachedData cached = transcoder.encode("John");
     * int flags = cached.getFlags();              // 0 (this transcoder never sets flags)
     * byte[] serialized = cached.getData();       // non-empty Kryo payload (serialized.length > 0)
     * String back = transcoder.decode(cached);    // "John" -- round-trips equal
     *
     * // Encode null is supported: a non-null CachedData is produced...
     * CachedData nullData = transcoder.encode(null);   // never null
     * String none = transcoder.decode(nullData);       // null -- decodes back to null
     *
     * // Encoding past the configured maxSize is rejected
     * KryoTranscoder<String> tiny = new KryoTranscoder<>(8);
     * tiny.encode("payload larger than 8 bytes once Kryo-encoded");   // throws IllegalArgumentException
     * }</pre>
     *
     * @param o the object to encode and serialize (may be {@code null})
     * @return a {@link CachedData} containing the serialized bytes and metadata; never {@code null}
     * @throws IllegalArgumentException if the serialized size exceeds the configured {@code maxSize}
     * @throws RuntimeException if serialization fails due to Kryo-related errors
     * @see #decode(CachedData)
     * @see CachedData
     */
    @Override
    public CachedData encode(final T o) {
        // KryoParser.encode never returns null (it always returns the ByteArrayOutputStream's bytes,
        // writing a Kryo null-marker even for a null input), so a null check here would be dead code -
        // and CachedData rejects null data anyway, which would contradict this method's "never null" contract.
        final byte[] encoded = kryoParser.encode(o);
        if (encoded.length > maxSize) {
            throw new IllegalArgumentException("Encoded data size (" + encoded.length + " bytes) exceeds maxSize (" + maxSize + " bytes)");
        }
        return new CachedData(0, encoded, maxSize);
    }

    /**
     * Decodes cached data back into an object using Kryo deserialization.
     * Reconstructs the original object from its serialized byte-array representation. Supports
     * any object type that Kryo can handle, including complex object graphs with nested
     * structures — but not circular references with the default parser (see the class-level note:
     * reference tracking is disabled by default, so cyclic graphs fail at encode time and shared
     * references are duplicated rather than preserved).
     *
     * <p>This method is thread-safe. The underlying {@link KryoParser} maintains internal pools of
     * Kryo/Output/Input instances guarded by internal locks rather than {@code ThreadLocal}; it is
     * safe for concurrent use, though highly concurrent workloads may contend on the shared pool.
     *
     * <p><b>&#9888;&#65039; Class compatibility:</b> The class definitions of the objects being deserialized must be
     * available on the classpath. If the class structure has changed between encoding and decoding
     * (e.g., fields added or removed), deserialization may fail or produce unexpected results.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * KryoTranscoder<String> transcoder = new KryoTranscoder<>();
     *
     * // Basic encode-decode cycle
     * CachedData cached = transcoder.encode("Alice");
     * String decoded = transcoder.decode(cached);   // "Alice" -- equal to the encoded value
     *
     * // A null CachedData decodes to null
     * String fromNull = transcoder.decode((CachedData) null);   // null
     *
     * // Zero-length data also decodes to null (the empty-data short-circuit)
     * CachedData empty = new CachedData(0, new byte[0], 100);
     * String fromEmpty = transcoder.decode(empty);   // null
     *
     * // A value that was encoded as null decodes back to null
     * CachedData nullCached = transcoder.encode(null);
     * String nullDecoded = transcoder.decode(nullCached);   // null
     * }</pre>
     *
     * @param d the cached data to decode and deserialize; if {@code null}, {@code null} is returned
     * @return the deserialized object of type {@code T}, or {@code null} if {@code d} is
     *         {@code null}, its data is empty, or {@code null} was originally encoded
     * @throws RuntimeException if deserialization fails (e.g., corrupt data, class not found,
     *         or incompatible class version)
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
     * Returns the maximum allowed size, in bytes, for cached objects.
     * Objects whose serialized size exceeds this value will fail to cache with an
     * {@link IllegalArgumentException} during {@link #encode(Object)}.
     *
     * <p>The returned value is fixed at construction time. It lets callers query the size limit
     * and decide whether to attempt caching particular objects.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * KryoTranscoder<String> transcoder = new KryoTranscoder<>(1024 * 1024);
     *
     * // Query the configured max size
     * int maxSize = transcoder.getMaxSize();   // 1048576 (1MB) -- the value passed to the constructor
     *
     * // A default transcoder reports CachedData.MAX_SIZE
     * int defaultMax = new KryoTranscoder<String>().getMaxSize();   // CachedData.MAX_SIZE
     *
     * // Compare transcoders
     * KryoTranscoder<String> smallTranscoder = new KryoTranscoder<>(512 * 1024);
     * KryoTranscoder<String> largeTranscoder = new KryoTranscoder<>(2 * 1024 * 1024);
     * int small = smallTranscoder.getMaxSize();   // 524288 (512KB)
     * int large = largeTranscoder.getMaxSize();   // 2097152 (2MB)
     * int diff = large - small;                   // 1572864
     * }</pre>
     *
     * @return the maximum size, in bytes, for objects that can be cached
     * @see #encode(Object)
     * @see CachedData#MAX_SIZE
     */
    @Override
    public int getMaxSize() {
        return maxSize;
    }
}
