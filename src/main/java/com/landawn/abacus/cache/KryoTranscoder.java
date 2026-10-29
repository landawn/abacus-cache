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

import java.io.IOException;
import java.io.OutputStream;

import com.esotericsoftware.kryo.KryoException;
import com.landawn.abacus.exception.UncheckedIOException;
import com.landawn.abacus.parser.KryoParser;
import com.landawn.abacus.parser.KryoSerConfig;
import com.landawn.abacus.parser.ParserFactory;
import com.landawn.abacus.util.ByteArrayOutputStream;
import com.landawn.abacus.util.ExceptionUtil;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Objectory;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

/**
 * A Memcached {@link Transcoder} implementation that uses Kryo for serialization.
 * Values do not need to implement {@link java.io.Serializable}; their classes must instead be
 * supported by the configured Kryo serializers. Payload size and serialization cost depend on
 * the object graph and serializer configuration.
 *
 * <p>Every reader and writer of a cache key must use compatible Kryo versions, registrations,
 * serializers, and class definitions. These payloads are not interchangeable with the stock
 * SpyMemcached transcoder's format, even though this transcoder stores a flag value of zero.
 * Invalidate existing entries or use a separate key namespace when changing formats.
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
 * Registered custom serializers must also support concurrent use, and callers must not mutate
 * an object graph while it is being encoded.
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

    /**
     * Returns the shared parser after checking the optional dependency.
     *
     * @return the default Kryo parser
     * @throws IllegalStateException if the optional Kryo dependency is unavailable
     */
    private static KryoParser defaultKryoParser() throws IllegalStateException {
        if (!ParserFactory.isKryoParserAvailable()) {
            throw new IllegalStateException("Kryo is required by KryoTranscoder but is not available;"
                    + " add the optional com.esotericsoftware:kryo dependency, make sure abacus-common's KryoParser can be"
                    + " initialized (on JDK 17+ it may need --add-opens for java.base packages), or use another transcoder");
        }

        // Touch the lazy holder only after the normal-method availability check. Throwing from the
        // holder initializer would wrap the documented IllegalStateException in
        // ExceptionInInitializerError and permanently poison this class-init path.
        return DefaultParserHolder.DEFAULT_KRYO_PARSER;
    }

    /** Makes the streaming {@link KryoParser#serialize} write the same bytes as {@link KryoParser#encode}. */
    private static final KryoSerConfig ENCODING_CONFIG = new KryoSerConfig().setWriteClass(true);

    private final KryoParser kryoParser;

    private final int maxSize;

    /**
     * Validates the encoded item size limit.
     *
     * @param maxSize the maximum encoded size in bytes
     * @return the validated limit
     * @throws IllegalArgumentException if {@code maxSize} is not positive
     */
    private static int checkMaxSize(final int maxSize) throws IllegalArgumentException {
        return N.checkArgPositive(maxSize, cs.maxSize);
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
    public KryoTranscoder() throws IllegalStateException {
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
    public KryoTranscoder(final int maxSize) throws IllegalArgumentException, IllegalStateException {
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
     * stable registration IDs. Registrations without explicit IDs can receive process-dependent
     * assignments; calling registration methods in the same order does not guarantee identical IDs.
     *
     * <p><b>Thread Safety:</b> The supplied {@link KryoParser} must be safe for concurrent use; the
     * bundled parsers pool Kryo/Output/Input instances internally. Complete all registrations and
     * configuration before passing this transcoder to concurrent clients.
     *
     * @param kryoParser the Kryo parser to use for serialization; must not be {@code null}
     * @throws IllegalArgumentException if {@code kryoParser} is {@code null}
     * @see #KryoTranscoder(int, KryoParser)
     */
    public KryoTranscoder(final KryoParser kryoParser) throws IllegalArgumentException {
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
    public KryoTranscoder(final int maxSize, final KryoParser kryoParser) throws IllegalArgumentException {
        checkMaxSize(maxSize);
        N.checkArgNotNull(kryoParser, cs.kryoParser);

        this.maxSize = maxSize;
        this.kryoParser = kryoParser;
    }

    /**
     * Indicates whether this transcoder supports asynchronous decoding.
     * This implementation always returns {@code false}, opting out of SpyMemcached's decoder
     * executor. Decoding cost depends on the payload and configured serializers.
     *
     * <p>This method is called by SpyMemcached to determine whether the decode operation should
     * be scheduled on a worker thread. Returning {@code false} makes SpyMemcached decode lazily
     * on the thread that retrieves the future's value; a timed future wait does not bound this
     * synchronous decoding work.
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
     * array is wrapped in a {@link CachedData} with flag value {@code 0}. The maximum size is
     * enforced while writing the payload, so an oversized value need not be fully serialized or
     * copied before rejection; the exception message therefore reports the size reached when
     * serialization stopped, a lower bound of the full encoded size. If a serializer catches the
     * rejection and then fails in another way, that failure is attached to the
     * {@code IllegalArgumentException} as a suppressed exception. Kryo's intermediate output buffer
     * and allocations made by custom serializers are separate from this encoded-payload limit. The
     * limit is not recorded in the returned {@code CachedData}.
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
     * @throws RuntimeException if a Kryo serializer cannot encode {@code o}; normally a
     *         {@link KryoException}, including for cycles through object fields and for a serializer that
     *         fails with an I/O cause (for example, Kryo's {@code JavaSerializer} meeting a non-serializable field)
     * @throws StackOverflowError if the object graph contains a cycle made only of collections, maps,
     *         or arrays (their serializers do not wrap the overflow)
     * @throws IllegalArgumentException if the serialized size exceeds the configured {@code maxSize},
     *         or if the parser's Kryo registrations assign one registration ID to two different classes
     * @see #decode(CachedData)
     * @see CachedData
     */
    @Override
    public CachedData encode(final T o) throws RuntimeException, StackOverflowError, IllegalArgumentException {
        final ByteArrayOutputStream bytes = Objectory.createByteArrayOutputStream();
        final SizeLimitedOutputStream output = new SizeLimitedOutputStream(bytes, maxSize);

        try {
            // writeClass=true preserves KryoParser.encode's wire format, while the streaming path
            // stops oversized values before they can grow an unrestricted buffer and a full copy.
            kryoParser.serialize(o, ENCODING_CONFIG, output);
            output.throwIfRejected(); // A custom serializer may have swallowed a rejected write.
            return new CachedData(0, bytes.toByteArray(), maxSize);
        } catch (final RuntimeException e) {
            final IllegalArgumentException sizeFailure = output.sizeFailure;

            // Field serializers can wrap the stream's rejection in a KryoException. Retain the
            // transcoder's IllegalArgumentException contract without changing unrelated failures,
            // and keep a different failure (raised after a serializer swallowed the rejection).
            if (sizeFailure != null) {
                if (!isCausedBy(e, sizeFailure)) {
                    sizeFailure.addSuppressed(e);
                }
                throw sizeFailure;
            }

            // The streaming overload reports a KryoException that has an IOException cause as an
            // UncheckedIOException. This stream never throws IOException, so the cause came from a
            // serializer (e.g. Java serialization of a non-Serializable field), which encode has
            // always reported as a KryoException.
            if (e instanceof UncheckedIOException && e.getCause() instanceof IOException) {
                throw KryoFailures.ofEncode(o, (IOException) e.getCause());
            }

            throw e;
        } finally {
            Objectory.recycle(bytes);
        }
    }

    private static boolean isCausedBy(final Throwable e, final Throwable cause) {
        // Throwable.initCause rejects only self-causation, so a serializer's failure can carry a
        // cyclic cause chain (A -> B -> A); findCause stops at the first repeated link instead of
        // spinning forever like a plain getCause() walk.
        return ExceptionUtil.findCause(e, t -> t == cause).isPresent();
    }

    /** Limits writes before the pooled buffer grows; the usual small-entry buffer remains reusable. */
    private static final class SizeLimitedOutputStream extends OutputStream {
        private final ByteArrayOutputStream bytes;
        private final int maxSize;
        private IllegalArgumentException sizeFailure;

        SizeLimitedOutputStream(final ByteArrayOutputStream bytes, final int maxSize) {
            this.bytes = bytes;
            this.maxSize = maxSize;
        }

        @Override
        public void write(final int value) {
            checkSize(1);
            bytes.write(value);
        }

        @Override
        public void write(final byte[] data, final int offset, final int length) {
            checkSize(length);
            bytes.write(data, offset, length);
        }

        private void checkSize(final int length) {
            if (sizeFailure == null && length > maxSize - bytes.size()) {
                // Serialization stops here, so the full size is unknown; report what it reached.
                sizeFailure = new IllegalArgumentException(
                        "Encoded data size (at least " + ((long) bytes.size() + length) + " bytes) exceeds maxSize (" + maxSize + " bytes)");
            }

            throwIfRejected();
        }

        void throwIfRejected() {
            if (sizeFailure != null) {
                throw sizeFailure;
            }
        }
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
     * A type may encode successfully yet fail to decode if it lacks a no-argument constructor
     * and no configured serializer provides its instantiation. Custom serializers and class
     * registrations must match those used to encode the entry. Kryo payloads carry no format
     * marker, so foreign data is not always detected: a counter's ASCII digits always fail, but
     * other bytes (for example, a raw string stored by the stock transcoder) can decode into an
     * unrelated object, such as an empty {@code OptionalInt}.
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
     * @throws IllegalArgumentException if the parser's Kryo registrations assign one registration ID to
     *         two different classes
     * @throws RuntimeException if the payload is corrupt, truncated, uses an unavailable or incompatible
     *         class, or was not written with compatible Kryo serializers (for example, a counter's ASCII
     *         digits); normally a {@link KryoException}. A bare {@link IncompatibleClassChangeError} or an
     *         {@link InstantiationError} raised while decoding is converted to a {@code KryoException}
     *         with that error as its cause: foreign bytes and an incompatibly changed application class
     *         can raise the same error, and its type cannot tell them apart. The other
     *         {@code IncompatibleClassChangeError} subclasses (such as {@link NoSuchMethodError}), which
     *         only an application class incompatibility raises, propagate unchanged.
     * @see #encode(Object)
     * @see CachedData#getData()
     */
    @Override
    public T decode(final CachedData d) throws RuntimeException {
        if (d == null) {
            return null;
        }
        final byte[] data = d.getData();
        if (data == null || data.length == 0) {
            return null;
        }

        try {
            return kryoParser.decode(data);
        } catch (final IncompatibleClassChangeError e) {
            // Kryo's generated (ReflectASM) constructor accessor executes `new` directly, so a foreign
            // payload whose leading bytes resolve to an abstract registered class (e.g. the ASCII
            // digits '6'/'9' of a memcached counter -> InputStream/OutputStream) surfaces as an
            // InstantiationError rather than the documented KryoException. Other foreign bytes (e.g.
            // a raw string stored by another transcoder) can decode into a type-confused object graph
            // whose interface calls throw the parent IncompatibleClassChangeError. An incompatibly
            // changed application class can raise those same two errors, which the type cannot tell
            // apart, so the message names both causes and keeps the error. The other subclasses
            // (NoSuchMethodError, IllegalAccessError, ...) come only from such a class change, so
            // they propagate unchanged.
            if (e.getClass() != IncompatibleClassChangeError.class && !(e instanceof InstantiationError)) {
                throw e;
            }

            throw KryoFailures.ofDecode(e);
        }
    }

    /**
     * Builds the {@link KryoException}s for encode and decode failures. Kept in a nested class so
     * the only bytecode reference to the Kryo exception type is resolved lazily, at the moment of
     * such a failure: a {@code throw new KryoException(...)} in {@link #encode(Object)} or
     * {@link #decode(CachedData)} itself would make the verifier load the Kryo class when linking
     * this class, replacing the actionable {@link IllegalStateException} raised when Kryo is absent
     * with a bare {@code NoClassDefFoundError}.
     */
    private static final class KryoFailures {
        private KryoFailures() {
        }

        static RuntimeException ofEncode(final Object value, final IOException cause) {
            return new KryoException("A Kryo serializer failed with an I/O error while encoding a value of type "
                    + (value == null ? "null" : value.getClass().getName()) + ": " + cause, cause);
        }

        static RuntimeException ofDecode(final IncompatibleClassChangeError e) {
            return new KryoException("Cannot decode the cached bytes as a Kryo payload (" + e.getMessage()
                    + "): either the entry was not written by a compatible KryoTranscoder (e.g. an incr/decr counter), or a class used while decoding"
                    + " is binary-incompatible with the code that uses it", e);
        }
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
