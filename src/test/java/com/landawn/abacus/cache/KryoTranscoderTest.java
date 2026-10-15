/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.NotSerializableException;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.landawn.abacus.exception.UncheckedIOException;
import com.landawn.abacus.parser.KryoParser;
import com.landawn.abacus.parser.KryoSerConfig;
import com.landawn.abacus.parser.ParserFactory;

import net.spy.memcached.CachedData;

class KryoTranscoderTest {

    @Nested
    class Construction {

        @Test
        void missingKryoIsAnActionableConfigurationFailure() {
            try (MockedStatic<ParserFactory> parsers = Mockito.mockStatic(ParserFactory.class)) {
                parsers.when(ParserFactory::isKryoParserAvailable).thenReturn(false);
                final IllegalStateException failure = assertThrows(IllegalStateException.class, KryoTranscoder::new);
                assertTrue(failure.getMessage().contains("Kryo is required"));
                assertThrows(IllegalArgumentException.class, () -> new KryoTranscoder<>(0));
            }
        }

        /** The caller-supplied-parser constructors: round-trip through a custom parser, and null rejection. */
        @Test
        void constructorCustomParser() {
            final com.landawn.abacus.parser.KryoParser parser = com.landawn.abacus.parser.ParserFactory.createKryoParser();

            final KryoTranscoder<String> defaultSize = new KryoTranscoder<>(parser);
            assertEquals(CachedData.MAX_SIZE, defaultSize.getMaxSize());
            assertEquals("round-trip", defaultSize.decode(defaultSize.encode("round-trip")));

            final KryoTranscoder<String> customSize = new KryoTranscoder<>(1024, parser);
            assertEquals(1024, customSize.getMaxSize());
            assertEquals("round-trip", customSize.decode(customSize.encode("round-trip")));

            assertThrows(IllegalArgumentException.class, () -> new KryoTranscoder<String>((com.landawn.abacus.parser.KryoParser) null));
            assertThrows(IllegalArgumentException.class, () -> new KryoTranscoder<String>(1024, null));
        }

        @Test
        void constructorDefaultUsesDefaultMaxSize() {
            final KryoTranscoder<String> tx = new KryoTranscoder<>();
            assertEquals(CachedData.MAX_SIZE, tx.getMaxSize());
        }

        @Test
        void constructorNonPositiveMaxSize() {
            assertThrows(IllegalArgumentException.class, () -> new KryoTranscoder<String>(0));
            assertThrows(IllegalArgumentException.class, () -> new KryoTranscoder<String>(-1));
        }

        @Test
        void constructorWithMaxSize() {
            final KryoTranscoder<String> tx = new KryoTranscoder<>(2048);
            assertEquals(2048, tx.getMaxSize());
        }

        // Validation And Failures

        @Test
        void missingKryoStillProducesActionableErrorsOnRepeatedConstruction() throws Exception {
            final ClassLoader loader = new KryoHidingClassLoader(getClass().getClassLoader());
            final Class<?> type = Class.forName(KryoTranscoder.class.getName(), false, loader);
            final Constructor<?> defaultConstructor = type.getConstructor();
            final Constructor<?> sizedConstructor = type.getConstructor(int.class);

            for (int attempt = 0; attempt < 2; attempt++) {
                final InvocationTargetException defaultFailure = assertThrows(InvocationTargetException.class, defaultConstructor::newInstance);
                assertEquals(IllegalStateException.class, defaultFailure.getCause().getClass());
                assertTrue(defaultFailure.getCause().getMessage().contains("com.esotericsoftware:kryo"));
                final InvocationTargetException sizedFailure = assertThrows(InvocationTargetException.class, () -> sizedConstructor.newInstance(64));
                assertEquals(IllegalStateException.class, sizedFailure.getCause().getClass());
                final InvocationTargetException invalidSize = assertThrows(InvocationTargetException.class, () -> sizedConstructor.newInstance(0));
                assertEquals(IllegalArgumentException.class, invalidSize.getCause().getClass());
            }
        }
    }

    @Nested
    class Encoding {

        @Test
        void encodedBytesRemainCompatibleWithTheConfiguredParser() {
            final KryoParser parser = ParserFactory.createKryoParser();
            final KryoTranscoder<Object> transcoder = new KryoTranscoder<>(parser);
            final HashMap<String, Object> map = new HashMap<>();
            map.put("number", 42L);
            map.put("unicode", "cache \uD83D\uDE03 \u4E2D");
            final List<Object> values = Arrays.asList(null, "text", "\uD83D\uDE03\u4E2D", 42L, new byte[0], new byte[16_500],
                    new int[] { Integer.MIN_VALUE, 0, Integer.MAX_VALUE }, map);

            for (final Object value : values) {
                final byte[] originalFormat = parser.encode(value);
                final CachedData encoded = transcoder.encode(value);
                assertEquals(0, encoded.getFlags());
                assertArrayEquals(originalFormat, encoded.getData());
                assertArrayEquals(originalFormat, parser.encode(transcoder.decode(encoded)));
            }
        }

        /**
         * Round-trip regression for encoding {@code null}.
         *
         * <p>{@code encode(null)} must produce a non-null {@link CachedData} backed by a non-empty
         * byte array (Kryo writes a class/null marker, so the payload is never empty), and decoding
         * that payload must yield {@code null}. This pins the encode/decode symmetry for the null
         * value and confirms the produced payload is distinct from the empty-data short-circuit in
         * {@link KryoTranscoder#decode(CachedData)}.
         */
        @Test
        void encodeNullValueProducesNonEmptyDecodableToNull() {
            final KryoTranscoder<String> tx = new KryoTranscoder<>();
            final CachedData cd = tx.encode(null);
            assertNotNull(cd);
            assertNotNull(cd.getData());
            assertTrue(cd.getData().length > 0);
            assertNull(tx.decode(cd));
        }

        @Test
        void encodeRoundTrip() {
            final KryoTranscoder<String> tx = new KryoTranscoder<>();
            final CachedData cd = tx.encode("hello");
            assertNotNull(cd);
            assertEquals(0, cd.getFlags());
            final String decoded = tx.decode(cd);
            assertEquals("hello", decoded);
        }

        @Test
        void encodeRoundTripComplexObject() {
            final KryoTranscoder<Object> tx = new KryoTranscoder<>();
            final java.util.HashMap<String, Integer> map = new java.util.HashMap<>();
            map.put("a", 1);
            map.put("b", 2);
            final CachedData cd = tx.encode(map);
            assertNotNull(cd);
            final Object decoded = tx.decode(cd);
            assertTrue(decoded instanceof java.util.Map);
            @SuppressWarnings("unchecked")
            final java.util.Map<String, Integer> roundTrip = (java.util.Map<String, Integer>) decoded;
            assertEquals(Integer.valueOf(1), roundTrip.get("a"));
            assertEquals(Integer.valueOf(2), roundTrip.get("b"));
        }
    }

    @Nested
    class Decoding {

        @Test
        void missingPayloadIsTreatedAsAnEmptyEntryWithoutDecoding() {
            final KryoParser parser = Mockito.mock(KryoParser.class);
            final CachedData data = Mockito.mock(CachedData.class);
            Mockito.when(data.getData()).thenReturn(null);
            assertNull(new KryoTranscoder<>(parser).decode(data));
            Mockito.verifyNoInteractions(parser);
        }

        @Test
        void asyncDecodeAlwaysFalse() {
            final KryoTranscoder<String> tx = new KryoTranscoder<>();
            assertFalse(tx.asyncDecode(null));
            assertFalse(tx.asyncDecode(new CachedData(0, new byte[0], 100)));
        }

        @Test
        void decodeEmptyData() {
            final KryoTranscoder<String> tx = new KryoTranscoder<>();
            // CachedData with zero-length byte array decodes to null.
            final CachedData cd = new CachedData(0, new byte[0], 100);
            assertNull(tx.decode(cd));
        }

        /** Sweep of counter-shaped payloads: every one must fail as a KryoException, never as an Error or other type. */
        @Test
        void decodeForeignCounterPayloadSweepAlwaysKryoException() {
            final KryoTranscoder<Object> tx = new KryoTranscoder<>();

            for (int i = 0; i < 10_000; i++) {
                final String counter = Integer.toString(i);
                final CachedData cd = new CachedData(0, counter.getBytes(java.nio.charset.StandardCharsets.US_ASCII), CachedData.MAX_SIZE);
                assertThrows(com.esotericsoftware.kryo.KryoException.class, () -> tx.decode(cd), counter);
            }

            final CachedData maxCounter = new CachedData(0, "18446744073709551615".getBytes(java.nio.charset.StandardCharsets.US_ASCII), CachedData.MAX_SIZE);
            assertThrows(com.esotericsoftware.kryo.KryoException.class, () -> tx.decode(maxCounter));
        }

        /**
         * Regression test: decoding a memcached counter's ASCII digits (a payload not written by this
         * transcoder) must fail with the documented {@link com.esotericsoftware.kryo.KryoException}.
         * Values starting with '6' or '9' previously leaked an {@link InstantiationError} from Kryo's
         * generated constructor accessor (the digit resolves to the abstract InputStream/OutputStream).
         */
        @Test
        void decodeForeignCounterPayloadThrowsKryoExceptionNotError() {
            final KryoTranscoder<Object> tx = new KryoTranscoder<>();

            for (final String counter : new String[] { "6", "9", "64", "99", "1", "42" }) {
                final CachedData cd = new CachedData(0, counter.getBytes(java.nio.charset.StandardCharsets.US_ASCII), CachedData.MAX_SIZE);
                assertThrows(com.esotericsoftware.kryo.KryoException.class, () -> tx.decode(cd), counter);
            }
        }

        @Test
        void decodeNullCachedData() {
            final KryoTranscoder<String> tx = new KryoTranscoder<>();
            assertNull(tx.decode(null));
        }
    }

    @Nested
    class SizeLimits {

        /**
         * Boundary regression for the {@code encoded.length > maxSize} size check.
         *
         * <p>The size limit must be enforced with a strictly-greater comparison: a payload whose
         * serialized length is exactly {@code maxSize} must be accepted, while a payload one byte
         * larger than the limit must be rejected. This guards against an off-by-one regression
         * (e.g. switching the comparison to {@code >=}) that would wrongly reject exact-fit payloads,
         * and keeps the wrapper's check consistent with {@link CachedData}'s own
         * {@code data.length > maxSize} validation.
         */
        @Test
        void encodeBoundaryExactMaxSizeAccepted() {
            final String value = "boundary-check-payload";

            // Measure the exact serialized length using a generous transcoder.
            final int exactLength = new KryoTranscoder<String>().encode(value).getData().length;

            // maxSize == serialized length: must be accepted (length is not > maxSize).
            final KryoTranscoder<String> exactFit = new KryoTranscoder<>(exactLength);
            final CachedData cd = exactFit.encode(value);
            assertNotNull(cd);
            assertEquals(exactLength, cd.getData().length);
            assertEquals(value, exactFit.decode(cd));

            // maxSize == serialized length - 1: must be rejected (length is > maxSize).
            final KryoTranscoder<String> tooSmall = new KryoTranscoder<>(exactLength - 1);
            assertThrows(IllegalArgumentException.class, () -> tooSmall.encode(value));
        }

        @Test
        void encodeExceedsMaxSize() {
            // Tiny limit so any non-trivial object exceeds it.
            final KryoTranscoder<String> tx = new KryoTranscoder<>(8);
            final String big = "this string is definitely larger than 8 bytes after kryo encoding";
            assertThrows(IllegalArgumentException.class, () -> tx.encode(big));
        }

        // Membership And Views

        @Test
        void sizeRejectionReportsTheSizeReachedAndTheLimit() {
            final KryoTranscoder<Object> transcoder = new KryoTranscoder<>(64, chunkedParser(new AtomicInteger()));

            // The holder's field serializer wraps the rejection in a KryoException before encode unwraps it.
            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> transcoder.encode(new PayloadHolder()));
            final Matcher matcher = Pattern.compile("Encoded data size \\(at least (\\d+) bytes\\) exceeds maxSize \\(64 bytes\\)").matcher(e.getMessage());
            assertTrue(matcher.matches(), e.getMessage());
            assertTrue(Long.parseLong(matcher.group(1)) > 64, e.getMessage());
            assertEquals(0, e.getSuppressed().length, "a wrapped rejection must not be suppressed into itself");
        }

        // Validation And Failures

        @Test
        void failureAfterASwallowedRejectionIsKeptAsSuppressed() {
            final KryoParser parser = ParserFactory.createKryoParser();
            final IllegalStateException later = new IllegalStateException("later serializer failure");
            parser.register(ChunkedPayload.class, new Serializer<ChunkedPayload>() {
                @Override
                public void write(final Kryo kryo, final Output output, final ChunkedPayload object) {
                    output.writeBytes(new byte[128]);
                    try {
                        output.flush();
                    } catch (final RuntimeException ignored) {
                        // Swallow the rejection, then fail in an unrelated way.
                    }
                    throw later;
                }

                @Override
                public ChunkedPayload read(final Kryo kryo, final Input input, final Class<? extends ChunkedPayload> type) {
                    throw new UnsupportedOperationException();
                }
            }, 50_001);

            final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> new KryoTranscoder<>(64, parser).encode(new ChunkedPayload()));
            assertArrayEquals(new Throwable[] { later }, e.getSuppressed());
        }

        @Test
        void nestedOversizedPayloadStillReportsTheSizeLimitAsAnArgumentError() {
            final AtomicInteger chunksWritten = new AtomicInteger();
            final KryoParser parser = chunkedParser(chunksWritten);
            final KryoTranscoder<Object> transcoder = new KryoTranscoder<>(64, parser);

            assertThrows(IllegalArgumentException.class, () -> transcoder.encode(new PayloadHolder()));
            assertTrue(chunksWritten.get() < ChunkedPayload.CHUNKS, "Kryo's field-serializer exception wrapping must not disable early size enforcement");
        }

        @Test
        void serializerCannotTurnARejectedWriteIntoASuccessfulTruncatedEntry() {
            final KryoParser parser = ParserFactory.createKryoParser();
            parser.register(ChunkedPayload.class, new Serializer<ChunkedPayload>() {
                @Override
                public void write(final Kryo kryo, final Output output, final ChunkedPayload object) {
                    output.writeBytes(new byte[128]);
                    try {
                        output.flush();
                    } catch (final RuntimeException ignored) {
                        // A custom serializer may catch an output failure; the transcoder must still reject the entry.
                        output.setPosition(0);
                    }
                }

                @Override
                public ChunkedPayload read(final Kryo kryo, final Input input, final Class<? extends ChunkedPayload> type) {
                    throw new UnsupportedOperationException();
                }
            }, 50_001);

            assertThrows(IllegalArgumentException.class, () -> new KryoTranscoder<>(64, parser).encode(new ChunkedPayload()));
        }

        // Other Behavior

        @Test
        void customSerializerBytesAndExactSizeBoundaryArePreserved() {
            final KryoParser parser = ParserFactory.createKryoParser();
            parser.register(ChunkedPayload.class, new Serializer<ChunkedPayload>() {
                @Override
                public void write(final Kryo kryo, final Output output, final ChunkedPayload object) {
                    output.writeBytes(new byte[16_500]);
                }

                @Override
                public ChunkedPayload read(final Kryo kryo, final Input input, final Class<? extends ChunkedPayload> type) {
                    input.readBytes(16_500);
                    return new ChunkedPayload();
                }
            }, 50_001);
            final ChunkedPayload value = new ChunkedPayload();
            final byte[] originalFormat = parser.encode(value);
            final KryoTranscoder<ChunkedPayload> exactFit = new KryoTranscoder<>(originalFormat.length, parser);

            assertArrayEquals(originalFormat, exactFit.encode(value).getData());
            assertArrayEquals(originalFormat, parser.encode(exactFit.decode(exactFit.encode(value))));
            assertThrows(IllegalArgumentException.class, () -> new KryoTranscoder<>(originalFormat.length - 1, parser).encode(value));
        }

        @Test
        void oversizedPayloadStopsSerializationBeforeTheWholeValueIsWritten() {
            final AtomicInteger chunksWritten = new AtomicInteger();
            final KryoParser parser = chunkedParser(chunksWritten);
            final KryoTranscoder<Object> transcoder = new KryoTranscoder<>(64, parser);

            assertThrows(IllegalArgumentException.class, () -> transcoder.encode(new ChunkedPayload()));
            assertTrue(chunksWritten.get() < ChunkedPayload.CHUNKS,
                    "The size limit must abort serialization instead of first allocating the complete oversized payload");
            assertEquals("usable after a rejected entry", transcoder.decode(transcoder.encode("usable after a rejected entry")));
        }
    }

    @Nested
    class FailureHandling {

        @Test
        void singleByteSerializerWritesRespectTheExactSizeLimit() throws Exception {
            final KryoParser parser = Mockito.mock(KryoParser.class);
            Mockito.doAnswer(invocation -> {
                final OutputStream output = invocation.getArgument(2);
                output.write(1);
                output.write(2);
                output.write(3);
                return null;
            }).when(parser).serialize(Mockito.any(), Mockito.any(KryoSerConfig.class), Mockito.any(OutputStream.class));
            assertArrayEquals(new byte[] { 1, 2, 3 }, new KryoTranscoder<>(3, parser).encode("value").getData());
            assertThrows(IllegalArgumentException.class, () -> new KryoTranscoder<>(2, parser).encode("value"));
        }

        @Test
        void nullValueIoFailureRetainsItsCauseAndDescriptiveType() throws Exception {
            final KryoParser parser = Mockito.mock(KryoParser.class);
            final IOException failure = new IOException("serializer I/O failed");
            Mockito.doThrow(new UncheckedIOException(failure))
                    .when(parser)
                    .serialize(Mockito.isNull(), Mockito.any(KryoSerConfig.class), Mockito.any(OutputStream.class));
            final KryoException error = assertThrows(KryoException.class, () -> new KryoTranscoder<>(parser).encode(null));
            assertSame(failure, error.getCause());
            assertTrue(error.getMessage().contains("type null"));
        }

        /** Object-array cycles use Kryo's array serializer, which does not wrap the stack overflow. */
        @Test
        void encodeSelfReferentialObjectArrayPropagatesStackOverflowError() {
            final KryoTranscoder<Object> tx = new KryoTranscoder<>(com.landawn.abacus.parser.ParserFactory.createKryoParser());
            final Object[] cycle = new Object[1];
            cycle[0] = cycle;

            assertThrows(StackOverflowError.class, () -> tx.encode(cycle));
        }

        // Validation And Failures

        @Test
        void serializerIoFailureIsStillReportedAsKryoException() {
            final KryoParser parser = ParserFactory.createKryoParser();
            parser.register(JavaSerializedHolder.class, new JavaSerializer(), 50_002);

            final KryoException e = assertThrows(KryoException.class, () -> new KryoTranscoder<>(parser).encode(new JavaSerializedHolder()));
            assertInstanceOf(NotSerializableException.class, e.getCause());
            assertTrue(e.getMessage().contains(JavaSerializedHolder.class.getName()), e.getMessage());
        }

        @Test
        void unrelatedSerializerFailureKeepsItsIdentity() {
            final KryoParser parser = ParserFactory.createKryoParser();
            final IllegalStateException failure = new IllegalStateException("serializer failure");
            parser.register(ChunkedPayload.class, new Serializer<ChunkedPayload>() {
                @Override
                public void write(final Kryo kryo, final Output output, final ChunkedPayload object) {
                    throw failure;
                }

                @Override
                public ChunkedPayload read(final Kryo kryo, final Input input, final Class<? extends ChunkedPayload> type) {
                    throw new UnsupportedOperationException();
                }
            }, 50_001);

            assertSame(failure, assertThrows(IllegalStateException.class, () -> new KryoTranscoder<>(parser).encode(new ChunkedPayload())));
        }
    }

    private static KryoParser chunkedParser(final AtomicInteger chunksWritten) {
        final KryoParser parser = ParserFactory.createKryoParser();
        parser.register(ChunkedPayload.class, new Serializer<ChunkedPayload>() {
            @Override
            public void write(final Kryo kryo, final Output output, final ChunkedPayload object) {
                final byte[] chunk = new byte[4096];
                for (int i = 0; i < ChunkedPayload.CHUNKS; i++) {
                    output.writeBytes(chunk);
                    chunksWritten.incrementAndGet();
                }
            }

            @Override
            public ChunkedPayload read(final Kryo kryo, final Input input, final Class<? extends ChunkedPayload> type) {
                throw new UnsupportedOperationException();
            }
        }, 50_001);
        return parser;
    }

    /** Reload the Abacus classes so ParserFactory's once-per-loader optional dependency probe runs again. */
    private static final class KryoHidingClassLoader extends ClassLoader {
        KryoHidingClassLoader(final ClassLoader parent) {
            super(parent);
        }

        @Override
        protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
            if (name.startsWith("com.esotericsoftware.")) {
                throw new ClassNotFoundException(name);
            }
            if (!name.startsWith("com.landawn.abacus.")) {
                return super.loadClass(name, resolve);
            }
            synchronized (getClassLoadingLock(name)) {
                Class<?> type = findLoadedClass(name);
                if (type == null) {
                    try (InputStream input = getParent().getResourceAsStream(name.replace('.', '/') + ".class")) {
                        if (input == null) {
                            throw new ClassNotFoundException(name);
                        }
                        final byte[] bytes = input.readAllBytes();
                        type = defineClass(name, bytes, 0, bytes.length);
                    } catch (final IOException e) {
                        throw new ClassNotFoundException(name, e);
                    }
                }
                if (resolve) {
                    resolveClass(type);
                }
                return type;
            }
        }
    }

    public static final class PayloadHolder {
        public ChunkedPayload payload = new ChunkedPayload();
    }

    public static final class ChunkedPayload {
        private static final int CHUNKS = 128;
    }

    /** Serializable itself, but Java serialization fails on its non-serializable field. */
    public static final class JavaSerializedHolder implements Serializable {
        private static final long serialVersionUID = 1L;
        @SuppressWarnings("serial")
        public Object field = new Object();
    }
}
