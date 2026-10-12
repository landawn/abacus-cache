package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.NotSerializableException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.landawn.abacus.TestBase;
import com.landawn.abacus.parser.KryoParser;
import com.landawn.abacus.parser.ParserFactory;

import net.spy.memcached.CachedData;

/** Regression coverage for bounded serialization without changing the cached wire format. */
public class KryoTranscoderMemoryUnitTest extends TestBase {

    @Test
    public void missingKryoStillProducesActionableErrorsOnRepeatedConstruction() throws Exception {
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

    @Test
    public void oversizedPayloadStopsSerializationBeforeTheWholeValueIsWritten() {
        final AtomicInteger chunksWritten = new AtomicInteger();
        final KryoParser parser = chunkedParser(chunksWritten);
        final KryoTranscoder<Object> transcoder = new KryoTranscoder<>(64, parser);

        assertThrows(IllegalArgumentException.class, () -> transcoder.encode(new ChunkedPayload()));
        assertTrue(chunksWritten.get() < ChunkedPayload.CHUNKS,
                "The size limit must abort serialization instead of first allocating the complete oversized payload");
        assertEquals("usable after a rejected entry", transcoder.decode(transcoder.encode("usable after a rejected entry")));
    }

    @Test
    public void nestedOversizedPayloadStillReportsTheSizeLimitAsAnArgumentError() {
        final AtomicInteger chunksWritten = new AtomicInteger();
        final KryoParser parser = chunkedParser(chunksWritten);
        final KryoTranscoder<Object> transcoder = new KryoTranscoder<>(64, parser);

        assertThrows(IllegalArgumentException.class, () -> transcoder.encode(new PayloadHolder()));
        assertTrue(chunksWritten.get() < ChunkedPayload.CHUNKS,
                "Kryo's field-serializer exception wrapping must not disable early size enforcement");
    }

    @Test
    public void encodedBytesRemainCompatibleWithTheConfiguredParser() {
        final KryoParser parser = ParserFactory.createKryoParser();
        final KryoTranscoder<Object> transcoder = new KryoTranscoder<>(parser);
        final HashMap<String, Object> map = new HashMap<>();
        map.put("number", 42L);
        map.put("unicode", "cache \uD83D\uDE03 \u4E2D");
        final List<Object> values = Arrays.asList(null, "text", "\uD83D\uDE03\u4E2D", 42L, new byte[0],
                new byte[16_500], new int[] { Integer.MIN_VALUE, 0, Integer.MAX_VALUE }, map);

        for (final Object value : values) {
            final byte[] originalFormat = parser.encode(value);
            final CachedData encoded = transcoder.encode(value);
            assertEquals(0, encoded.getFlags());
            assertArrayEquals(originalFormat, encoded.getData());
            assertArrayEquals(originalFormat, parser.encode(transcoder.decode(encoded)));
        }
    }

    @Test
    public void customSerializerBytesAndExactSizeBoundaryArePreserved() {
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
    public void unrelatedSerializerFailureKeepsItsIdentity() {
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

    @Test
    public void serializerCannotTurnARejectedWriteIntoASuccessfulTruncatedEntry() {
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

    @Test
    public void sizeRejectionReportsTheSizeReachedAndTheLimit() {
        final KryoTranscoder<Object> transcoder = new KryoTranscoder<>(64, chunkedParser(new AtomicInteger()));

        // The holder's field serializer wraps the rejection in a KryoException before encode unwraps it.
        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> transcoder.encode(new PayloadHolder()));
        final Matcher matcher = Pattern.compile("Encoded data size \\(at least (\\d+) bytes\\) exceeds maxSize \\(64 bytes\\)").matcher(e.getMessage());
        assertTrue(matcher.matches(), e.getMessage());
        assertTrue(Long.parseLong(matcher.group(1)) > 64, e.getMessage());
        assertEquals(0, e.getSuppressed().length, "a wrapped rejection must not be suppressed into itself");
    }

    @Test
    public void failureAfterASwallowedRejectionIsKeptAsSuppressed() {
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

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new KryoTranscoder<>(64, parser).encode(new ChunkedPayload()));
        assertArrayEquals(new Throwable[] { later }, e.getSuppressed());
    }

    @Test
    public void serializerIoFailureIsStillReportedAsKryoException() {
        final KryoParser parser = ParserFactory.createKryoParser();
        parser.register(JavaSerializedHolder.class, new JavaSerializer(), 50_002);

        final KryoException e = assertThrows(KryoException.class, () -> new KryoTranscoder<>(parser).encode(new JavaSerializedHolder()));
        assertInstanceOf(NotSerializableException.class, e.getCause());
        assertTrue(e.getMessage().contains(JavaSerializedHolder.class.getName()), e.getMessage());
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
