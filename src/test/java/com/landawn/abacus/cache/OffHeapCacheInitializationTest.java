/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.landawn.abacus.parser.ParserFactory;
import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ClassUtil;
import com.landawn.abacus.util.Throwables;

/** Startup and scheduler failures require fresh static state, isolated from the rest of the suite. */
class OffHeapCacheInitializationTest {

    // Optional dependencies

    @Test
    void missingKryoFallsBackToJsonForObjectRoundTrips() throws Exception {
        final Object jsonParser = ParserFactory.createJsonParser();
        try (MockedStatic<ParserFactory> parsers = mockStatic(ParserFactory.class, Mockito.CALLS_REAL_METHODS);
             IsolatedOffHeapClasses classes = new IsolatedOffHeapClasses(false)) {
            parsers.when(ParserFactory::isKryoParserAvailable).thenReturn(false);
            final Cache<String, Object> cache = classes.newCache();
            try {
                assertSame(jsonParser.getClass(), classes.field("PARSER").get(null).getClass());
                assertTrue(cache.put("key", "a value with \"quotes\""));
                assertEquals("a value with \"quotes\"", cache.getOrNull("key"));
            } finally {
                cache.close();
            }
        }
    }

    @Test
    void unavailableUnsafeReportsTheInitializationCause() throws Exception {
        try (IsolatedOffHeapClasses classes = new IsolatedOffHeapClasses(false)) {
            classes.base(); // Finish parser/executor initialization before denying Unsafe access.
            final SecurityException denied = new SecurityException("Unsafe access denied");
            try (MockedStatic<ClassUtil> access = mockStatic(ClassUtil.class, Mockito.CALLS_REAL_METHODS)) {
                access.when(() -> ClassUtil.setAccessible(any(Field.class), eq(true))).thenThrow(denied);
                final ExceptionInInitializerError failure = assertThrows(ExceptionInInitializerError.class, () -> classes.type("OffHeapCache"));

                assertTrue(failure.getCause().getMessage().contains("Failed to initialize sun.misc.Unsafe"));
                assertSame(denied, failure.getCause().getCause());
            }
        }
    }

    // Scheduling and lifecycle

    @Test
    @SuppressWarnings("unchecked")
    void pendingVacationIsSingleFlightAndSafeAfterClose() throws Exception {
        final List<Throwables.Runnable<? extends Exception>> tasks = new ArrayList<>();
        try (MockedConstruction<AsyncExecutor> executors = mockConstruction(AsyncExecutor.class, (executor, context) -> doAnswer(invocation -> {
            tasks.add(invocation.getArgument(0));
            return null;
        }).when(executor).execute(any(Throwables.Runnable.class)));
             IsolatedOffHeapClasses classes = new IsolatedOffHeapClasses(true)) {
            final Cache<String, Object> cache = classes.newCache();
            try {
                assertTrue(cache.put("occupant", new byte[1024 * 1024]));
                assertFalse(cache.put("first", new byte[8192]));
                assertFalse(cache.put("second", new byte[8192]));
                assertEquals(1, tasks.size(), "a pending pass suppresses additional submissions");
                assertTrue(classes.vacating(cache).get());

                cache.close();
                tasks.get(0).run();
                assertTrue(cache.isClosed());
                assertFalse(classes.vacating(cache).get());
                assertThrows(IllegalStateException.class, () -> cache.put("after-close", new byte[1]));
            } finally {
                cache.close();
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void recentlyFinishedVacationDebouncesFurtherPressure() throws Exception {
        final List<Throwables.Runnable<? extends Exception>> tasks = new ArrayList<>();
        try (MockedConstruction<AsyncExecutor> executors = mockConstruction(AsyncExecutor.class, (executor, context) -> doAnswer(invocation -> {
            tasks.add(invocation.getArgument(0));
            return null;
        }).when(executor).execute(any(Throwables.Runnable.class)));
             IsolatedOffHeapClasses classes = new IsolatedOffHeapClasses(true)) {
            final Cache<String, Object> cache = classes.newCache();
            try {
                assertTrue(cache.put("occupant", new byte[1024 * 1024]));
                classes.field("lastVacateFinishedNanos").setLong(cache, System.nanoTime());
                assertFalse(cache.put("crowded-out", new byte[8192]));
                assertTrue(tasks.isEmpty());
                assertFalse(classes.vacating(cache).get());
                assertEquals(1, cache.size());
            } finally {
                cache.close();
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void rejectedVacationCanBeRetriedAfterTheExecutorRecovers() throws Exception {
        final List<Throwables.Runnable<? extends Exception>> tasks = new ArrayList<>();
        try (MockedConstruction<AsyncExecutor> executors = mockConstruction(AsyncExecutor.class);
             IsolatedOffHeapClasses classes = new IsolatedOffHeapClasses(true)) {
            final Cache<String, Object> cache = classes.newCache();
            try {
                final AsyncExecutor executor = executors.constructed().get(0);
                final RejectedExecutionException rejected = new RejectedExecutionException("executor overloaded");
                doThrow(rejected).when(executor).execute(any(Throwables.Runnable.class));
                assertTrue(cache.put("occupant", new byte[1024 * 1024]));

                assertSame(rejected, assertThrows(RejectedExecutionException.class, () -> cache.put("crowded-out", new byte[8192])));
                assertFalse(classes.vacating(cache).get());
                assertEquals(1, cache.size());

                doAnswer(invocation -> {
                    tasks.add(invocation.getArgument(0));
                    return null;
                }).when(executor).execute(any(Throwables.Runnable.class));
                assertFalse(cache.put("retry", new byte[8192]));
                assertEquals(1, tasks.size());
                tasks.get(0).run();
                assertFalse(classes.vacating(cache).get());
                assertEquals(0, cache.size());
                assertTrue(cache.put("after-recovery", new byte[] { 1 }));
                assertArrayEquals(new byte[] { 1 }, (byte[]) cache.getOrNull("after-recovery"));
            } finally {
                cache.close();
            }
        }
    }

    /**
     * Reloads only the off-heap implementation (and, when requested, its shared executor).
     * Public cache/store/statistics types remain shared with the parent. Each isolated scheduler
     * is shut down on close, and each test closes its cache to release native memory and its hook.
     */
    private static final class IsolatedOffHeapClasses implements AutoCloseable {
        private final boolean isolateExecutor;
        private final ClassLoader loader;

        IsolatedOffHeapClasses(final boolean isolateExecutor) {
            this.isolateExecutor = isolateExecutor;
            loader = new ClassLoader(OffHeapCacheInitializationTest.class.getClassLoader()) {
                @Override
                protected Class<?> loadClass(final String name, final boolean resolve) throws ClassNotFoundException {
                    final String prefix = "com.landawn.abacus.cache.";
                    final String simpleName = name.startsWith(prefix) ? name.substring(prefix.length()).split("\\$")[0] : "";
                    if (!simpleName.equals("OffHeapCache") && !simpleName.equals("AbstractOffHeapCache")
                            && !(isolateExecutor && simpleName.equals("AbstractCache"))) {
                        return super.loadClass(name, resolve);
                    }
                    synchronized (getClassLoadingLock(name)) {
                        Class<?> loaded = findLoadedClass(name);
                        if (loaded == null) {
                            try (InputStream input = getParent().getResourceAsStream(name.replace('.', '/') + ".class")) {
                                if (input == null) {
                                    throw new ClassNotFoundException(name);
                                }
                                final byte[] bytes = input.readAllBytes();
                                // Preserve the production code source so coverage agents instrument these copies too.
                                loaded = defineClass(name, bytes, 0, bytes.length, OffHeapCache.class.getProtectionDomain());
                            } catch (final IOException e) {
                                throw new ClassNotFoundException(name, e);
                            }
                        }
                        if (resolve) {
                            resolveClass(loaded);
                        }
                        return loaded;
                    }
                }
            };
            loader.setDefaultAssertionStatus(true);
        }

        Class<?> type(final String simpleName) throws ClassNotFoundException {
            return Class.forName("com.landawn.abacus.cache." + simpleName, true, loader);
        }

        Class<?> base() throws ClassNotFoundException {
            return type("AbstractOffHeapCache");
        }

        Field field(final String name) throws ReflectiveOperationException {
            final Field field = base().getDeclaredField(name);
            field.setAccessible(true);
            return field;
        }

        @SuppressWarnings("unchecked")
        Cache<String, Object> newCache() throws ReflectiveOperationException {
            final Constructor<?> constructor = type("OffHeapCache").getDeclaredConstructor(int.class, long.class);
            constructor.setAccessible(true);
            return (Cache<String, Object>) constructor.newInstance(1, 0L);
        }

        AtomicBoolean vacating(final Cache<?, ?> cache) throws ReflectiveOperationException {
            return (AtomicBoolean) field("vacating").get(cache);
        }

        @Override
        public void close() throws Exception {
            ((ExecutorService) field("SCHEDULED_EXECUTOR").get(null)).shutdownNow();
            if (isolateExecutor) {
                final Field executor = type("AbstractCache").getDeclaredField("asyncExecutor");
                executor.setAccessible(true);
                ((AsyncExecutor) executor.get(null)).shutdown();
            }
        }
    }
}
