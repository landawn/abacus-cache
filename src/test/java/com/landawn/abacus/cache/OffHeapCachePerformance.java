package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import com.landawn.abacus.util.Profiler;

/** Opt-in workloads. Configure cache.performance.iterations, threads, and capacityMb to scale them. */
class OffHeapCachePerformance {

    private static final int ITERATIONS = Integer.getInteger("cache.performance.iterations", 1_000);
    private static final int THREADS = Integer.getInteger("cache.performance.threads", 8);
    private static final int CAPACITY_MB = Integer.getInteger("cache.performance.capacityMb", 64);

    // Writes

    @ParameterizedTest
    @EnumSource(OffHeapBackend.class)
    void putWithVariedPayloadsRoundTripsAndRemoves(final OffHeapBackend backend) {
        final AbstractOffHeapCache<String, Account> cache = backend.create(CAPACITY_MB, 0);
        try {
            for (int i = 0; i < ITERATIONS; i++) {
                final Account value = account(i, i % 1_000);
                final String key = value.getEmailAddress();
                assertTrue(cache.put(key, value));
                assertEquals(value, cache.getOrNull(key));
                if (i % 3 == 0) {
                    cache.remove(key);
                    assertNull(cache.getOrNull(key));
                }
            }
            cache.clear();
            assertEquals(0, cache.size());
            assertEquals(0, cache.stats().dataSize());
        } finally {
            cache.close();
        }
    }

    // Concurrency

    @ParameterizedTest(name = "{0}, payload repetitions={1}")
    @MethodSource("workloads")
    void concurrentPutGetRemovePreservesValues(final OffHeapBackend backend, final int repetitions) {
        final AbstractOffHeapCache<String, Account> cache = backend.create(CAPACITY_MB, 0);
        final AtomicLong sequence = new AtomicLong();
        try {
            final var result = Profiler.run(THREADS, ITERATIONS, 1, () -> {
                final long id = sequence.incrementAndGet();
                final Account value = account(id, repetitions);
                final String key = value.getEmailAddress();
                if (cache.put(key, value)) {
                    assertEquals(value, cache.getOrNull(key));
                }
                if (id % 3 == 0) {
                    cache.remove(key);
                    assertNull(cache.getOrNull(key));
                }
            });
            result.printResult();
            assertTrue(result.getAllFailedMethodStatisticsList().isEmpty());
        } finally {
            cache.close();
        }
    }

    @ParameterizedTest
    @EnumSource(OffHeapBackend.class)
    void concurrentReadsPreserveLargeObject(final OffHeapBackend backend) {
        final AbstractOffHeapCache<String, Account> cache = backend.create(CAPACITY_MB, 0);
        final Account value = account(1, 100);
        try {
            assertTrue(cache.put("account", value));
            final var result = Profiler.run(THREADS, ITERATIONS, 1, () -> assertEquals(value, cache.getOrNull("account")));
            result.printResult();
            assertTrue(result.getAllFailedMethodStatisticsList().isEmpty());
        } finally {
            cache.close();
        }
    }

    // Other Behavior

    @Test
    void ehcacheComparisonRoundTripsLargeObjects() {
        try (final org.ehcache.CacheManager manager = org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder()
                .withCache("comparison",
                        org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, Account.class,
                                org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder()
                                        .heap(1_000)
                                        .offheap(CAPACITY_MB, org.ehcache.config.units.MemoryUnit.MB)))
                .withSerializer(Account.class, KryoSerializer.class)
                .build(true)) {
            final org.ehcache.Cache<String, Account> cache = manager.getCache("comparison", String.class, Account.class);
            final AtomicLong sequence = new AtomicLong();
            final var result = Profiler.run(THREADS, ITERATIONS, 1, () -> {
                final long id = sequence.incrementAndGet();
                final Account value = account(id, 100);
                final String key = value.getEmailAddress();
                cache.put(key, value);
                assertEquals(value, cache.get(key));
                if (id % 3 == 0) {
                    cache.remove(key);
                    assertNull(cache.get(key));
                }
            });
            result.printResult();
            assertTrue(result.getAllFailedMethodStatisticsList().isEmpty());
        }
    }

    private static Stream<Arguments> workloads() {
        return Stream.of(Arguments.of(OffHeapBackend.UNSAFE, 1), Arguments.of(OffHeapBackend.FOREIGN_MEMORY, 1), Arguments.of(OffHeapBackend.UNSAFE, 100),
                Arguments.of(OffHeapBackend.FOREIGN_MEMORY, 100));
    }

    private static Account account(final long id, final int repetitions) {
        final Account account = new Account();
        account.setId(id);
        account.setGui("account-" + id);
        account.setEmailAddress("account-" + id + "@example.test");
        account.setFirstName("abcdefgh".repeat(repetitions));
        return account;
    }

    public static final class KryoSerializer implements org.ehcache.spi.serialization.Serializer<Account> {
        private final com.landawn.abacus.parser.KryoParser parser = com.landawn.abacus.parser.ParserFactory.createKryoParser();

        public KryoSerializer(final ClassLoader classLoader) {
        }

        @Override
        public java.nio.ByteBuffer serialize(final Account object) {
            return java.nio.ByteBuffer.wrap(parser.encode(object));
        }

        @Override
        public Account read(final java.nio.ByteBuffer binary) {
            final java.nio.ByteBuffer copy = binary.duplicate();
            final byte[] bytes = new byte[copy.remaining()];
            copy.get(bytes);
            return parser.decode(bytes);
        }

        @Override
        public boolean equals(final Account object, final java.nio.ByteBuffer binary) {
            return object.equals(read(binary));
        }
    }
}
