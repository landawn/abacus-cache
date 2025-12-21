/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.spi.serialization.SerializerException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import com.landawn.abacus.cache.OffHeapCache25;
import com.landawn.abacus.cache.OffHeapStore;
import com.landawn.abacus.parser.Parser;
import com.landawn.abacus.parser.ParserFactory;
import com.landawn.abacus.type.ByteBufferType;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.type.TypeFactory;

//--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED

@Tag("2025")
public class OffHeapCache25Test {
    private static final Random rand = new Random();
    static final Parser<?, ?> parser = ParserFactory.isKryoAvailable() ? ParserFactory.createKryoParser() : ParserFactory.createJSONParser();
    static final ByteBufferType bbType = (ByteBufferType) ((Type<?>) TypeFactory.getType(ByteBufferType.BYTE_BUFFER));
    static final OffHeapStore<String> offHeapStore;
    static {
        RocksDB.loadLibrary();

        RocksDB db = null;
        try {
            final Options options = new Options().setCreateIfMissing(true);
            db = RocksDB.open(options, "rocksdb-data");
        } catch (final RocksDBException e) {
            throw new RuntimeException(e);
        }

        final RocksDB finalDB = db;
        offHeapStore = new OffHeapStore<>() {
            @Override
            public boolean put(final String key, final byte[] value) {
                try {
                    finalDB.put(key.getBytes(), value);
                    return true;
                } catch (final RocksDBException e) {
                    return false;
                }
            }

            @Override
            public byte[] get(final String key) {
                try {
                    return finalDB.get(key.getBytes());
                } catch (final RocksDBException e) {
                    return null;
                }
            }

            @Override
            public boolean remove(final String key) {
                try {
                    finalDB.delete(key.getBytes());
                    return true;
                } catch (final RocksDBException e) {
                    return false;
                }
            }
        };
    }

    static final OffHeapCache25<String, Account> cache = OffHeapCache25.<String, Account> builder()
            .capacityInMB(4096)
            .maxBlockSizeInBytes(16001)
            .evictDelay(3000)
            .defaultLiveTime(1000_000)
            .defaultMaxIdleTime(1000_000)
            .build();   // new OffHeapCache25<>(4096, 3000, 1000_000, 1000_000);

    static final OffHeapCache25<String, Account> persistentCache = OffHeapCache25.<String, Account> builder()
            .capacityInMB(100)
            .maxBlockSizeInBytes(16001)
            .evictDelay(3000)
            .defaultLiveTime(1000_000)
            .defaultMaxIdleTime(1000_000)
            .offHeapStore(offHeapStore)
            .statsTimeOnDisk(true)
            .testerForLoadingItemFromDiskToMemory((activityPrint, _, _) -> activityPrint.getAccessCount() >= 100)
            .build();   // new OffHeapCache25<>(4096, 3000, 1000_000, 1000_000);
    // private static final OffHeapCache<String, String> ohcache = new OffHeapCache<>(1204, 3000, 1000_000, 1000_000);

    private static final long start = System.currentTimeMillis();
    private static final AtomicInteger counter = new AtomicInteger();

    @Test
    public void test_ByteBuffer() {
        ByteBuffer bb = ByteBuffer.allocate(1024);
        bb.put("abc".getBytes());
        bb.put("123".getBytes());
        N.println(bb.position());
        byte[] bytes = ByteBufferType.byteArrayOf(bb);
        N.println(bb.position());

        assertEquals("abc123", new String(bytes));

        bb = ByteBuffer.wrap("abc123".getBytes(), 6, 0);
        bytes = ByteBufferType.byteArrayOf(bb);
        N.println(bb.position());

        assertEquals("abc123", new String(bytes));

        bb = ByteBuffer.wrap(N.EMPTY_BYTE_ARRAY);
        N.println(bbType.stringOf(bb));
        assertEquals("", bbType.stringOf(bb));
        N.println(bbType.valueOf(bbType.stringOf(bb)));

        bb = ByteBuffer.wrap(N.EMPTY_BYTE_ARRAY, 0, 0);
        N.println(bbType.stringOf(bb));
        assertEquals("", bbType.stringOf(bb));
        N.println(bbType.valueOf(bbType.stringOf(bb)));
    }

    @Test
    public void test_put_get_01() {
        final String key = "abc";
        final Account account = new Account();
        account.setFirstName("123");
        cache.put(key, account);
        assertEquals(account, cache.gett(key));
    }

    @Test
    public void test_put_get_02() {
        for (int i = 0; i < 1000; i++) {
            final Account account = createAccount(Account.class);
            final StringBuilder sb = Objectory.createStringBuilder();

            int tmp = Math.abs(rand.nextInt(1000));

            while (tmp-- > 0) {
                sb.append(account.getGui()).append('\\');
            }

            account.setFirstName(sb.toString());

            Objectory.recycle(sb);

            final String key = account.getEmailAddress();
            cache.put(key, account);
            final Account account2 = cache.get(key).orElse(null);

            assertEquals(account, account2);

            if (i % 3 == 0) {
                cache.remove(key);
                assertNull(cache.get(key).orElse(null));
            }

            if (counter.incrementAndGet() % 100 == 0) {
                N.println("=========" + counter.get() + ": " + (System.currentTimeMillis() - start));
            }
        }
    }

    @Test
    public void test_stats() {
        for (int i = 0; i < 1000; i++) {
            final Account account = createAccount(Account.class);
            final StringBuilder sb = Objectory.createStringBuilder();

            int tmp = Math.abs(rand.nextInt(1000));

            while (tmp-- > 0) {
                sb.append(account.getGui()).append('\\');
            }

            account.setFirstName(sb.toString());

            Objectory.recycle(sb);

            final String key = account.getEmailAddress();
            cache.put(key, account);
            final Account account2 = cache.get(key).orElse(null);

            assertEquals(account, account2);

            if (i % 3 == 0) {
                for (int j = 0; j < 100; j++) {
                    cache.put(Strings.uuid(), account);
                }
            }

            if (counter.incrementAndGet() % 100 == 0) {
                N.println(Strings.repeat("=", 80));
                N.println(cache.stats());
            }
        }

        cache.clear();
        N.println(cache.stats());

        N.sleep(4000);
        N.println(cache.stats());
    }

    @Test
    public void test_persistentCache() {
        for (int i = 0; i < 1000; i++) {
            final Account account = createAccount(Account.class);
            final StringBuilder sb = Objectory.createStringBuilder();

            int tmp = Math.abs(rand.nextInt(1000));

            while (tmp-- > 0) {
                sb.append(account.getGui()).append('\\');
            }

            account.setFirstName(sb.toString());

            Objectory.recycle(sb);

            final String key = account.getEmailAddress();
            persistentCache.put(key, account);
            final Account account2 = persistentCache.get(key).orElse(null);

            assertEquals(account, account2);

            if (i % 3 == 0) {
                persistentCache.remove(key);

                for (int j = 0; j < 100; j++) {
                    persistentCache.put(Strings.uuid(), account);
                }
            }

            if (counter.incrementAndGet() % 100 == 0) {
                N.println(Strings.repeat("=", 80));
                N.println(persistentCache.stats());
            }
        }

        final int size = persistentCache.size();
        final long start = System.currentTimeMillis();
        persistentCache.clear();
        N.println("persistentCache.clear(" + size + "): " + (System.currentTimeMillis() - start));

        N.println(persistentCache.stats());

        N.sleep(4000);
        N.println(persistentCache.stats());
    }

    @Test
    public void test_loadingItemFromDiskToMemory() {
        for (int i = 0; i < 20000; i++) {
            final Account account = createAccount(Account.class);
            final StringBuilder sb = Objectory.createStringBuilder();

            int tmp = Math.abs(rand.nextInt(1000));

            while (tmp-- > 0) {
                sb.append(account.getGui()).append('\\');
            }

            account.setFirstName(sb.toString());

            Objectory.recycle(sb);

            final String key = account.getEmailAddress();
            persistentCache.put(key, account);
            final Account account2 = persistentCache.get(key).orElse(null);

            assertEquals(account, account2);
        }

        final List<String> keys = new ArrayList<>(persistentCache.keySet());

        final Account account = createAccount(Account.class);
        persistentCache.put(account.getEmailAddress(), account);

        for (int i = 0; i < 100; i++) {
            persistentCache.get(account.getEmailAddress());
        }

        keys.stream().skip(100).forEach(key -> {
            persistentCache.remove(key);
        });

        persistentCache.get(account.getEmailAddress());

        persistentCache.clear();
    }

    private Account createAccount(final Class<Account> cls) {
        return N.fill(cls);
    }

    @Test
    public void test_perf() {
        Profiler.run(8, 10000, 1, () -> {
            final Account account = createAccount(Account.class);

            final String key = account.getEmailAddress();
            cache.put(key, account);
            final Account account2 = cache.get(key).orElse(null);

            assertEquals(account.getId(), account2.getId());

            if (Math.abs(rand.nextInt()) % 3 == 0) {
                cache.remove(key);
                assertNull(cache.get(key).orElse(null));
            }
        }).printResult();
    }

    @Test
    public void test_perf_big_Object() {

        final String longFirstName = Strings.repeat(Strings.uuid(), 100);

        Profiler.run(16, 90000, 1, "OffHeapCache25", () -> {
            final Account account = createAccount(Account.class);
            account.setFirstName(longFirstName);

            final String key = account.getEmailAddress();
            if (cache.put(key, account)) {
                final Account account2 = cache.get(key).orElse(null);
                assertEquals(account.getId(), account2.getId());
            }

            if (Math.abs(rand.nextInt()) % 3 == 0) {
                cache.remove(key);
                assertNull(cache.get(key).orElse(null));
            }
        }).printResult();

    }

    @Test
    public void test_perf_vs_ehcache() {

        final CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
                .withCache("myCache",
                        CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, Account.class,
                                ResourcePoolsBuilder.newResourcePoolsBuilder()
                                        .heap(1000000, EntryUnit.ENTRIES) // Heap storage
                                        .offheap(4096, MemoryUnit.MB)))
                .withSerializer(Account.class, KryoSerializer.class) // Off-heap storage
                .build(true);

        final Cache<String, Account> ehCache = cacheManager.getCache("myCache", String.class, Account.class);

        final String longFirstName = Strings.repeat(Strings.uuid(), 100);

        Profiler.run(16, 90000, 1, "ehcache", () -> {
            final Account account = createAccount(Account.class);
            account.setFirstName(longFirstName);

            final String key = account.getEmailAddress();
            ehCache.put(key, account);
            final Account account2 = ehCache.get(key);
            assertEquals(account.getGui(), account2.getGui());

            if (Math.abs(rand.nextInt()) % 3 == 0) {
                ehCache.remove(key);
                assertNull(ehCache.get(key));
            }
        }).printResult();

    }

    public static class KryoSerializer implements org.ehcache.spi.serialization.Serializer<Account> {

        public KryoSerializer(final ClassLoader classLoader) {
            //
        }

        @Override
        public ByteBuffer serialize(final Account object) throws SerializerException {
            final ByteArrayOutputStream output = new ByteArrayOutputStream();
            parser.serialize(object, output);
            return ByteBuffer.wrap(output.array(), 0, output.size());
        }

        @Override
        public Account read(final ByteBuffer binary) throws ClassNotFoundException, SerializerException {
            return parser.deserialize(new ByteArrayInputStream(binary.array()), Account.class);
        }

        @Override
        public boolean equals(final Account object, final ByteBuffer binary) throws ClassNotFoundException, SerializerException {
            return false;
        }
    }

}
