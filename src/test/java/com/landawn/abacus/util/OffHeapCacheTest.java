/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.cache.OffHeapCache;
import com.landawn.abacus.cache.OffHeapStore;
import com.landawn.abacus.type.ByteBufferType;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.type.TypeFactory;
import com.landawn.abacus.util.Profiler.MultiLoopsStatistics;

//--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED

@Tag("2025")
public class OffHeapCacheTest {
    private static final Random rand = new Random();

    private static final ByteBufferType bbType = (ByteBufferType) ((Type<?>) TypeFactory.getType(ByteBufferType.BYTE_BUFFER));
    private static final OffHeapCache<String, Account> cache = OffHeapCache.<String, Account> builder()
            .capacityInMB(4096)
            .evictDelay(3000)
            .defaultLiveTime(1000_000)
            .defaultMaxIdleTime(1000_000)
            .build();
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
    public void test_put_get() {
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

            if (counter.incrementAndGet() % 10000 == 0) {
                N.println("=========" + counter.get() + ": " + (System.currentTimeMillis() - start));
            }
        }
    }

    private Account createAccount(final Class<Account> cls) {
        Account account = Beans.newRandomBean(cls);
        account.setEmailAddress(Strings.uuid());
        return account;
    }

    /**
     * Regression test for the totalDataSize double-counting bug in AbstractOffHeapCache.put().
     *
     * <p>StoreWrapper's constructor already accounts for totalDataSize, but put() also added it
     * unconditionally for every wrapper type, while StoreWrapper.destroy() only subtracts it once.
     * As a result, for every disk-stored entry the reported dataSize was double the real value and
     * never reconciled (it leaked permanently).
     *
     * <p>Before the fix: after a single disk-only put, stats().dataSize() == 2 * dataSizeOnDisk(),
     * and after removing the entry dataSize() stays non-zero. After the fix dataSize() ==
     * dataSizeOnDisk() and returns to 0 once the entry is removed.
     */
    @Test
    public void test_totalDataSize_doubleCount_for_disk_entries() {
        final java.util.Map<String, byte[]> memStore = new java.util.concurrent.ConcurrentHashMap<>();

        final OffHeapStore<String> inMemoryStore = new OffHeapStore<>() {
            @Override
            public boolean put(final String key, final byte[] value) {
                memStore.put(key, value);
                return true;
            }

            @Override
            public byte[] get(final String key) {
                return memStore.get(key);
            }

            @Override
            public boolean remove(final String key) {
                memStore.remove(key);
                return true;
            }
        };

        final OffHeapCache<String, Account> diskCache = OffHeapCache.<String, Account> builder()
                .capacityInMB(8)
                .evictDelay(0)
                .defaultLiveTime(1000_000)
                .defaultMaxIdleTime(1000_000)
                .offHeapStore(inMemoryStore)
                .storeSelector((k, v, size) -> 2) // disk only
                .build();

        try {
            final Account account = createAccount(Account.class);
            account.setFirstName(Strings.repeat("x", 5000));
            final String key = account.getEmailAddress();

            assertTrue(diskCache.put(key, account));

            final var statsAfterPut = diskCache.stats();
            // The single on-disk entry must be counted exactly once in dataSize.
            assertEquals(statsAfterPut.dataSizeOnDisk(), statsAfterPut.dataSize());
            assertEquals(account, diskCache.get(key).orElse(null));

            diskCache.remove(key);

            final var statsAfterRemove = diskCache.stats();
            assertEquals(0L, statsAfterRemove.dataSizeOnDisk());
            assertEquals(0L, statsAfterRemove.dataSize());
        } finally {
            diskCache.close();
        }
    }

    /**
     * Regression test for the "no expiration" contract bug in AbstractOffHeapCache.put().
     *
     * <p>The OffHeapCache Builder javadoc documents {@code defaultLiveTime}/{@code defaultMaxIdleTime}
     * of {@code 0} (the Builder default) as "no TTL expiration" / "no idle timeout". But
     * {@code put(key, value)} forwards those defaults to {@code put(key, value, 0, 0)}, which built a
     * Wrapper whose underlying {@code ActivityPrint} throws {@code IllegalArgumentException} for any
     * value {@code <= 0}.
     *
     * <p>Before the fix: a default-built cache threw {@code IllegalArgumentException} on the simple
     * {@code put(key, value)} call. After the fix: non-positive live/idle times are treated as
     * "no expiration" and the entry is stored and retrievable.
     */
    @Test
    public void test_put_withDefaultZeroExpiration_doesNotThrow() {
        final OffHeapCache<String, Account> noExpiryCache = OffHeapCache.<String, Account> builder()
                .capacityInMB(8)
                .evictDelay(0)
                // defaultLiveTime / defaultMaxIdleTime intentionally left at the Builder default (0)
                .build();

        try {
            final Account account = createAccount(Account.class);
            final String key = account.getEmailAddress();

            // Documented "no expiration" usage - must NOT throw and must store the value.
            assertTrue(noExpiryCache.put(key, account));
            assertEquals(account, noExpiryCache.get(key).orElse(null));

            // Explicit non-positive values must behave the same way.
            assertTrue(noExpiryCache.put("k2", account, 0, 0));
            assertTrue(noExpiryCache.put("k3", account, -1, -1));
            assertEquals(account, noExpiryCache.get("k2").orElse(null));
            assertEquals(account, noExpiryCache.get("k3").orElse(null));
        } finally {
            noExpiryCache.close();
        }
    }

    @Test
    public void test_put_get_2() {
        final MultiLoopsStatistics result = Profiler.run(32, 3, 1, this::test_put_get);

        result.printResult();

        assertTrue(result.getAllFailedMethodStatisticsList().size() == 0);
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
        final AtomicLong counter = new AtomicLong();

        Profiler.run(16, 100_000, 3, () -> {
            final Account account = createAccount(Account.class);
            account.setFirstName(longFirstName);

            final String key = account.getEmailAddress();
            if (cache.put(key, account)) {
                final Account account2 = cache.get(key).orElse(null);
                assertEquals(account.getId(), account2.getId());
            }

            if (counter.incrementAndGet() % 10 < 9) {
                cache.remove(key);
                assertNull(cache.get(key).orElse(null));
            }
        }).printResult();

        final Account account = createAccount(Account.class);
        account.setFirstName(Strings.repeat(Strings.uuid(), 16));
        cache.put(account.getEmailAddress(), account);

        Profiler.run(16, 100_000, 3, () -> {
            assertEquals(account, cache.get(account.getEmailAddress()).orElse(null));
        }).printResult();

    }
}
