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

    /**
     * Regression test for the disk→disk put data-loss bug in
     * {@link com.landawn.abacus.cache.AbstractOffHeapCache#putToDisk}.
     *
     * <p>Scenario: put key K (disk) → put key K (disk again with different value). Before the fix,
     * putToDisk wrote the NEW bytes to the disk store under key K, then _pool.put(K, newWrapper)
     * destroyed the prior StoreWrapper(K) whose destroy() called offHeapStore.remove(K) — wiping
     * the bytes we had just written. A subsequent get(K) returned null.
     *
     * <p>After the fix: putToDisk pre-removes any prior pool entry (so its destroy operates on
     * the OLD bytes) before writing new bytes; the new bytes survive and a subsequent get(K)
     * returns the new value.
     */
    @Test
    public void test_putToDisk_disk_to_disk_replace_preserves_new_value() {
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
                return memStore.remove(key) != null;
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
            final String key = "shared-key";

            final Account first = createAccount(Account.class);
            first.setFirstName(Strings.repeat("a", 5000));
            assertTrue(diskCache.put(key, first));
            assertEquals(first, diskCache.get(key).orElse(null));

            // Replace under the same key with a different on-disk value.
            final Account second = createAccount(Account.class);
            second.setFirstName(Strings.repeat("b", 5000));
            assertTrue(diskCache.put(key, second));

            // Critical assertion: the value MUST be the new one and MUST NOT be null.
            final Account fetched = diskCache.get(key).orElse(null);
            assertEquals(second, fetched);
            // And the disk store must hold exactly one entry for this key.
            assertTrue(memStore.containsKey(key));
            assertEquals(1, memStore.size());

            // Sanity: stats reflect exactly one on-disk entry.
            final var stats = diskCache.stats();
            assertEquals(1L, stats.sizeOnDisk());
            assertEquals(stats.dataSizeOnDisk(), stats.dataSize());
        } finally {
            diskCache.close();
        }
    }

    @Test
    public void test_put_get_2() {
        final MultiLoopsStatistics result = Profiler.run(32, 3, 1, this::test_put_get);

        result.printResult();

        assertTrue(result.getAllFailedMethodStatisticsList().size() == 0);
    }

    /**
     * Regression test for the LongSummaryStatistics race in
     * {@link com.landawn.abacus.cache.AbstractOffHeapCache#stats}.
     *
     * <p>Before the fix, {@code stats()} read {@code totalWriteToDiskTimeStats} /
     * {@code totalReadFromDiskTimeStats} without synchronization while concurrent puts/gets
     * were calling {@code accept(...)} under the same monitor. Because
     * {@code LongSummaryStatistics} is not thread-safe, the reader could observe a torn
     * snapshot — e.g. {@code count > 0} but {@code min == Long.MAX_VALUE} (so the produced
     * {@code MinMaxAvg.min} would be a wildly wrong, huge double). After the fix the reader
     * snapshots under the same monitor, so {@code min}, {@code max}, and {@code average} are
     * all consistent with a non-zero count.
     */
    @Test
    public void test_stats_under_concurrent_disk_writes_consistent() throws Exception {
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
                return memStore.remove(key) != null;
            }
        };

        final OffHeapCache<String, Account> diskCache = OffHeapCache.<String, Account> builder()
                .capacityInMB(8)
                .evictDelay(0)
                .defaultLiveTime(1000_000)
                .defaultMaxIdleTime(1000_000)
                .offHeapStore(inMemoryStore)
                .storeSelector((k, v, size) -> 2) // disk only — exercises totalWriteToDiskTimeStats
                .statsTimeOnDisk(true)
                .build();

        try {
            final java.util.concurrent.atomic.AtomicBoolean stop = new java.util.concurrent.atomic.AtomicBoolean(false);
            final java.util.concurrent.atomic.AtomicReference<AssertionError> failure = new java.util.concurrent.atomic.AtomicReference<>();

            final Thread writer = new Thread(() -> {
                int i = 0;
                while (!stop.get()) {
                    final Account a = createAccount(Account.class);
                    a.setFirstName(Strings.repeat("x", 2048));
                    diskCache.put("k" + (i++ % 64), a);
                }
            });

            final Thread reader = new Thread(() -> {
                while (!stop.get()) {
                    final var s = diskCache.stats();
                    final var w = s.writeToDiskTimeStats();
                    // If any writes have happened (count > 0 inside the synchronized block),
                    // min/max must be real, finite, non-negative numbers — never Long.MAX_VALUE
                    // / Long.MIN_VALUE leaking through. We can't observe count directly via the
                    // public stats record, but writeCount() gives us the analog.
                    if (s.putCountToDisk() > 0) {
                        if (w.min() < 0 || w.max() < 0 || w.min() > w.max() + 1) {
                            failure.compareAndSet(null,
                                    new AssertionError("Torn stats: writeCount=" + s.putCountToDisk() + " min=" + w.min() + " max=" + w.max()));
                            return;
                        }
                    }
                }
            });

            writer.start();
            reader.start();
            Thread.sleep(750);
            stop.set(true);
            writer.join(5_000);
            reader.join(5_000);

            if (failure.get() != null) {
                throw failure.get();
            }
            assertTrue(diskCache.stats().putCountToDisk() > 0, "expected at least one disk write during the run");
        } finally {
            diskCache.close();
        }
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
