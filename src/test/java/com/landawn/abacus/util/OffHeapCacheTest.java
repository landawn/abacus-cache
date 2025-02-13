/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.landawn.abacus.cache.OffHeapCache;
import com.landawn.abacus.type.ByteBufferType;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.type.TypeFactory;
import com.landawn.abacus.util.Profiler.MultiLoopsStatistics;

/**
 *
 * @since 0.8
 *
 * @author Haiyang Li
 */
public class OffHeapCacheTest {
    private static final Random rand = new Random();

    private static final ByteBufferType bbType = (ByteBufferType) ((Type<?>) TypeFactory.getType(ByteBufferType.BYTE_BUFFER));
    private static final OffHeapCache<String, Account> cache = new OffHeapCache<>(4096, 3000, 1000_000, 1000_000);
    // private static final OffHeapCache<String, String> ohcache = new OffHeapCache<>(1204, 3000, 1000_000, 1000_000);

    private static final long start = System.currentTimeMillis();
    private static final AtomicInteger counter = new AtomicInteger();

    /**
     *
     */
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

    /**
     *
     */
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
        return N.fill(cls);
    }

    /**
     *
     */
    @Test
    public void test_put_get_2() {
        final MultiLoopsStatistics result = Profiler.run(32, 3, 1, this::test_put_get);

        result.printResult();

        assertTrue(result.getAllFailedMethodStatisticsList().size() == 0);
    }

    /**
     *
     */
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

    /**
     *
     */
    @Test
    public void test_perf_big_Object() {

        final String longFirstName = Strings.repeat(Strings.uuid(), 100);

        Profiler.run(16, 10000, 3, () -> {
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
}
