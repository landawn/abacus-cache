/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.type.ByteBufferType;
import com.landawn.abacus.type.Type;
import com.landawn.abacus.type.TypeFactory;
import com.landawn.abacus.util.Beans;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Objectory;
import com.landawn.abacus.util.Profiler;
import com.landawn.abacus.util.Profiler.MultiLoopsStatistics;
import com.landawn.abacus.util.Strings;

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

    /**
     * Null-key handling is now consistent across all four key operations: {@code put} already
     * rejected a null key, and {@code getOrNull}/{@code remove}/{@code containsKey} now reject it
     * too (previously they silently delegated to the pool). {@code put} also rejects a null value.
     */
    @Test
    public void test_nullKey_and_nullValue_rejected_consistently() {
        assertThrows(IllegalArgumentException.class, () -> cache.getOrNull(null));
        assertThrows(IllegalArgumentException.class, () -> cache.remove(null));
        assertThrows(IllegalArgumentException.class, () -> cache.containsKey(null));
        assertThrows(IllegalArgumentException.class, () -> cache.put(null, new Account(), 0, 0));
        assertThrows(IllegalArgumentException.class, () -> cache.put("k", null, 0, 0));
    }

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
     * Regression test for the negative-evictDelay contract violation.
     *
     * <p>Every constructor/builder javadoc documents "0 or negative disables automatic eviction",
     * but a negative value was passed straight to the underlying pool, whose constructor throws
     * {@code IllegalArgumentException} — and because the off-heap memory had already been allocated
     * before pool creation, the whole native allocation leaked with it. The fix clamps the value
     * before handing it to the pool (and releases the allocation should any later init step fail).
     */
    @Test
    public void test_builder_negativeEvictDelay_disablesEvictionInsteadOfThrowing() {
        final OffHeapCache<String, byte[]> c = OffHeapCache.<String, byte[]> builder().capacityInMB(1).evictDelay(-1).build();
        try {
            assertTrue(c.put("k", new byte[] { 1, 2, 3 }));
            assertArrayEquals(new byte[] { 1, 2, 3 }, c.getOrNull("k"));
        } finally {
            c.close();
        }
    }

    /**
     * Regression test for the post-close {@code put()} use-after-free guard on the real
     * {@code Unsafe}-backed allocator. {@code close()} frees the off-heap allocation, so a
     * subsequent {@code put()} must fail fast with {@link IllegalStateException} <em>before</em>
     * reaching the native copy rather than write into freed memory. (The abstract-level test
     * {@code AbstractOffHeapCacheTest#testPutAfterCloseFailsBeforeNativeCopy} verifies the ordering
     * with a fake subclass; this exercises the concrete {@link OffHeapCache}.)
     */
    @Test
    public void test_putAfterClose_throwsIllegalStateException() {
        final OffHeapCache<String, byte[]> c = OffHeapCache.<String, byte[]> builder().capacityInMB(1).build();
        c.close();
        assertThrows(IllegalStateException.class, () -> c.put("k", new byte[] { 1, 2, 3 }));
    }

    /**
     * Regression test for the array-retention hazard on the disk-spill path.
     *
     * <p>When the serialized size exactly matched the buffer length (always true for {@code byte[]}
     * values), {@code putToDisk()} handed the CALLER'S OWN array to {@code OffHeapStore.put}. The
     * OffHeapStore contract leaves defensive copying implementation-specific, so a store that
     * retains the array (like this in-memory one) saw its cached bytes silently change when the
     * caller later mutated the array. The fix always passes a private copy.
     */
    @Test
    public void test_putToDisk_handsStoreAPrivateCopy() {
        final java.util.Map<String, byte[]> memStore = new java.util.concurrent.ConcurrentHashMap<>();
        final OffHeapStore<String> retainingStore = new OffHeapStore<>() {
            @Override
            public boolean put(final String key, final byte[] value) {
                memStore.put(key, value); // retains the array reference - allowed by the contract
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

        final OffHeapCache<String, byte[]> c = OffHeapCache.<String, byte[]> builder()
                .capacityInMB(1)
                .evictDelay(0)
                .offHeapStore(retainingStore)
                .storeSelector((k, v, size) -> 2) // disk only
                .build();
        try {
            final byte[] value = { 10, 20, 30, 40 };
            assertTrue(c.put("k", value));

            // The caller mutates its own array after the put...
            value[0] = 99;

            // ...and the cached entry must be unaffected.
            assertArrayEquals(new byte[] { 10, 20, 30, 40 }, c.getOrNull("k"));
        } finally {
            c.close();
        }
    }

    /**
     * Regression test for disk-byte ownership across same-key replacements.
     *
     * <p>The offHeapStore is keyed by the cache key alone, so when a put replaces an existing
     * disk-spilled entry, the replaced wrapper must not delete the store bytes that now belong to
     * the replacing wrapper. Ownership is now tracked per key: replacement transfers it, reads of
     * the current entry succeed, and removing the entry deletes the store bytes exactly once.
     */
    @Test
    public void test_putToDisk_replacement_keepsCurrentBytes_and_removeDeletesThem() {
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

        final OffHeapCache<String, byte[]> c = OffHeapCache.<String, byte[]> builder()
                .capacityInMB(1)
                .evictDelay(0)
                .offHeapStore(inMemoryStore)
                .storeSelector((k, v, size) -> 2) // disk only
                .build();
        try {
            assertTrue(c.put("k", new byte[] { 1, 1, 1 }));
            assertTrue(c.put("k", new byte[] { 2, 2, 2 })); // replaces the disk entry under the same key
            assertEquals(1, memStore.size());
            assertArrayEquals(new byte[] { 2, 2, 2 }, c.getOrNull("k"));

            c.remove("k");
            assertNull(c.getOrNull("k"));
            assertTrue(memStore.isEmpty());

            final OffHeapCacheStats stats = c.stats();
            assertEquals(0L, stats.sizeOnDisk());
            assertEquals(0L, stats.dataSizeOnDisk());
        } finally {
            c.close();
        }
    }

    /**
     * Regression coverage for the pooled-buffer handling on the serialization-failure path in
     * {@code AbstractOffHeapCache.put()}. The pooled {@code ByteArrayOutputStream} is obtained
     * before the main try/finally that recycles it, so a serializer that throws used to skip
     * {@code Objectory.recycle(os)} and leak the pooled buffer. The fix recycles the buffer on the
     * failure path and nulls the reference so the outer finally cannot double-recycle it.
     *
     * <p>This test drives many serialization failures interleaved with successful puts/gets: the
     * serializer exception must propagate cleanly, and—critically—the shared Objectory buffer pool
     * must not be corrupted (a double-recycle could hand a still-referenced buffer to a later
     * successful put), so every interleaved normal put/get must round-trip correctly.
     */
    @Test
    public void test_put_serializerThrows_propagates_and_cacheStaysUsable() {
        final OffHeapCache<String, String> c = OffHeapCache.<String, String> builder().capacityInMB(8).evictDelay(0).serializer((v, os) -> {
            if (v.startsWith("BOOM")) {
                throw new IllegalStateException("serialization failed for: " + v);
            }
            final byte[] b = v.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            os.write(b, 0, b.length);
        }).deserializer((bytes, type) -> new String(bytes, java.nio.charset.StandardCharsets.UTF_8)).build();

        try {
            for (int i = 0; i < 300; i++) {
                final int n = i;
                // A throwing serializer must propagate its exception out of put()...
                assertThrows(IllegalStateException.class, () -> c.put("BOOM" + n, "BOOM" + n));
                // ...without corrupting the cache or the shared buffer pool: a normal put/get
                // must still round-trip on every iteration.
                assertTrue(c.put("ok" + n, "value" + n));
                assertEquals("value" + n, c.get("ok" + n).orElse(null));
            }
        } finally {
            c.close();
        }
    }

    /**
     * Regression coverage for the "return inside finally" exception-swallowing bug in
     * {@code AbstractOffHeapCache.put()}. The put body's {@code finally} block ended with a bare
     * {@code return false} whenever no wrapper was produced. A {@code return} executed in a
     * {@code finally} silently discards any exception propagating out of the {@code try}, so an
     * {@code offHeapStore.put()} that threw (or any copy/allocation failure inside the try) was
     * converted into a silent {@code put == false} instead of surfacing the real error — the value
     * was dropped with no signal that the disk store was broken.
     *
     * <p>After the fix the "could not store" handling (vacate + {@code return false}) was moved out
     * of the {@code finally}, so a genuine exception now propagates while the {@code finally} still
     * releases any allocated slots. This test forces the disk-only path with a store that throws for
     * "BOOM" keys and succeeds for "ok" keys: the throw must propagate out of {@code put()}, and the
     * cache must stay fully usable (every interleaved normal put/get round-trips).
     */
    @Test
    public void test_put_offHeapStoreThrows_propagates_and_cacheStaysUsable() {
        final java.util.Map<String, byte[]> memStore = new java.util.concurrent.ConcurrentHashMap<>();

        final OffHeapStore<String> throwingStore = new OffHeapStore<>() {
            @Override
            public boolean put(final String key, final byte[] value) {
                if (key.startsWith("BOOM")) {
                    throw new IllegalStateException("disk store failed for: " + key);
                }
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

        final OffHeapCache<String, byte[]> c = OffHeapCache.<String, byte[]> builder()
                .capacityInMB(8)
                .evictDelay(0)
                .defaultLiveTime(1000_000)
                .defaultMaxIdleTime(1000_000)
                .offHeapStore(throwingStore)
                .storeSelector((k, v, size) -> 2) // disk only — route every put through offHeapStore.put
                .build();

        try {
            for (int i = 0; i < 200; i++) {
                final int n = i;
                final byte[] boom = new byte[128];
                // A throwing offHeapStore.put must propagate out of put() (it used to be silently
                // swallowed as `return false` by the return-in-finally).
                assertThrows(IllegalStateException.class, () -> c.put("BOOM" + n, boom));
                // ...and the cache must stay usable: a normal disk-only put/get round-trips.
                final byte[] ok = ("value" + n).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                assertTrue(c.put("ok" + n, ok));
                assertArrayEquals(ok, c.get("ok" + n).orElse(null));
            }
        } finally {
            c.close();
        }
    }

    /**
     * Durable boundary round-trip coverage for the off-heap memory arithmetic. Sweeps sizes that
     * exercise the single-slot path, the multi-slot path (values larger than maxBlockSize), and the
     * multi-segment-spanning path (values larger than the 1 MB segment), crossing the fixed
     * MIN_BLOCK_SIZE (64), default maxBlockSize (8192) and SEGMENT_SIZE (1 MB) boundaries and their
     * ±1 neighbours. Each value carries a size-dependent byte pattern so any truncation, wrong
     * copy length, or multi-slot reassembly offset error is caught by the exact assertArrayEquals.
     */
    @Test
    public void test_boundary_sizes_roundtrip_exact() {
        final OffHeapCache<String, byte[]> c = OffHeapCache.<String, byte[]> builder().capacityInMB(64).evictDelay(0).build();
        try {
            final int[] sizes = { 0, 1, 63, 64, 65, 127, 128, 129, 8191, 8192, 8193, 16383, 16384, 16385, 24583, 1048575, 1048576, 1048577, 2097152, 3158073 };
            for (final int size : sizes) {
                final byte[] v = new byte[size];
                for (int i = 0; i < size; i++) {
                    v[i] = (byte) (size * 31 + i);
                }
                final String key = "k" + size;
                assertTrue(c.put(key, v), "put failed for size " + size);
                assertArrayEquals(v, c.get(key).orElse(null), "round-trip mismatch for size " + size);
            }
        } finally {
            c.close();
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
    public void test_failed_memory_replacement_preserves_existing_entry() {
        final OffHeapCache<String, byte[]> c = OffHeapCache.<String, byte[]> builder().capacityInMB(1).evictDelay(0).build();

        try {
            final byte[] oldValue = new byte[] { 1, 2, 3, 4 };

            assertTrue(c.put("k", oldValue));
            assertFalse(c.put("k", new byte[2 * 1024 * 1024]));
            assertArrayEquals(oldValue, c.get("k").orElse(null));
        } finally {
            c.close();
        }
    }

    @Test
    public void test_failed_disk_replacement_preserves_existing_entry() {
        final java.util.Map<String, byte[]> memStore = new java.util.concurrent.ConcurrentHashMap<>();
        final AtomicBoolean failWrites = new AtomicBoolean(false);

        final OffHeapStore<String> inMemoryStore = new OffHeapStore<>() {
            @Override
            public boolean put(final String key, final byte[] value) {
                if (failWrites.get()) {
                    return false;
                }

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
                .storeSelector((k, v, size) -> 2)
                .build();

        try {
            final String key = "shared-key";
            final Account first = createAccount(Account.class);
            first.setFirstName(Strings.repeat("a", 5000));
            assertTrue(diskCache.put(key, first));

            final Account second = createAccount(Account.class);
            second.setFirstName(Strings.repeat("b", 5000));
            failWrites.set(true);

            assertFalse(diskCache.put(key, second));
            assertEquals(first, diskCache.get(key).orElse(null));
            assertTrue(memStore.containsKey(key));
            assertEquals(1, memStore.size());

            final var stats = diskCache.stats();
            assertEquals(1L, stats.sizeOnDisk());
            assertEquals(stats.dataSizeOnDisk(), stats.dataSize());
        } finally {
            diskCache.close();
        }
    }

    @Test
    public void test_throwing_disk_replacement_preserves_existing_entry() {
        final java.util.Map<String, byte[]> memStore = new java.util.concurrent.ConcurrentHashMap<>();
        final AtomicBoolean failWrites = new AtomicBoolean(false);

        final OffHeapStore<String> throwingStore = new OffHeapStore<>() {
            @Override
            public boolean put(final String key, final byte[] value) {
                if (failWrites.get()) {
                    throw new IllegalStateException("disk store failed for: " + key);
                }

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
                .offHeapStore(throwingStore)
                .storeSelector((k, v, size) -> 2)
                .build();

        try {
            final String key = "shared-key-throwing";
            final Account first = createAccount(Account.class);
            first.setFirstName(Strings.repeat("a", 5000));
            assertTrue(diskCache.put(key, first));

            final Account second = createAccount(Account.class);
            second.setFirstName(Strings.repeat("b", 5000));
            failWrites.set(true);

            assertThrows(IllegalStateException.class, () -> diskCache.put(key, second));
            assertEquals(first, diskCache.get(key).orElse(null));
            assertTrue(memStore.containsKey(key));
            assertEquals(1, memStore.size());

            final var stats = diskCache.stats();
            assertEquals(1L, stats.sizeOnDisk());
            assertEquals(stats.dataSizeOnDisk(), stats.dataSize());
        } finally {
            diskCache.close();
        }
    }

    @Test
    public void test_clear_reclaims_empty_segments_for_different_slot_sizes() {
        final OffHeapCache<String, byte[]> c = OffHeapCache.<String, byte[]> builder().capacityInMB(1).maxBlockSizeInBytes(1024 * 1024).evictDelay(0).build();

        try {
            assertTrue(c.put("small", new byte[64]));
            c.clear();

            assertTrue(c.put("large", new byte[256 * 1024]));
        } finally {
            c.close();
        }
    }

    @Test
    public void test_missing_disk_bytes_not_counted_as_disk_hit_and_stale_wrapper_removed() {
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
                .storeSelector((k, v, size) -> 2)
                .build();

        try {
            final Account account = createAccount(Account.class);
            account.setFirstName(Strings.repeat("x", 5000));
            final String key = account.getEmailAddress();

            assertTrue(diskCache.put(key, account));
            memStore.clear();

            assertNull(diskCache.get(key).orElse(null));

            final var stats = diskCache.stats();
            assertEquals(0L, stats.hitCountByDisk());
            assertEquals(0L, stats.sizeOnDisk());
            assertEquals(0L, stats.dataSizeOnDisk());
            assertEquals(0L, stats.dataSize());
        } finally {
            diskCache.close();
        }
    }

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

    /** Exercises the keySet / size / clear / containsKey / isClosed lifecycle on AbstractOffHeapCache. */
    @Test
    public void test_OffHeapCache_keySet_size_clear_lifecycle() {
        final OffHeapCache<String, byte[]> c = OffHeapCache.<String, byte[]> builder().capacityInMB(1).build();
        try {
            c.put("a", new byte[256]);
            c.put("b", new byte[256]);
            assertTrue(c.containsKey("a"));
            assertTrue(c.size() >= 1);
            assertTrue(c.keySet().contains("a"));

            // remove + verify
            c.remove("a");
            assertNull(c.get("a").orElse(null));

            // clear empties the cache
            c.clear();
            assertEquals(0, c.size());

            // isClosed is false before close, true after
            assertFalse(c.isClosed());
        } finally {
            c.close();
        }

        // Second close is idempotent and isClosed reflects state.
        final OffHeapCache<String, byte[]> c2 = OffHeapCache.<String, byte[]> builder().capacityInMB(1).build();
        c2.close();
        c2.close();
        assertTrue(c2.isClosed());
    }

    /** Verifies stats() returns a non-null snapshot after some traffic. */
    @Test
    public void test_OffHeapCache_stats_returns_snapshot() {
        final OffHeapCache<String, byte[]> c = OffHeapCache.<String, byte[]> builder().capacityInMB(1).build();
        try {
            c.put("k", new byte[256]);
            c.get("k");
            assertNotNull(c.stats());
        } finally {
            c.close();
        }
    }

    @Test
    public void test_OffHeapCache_simpleConstructor_capacityOnly() {
        // Exercises the (capacityInMB) and (capacityInMB, evictDelay) constructors via the Builder
        // path. The Builder is the public API; the underlying simple constructors are package-private.
        final OffHeapCache<String, byte[]> c = OffHeapCache.<String, byte[]> builder().capacityInMB(1).build();
        try {
            assertTrue(c.put("k", new byte[256]));
            assertNotNull(c.get("k"));
        } finally {
            c.close();
        }
    }

    @Test
    public void test_OffHeapCache_builder_defaults() {
        // Builder with default values for everything other than capacity.
        final OffHeapCache<String, byte[]> c = OffHeapCache.<String, byte[]> builder()
                .capacityInMB(1)
                .maxBlockSizeInBytes(0) // 0 should fall back to default
                .build();
        try {
            assertTrue(c.put("k", new byte[256]));
        } finally {
            c.close();
        }
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

    /** {@code OffHeapCache(capacityInMB)} delegates to the (capacity, evictDelay=3000) constructor. */
    @Test
    public void testConstructor_capacityOnly() {
        try (OffHeapCache<String, byte[]> cache = new OffHeapCache<>(1)) {
            assertFalse(cache.isClosed());
            final byte[] value = { 1, 2, 3, 4 };
            assertTrue(cache.put("k", value));
            assertArrayEquals(value, cache.getOrNull("k"));
        }
    }

    /** {@code OffHeapCache(capacityInMB, evictDelay)} delegates to the default-TTL constructor. */
    @Test
    public void testConstructor_capacityAndEvictDelay() {
        try (OffHeapCache<String, byte[]> cache = new OffHeapCache<>(1, 0L)) {
            final byte[] value = { 5, 6, 7 };
            assertTrue(cache.put("k", value));
            assertArrayEquals(value, cache.getOrNull("k"));
            assertNotNull(cache.stats());
        }
    }

    /** {@code OffHeapCache(capacityInMB, evictDelay, defaultLiveTime, defaultMaxIdleTime)} full basic form. */
    @Test
    public void testConstructor_capacityEvictDelayLiveTimeIdleTime() {
        try (OffHeapCache<String, byte[]> cache = new OffHeapCache<>(1, 0L, 60_000L, 60_000L)) {
            final byte[] value = { 8, 9 };
            assertTrue(cache.put("k", value));
            assertArrayEquals(value, cache.getOrNull("k"));
        }
    }

}
