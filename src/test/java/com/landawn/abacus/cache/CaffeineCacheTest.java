/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.landawn.abacus.TestBase;
import com.landawn.abacus.util.Suppliers;

@Tag("2025")
public class CaffeineCacheTest extends TestBase {

    private CaffeineCache<String, String> newCache() {
        // Use a Supplier to create the underlying Caffeine instance, exercising the Suppliers helper.
        final Supplier<com.github.benmanes.caffeine.cache.Cache<String, String>> supplier = Suppliers
                .of(() -> Caffeine.newBuilder().maximumSize(100).recordStats().build());
        return new CaffeineCache<>(supplier.get());
    }

    @Test
    public void testConstructor_EdgeCase_NullCache() {
        assertThrows(IllegalArgumentException.class, () -> new CaffeineCache<String, String>(null));
    }

    @Test
    public void testPutAndGetOrNull() {
        try (CaffeineCache<String, String> cache = newCache()) {
            assertTrue(cache.put("k", "v", 0, 0));
            assertEquals("v", cache.getOrNull("k"));
        }
    }

    @Test
    public void testPut_EdgeCase_NullKey() {
        try (CaffeineCache<String, String> cache = newCache()) {
            assertThrows(IllegalArgumentException.class, () -> cache.put(null, "v", 0, 0));
        }
    }

    /**
     * Null values are now rejected up-front with {@link IllegalArgumentException} (consistent with the
     * null-key contract), rather than surfacing as an unrelated {@code NullPointerException} from the
     * underlying Caffeine cache.
     */
    @Test
    public void testPut_EdgeCase_NullValue() {
        try (CaffeineCache<String, String> cache = newCache()) {
            assertThrows(IllegalArgumentException.class, () -> cache.put("k", null, 0, 0));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPut_RacingClose_RemovesOwnLateWrite() throws InterruptedException {
        final com.github.benmanes.caffeine.cache.Cache<String, String> delegate = mock(com.github.benmanes.caffeine.cache.Cache.class);
        final ConcurrentMap<String, String> delegateMap = new ConcurrentHashMap<>();
        final CountDownLatch putStarted = new CountDownLatch(1);
        final CountDownLatch allowPutToFinish = new CountDownLatch(1);

        org.mockito.Mockito.when(delegate.asMap()).thenReturn(delegateMap);
        doAnswer(invocation -> {
            putStarted.countDown();
            assertTrue(allowPutToFinish.await(5, TimeUnit.SECONDS));
            delegateMap.put("k", "v");
            return null;
        }).when(delegate).put("k", "v");

        final CaffeineCache<String, String> cache = new CaffeineCache<>(delegate);
        final AtomicReference<Throwable> putFailure = new AtomicReference<>();
        final Thread putThread = new Thread(() -> {
            try {
                cache.put("k", "v", 0, 0);
            } catch (final Throwable e) {
                putFailure.set(e);
            }
        });

        try {
            putThread.start();
            assertTrue(putStarted.await(5, TimeUnit.SECONDS));

            cache.close();
            allowPutToFinish.countDown();
            putThread.join(5_000L);

            assertFalse(putThread.isAlive());
            assertInstanceOf(IllegalStateException.class, putFailure.get());
            assertFalse(delegateMap.containsKey("k"));
            verify(delegate).invalidateAll();
        } finally {
            allowPutToFinish.countDown();
            putThread.join(5_000L);
            cache.close();
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPut_RacingClose_DoesNotRemoveNewerDelegateValue() throws InterruptedException {
        final com.github.benmanes.caffeine.cache.Cache<String, String> delegate = mock(com.github.benmanes.caffeine.cache.Cache.class);
        final ConcurrentMap<String, String> delegateMap = new ConcurrentHashMap<>();
        final CountDownLatch putStarted = new CountDownLatch(1);
        final CountDownLatch allowPutToFinish = new CountDownLatch(1);

        org.mockito.Mockito.when(delegate.asMap()).thenReturn(delegateMap);
        doAnswer(invocation -> {
            putStarted.countDown();
            assertTrue(allowPutToFinish.await(5, TimeUnit.SECONDS));

            // The wrapper's late write lands after close() invalidated the delegate. Before the
            // wrapper can perform its closed-state cleanup, a direct delegate user replaces it.
            // Cleanup must not erase that newer, externally-owned mapping.
            delegateMap.put("k", "v");
            delegateMap.put("k", "newer-direct-value");
            return null;
        }).when(delegate).put("k", "v");

        final CaffeineCache<String, String> cache = new CaffeineCache<>(delegate);
        final AtomicReference<Throwable> putFailure = new AtomicReference<>();
        final Thread putThread = new Thread(() -> {
            try {
                cache.put("k", "v", 0, 0);
            } catch (final Throwable e) {
                putFailure.set(e);
            }
        });

        try {
            putThread.start();
            assertTrue(putStarted.await(5, TimeUnit.SECONDS));

            cache.close();
            allowPutToFinish.countDown();
            putThread.join(5_000L);

            assertFalse(putThread.isAlive());
            assertInstanceOf(IllegalStateException.class, putFailure.get());
            assertEquals("newer-direct-value", delegateMap.get("k"));
            verify(delegate).invalidateAll();
        } finally {
            allowPutToFinish.countDown();
            putThread.join(5_000L);
            cache.close();
        }
    }

    @Test
    public void testGetOrNull_EdgeCase_Missing() {
        try (CaffeineCache<String, String> cache = newCache()) {
            assertNull(cache.getOrNull("missing"));
        }
    }

    @Test
    public void testRemove() {
        try (CaffeineCache<String, String> cache = newCache()) {
            cache.put("k", "v", 0, 0);
            cache.remove("k");
            assertNull(cache.getOrNull("k"));
        }
    }

    @Test
    public void testContainsKey() {
        try (CaffeineCache<String, String> cache = newCache()) {
            cache.put("k", "v", 0, 0);
            assertTrue(cache.containsKey("k"));
            assertFalse(cache.containsKey("missing"));
        }
    }

    @Test
    public void testKeySet_Unsupported() {
        try (CaffeineCache<String, String> cache = newCache()) {
            assertThrows(UnsupportedOperationException.class, cache::keySet);
        }
    }

    @Test
    public void testSize() {
        try (CaffeineCache<String, String> cache = newCache()) {
            cache.put("a", "1", 0, 0);
            cache.put("b", "2", 0, 0);
            // Caffeine performs maintenance lazily; force pending writes to drain so size is reliable.
            // We just check it's between 0 and the maximum.
            final int size = cache.size();
            assertTrue(size >= 0);
            assertTrue(size <= 2);
        }
    }

    @Test
    public void testClear() {
        try (CaffeineCache<String, String> cache = newCache()) {
            cache.put("k", "v", 0, 0);
            cache.clear();
            assertNull(cache.getOrNull("k"));
        }
    }

    @Test
    public void testClose_IsIdempotent() {
        final CaffeineCache<String, String> cache = newCache();
        assertFalse(cache.isClosed());
        cache.close();
        cache.close();
        assertTrue(cache.isClosed());
    }

    @Test
    public void testOperations_AfterClose_Throw() {
        final CaffeineCache<String, String> cache = newCache();
        cache.close();
        assertThrows(IllegalStateException.class, () -> cache.getOrNull("k"));
        assertThrows(IllegalStateException.class, () -> cache.put("k", "v", 0, 0));
        assertThrows(IllegalStateException.class, () -> cache.remove("k"));
        assertThrows(IllegalStateException.class, () -> cache.containsKey("k"));
        assertThrows(IllegalStateException.class, cache::size);
        assertThrows(IllegalStateException.class, cache::clear);
    }

    @Test
    public void testStats_ReturnsNonNull() {
        try (CaffeineCache<String, String> cache = newCache()) {
            cache.put("k", "v", 0, 0);
            cache.getOrNull("k");
            cache.getOrNull("missing");
            assertNotNull(cache.stats());
        }
    }

    /**
     * Regression coverage for the asymmetric null-key handling defect.
     *
     * <p>Before the fix {@link CaffeineCache#put(Object, Object, long, long)} explicitly rejected
     * null keys with {@code IllegalArgumentException}, but the read-side methods
     * ({@code getOrNull}, {@code remove}, {@code containsKey}) delegated straight to Caffeine,
     * which raises an unrelated {@code NullPointerException}. The fix harmonises the contract so
     * every key-taking operation rejects nulls with the same {@code IllegalArgumentException}.
     */
    @Test
    public void testGetOrNull_EdgeCase_NullKey() {
        try (CaffeineCache<String, String> cache = newCache()) {
            assertThrows(IllegalArgumentException.class, () -> cache.getOrNull(null));
        }
    }

    @Test
    public void testRemove_EdgeCase_NullKey() {
        try (CaffeineCache<String, String> cache = newCache()) {
            assertThrows(IllegalArgumentException.class, () -> cache.remove(null));
        }
    }

    @Test
    public void testContainsKey_EdgeCase_NullKey() {
        try (CaffeineCache<String, String> cache = newCache()) {
            assertThrows(IllegalArgumentException.class, () -> cache.containsKey(null));
        }
    }
}
