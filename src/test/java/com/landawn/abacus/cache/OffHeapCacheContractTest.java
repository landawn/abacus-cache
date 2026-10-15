package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

class OffHeapCacheContractTest {

    // Construction

    @ParameterizedTest
    @EnumSource(OffHeapBackend.class)
    void builderWithDefaultsStoresAndRetrievesBytes(final OffHeapBackend backend) {
        final AbstractOffHeapCache<String, byte[]> cache = backend.createWithDefaults();
        try {
            final byte[] value = { 1, 2, 3 };
            assertTrue(cache.put("key", value));
            assertArrayEquals(value, cache.getOrNull("key"));
        } finally {
            cache.close();
        }
    }

    @ParameterizedTest
    @EnumSource(OffHeapBackend.class)
    void builderWithNegativeEvictDelayDisablesEviction(final OffHeapBackend backend) {
        final AbstractOffHeapCache<String, byte[]> cache = backend.create(1, -1);
        try {
            assertTrue(cache.put("key", new byte[] { 1, 2, 3 }));
            assertArrayEquals(new byte[] { 1, 2, 3 }, cache.getOrNull("key"));
        } finally {
            cache.close();
        }
    }

    // Writes

    @ParameterizedTest
    @EnumSource(OffHeapBackend.class)
    void putAfterCloseRejectsBeforeNativeCopy(final OffHeapBackend backend) {
        final AbstractOffHeapCache<String, byte[]> cache = backend.create(1, 0);
        cache.close();
        assertThrows(IllegalStateException.class, () -> cache.put("key", new byte[] { 1, 2, 3 }));
    }

    @ParameterizedTest(name = "{0}: {1} bytes")
    @MethodSource("boundarySizes")
    void putAtAllocationBoundariesPreservesEveryByte(final OffHeapBackend backend, final int size) {
        final AbstractOffHeapCache<String, byte[]> cache = backend.create(8, 0);
        try {
            final byte[] value = new byte[size];
            for (int i = 0; i < size; i++) {
                value[i] = (byte) (size * 31 + i);
            }
            assertTrue(cache.put("key", value));
            assertArrayEquals(value, cache.get("key").orElse(null));
        } finally {
            cache.close();
        }
    }

    @ParameterizedTest
    @EnumSource(OffHeapBackend.class)
    void putWithAccountRoundTripsObject(final OffHeapBackend backend) {
        final AbstractOffHeapCache<String, Account> cache = backend.create(1, 0);
        try {
            final Account value = new Account();
            value.setFirstName("123");
            assertTrue(cache.put("account", value));
            assertEquals(value, cache.getOrNull("account"));
        } finally {
            cache.close();
        }
    }

    // Removals

    @ParameterizedTest
    @EnumSource(OffHeapBackend.class)
    void removeAndClearUpdateKeysAndLifecycle(final OffHeapBackend backend) {
        final AbstractOffHeapCache<String, byte[]> cache = backend.create(1, 0);
        try {
            assertTrue(cache.put("a", new byte[256]));
            assertTrue(cache.put("b", new byte[256]));
            assertTrue(cache.containsKey("a"));
            assertEquals(2, cache.size());
            assertEquals(Set.of("a", "b"), cache.keySet());
            cache.remove("a");
            assertNull(cache.getOrNull("a"));
            assertEquals(Set.of("b"), cache.keySet());
            cache.clear();
            assertEquals(0, cache.size());
            assertTrue(cache.keySet().isEmpty());
            assertFalse(cache.isClosed());
        } finally {
            cache.close();
        }
        cache.close();
        assertTrue(cache.isClosed());
    }

    // Statistics

    @ParameterizedTest
    @EnumSource(OffHeapBackend.class)
    void statsAfterKnownTrafficReturnsAnImmutableSnapshot(final OffHeapBackend backend) {
        final AbstractOffHeapCache<String, byte[]> cache = backend.create(1, 0);
        try {
            assertTrue(cache.put("key", new byte[256]));
            assertArrayEquals(new byte[256], cache.getOrNull("key"));
            assertNull(cache.getOrNull("missing"));
            final OffHeapCacheStats snapshot = cache.stats();
            assertEquals(1, snapshot.size());
            assertEquals(1, snapshot.putCount());
            assertEquals(2, snapshot.getCount());
            assertEquals(1, snapshot.hitCount());
            assertEquals(1, snapshot.missCount());
            assertEquals(256, snapshot.dataSize());
            cache.clear();
            assertEquals(0, cache.stats().size());
            assertEquals(0, cache.stats().dataSize());
            assertEquals(1, snapshot.size());
            assertEquals(256, snapshot.dataSize());
        } finally {
            cache.close();
        }
    }

    // Validation And Failures

    @ParameterizedTest
    @EnumSource(OffHeapBackend.class)
    void keyedOperationsWithNullArgumentsRejectBeforeAccess(final OffHeapBackend backend) {
        final AbstractOffHeapCache<String, Account> cache = backend.create(1, 0);
        try {
            assertThrows(IllegalArgumentException.class, () -> cache.getOrNull(null));
            assertThrows(IllegalArgumentException.class, () -> cache.remove(null));
            assertThrows(IllegalArgumentException.class, () -> cache.containsKey(null));
            assertThrows(IllegalArgumentException.class, () -> cache.put(null, new Account(), 0, 0));
            assertThrows(IllegalArgumentException.class, () -> cache.put("key", null, 0, 0));
        } finally {
            cache.close();
        }
    }

    private static Stream<Arguments> boundarySizes() {
        final int[] sizes = { 0, 1, 63, 64, 65, 127, 128, 129, 8191, 8192, 8193, 16383, 16384, 16385, 24583, 1048575, 1048576, 1048577, 2097152, 3158073 };
        return Arrays.stream(OffHeapBackend.values()).flatMap(backend -> Arrays.stream(sizes).mapToObj(size -> Arguments.of(backend, size)));
    }
}
