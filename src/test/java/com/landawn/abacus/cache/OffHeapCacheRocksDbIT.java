package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

class OffHeapCacheRocksDbIT {

    // Other Behavior

    @ParameterizedTest
    @EnumSource(OffHeapBackend.class)
    void diskReadWhenPromotionEnabledMovesValueIntoMemory(final OffHeapBackend backend, @TempDir final Path directory) throws Exception {
        try (final RocksDbStore store = new RocksDbStore(directory)) {
            final AtomicBoolean promote = new AtomicBoolean();
            final AbstractOffHeapCache<String, Account> cache = backend.createDiskCache(store, promote);
            try {
                final Account account = new Account();
                account.setFirstName("Ada");
                assertTrue(cache.put("account", account));
                assertEquals(account, cache.getOrNull("account"));
                assertEquals(1, cache.stats().sizeOnDisk());
                promote.set(true);
                assertEquals(account, cache.getOrNull("account"));
                assertEquals(0, cache.stats().sizeOnDisk());
                assertEquals(1, cache.size());
                assertEquals(1, cache.stats().putCount());
                assertNull(store.get("account"));
                assertEquals(account, cache.getOrNull("account"));
            } finally {
                cache.close();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(OffHeapBackend.class)
    void diskStoreRoundTripsReplacesAndClearsAccounts(final OffHeapBackend backend, @TempDir final Path directory) throws Exception {
        try (final RocksDbStore store = new RocksDbStore(directory)) {
            final AbstractOffHeapCache<String, Account> cache = backend.createDiskCache(store, new AtomicBoolean());
            try {
                final Account account = new Account();
                account.setFirstName("Ada");
                assertTrue(cache.put("account", account));
                assertNotNull(store.get("account"));
                assertEquals(account, cache.getOrNull("account"));
                account.setFirstName("Grace");
                assertTrue(cache.put("account", account));
                assertEquals(account, cache.getOrNull("account"));
                assertEquals(1, cache.stats().sizeOnDisk());
                cache.remove("account");
                assertNull(store.get("account"));
                assertNull(cache.getOrNull("account"));
                assertTrue(cache.put("other", account));
                cache.clear();
                assertEquals(0, cache.size());
                assertEquals(0, cache.stats().dataSizeOnDisk());
                assertNull(store.get("other"));
            } finally {
                cache.close();
            }
        }
    }

    private static final class RocksDbStore implements OffHeapStore<String> {
        private final Options options;
        private final RocksDB database;
        private boolean closed;

        RocksDbStore(final Path directory) throws RocksDBException {
            RocksDB.loadLibrary();
            options = new Options().setCreateIfMissing(true);
            try {
                database = RocksDB.open(options, directory.resolve("cache").toString());
            } catch (final RocksDBException | RuntimeException | Error failure) {
                options.close();
                throw failure;
            }
        }

        @Override
        public boolean put(final String key, final byte[] value) {
            try {
                database.put(key.getBytes(StandardCharsets.UTF_8), value);
                return true;
            } catch (final RocksDBException failure) {
                throw new AssertionError("RocksDB put failed", failure);
            }
        }

        @Override
        public byte[] get(final String key) {
            try {
                return database.get(key.getBytes(StandardCharsets.UTF_8));
            } catch (final RocksDBException failure) {
                throw new AssertionError("RocksDB get failed", failure);
            }
        }

        @Override
        public boolean remove(final String key) {
            try {
                database.delete(key.getBytes(StandardCharsets.UTF_8));
                return true;
            } catch (final RocksDBException failure) {
                throw new AssertionError("RocksDB delete failed", failure);
            }
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                try {
                    database.close();
                } finally {
                    options.close();
                }
            }
        }
    }
}
