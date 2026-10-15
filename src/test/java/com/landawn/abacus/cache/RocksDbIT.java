package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

class RocksDbIT {

    // Writes

    @Test
    void putGetAndDeleteRoundTripBytes(@TempDir final Path directory) throws Exception {
        RocksDB.loadLibrary();
        try (final Options options = new Options().setCreateIfMissing(true);
             final RocksDB database = RocksDB.open(options, directory.resolve("cache").toString())) {
            final byte[] key = "hello".getBytes(StandardCharsets.UTF_8);
            final byte[] value = "world".getBytes(StandardCharsets.UTF_8);
            database.put(key, value);
            assertArrayEquals(value, database.get(key));
            database.delete(key);
            assertNull(database.get(key));
        }
    }
}
