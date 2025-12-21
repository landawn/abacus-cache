package com.landawn.abacus.util;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

@Tag("2025")
public class RocksDBExample {

    static {
        RocksDB.loadLibrary();
    }

    @Test
    public void test_01() {
        try (final Options options = new Options().setCreateIfMissing(true); final RocksDB db = RocksDB.open(options, "rocksdb-data")) {

            // Put a key-value pair
            db.put("hello".getBytes(), "world".getBytes());

            // Get a value by key
            final byte[] value = db.get("hello".getBytes());
            System.out.println("Retrieved: " + new String(value));

            // Delete a key
            db.delete("hello".getBytes());
        } catch (final RocksDBException e) {
            e.printStackTrace();
        }
    }

}
