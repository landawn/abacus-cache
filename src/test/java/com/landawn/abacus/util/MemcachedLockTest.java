/*
 * Copyright (c) 2015, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.util;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("2025")
public class MemcachedLockTest {
    final String url = "localhost:11211";
    final MemcachedLock<String, Long> memcachedLock = new MemcachedLock<>(url);

    @Test
    public void test_lock() {
        String key = "mysql";
        memcachedLock.lock(key, 10000);

        assertTrue(memcachedLock.isLocked(key));

        Object value = memcachedLock.get(key);

        N.println(value);
    }

}
