/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.TestBase;

import net.spy.memcached.CachedData;

@Tag("2025")
public class KryoTranscoderTest extends TestBase {

    @Test
    public void testConstructor_Default_UsesDefaultMaxSize() {
        final KryoTranscoder<String> tx = new KryoTranscoder<>();
        assertEquals(CachedData.MAX_SIZE, tx.getMaxSize());
    }

    @Test
    public void testConstructor_WithMaxSize() {
        final KryoTranscoder<String> tx = new KryoTranscoder<>(2048);
        assertEquals(2048, tx.getMaxSize());
    }

    @Test
    public void testConstructor_EdgeCase_NonPositiveMaxSize() {
        assertThrows(IllegalArgumentException.class, () -> new KryoTranscoder<String>(0));
        assertThrows(IllegalArgumentException.class, () -> new KryoTranscoder<String>(-1));
    }

    @Test
    public void testAsyncDecode_AlwaysFalse() {
        final KryoTranscoder<String> tx = new KryoTranscoder<>();
        assertFalse(tx.asyncDecode(null));
        assertFalse(tx.asyncDecode(new CachedData(0, new byte[0], 100)));
    }

    @Test
    public void testEncode_RoundTrip() {
        final KryoTranscoder<String> tx = new KryoTranscoder<>();
        final CachedData cd = tx.encode("hello");
        assertNotNull(cd);
        assertEquals(0, cd.getFlags());
        final String decoded = tx.decode(cd);
        assertEquals("hello", decoded);
    }

    @Test
    public void testEncode_EdgeCase_NullValue() {
        final KryoTranscoder<String> tx = new KryoTranscoder<>();
        final CachedData cd = tx.encode(null);
        // Encoding null still produces a CachedData; decoding it returns null.
        assertNotNull(cd);
        assertNull(tx.decode(cd));
    }

    @Test
    public void testEncode_EdgeCase_ExceedsMaxSize() {
        // Tiny limit so any non-trivial object exceeds it.
        final KryoTranscoder<String> tx = new KryoTranscoder<>(8);
        final String big = "this string is definitely larger than 8 bytes after kryo encoding";
        assertThrows(IllegalArgumentException.class, () -> tx.encode(big));
    }

    @Test
    public void testDecode_EdgeCase_NullCachedData() {
        final KryoTranscoder<String> tx = new KryoTranscoder<>();
        assertNull(tx.decode(null));
    }

    @Test
    public void testDecode_EdgeCase_EmptyData() {
        final KryoTranscoder<String> tx = new KryoTranscoder<>();
        // CachedData with zero-length byte array decodes to null.
        final CachedData cd = new CachedData(0, new byte[0], 100);
        assertNull(tx.decode(cd));
    }

    @Test
    public void testEncode_RoundTrip_ComplexObject() {
        final KryoTranscoder<Object> tx = new KryoTranscoder<>();
        final java.util.HashMap<String, Integer> map = new java.util.HashMap<>();
        map.put("a", 1);
        map.put("b", 2);
        final CachedData cd = tx.encode(map);
        assertNotNull(cd);
        final Object decoded = tx.decode(cd);
        assertTrue(decoded instanceof java.util.Map);
        @SuppressWarnings("unchecked")
        final java.util.Map<String, Integer> roundTrip = (java.util.Map<String, Integer>) decoded;
        assertEquals(Integer.valueOf(1), roundTrip.get("a"));
        assertEquals(Integer.valueOf(2), roundTrip.get("b"));
    }
}
