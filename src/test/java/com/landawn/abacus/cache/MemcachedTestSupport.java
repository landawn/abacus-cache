package com.landawn.abacus.cache;

/** Shared configuration for integration tests that own and flush a dedicated Memcached server. */
public final class MemcachedTestSupport {
    private MemcachedTestSupport() {
    }

    public static String serverUrl() {
        final String url = System.getProperty("cache.memcached.serverUrl");
        if (url == null || url.isBlank()) {
            throw new IllegalStateException("Set -Dcache.memcached.serverUrl=host:port to a dedicated test server; integration tests flush its data");
        }
        return url;
    }
}
