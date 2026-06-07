/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A minimal in-memory {@link Cache} used purely as a custom-provider fixture for
 * {@code CacheFactory.createCache("<fully-qualified-name>(<url>)")}. It exposes a single
 * {@code (String)} constructor so {@code TypeAttrParser.newInstance} can instantiate it from a
 * provider specification, exercising the custom-class branch of {@code createCache}.
 */
public class DummyProviderCache<K, V> extends AbstractCache<K, V> {

    private final String serverUrl;
    private final Map<K, V> store = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    public DummyProviderCache(final String serverUrl) {
        this.serverUrl = serverUrl;
    }

    public String serverUrl() {
        return serverUrl;
    }

    @Override
    public V getOrNull(final K key) {
        return store.get(key);
    }

    @Override
    public boolean put(final K key, final V value, final long liveTime, final long maxIdleTime) {
        store.put(key, value);
        return true;
    }

    @Override
    public void remove(final K key) {
        store.remove(key);
    }

    @Override
    public boolean containsKey(final K key) {
        return store.containsKey(key);
    }

    @Override
    public Set<K> keySet() {
        return store.keySet();
    }

    @Override
    public int size() {
        return store.size();
    }

    @Override
    public void clear() {
        store.clear();
    }

    @Override
    public void close() {
        closed = true;
        store.clear();
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}
