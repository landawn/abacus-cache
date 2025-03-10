/*
 * Copyright (C) 2015 HaiYang Li
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.landawn.abacus.cache;

import java.util.Map;
import java.util.Set;

import org.ehcache.Cache;
import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoadingException;
import org.ehcache.spi.loaderwriter.CacheWritingException;

/**
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class Ehcache<K, V> extends AbstractCache<K, V> {

    private final Cache<K, V> cacheImpl;

    private boolean isClosed = false;

    /**
     *
     *
     * @param cache
     */
    public Ehcache(final Cache<K, V> cache) {
        cacheImpl = cache;
    }

    /**
     * Gets the t.
     *
     * @param k
     * @return
     */
    @Override
    public V gett(final K k) {
        assertNotClosed();

        return cacheImpl.get(k);
    }

    /**
     *
     * @param k
     * @param v
     * @param liveTime
     * @param maxIdleTime
     * @return true, if successful
     */
    @Override
    public boolean put(final K k, final V v, final long liveTime, final long maxIdleTime) {
        assertNotClosed();

        cacheImpl.put(k, v); // TODO

        return true;
    }

    /**
     *
     * @param k
     */
    @Override
    public void remove(final K k) {
        assertNotClosed();

        cacheImpl.remove(k);
    }

    /**
     *
     * @param k
     * @return true, if successful
     */
    @Override
    public boolean containsKey(final K k) {
        assertNotClosed();

        return cacheImpl.containsKey(k);
    }

    public V putIfAbsent(final K key, final V value) throws CacheLoadingException, CacheWritingException {
        assertNotClosed();

        return cacheImpl.putIfAbsent(key, value);
    }

    public Map<K, V> getAll(final Set<? extends K> keys) throws BulkCacheLoadingException {
        assertNotClosed();

        return cacheImpl.getAll(keys);
    }

    public void putAll(final Map<? extends K, ? extends V> entries) throws BulkCacheWritingException {
        assertNotClosed();

        cacheImpl.putAll(entries);
    }

    public void removeAll(final Set<? extends K> keys) throws BulkCacheWritingException {
        assertNotClosed();

        cacheImpl.removeAll(keys);
    }

    /**
     *
     *
     * @return
     * @throws UnsupportedOperationException
     * @Deprecated Unsupported operation
     */
    @Deprecated
    @Override
    public Set<K> keySet() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     *
     *
     * @return
     * @throws UnsupportedOperationException
     * @Deprecated Unsupported operation
     */
    @Deprecated
    @Override
    public int size() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Clear.
     */
    @Override
    public void clear() {
        assertNotClosed();

        cacheImpl.clear();
    }

    /**
     * Close.
     */
    @Override
    public synchronized void close() {
        assertNotClosed();

        clear();

        isClosed = true;
    }

    /**
     * Checks if is closed.
     *
     * @return true, if is closed
     */
    @Override
    public boolean isClosed() {
        return isClosed;
    }

    /**
     * Assert not closed.
     */
    protected void assertNotClosed() {
        if (isClosed) {
            throw new IllegalStateException("This object pool has been closed");
        }
    }
}
