/*
 * Copyright (C) 2017 HaiYang Li
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

import java.util.Set;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.landawn.abacus.util.Numbers;

/**
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class CaffeineCache<K, V> extends AbstractCache<K, V> {

    private final Cache<K, V> cacheImpl;

    private boolean isClosed = false;

    /**
     *
     *
     * @param cache
     */
    public CaffeineCache(final Cache<K, V> cache) {
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

        return cacheImpl.getIfPresent(k);
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

        cacheImpl.invalidate(k);
    }

    /**
     *
     * @param k
     * @return true, if successful
     */
    @Override
    public boolean containsKey(final K k) {
        assertNotClosed();

        return get(k).isPresent();
    }

    /**
     *
     *
     * @return
     * @throws UnsupportedOperationException
     */
    @Override
    public Set<K> keySet() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     *
     *
     * @return
     */
    @Override
    public int size() {
        assertNotClosed();

        return Numbers.toIntExact(cacheImpl.estimatedSize());
    }

    /**
     * Clear.
     */
    @Override
    public void clear() {
        assertNotClosed();

        cacheImpl.cleanUp();
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
     *
     * @return
     * @see Cache#stats()
     */
    public CacheStats stats() {
        return cacheImpl.stats();
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
