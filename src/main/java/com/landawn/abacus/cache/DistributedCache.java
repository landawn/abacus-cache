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

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.landawn.abacus.util.Charsets;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Strings;

/**
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class DistributedCache<K, V> extends AbstractCache<K, V> {

    protected static final int DEFAULT_MAX_FAILED_NUMBER = 100;

    protected static final long DEFAULT_RETRY_DELAY = 1000;

    // ...
    private final DistributedCacheClient<V> dcc;

    private final String keyPrefix;

    private final int maxFailedNumForRetry;

    private final long retryDelay;

    // ...
    private final AtomicInteger failedCounter = new AtomicInteger();

    private volatile long lastFailedTime = 0;

    private boolean isClosed = false;

    protected DistributedCache(final DistributedCacheClient<V> dcc) {
        this(dcc, Strings.EMPTY, DEFAULT_MAX_FAILED_NUMBER, DEFAULT_RETRY_DELAY);
    }

    protected DistributedCache(final DistributedCacheClient<V> dcc, final String keyPrefix) {
        this(dcc, keyPrefix, DEFAULT_MAX_FAILED_NUMBER, DEFAULT_RETRY_DELAY);
    }

    protected DistributedCache(final DistributedCacheClient<V> dcc, final String keyPrefix, final int maxFailedNumForRetry, final long retryDelay) {
        this.keyPrefix = Strings.isEmpty(keyPrefix) ? Strings.EMPTY : keyPrefix;
        this.dcc = dcc;
        this.maxFailedNumForRetry = maxFailedNumForRetry;
        this.retryDelay = retryDelay;
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

        if ((failedCounter.get() > maxFailedNumForRetry) && ((System.currentTimeMillis() - lastFailedTime) < retryDelay)) {
            return null;
        }

        V result = null;
        boolean isOK = false;

        try {
            result = dcc.get(generateKey(k));

            isOK = true;
        } finally {
            if (isOK) {
                failedCounter.set(0);
            } else {
                lastFailedTime = System.currentTimeMillis();
                failedCounter.incrementAndGet();
            }
        }

        return result;
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

        return dcc.set(generateKey(k), v, liveTime);
    }

    /**
     * Always return {@code null}.
     *
     * @param k
     */
    @Override
    public void remove(final K k) {
        assertNotClosed();

        dcc.delete(generateKey(k));
    }

    /**
     *
     * @param k
     * @return true, if successful
     */
    @Override
    public boolean containsKey(final K k) {
        return get(k) != null;
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
     * @throws UnsupportedOperationException
     */
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

        dcc.flushAll();
    }

    /**
     * Close.
     */
    @Override
    public synchronized void close() {
        if (isClosed()) {
            return;
        }

        dcc.disconnect();

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
     * @param k
     * @return
     */
    protected String generateKey(final K k) {
        return Strings.isEmpty(keyPrefix) ? Strings.base64Encode(N.stringOf(k).getBytes(Charsets.UTF_8))
                : (keyPrefix + Strings.base64Encode(N.stringOf(k).getBytes(Charsets.UTF_8)));
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
