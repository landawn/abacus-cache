/*
 * Copyright (C) 2015 HaiYang Li
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.landawn.abacus.cache;

import java.util.concurrent.TimeUnit;

import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Properties;
import com.landawn.abacus.util.u.Optional;

/**
 *
 * @author Haiyang Li
 * @param <K> the key type
 * @param <V> the value type
 * @since 0.8
 */
public abstract class AbstractCache<K, V> implements Cache<K, V> {

    protected static final AsyncExecutor asyncExecutor = new AsyncExecutor(//
            N.max(64, IOUtil.CPU_CORES * 8), // coreThreadPoolSize
            N.max(128, IOUtil.CPU_CORES * 16), // maxThreadPoolSize
            180L, TimeUnit.SECONDS);

    protected final Properties<String, Object> properties = new Properties<>();

    protected long defaultLiveTime;

    protected long defaultMaxIdleTime;

    protected AbstractCache() {
        this(DEFAULT_LIVE_TIME, DEFAULT_MAX_IDLE_TIME);
    }

    protected AbstractCache(final long defaultLiveTime, final long defaultMaxIdleTime) {
        this.defaultLiveTime = defaultLiveTime;
        this.defaultMaxIdleTime = defaultMaxIdleTime;
    }

    /**
     *
     * @param k
     * @return
     */
    @Override
    public Optional<V> get(final K k) {
        return Optional.ofNullable(gett(k));
    }

    /**
     *
     * @param key
     * @param value
     * @return true, if successful
     */
    @Override
    public boolean put(final K key, final V value) {
        return put(key, value, defaultLiveTime, defaultMaxIdleTime);
    }

    /**
     *
     * @param k
     * @return
     */
    @Override
    public ContinuableFuture<Optional<V>> asyncGet(final K k) {
        return asyncExecutor.execute(() -> get(k));
    }

    /**
     *
     * @param k
     * @return
     */
    @Override
    public ContinuableFuture<V> asyncGett(final K k) {
        return asyncExecutor.execute(() -> gett(k));
    }

    /**
     *
     * @param k
     * @param v
     * @return
     */
    @Override
    public ContinuableFuture<Boolean> asyncPut(final K k, final V v) {
        return asyncExecutor.execute(() -> put(k, v));
    }

    /**
     *
     * @param k
     * @param v
     * @param liveTime
     * @param maxIdleTime
     * @return
     */
    @Override
    public ContinuableFuture<Boolean> asyncPut(final K k, final V v, final long liveTime, final long maxIdleTime) {
        return asyncExecutor.execute(() -> put(k, v, liveTime, maxIdleTime));
    }

    /**
     *
     * @param k
     * @return
     */
    @Override
    public ContinuableFuture<Void> asyncRemove(final K k) {
        return asyncExecutor.execute(() -> {
            remove(k);

            return null;
        });
    }

    /**
     * Async contains key.
     *
     * @param k
     * @return
     */
    @Override
    public ContinuableFuture<Boolean> asyncContainsKey(final K k) {
        return asyncExecutor.execute(() -> containsKey(k));
    }

    /**
     * Gets the properties.
     *
     * @return
     */
    @Override
    public Properties<String, Object> getProperties() {
        return properties;
    }

    /**
     * Gets the property.
     *
     * @param <T>
     * @param propName
     * @return
     */
    @Override
    public <T> T getProperty(final String propName) {
        return (T) properties.get(propName);
    }

    /**
     * Sets the property.
     *
     * @param <T>
     * @param propName
     * @param propValue
     * @return
     */
    @Override
    public <T> T setProperty(final String propName, final Object propValue) {
        return (T) properties.put(propName, propValue);
    }

    /**
     * Removes the property.
     *
     * @param <T>
     * @param propName
     * @return
     */
    @Override
    public <T> T removeProperty(final String propName) {
        return (T) properties.remove(propName);
    }
}
