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

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.landawn.abacus.exception.UncheckedIOException;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;
import com.landawn.abacus.parser.ParserFactory;
import com.landawn.abacus.util.AddrUtil;
import com.landawn.abacus.util.ExceptionUtil;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

/**
 *
 * @param <T>
 */
public class SpyMemcached<T> extends AbstractDistributedCacheClient<T> {

    static final Logger logger = LoggerFactory.getLogger(SpyMemcached.class);

    private final MemcachedClient mc;

    /**
     *
     *
     * @param serverUrl
     */
    public SpyMemcached(final String serverUrl) {
        this(serverUrl, DEFAULT_TIMEOUT);
    }

    /**
     *
     *
     * @param serverUrl
     * @param timeout
     */
    public SpyMemcached(final String serverUrl, final long timeout) {
        super(serverUrl);

        final Transcoder<Object> transcoder = ParserFactory.isKryoAvailable() ? new KryoTranscoder<>() : null;

        final ConnectionFactory connFactory = new DefaultConnectionFactory() {
            @Override
            public long getOperationTimeout() {
                return timeout;
            }

            @Override
            public Transcoder<Object> getDefaultTranscoder() {
                if (transcoder != null) {
                    return transcoder;
                } else {
                    return super.getDefaultTranscoder();
                }
            }
        };

        mc = createSpyMemcachedClient(serverUrl, connFactory);
    }

    /**
     *
     * @param key
     * @return
     */
    @SuppressWarnings("unchecked")
    @Override
    public T get(final String key) {
        return (T) mc.get(key);
    }

    /**
     *
     * @param key
     * @return
     */
    @SuppressWarnings("unchecked")
    public Future<T> asyncGet(final String key) {
        return (Future<T>) mc.asyncGet(key);
    }

    /**
     * Gets the bulk.
     *
     * @param keys
     * @return
     */
    @SuppressWarnings("unchecked")
    @Override
    public final Map<String, T> getBulk(final String... keys) {
        return (Map<String, T>) mc.getBulk(keys);
    }

    /**
     * Async get bulk.
     *
     * @param keys
     * @return
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public final Future<Map<String, T>> asyncGetBulk(final String... keys) {
        return (Future) mc.asyncGetBulk(keys);
    }

    /**
     * Gets the bulk.
     *
     * @param keys
     * @return
     */
    @SuppressWarnings("unchecked")
    @Override
    public Map<String, T> getBulk(final Collection<String> keys) {
        return (Map<String, T>) mc.getBulk(keys);
    }

    /**
     * Async get bulk.
     *
     * @param keys
     * @return
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Future<Map<String, T>> asyncGetBulk(final Collection<String> keys) {
        return (Future) mc.asyncGetBulk(keys);
    }

    /**
     *
     * @param key
     * @param obj
     * @param liveTime
     * @return true, if successful
     */
    @Override
    public boolean set(final String key, final T obj, final long liveTime) {
        return resultOf(mc.set(key, toSeconds(liveTime), obj));
    }

    /**
     *
     * @param key
     * @param obj
     * @param liveTime
     * @return
     */
    public Future<Boolean> asyncSet(final String key, final T obj, final long liveTime) {
        return mc.set(key, toSeconds(liveTime), obj);
    }

    /**
     *
     * @param key
     * @param obj
     * @param liveTime
     * @return true, if successful
     */
    public boolean add(final String key, final T obj, final long liveTime) {
        return resultOf(mc.add(key, toSeconds(liveTime), obj));
    }

    /**
     *
     * @param key
     * @param obj
     * @param liveTime
     * @return
     */
    public Future<Boolean> asyncAdd(final String key, final T obj, final long liveTime) {
        return mc.add(key, toSeconds(liveTime), obj);
    }

    /**
     *
     * @param key
     * @param obj
     * @param liveTime
     * @return true, if successful
     */
    public boolean replace(final String key, final T obj, final long liveTime) {
        return resultOf(mc.replace(key, toSeconds(liveTime), obj));
    }

    /**
     *
     * @param key
     * @param obj
     * @param liveTime
     * @return
     */
    public Future<Boolean> asyncReplace(final String key, final T obj, final long liveTime) {
        return mc.replace(key, toSeconds(liveTime), obj);
    }

    /**
     *
     * @param key
     * @return true, if successful
     */
    @Override
    public boolean delete(final String key) {
        return resultOf(mc.delete(key));
    }

    /**
     *
     * @param key
     * @return
     */
    public Future<Boolean> asyncDelete(final String key) {
        return mc.delete(key);
    }

    /**
     *
     * @param key
     * @return
     */
    @Override
    public long incr(final String key) {
        return mc.incr(key, 1);
    }

    /**
     *
     * @param key
     * @param deta
     * @return
     */
    @Override
    public long incr(final String key, final int deta) {
        return mc.incr(key, deta);
    }

    /**
     *
     * @param key
     * @param deta
     * @param defaultValue
     * @return
     */
    public long incr(final String key, final int deta, final long defaultValue) {
        return mc.incr(key, deta, defaultValue, -1);
    }

    /**
     *
     * @param key
     * @param deta
     * @param defaultValue
     * @param liveTime
     * @return
     */
    public long incr(final String key, final int deta, final long defaultValue, final long liveTime) {
        return mc.incr(key, deta, defaultValue, toSeconds(liveTime));
    }

    /**
     *
     * @param key
     * @return
     */
    @Override
    public long decr(final String key) {
        return mc.decr(key, 1);
    }

    /**
     *
     * @param key
     * @param deta
     * @return
     */
    @Override
    public long decr(final String key, final int deta) {
        return mc.decr(key, deta);
    }

    /**
     *
     * @param key
     * @param deta
     * @param defaultValue
     * @return
     */
    public long decr(final String key, final int deta, final long defaultValue) {
        return mc.decr(key, deta, defaultValue, -1);
    }

    /**
     *
     * @param key
     * @param deta
     * @param defaultValue
     * @param liveTime
     * @return
     */
    public long decr(final String key, final int deta, final long defaultValue, final long liveTime) {
        return mc.decr(key, deta, defaultValue, toSeconds(liveTime));
    }

    /**
     * Flush all.
     */
    @Override
    public void flushAll() {
        resultOf(mc.flush());
    }

    /**
     * Async flush all.
     *
     * @return
     */
    public Future<Boolean> asyncFlushAll() {
        return mc.flush();
    }

    /**
     *
     * @param delay
     * @return true, if successful
     */
    public boolean flushAll(final long delay) {
        return resultOf(mc.flush(toSeconds(delay)));
    }

    /**
     * Async flush all.
     *
     * @param delay
     * @return
     */
    public Future<Boolean> asyncFlushAll(final long delay) {
        return mc.flush(toSeconds(delay));
    }

    /**
     * Disconnect.
     */
    @Override
    public void disconnect() {
        mc.shutdown();
    }

    /**
     *
     * @param timeout
     */
    public void disconnect(final long timeout) {
        mc.shutdown(timeout, TimeUnit.MICROSECONDS);
    }

    /**
     *
     * @param <R>
     * @param future
     * @return
     */
    protected <R> R resultOf(final Future<R> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw ExceptionUtil.toRuntimeException(e, true);
        }
    }

    /**
     * Creates the spy memcached client.
     *
     * @param serverUrl
     * @param connFactory
     * @return
     * @throws UncheckedIOException the unchecked IO exception
     */
    protected static MemcachedClient createSpyMemcachedClient(final String serverUrl, final ConnectionFactory connFactory) throws UncheckedIOException {
        try {
            return new MemcachedClient(connFactory, AddrUtil.getAddressList(serverUrl));
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to create Memcached client.", e);
        }
    }
}
