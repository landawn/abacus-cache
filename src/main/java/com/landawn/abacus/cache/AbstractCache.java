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

import java.util.concurrent.TimeUnit;

import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Properties;
import com.landawn.abacus.util.u.Optional;

/**
 * Abstract base class for all cache implementations providing common functionality.
 * This class implements the asynchronous operations, property management, and default
 * behaviors defined in the Cache interface. It serves as the foundation for all
 * concrete cache implementations in the framework.
 *
 * <p>Key features provided:
 * <ul>
 * <li>Asynchronous operation implementations using a shared thread pool</li>
 * <li>Default TTL and idle time management</li>
 * <li>Property bag for custom configuration</li>
 * <li>Optional-based wrapper methods</li>
 * </ul>
 *
 * <p>Subclasses must implement:
 * <ul>
 * <li>{@link #getOrNull(Object)} - Direct value retrieval</li>
 * <li>{@link #put(Object, Object, long, long)} - Storage with expiration</li>
 * <li>{@link #remove(Object)} - Entry removal</li>
 * <li>{@link #containsKey(Object)} - Key existence check</li>
 * <li>{@link #keySet()} - Key enumeration (may throw {@link UnsupportedOperationException} if unsupported)</li>
 * <li>{@link #size()} - Entry count (may be approximate or unsupported depending on the implementation)</li>
 * <li>{@link #clear()} - Bulk removal</li>
 * <li>{@link #close()} - Resource cleanup</li>
 * <li>{@link #isClosed()} - State check</li>
 * </ul>
 *
 * <p>Example of extending this class:
 * <pre>{@code
 * public class MyCache<K, V> extends AbstractCache<K, V> {
 *     private final Map<K, V> storage = new ConcurrentHashMap<>();
 *     
 *     @Override
 *     public V getOrNull(K key) {
 *         return storage.get(key);
 *     }
 *     
 *     @Override
 *     public boolean put(K key, V value, long liveTime, long maxIdleTime) {
 *         storage.put(key, value);
 *         // Handle expiration logic
 *         return true;
 *     }
 *     // ... implement other abstract methods
 * }
 * }</pre>
 *
 * @param <K> the key type
 * @param <V> the value type
 * @see Cache
 * @see LocalCache
 * @see DistributedCache
 */
public abstract class AbstractCache<K, V> implements Cache<K, V> {

    /**
     * Shared async executor for all cache implementations.
     * Configured with a thread pool sized based on CPU cores to efficiently
     * handle asynchronous cache operations without overwhelming the system.
     * Core pool size is max(64, CPU_CORES * 8), max pool size is max(128, CPU_CORES * 16),
     * and threads are kept alive for 180 seconds.
     */
    protected static final AsyncExecutor asyncExecutor = new AsyncExecutor(//
            N.max(64, IOUtil.CPU_CORES * 8), // coreThreadPoolSize
            N.max(128, IOUtil.CPU_CORES * 16), // maxThreadPoolSize
            180L, TimeUnit.SECONDS);

    /**
     * Property bag for storing custom configuration and metadata.
     * Can be used by cache implementations and users to store arbitrary properties.
     */
    protected final Properties<String, Object> properties = new Properties<>();

    /**
     * Default time-to-live for cache entries in milliseconds.
     * Used when put() is called without explicit TTL.
     */
    protected final long defaultLiveTime;

    /**
     * Default maximum idle time for cache entries in milliseconds.
     * Used when put() is called without explicit idle time.
     */
    protected final long defaultMaxIdleTime;

    /**
     * Creates an AbstractCache with default expiration times.
     * Uses {@link #DEFAULT_LIVE_TIME} (3 hours) and {@link #DEFAULT_MAX_IDLE_TIME} (30 minutes)
     * as the default TTL and idle time for entries added without explicit expiration settings.
     */
    protected AbstractCache() {
        this(DEFAULT_LIVE_TIME, DEFAULT_MAX_IDLE_TIME);
    }

    /**
     * Creates an AbstractCache with custom default expiration times.
     * These defaults are used when entries are added without explicit expiration.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Create cache with 1 hour TTL and 15 minutes idle time
     * AbstractCache<String, User> cache = new MyCache<>(3600000L, 900000L);
     * }</pre>
     *
     * @param defaultLiveTime default TTL in milliseconds for new entries (use 0 for no expiration)
     * @param defaultMaxIdleTime default max idle time in milliseconds for new entries (use 0 for no idle timeout)
     */
    protected AbstractCache(final long defaultLiveTime, final long defaultMaxIdleTime) {
        this.defaultLiveTime = defaultLiveTime;
        this.defaultMaxIdleTime = defaultMaxIdleTime;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This base implementation delegates to {@link #getOrNull(Object)} and wraps the result via
     * {@link Optional#ofNullable(Object)}. Subclasses that need different semantics should
     * override {@link #getOrNull(Object)} rather than this method.
     */
    @Override
    public Optional<V> get(final K key) {
        return Optional.ofNullable(getOrNull(key));
    }

    /**
     * {@inheritDoc}
     *
     * <p>This base implementation delegates to {@link #put(Object, Object, long, long)} using the
     * {@code defaultLiveTime} and {@code defaultMaxIdleTime} configured at construction time.
     */
    @Override
    public boolean put(final K key, final V value) {
        return put(key, value, defaultLiveTime, defaultMaxIdleTime);
    }

    /**
     * {@inheritDoc}
     *
     * <p>This base implementation submits a call to {@link #get(Object)} on {@link #asyncExecutor}.
     */
    @Override
    public ContinuableFuture<Optional<V>> asyncGet(final K key) {
        return asyncExecutor.execute(() -> get(key));
    }

    /**
     * {@inheritDoc}
     *
     * <p>This base implementation submits a call to {@link #getOrNull(Object)} on {@link #asyncExecutor}.
     */
    @Override
    public ContinuableFuture<V> asyncGetOrNull(final K key) {
        return asyncExecutor.execute(() -> getOrNull(key));
    }

    /**
     * {@inheritDoc}
     *
     * <p>This base implementation submits a call to {@link #put(Object, Object)} on {@link #asyncExecutor}.
     */
    @Override
    public ContinuableFuture<Boolean> asyncPut(final K key, final V value) {
        return asyncExecutor.execute(() -> put(key, value));
    }

    /**
     * {@inheritDoc}
     *
     * <p>This base implementation submits a call to {@link #put(Object, Object, long, long)} on
     * {@link #asyncExecutor}.
     */
    @Override
    public ContinuableFuture<Boolean> asyncPut(final K key, final V value, final long liveTime, final long maxIdleTime) {
        return asyncExecutor.execute(() -> put(key, value, liveTime, maxIdleTime));
    }

    /**
     * {@inheritDoc}
     *
     * <p>This base implementation submits a call to {@link #remove(Object)} on {@link #asyncExecutor}
     * and completes the returned future with a {@code null} result when the removal finishes.
     */
    @Override
    public ContinuableFuture<Void> asyncRemove(final K key) {
        return asyncExecutor.execute(() -> {
            remove(key);

            return null;
        });
    }

    /**
     * {@inheritDoc}
     *
     * <p>This base implementation submits a call to {@link #containsKey(Object)} on {@link #asyncExecutor}.
     */
    @Override
    public ContinuableFuture<Boolean> asyncContainsKey(final K key) {
        return asyncExecutor.execute(() -> containsKey(key));
    }

    /**
     * {@inheritDoc}
     *
     * <p>This base implementation returns the {@link #properties} instance held by this cache.
     */
    @Override
    public Properties<String, Object> getProperties() {
        return properties;
    }

    /**
     * {@inheritDoc}
     *
     * <p>This base implementation looks up the value in {@link #properties} and returns it
     * via an unchecked cast to {@code T}.
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T getProperty(final String propName) {
        return (T) properties.get(propName);
    }

    /**
     * {@inheritDoc}
     *
     * <p>This base implementation writes the value to {@link #properties} and returns the
     * previous value via an unchecked cast to {@code T}.
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T setProperty(final String propName, final Object propValue) {
        return (T) properties.put(propName, propValue);
    }

    /**
     * {@inheritDoc}
     *
     * <p>This base implementation removes the entry from {@link #properties} and returns the
     * removed value via an unchecked cast to {@code T}.
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T removeProperty(final String propName) {
        return (T) properties.remove(propName);
    }
}