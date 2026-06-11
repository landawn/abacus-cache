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

import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.landawn.abacus.util.AsyncExecutor;
import com.landawn.abacus.util.ContinuableFuture;
import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.MoreExecutors;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Properties;
import com.landawn.abacus.util.u.Optional;

/**
 * Abstract base class for all cache implementations providing common functionality.
 * Implements the asynchronous operations, property management, and default
 * behaviors defined in the {@link Cache} interface, and serves as the foundation
 * for all concrete cache implementations in the framework.
 *
 * <p>Key features provided:
 * <ul>
 * <li>Asynchronous operation implementations using a shared thread pool.</li>
 * <li>Default TTL and idle time management.</li>
 * <li>Property bag for custom configuration.</li>
 * <li>{@link Optional}-based wrapper methods.</li>
 * </ul>
 *
 * <p>Subclasses must implement:
 * <ul>
 * <li>{@link #getOrNull(Object)} - direct value retrieval.</li>
 * <li>{@link #put(Object, Object, long, long)} - storage with expiration.</li>
 * <li>{@link #remove(Object)} - entry removal.</li>
 * <li>{@link #containsKey(Object)} - key existence check.</li>
 * <li>{@link #keySet()} - key enumeration (may throw {@link UnsupportedOperationException} if unsupported).</li>
 * <li>{@link #size()} - entry count (may be approximate or unsupported depending on the implementation).</li>
 * <li>{@link #clear()} - bulk removal.</li>
 * <li>{@link #close()} - resource cleanup.</li>
 * <li>{@link #isClosed()} - state check.</li>
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
     * Shared async executor used by all cache implementations.
     * Configured with a thread pool sized based on CPU cores to efficiently
     * handle asynchronous cache operations without overwhelming the system.
     * Core pool size is {@code max(64, CPU_CORES * 8)}; the work queue is unbounded,
     * so the pool does not grow beyond the core size. Idle threads are reclaimed
     * after 180 seconds. Worker threads are daemon threads and the pool is shut down
     * by a JVM exit hook, so an application that only used async cache operations can
     * still exit normally.
     */
    protected static final AsyncExecutor asyncExecutor = createAsyncExecutor();

    private static AsyncExecutor createAsyncExecutor() {
        final ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(//
                N.max(64, IOUtil.CPU_CORES * 8), // coreThreadPoolSize
                N.max(128, IOUtil.CPU_CORES * 16), // maxThreadPoolSize
                180L, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

        threadPoolExecutor.allowCoreThreadTimeOut(true);

        // Daemon threads + delayed-shutdown JVM exit hook: idle pool threads must not
        // prevent the JVM from exiting after the application finishes.
        return new AsyncExecutor(MoreExecutors.getExitingExecutorService(threadPoolExecutor));
    }

    /**
     * Property bag for storing custom configuration and metadata.
     * Can be used by cache implementations and users to store arbitrary properties.
     * Backed by a synchronized map so concurrent property access through this cache
     * (or the live view returned by {@link #getProperties()}) is safe; iteration over
     * the view still requires external synchronization, as usual for synchronized maps.
     */
    protected final Properties<String, Object> properties = new Properties<>() {
        {
            values = Collections.synchronizedMap(values);
        }
    };

    /**
     * Default time-to-live for cache entries, in milliseconds.
     * Used when {@link #put(Object, Object)} is called without an explicit TTL.
     */
    protected final long defaultLiveTime;

    /**
     * Default maximum idle time for cache entries, in milliseconds.
     * Used when {@link #put(Object, Object)} is called without an explicit idle time.
     */
    protected final long defaultMaxIdleTime;

    /**
     * Creates an {@code AbstractCache} with default expiration times.
     * Uses {@link Cache#DEFAULT_LIVE_TIME} (3 hours) and
     * {@link Cache#DEFAULT_MAX_IDLE_TIME} (30 minutes) as the default TTL
     * and idle time for entries added without explicit expiration settings.
     */
    protected AbstractCache() {
        this(DEFAULT_LIVE_TIME, DEFAULT_MAX_IDLE_TIME);
    }

    /**
     * Creates an {@code AbstractCache} with the supplied default expiration times.
     * These defaults are used when entries are added via {@link #put(Object, Object)}
     * without explicit expiration.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Create cache with 1 hour TTL and 15 minutes idle time
     * AbstractCache<String, User> cache = new MyCache<>(3600000L, 900000L);
     * }</pre>
     *
     * @param defaultLiveTime default TTL in milliseconds for new entries (use 0 or any non-positive value for no expiration)
     * @param defaultMaxIdleTime default max idle time in milliseconds for new entries (use 0 or any non-positive value for no idle timeout)
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
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, String> cache = new LocalCache<>(100, 0);
     * cache.put("k", "v");                       // seed an entry
     *
     * Optional<String> hit = cache.get("k");
     * hit.isPresent();                           // returns true
     * hit.get();                                 // returns "v"
     *
     * // Edge: missing key yields an empty Optional, not null.
     * Optional<String> miss = cache.get("absent");
     * miss.isPresent();                          // returns false
     * miss.orElse("fallback");                   // returns "fallback"
     * }</pre>
     *
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
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, String> cache = new LocalCache<>(100, 0);
     *
     * // Stores using the construction-time defaults (3h live, 30min idle by default).
     * boolean stored = cache.put("k", "v");      // returns true
     * cache.get("k").get();                      // returns "v"
     *
     * // Re-putting the same key overwrites the existing value.
     * cache.put("k", "v2");                      // returns true
     * cache.get("k").get();                      // returns "v2"
     * }</pre>
     *
     */
    @Override
    public boolean put(final K key, final V value) {
        return put(key, value, defaultLiveTime, defaultMaxIdleTime);
    }

    /**
     * {@inheritDoc}
     *
     * <p>This base implementation submits a call to {@link #get(Object)} on {@link #asyncExecutor}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, String> cache = new LocalCache<>(100, 0);
     * cache.put("k", "v");                       // seed an entry
     *
     * ContinuableFuture<Optional<String>> f = cache.asyncGet("k");
     * Optional<String> hit = f.get();            // throws InterruptedException, ExecutionException
     * hit.isPresent();                           // returns true
     * hit.get();                                 // returns "v"
     *
     * // Edge: missing key resolves to an empty Optional.
     * cache.asyncGet("absent").get().isPresent();  // returns false
     * }</pre>
     *
     */
    @Override
    public ContinuableFuture<Optional<V>> asyncGet(final K key) {
        return asyncExecutor.execute(() -> get(key));
    }

    /**
     * {@inheritDoc}
     *
     * <p>This base implementation submits a call to {@link #getOrNull(Object)} on {@link #asyncExecutor}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, String> cache = new LocalCache<>(100, 0);
     * cache.put("k", "v");                       // seed an entry
     *
     * ContinuableFuture<String> f = cache.asyncGetOrNull("k");
     * f.get();                                   // returns "v"; throws InterruptedException, ExecutionException
     *
     * // Edge: missing key resolves to null (no Optional wrapper).
     * cache.asyncGetOrNull("absent").get();      // returns null
     * }</pre>
     *
     */
    @Override
    public ContinuableFuture<V> asyncGetOrNull(final K key) {
        return asyncExecutor.execute(() -> getOrNull(key));
    }

    /**
     * {@inheritDoc}
     *
     * <p>This base implementation submits a call to {@link #put(Object, Object)} on {@link #asyncExecutor}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, String> cache = new LocalCache<>(100, 0);
     *
     * // Stores asynchronously using the construction-time default expiration.
     * ContinuableFuture<Boolean> f = cache.asyncPut("k", "v");
     * f.get();                                   // returns true; throws InterruptedException, ExecutionException
     * cache.getOrNull("k");                      // returns "v"
     *
     * // Overwriting an existing key also completes with true.
     * cache.asyncPut("k", "v2").get();           // returns true
     * cache.getOrNull("k");                      // returns "v2"
     * }</pre>
     *
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
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, String> cache = new LocalCache<>(100, 0);
     *
     * // Stores asynchronously with an explicit 5s live time and 5s idle time.
     * ContinuableFuture<Boolean> f = cache.asyncPut("k", "v", 5000L, 5000L);
     * f.get();                                   // returns true; throws InterruptedException, ExecutionException
     * cache.getOrNull("k");                      // returns "v"
     *
     * // Edge: liveTime <= 0 means no expiration by TTL.
     * cache.asyncPut("forever", "v", 0L, 0L).get();  // returns true
     * cache.getOrNull("forever");                    // returns "v"
     * }</pre>
     *
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
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, String> cache = new LocalCache<>(100, 0);
     * cache.put("k", "v");                       // seed an entry
     *
     * ContinuableFuture<Void> f = cache.asyncRemove("k");
     * f.get();                                   // returns null (Void); throws InterruptedException, ExecutionException
     * cache.getOrNull("k");                      // returns null (entry removed)
     *
     * // Edge: removing an absent key still completes normally with a null result.
     * cache.asyncRemove("absent").get();         // returns null
     * }</pre>
     *
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
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, String> cache = new LocalCache<>(100, 0);
     * cache.put("k", "v");                       // seed an entry
     *
     * ContinuableFuture<Boolean> f = cache.asyncContainsKey("k");
     * f.get();                                   // returns true; throws InterruptedException, ExecutionException
     *
     * // Edge: key never stored resolves to false.
     * cache.asyncContainsKey("absent").get();    // returns false
     * }</pre>
     *
     */
    @Override
    public ContinuableFuture<Boolean> asyncContainsKey(final K key) {
        return asyncExecutor.execute(() -> containsKey(key));
    }

    /**
     * {@inheritDoc}
     *
     * <p>This base implementation returns the {@link #properties} instance held by this cache.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, String> cache = new LocalCache<>(100, 0);
     *
     * Properties<String, Object> props = cache.getProperties();
     * props.isEmpty();                           // returns true (no properties set yet)
     *
     * // The same backing instance is returned on each call and reflects later writes.
     * cache.setProperty("region", "us-east");    // returns null (no previous mapping)
     * cache.getProperties() == props;            // returns true (same instance)
     * cache.getProperties().get("region");       // returns "us-east"
     * }</pre>
     *
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
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, String> cache = new LocalCache<>(100, 0);
     * cache.setProperty("ttlSeconds", 60);       // returns null (no previous mapping)
     *
     * Integer ttl = cache.getProperty("ttlSeconds");
     * ttl.intValue();                            // returns 60
     *
     * // Edge: an unset property returns null ({@code null} is returned regardless of the inferred type {@code T}).
     * String missing = cache.getProperty("absent");  // returns null
     * }</pre>
     *
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
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, String> cache = new LocalCache<>(100, 0);
     *
     * // First write for a name has no previous mapping.
     * String prev1 = cache.setProperty("name", "alpha");  // returns null
     * cache.getProperty("name");                          // returns "alpha"
     *
     * // Re-setting the same name returns the value it replaced.
     * String prev2 = cache.setProperty("name", "beta");   // returns "alpha"
     * cache.getProperty("name");                          // returns "beta"
     * }</pre>
     *
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
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, String> cache = new LocalCache<>(100, 0);
     * cache.setProperty("foo", "bar");           // returns null (no previous mapping)
     *
     * String removed = cache.removeProperty("foo");
     * removed;                                   // returns "bar"
     * cache.getProperty("foo");                  // returns null (entry removed)
     *
     * // Edge: removing a name that was never set returns null.
     * cache.removeProperty("never-set");         // returns null
     * }</pre>
     *
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T removeProperty(final String propName) {
        return (T) properties.remove(propName);
    }
}