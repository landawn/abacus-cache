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
import java.util.ConcurrentModificationException;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

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
 * <li>{@link #close()} - explicit early retirement when the owning application requires it.</li>
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
     * Core pool size is {@code max(64, CPU_CORES * 8)} and the configured maximum is
     * {@code max(128, CPU_CORES * 16)}; because the work queue is unbounded, the pool never
     * grows beyond the core size (the maximum is effectively unused). Idle threads are reclaimed
     * after 180 seconds. Worker threads are daemon threads and the pool is shut down
     * by a JVM exit hook, so an application that only used async cache operations can
     * still exit normally.
     */
    protected static final AsyncExecutor asyncExecutor = createAsyncExecutor();

    /**
     * Creates the shared executor and registers its JVM shutdown hook.
     *
     * @return the executor used by asynchronous cache operations
     * @throws IllegalStateException if JVM shutdown has begun before the hook can be registered
     */
    private static AsyncExecutor createAsyncExecutor() throws IllegalStateException {
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
     * Backed by a synchronized map so property operations through this cache or the live view
     * returned by {@link #getProperties()} are serialized. The custom wrapper below also delegates
     * compound {@link java.util.Map} operations such as {@code putIfAbsent}, {@code compute}, and
     * {@code merge} directly to that synchronized map. Copying and string formatting traverse
     * the map under the same lock. Individual operations on the live
     * collection views are synchronized too, but iterators obtained from those views are not safe
     * while another thread mutates the properties because the backing map's mutex is internal.
     * Use {@link Properties#copy()} to obtain a stable snapshot for traversal.
     */
    protected final Properties<String, Object> properties = new SynchronizedProperties<>();

    /**
     * A {@link Properties} implementation whose entire {@link java.util.Map} surface delegates
     * compound/default operations to one synchronized backing map. Traversals implemented by
     * {@code Properties} itself also need synchronization or a snapshot of that map.
     */
    private static final class SynchronizedProperties<K, V> extends Properties<K, V> {

        SynchronizedProperties() {
            values = Collections.synchronizedMap(values);
        }

        @Override
        public V putIfAbsent(final K key, final V value) {
            return values.putIfAbsent(key, value);
        }

        @Override
        public V getOrDefault(final Object key, final V defaultValue) {
            return values.getOrDefault(key, defaultValue);
        }

        @Override
        public boolean remove(final Object key, final Object value) {
            return values.remove(key, value);
        }

        @Override
        public V replace(final K key, final V value) {
            return values.replace(key, value);
        }

        @Override
        public boolean replace(final K key, final V oldValue, final V newValue) {
            return values.replace(key, oldValue, newValue);
        }

        /**
         * {@inheritDoc}
         *
         * @throws IllegalArgumentException if {@code action} is {@code null}
         * @throws RuntimeException if {@code action} throws while processing an entry
         * @throws ConcurrentModificationException if {@code action} structurally modifies this map during traversal
         */
        @Override
        public void forEach(final BiConsumer<? super K, ? super V> action) throws IllegalArgumentException, RuntimeException, ConcurrentModificationException {
            N.checkArgNotNull(action, cs.action);

            values.forEach(action);
        }

        /**
         * {@inheritDoc}
         *
         * @throws IllegalArgumentException if {@code function} is {@code null}
         * @throws RuntimeException if {@code function} throws while computing a replacement value
         * @throws ConcurrentModificationException if {@code function} structurally modifies this map during traversal
         */
        @Override
        public void replaceAll(final BiFunction<? super K, ? super V, ? extends V> function)
                throws IllegalArgumentException, RuntimeException, ConcurrentModificationException {
            N.checkArgNotNull(function, cs.function);

            values.replaceAll(function);
        }

        /**
         * {@inheritDoc}
         *
         * @throws IllegalArgumentException if {@code mappingFunction} is {@code null}
         * @throws RuntimeException if {@code mappingFunction} throws while computing a missing value
         * @throws ConcurrentModificationException if {@code mappingFunction} structurally modifies this map
         */
        @Override
        public V computeIfAbsent(final K key, final Function<? super K, ? extends V> mappingFunction)
                throws IllegalArgumentException, RuntimeException, ConcurrentModificationException {
            N.checkArgNotNull(mappingFunction, cs.mappingFunction);

            return values.computeIfAbsent(key, mappingFunction);
        }

        /**
         * {@inheritDoc}
         *
         * @throws IllegalArgumentException if {@code remappingFunction} is {@code null}
         * @throws RuntimeException if {@code remappingFunction} throws while updating a present non-null value
         * @throws ConcurrentModificationException if {@code remappingFunction} structurally modifies this map
         */
        @Override
        public V computeIfPresent(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction)
                throws IllegalArgumentException, RuntimeException, ConcurrentModificationException {
            N.checkArgNotNull(remappingFunction, cs.remappingFunction);

            return values.computeIfPresent(key, remappingFunction);
        }

        /**
         * {@inheritDoc}
         *
         * @throws IllegalArgumentException if {@code remappingFunction} is {@code null}
         * @throws RuntimeException if {@code remappingFunction} throws while computing the new mapping
         * @throws ConcurrentModificationException if {@code remappingFunction} structurally modifies this map
         */
        @Override
        public V compute(final K key, final BiFunction<? super K, ? super V, ? extends V> remappingFunction)
                throws IllegalArgumentException, RuntimeException, ConcurrentModificationException {
            N.checkArgNotNull(remappingFunction, cs.remappingFunction);

            return values.compute(key, remappingFunction);
        }

        /**
         * {@inheritDoc}
         *
         * @throws NullPointerException if {@code value} is {@code null}, as required by {@link java.util.Map#merge}
         * @throws IllegalArgumentException if {@code remappingFunction} is {@code null}
         * @throws RuntimeException if {@code remappingFunction} throws while combining a present non-null value with {@code value}
         * @throws ConcurrentModificationException if {@code remappingFunction} structurally modifies this map
         */
        @Override
        public V merge(final K key, final V value, final BiFunction<? super V, ? super V, ? extends V> remappingFunction)
                throws NullPointerException, IllegalArgumentException, RuntimeException, ConcurrentModificationException {
            // Preserve Map.merge's null-value exception while validating in parameter order.
            Objects.requireNonNull(value);
            N.checkArgNotNull(remappingFunction, cs.remappingFunction);

            return values.merge(key, value, remappingFunction);
        }

        @Override
        public Properties<K, V> copy() {
            synchronized (values) {
                return Properties.create(values);
            }
        }

        @Override
        public String toString() {
            synchronized (values) {
                return super.toString();
            }
        }
    }

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
     * These values are passed as the requested defaults when entries are added via
     * {@link #put(Object, Object)}. A concrete adapter may ignore expiration controls that its
     * backing cache does not support; see {@link Cache#put(Object, Object, long, long)}.
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
     * {@link Optional#ofNullable(Object)}. Consequently, if a concrete cache permits a key to be
     * explicitly mapped to {@code null}, this method returns an empty optional for that mapping.
     * Such a mapping is distinguishable from absence only if the concrete implementation's
     * {@link #containsKey(Object)} contract says so. Subclasses that need different semantics
     * should override {@link #getOrNull(Object)} rather than this method.
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
     * @throws IllegalStateException {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     * @throws RuntimeException if the delegated cache operation propagates a backend, serialization, or loading failure
     */
    @Override
    public Optional<V> get(final K key) throws IllegalStateException, IllegalArgumentException, RuntimeException {
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
     * @throws IllegalStateException {@inheritDoc}
     * @throws IllegalArgumentException {@inheritDoc}
     * @throws RuntimeException if the delegated cache operation propagates a backend write, serialization, or cache callback failure
     * @throws StackOverflowError if the delegated Kryo-backed distributed client encounters a cycle made only of
     *         collections, maps, or arrays while serializing {@code value}
     */
    @Override
    public boolean put(final K key, final V value) throws IllegalStateException, IllegalArgumentException, RuntimeException, StackOverflowError {
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
     * @throws IllegalStateException if the shared {@link #asyncExecutor} has been explicitly shut down
     * @throws RejectedExecutionException if its backing thread pool rejects the task, including during JVM shutdown
     */
    @Override
    public ContinuableFuture<Optional<V>> asyncGet(final K key) throws IllegalStateException, RejectedExecutionException {
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
     * @throws IllegalStateException if the shared {@link #asyncExecutor} has been explicitly shut down
     * @throws RejectedExecutionException if its backing thread pool rejects the task, including during JVM shutdown
     */
    @Override
    public ContinuableFuture<V> asyncGetOrNull(final K key) throws IllegalStateException, RejectedExecutionException {
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
     * @throws IllegalStateException if the shared {@link #asyncExecutor} has been explicitly shut down
     * @throws RejectedExecutionException if its backing thread pool rejects the task, including during JVM shutdown
     */
    @Override
    public ContinuableFuture<Boolean> asyncPut(final K key, final V value) throws IllegalStateException, RejectedExecutionException {
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
     * @throws IllegalStateException if the shared {@link #asyncExecutor} has been explicitly shut down
     * @throws RejectedExecutionException if its backing thread pool rejects the task, including during JVM shutdown
     */
    @Override
    public ContinuableFuture<Boolean> asyncPut(final K key, final V value, final long liveTime, final long maxIdleTime)
            throws IllegalStateException, RejectedExecutionException {
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
     * @throws IllegalStateException if the shared {@link #asyncExecutor} has been explicitly shut down
     * @throws RejectedExecutionException if its backing thread pool rejects the task, including during JVM shutdown
     */
    @Override
    public ContinuableFuture<Void> asyncRemove(final K key) throws IllegalStateException, RejectedExecutionException {
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
     * @throws IllegalStateException if the shared {@link #asyncExecutor} has been explicitly shut down
     * @throws RejectedExecutionException if its backing thread pool rejects the task, including during JVM shutdown
     */
    @Override
    public ContinuableFuture<Boolean> asyncContainsKey(final K key) throws IllegalStateException, RejectedExecutionException {
        return asyncExecutor.execute(() -> containsKey(key));
    }

    /**
     * {@inheritDoc}
     *
     * <p>This base implementation returns the {@link #properties} instance held by this cache.
     * Individual map operations, including compound operations such as {@code compute} and
     * {@code merge}, are synchronized. Iteration over {@code keySet()}, {@code values()}, or
     * {@code entrySet()} is not safe during concurrent mutation because the synchronized backing
     * map's mutex is not exposed; call {@link Properties#copy()} first when a stable traversal is
     * required.
     *
     * <p>The property bag accepts {@code null} names and values. As specified by {@link java.util.Map#merge},
     * {@code merge} still rejects a {@code null} incoming value with {@link NullPointerException}; a
     * {@code null} result from a remapping function removes the mapping.
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
     * via an unchecked cast to {@code T}. A {@code null} property name is accepted.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, String> cache = new LocalCache<>(100, 0);
     * cache.setProperty("ttlSeconds", 60);       // returns null (no previous mapping)
     *
     * Integer ttl = cache.getProperty("ttlSeconds");
     * ttl.intValue();                            // returns 60
     *
     * // Edge: an unset property returns null (regardless of the inferred type T).
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
     * previous value via an unchecked cast to {@code T}. Like the data operations, this convenience
     * mutator is lifecycle-guarded: once {@link #isClosed()} reports {@code true}, it throws
     * {@link IllegalStateException}. The read accessors ({@link #getProperty(String)},
     * {@link #getProperties()}) remain usable after close. The mutable map returned by
     * {@code getProperties()} is deliberately exposed directly, so callers that mutate that map
     * bypass this convenience method's lifecycle check.
     * A {@code null} property name or value is accepted while the cache is open.
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
     * @throws IllegalStateException if this cache has been closed
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T setProperty(final String propName, final Object propValue) throws IllegalStateException {
        assertNotClosedForPropertyMutation();

        return (T) properties.put(propName, propValue);
    }

    /**
     * {@inheritDoc}
     *
     * <p>This base implementation removes the entry from {@link #properties} and returns the
     * removed value via an unchecked cast to {@code T}. Like {@link #setProperty(String, Object)},
     * this convenience mutator is lifecycle-guarded and throws {@link IllegalStateException} once
     * the cache has been closed. Removing through the mutable map returned by
     * {@link #getProperties()} bypasses this method's lifecycle check.
     * A {@code null} property name is accepted while the cache is open.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, String> cache = new LocalCache<>(100, 0);
     * cache.setProperty("foo", "bar");           // returns null (no previous mapping)
     *
     * String removed = cache.removeProperty("foo");
     * assert "bar".equals(removed);
     * cache.getProperty("foo");                  // returns null (entry removed)
     *
     * // Edge: removing a name that was never set returns null.
     * cache.removeProperty("never-set");         // returns null
     * }</pre>
     *
     * @throws IllegalStateException if this cache has been closed
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T removeProperty(final String propName) throws IllegalStateException {
        assertNotClosedForPropertyMutation();

        return (T) properties.remove(propName);
    }

    /**
     * Rejects property mutation on a closed cache. Property reads remain usable after close (like
     * other configuration accessors), but mutating the property bag of a closed cache is almost
     * certainly a lifecycle bug in the caller, so it fails fast like the data operations do.
     *
     * @throws IllegalStateException if this cache has been closed
     */
    private void assertNotClosedForPropertyMutation() throws IllegalStateException {
        if (isClosed()) {
            throw new IllegalStateException("This cache has been closed");
        }
    }
}
