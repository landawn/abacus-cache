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
import java.util.concurrent.atomic.AtomicLong;

import com.landawn.abacus.util.Charsets;
import com.landawn.abacus.util.N;
import com.landawn.abacus.util.Strings;

/**
 * A wrapper cache implementation that provides a standardized Cache interface for distributed cache clients.
 * This class adds key prefixing, error handling with retry logic, and adapts distributed cache
 * client operations to the standard Cache interface. It's designed to work with any
 * DistributedCacheClient implementation like Memcached or Redis.
 * 
 * <br><br>
 * Key features:
 * <ul>
 * <li>Automatic key prefixing for namespace isolation</li>
 * <li>Base64 encoding of keys for compatibility</li>
 * <li>Retry logic with configurable failure threshold</li>
 * <li>Transparent error recovery</li>
 * <li>Adaptation of TTL-only expiration to TTL+idle interface</li>
 * </ul>
 * 
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
 * DistributedCache<String, User> cache = new DistributedCache<>(
 *     client,
 *     "myapp:",           // key prefix
 *     100,                // max failures before stopping retries
 *     1000                // retry delay in ms
 * );
 *
 * cache.put("user:123", user, 3600000, 1800000);
 * User cached = cache.gett("user:123");
 * }</pre>
 *
 * @param <K> the key type
 * @param <V> the value type
 * @see AbstractCache
 * @see DistributedCacheClient
 * @see CacheFactory
 */
public class DistributedCache<K, V> extends AbstractCache<K, V> {

    /**
     * Default maximum number of consecutive failures before stopping retry attempts.
     */
    protected static final int DEFAULT_MAX_FAILED_NUMBER = 100;

    /**
     * Default delay in milliseconds between retry attempts after failures.
     */
    protected static final long DEFAULT_RETRY_DELAY = 1000;

    // ...
    private final DistributedCacheClient<V> dcc;

    private final String keyPrefix;

    private final int maxFailedNumForRetry;

    private final long retryDelay;

    // ...
    private final AtomicInteger failedCounter = new AtomicInteger();

    private final AtomicLong lastFailedTime = new AtomicLong(0);

    private volatile boolean isClosed = false;

    /**
     * Creates a DistributedCache with default retry configuration.
     * Uses an empty key prefix and default retry parameters.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = new DistributedCache<>(client);
     * }</pre>
     *
     * @param dcc the distributed cache client to wrap
     * @throws IllegalArgumentException if dcc is null
     */
    protected DistributedCache(final DistributedCacheClient<V> dcc) {
        this(dcc, Strings.EMPTY, DEFAULT_MAX_FAILED_NUMBER, DEFAULT_RETRY_DELAY);
    }

    /**
     * Creates a DistributedCache with a key prefix.
     * All keys will be prefixed for namespace isolation.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = new DistributedCache<>(client, "myapp:");
     * }</pre>
     *
     * @param dcc the distributed cache client to wrap
     * @param keyPrefix the prefix to prepend to all keys (empty string or null for no prefix)
     * @throws IllegalArgumentException if dcc is null
     */
    protected DistributedCache(final DistributedCacheClient<V> dcc, final String keyPrefix) {
        this(dcc, keyPrefix, DEFAULT_MAX_FAILED_NUMBER, DEFAULT_RETRY_DELAY);
    }

    /**
     * Creates a DistributedCache with full configuration.
     * Allows customization of key prefix and retry behavior.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
     * DistributedCache<String, User> cache = new DistributedCache<>(client, "myapp:", 100, 1000);
     * }</pre>
     *
     * @param dcc the distributed cache client to wrap
     * @param keyPrefix the prefix to prepend to all keys (empty string or null for no prefix)
     * @param maxFailedNumForRetry maximum consecutive failures before stopping retries
     * @param retryDelay delay in milliseconds between retry attempts
     * @throws IllegalArgumentException if dcc is null
     */
    protected DistributedCache(final DistributedCacheClient<V> dcc, final String keyPrefix, final int maxFailedNumForRetry, final long retryDelay) {
        if (dcc == null) {
            throw new IllegalArgumentException("DistributedCacheClient cannot be null");
        }

        this.keyPrefix = Strings.isEmpty(keyPrefix) ? Strings.EMPTY : keyPrefix;
        this.dcc = dcc;
        this.maxFailedNumForRetry = maxFailedNumForRetry;
        this.retryDelay = retryDelay;
    }

    /**
     * Retrieves a value from the distributed cache by its key.
     * Includes retry logic that temporarily disables operations after too many failures.
     * Keys are automatically prefixed and Base64 encoded.
     *
     * <br><br>
     * When the failure count exceeds maxFailedNumForRetry and within the retry delay period,
     * this method returns null immediately without attempting the operation. Exceptions are
     * caught and null is returned, with the failure counter incremented.
     *
     * @param k the cache key
     * @return the cached value, or {@code null} if not found, expired, evicted, retry threshold exceeded, or on error
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public V gett(final K k) {
        assertNotClosed();

        if ((failedCounter.get() > maxFailedNumForRetry) && ((System.currentTimeMillis() - lastFailedTime.get()) < retryDelay)) {
            return null;
        }

        V result = null;
        boolean isOK = false;

        try {
            result = dcc.get(generateKey(k));
            isOK = true;
        } catch (final Exception e) {
            // Log the exception if needed, but don't rethrow
            // isOK = false;
        } finally {
            if (isOK) {
                failedCounter.set(0);
                lastFailedTime.set(0);
            } else {
                lastFailedTime.set(System.currentTimeMillis());
                failedCounter.incrementAndGet();
            }
        }

        return result;
    }

    /**
     * Stores a key-value pair in the distributed cache.
     * If the key already exists, its value will be replaced with the new TTL.
     * Keys are automatically prefixed and Base64 encoded.
     *
     * <br><br>
     * Note: Distributed caches typically only support TTL-based expiration.
     * The maxIdleTime parameter is ignored by this implementation.
     *
     * @param k the cache key
     * @param v the value to cache
     * @param liveTime the time-to-live in milliseconds (0 means no expiration)
     * @param maxIdleTime the maximum idle time in milliseconds (ignored by distributed caches)
     * @return {@code true} if the operation was successful
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public boolean put(final K k, final V v, final long liveTime, final long maxIdleTime) {
        assertNotClosed();

        return dcc.set(generateKey(k), v, liveTime);
    }

    /**
     * Removes a key-value pair from the distributed cache.
     * This operation returns void regardless of whether the key existed or the operation succeeded.
     *
     * @param k the cache key
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public void remove(final K k) {
        assertNotClosed();

        dcc.delete(generateKey(k));
    }

    /**
     * Checks if the cache contains a specific key with a non-null value.
     * This is implemented by attempting to retrieve the value, so it may
     * return {@code false} if retry threshold is exceeded or network errors occur.
     *
     * @param k the cache key
     * @return {@code true} if the key exists and has a non-null value, {@code false} otherwise or on error
     */
    @Override
    public boolean containsKey(final K k) {
        return gett(k) != null;
    }

    /**
     * Returns the set of keys in the cache.
     * This operation is not supported for distributed caches due to
     * performance and consistency concerns.
     *
     * @return never returns normally
     * @throws UnsupportedOperationException always thrown
     */
    @Override
    public Set<K> keySet() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns the number of entries in the cache.
     * This operation is not supported for distributed caches due to
     * performance and consistency concerns.
     *
     * @return never returns normally
     * @throws UnsupportedOperationException always thrown
     */
    @Override
    public int size() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Removes all entries from all connected cache servers.
     * This is a destructive operation that affects all data across all servers.
     * Use with extreme caution in production environments.
     *
     * @throws IllegalStateException if the cache has been closed
     */
    @Override
    public void clear() {
        assertNotClosed();

        dcc.flushAll();
    }

    /**
     * Closes the cache and disconnects from all distributed cache servers.
     * After closing, the cache cannot be used - subsequent operations will throw IllegalStateException.
     * This method is idempotent and thread-safe - calling multiple times has no additional effect after the first call.
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
     * Checks if the cache has been closed.
     *
     * @return {@code true} if the cache is closed
     */
    @Override
    public boolean isClosed() {
        return isClosed;
    }

    /**
     * Generates the actual cache key by applying prefix and Base64 encoding.
     * The key is converted to string, UTF-8 encoded, then Base64 encoded
     * to ensure compatibility with all distributed cache systems. The prefix
     * is added before the Base64 encoded key if configured.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Assuming keyPrefix is "myapp:"
     * String cacheKey = generateKey("user:123");
     * // Result: "myapp:dXNlcjoxMjM=" (prefix + Base64 of "user:123")
     * }</pre>
     *
     * @param k the original key
     * @return the prefixed and Base64-encoded cache key
     * @throws IllegalArgumentException if k is null
     */
    protected String generateKey(final K k) {
        if (k == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        return Strings.isEmpty(keyPrefix) ? Strings.base64Encode(N.stringOf(k).getBytes(Charsets.UTF_8))
                : (keyPrefix + Strings.base64Encode(N.stringOf(k).getBytes(Charsets.UTF_8)));
    }

    /**
     * Ensures the cache is not closed before performing operations.
     *
     * @throws IllegalStateException if the cache has been closed
     */
    protected void assertNotClosed() {
        if (isClosed) {
            throw new IllegalStateException("This cache has been closed");
        }
    }
}