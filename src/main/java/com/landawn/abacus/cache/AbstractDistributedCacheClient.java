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

import java.util.Collection;
import java.util.Map;

/**
 * Abstract base class for distributed cache client implementations.
 * This class provides common functionality and default implementations for methods
 * that are not universally supported across all distributed cache systems.
 * Concrete implementations like SpyMemcached and JRedis extend this class.
 * 
 * <br><br>
 * Key features:
 * <ul>
 * <li>Stores and provides access to server URL</li>
 * <li>Default implementations for optional operations</li>
 * <li>Utility method for time conversion</li>
 * </ul>
 * 
 * <br>
 * Subclasses must implement:
 * <ul>
 * <li>{@link #get(String)}</li>
 * <li>{@link #set(String, Object, long)}</li>
 * <li>{@link #delete(String)}</li>
 * <li>{@link #incr(String)} and {@link #incr(String, int)}</li>
 * <li>{@link #decr(String)} and {@link #decr(String, int)}</li>
 * <li>{@link #disconnect()}</li>
 * </ul>
 *
 * @param <T> the type of objects to be cached
 * @see DistributedCacheClient
 * @see SpyMemcached
 * @see JRedis
 */
public abstract class AbstractDistributedCacheClient<T> implements DistributedCacheClient<T> {

    private final String serverUrl;

    /**
     * Constructs an AbstractDistributedCacheClient with the specified server URL.
     * The server URL format is implementation-specific but typically includes
     * host and port information.
     *
     * @param serverUrl the server URL(s) for connecting to the distributed cache
     */
    protected AbstractDistributedCacheClient(final String serverUrl) {
        this.serverUrl = serverUrl;
    }

    /**
     * Returns the server URL(s) this client is connected to.
     * The format is implementation-specific and may include multiple servers.
     *
     * @return the server URL(s) for this client
     */
    @Override
    public String serverUrl() {
        return serverUrl;
    }

    /**
     * Retrieves multiple objects from the cache using varargs.
     * This default implementation throws UnsupportedOperationException.
     * Subclasses that support bulk operations should override this method.
     *
     * @param keys the cache keys
     * @return the map of cache keys to values (never returns normally in default implementation)
     * @throws UnsupportedOperationException always thrown by this default implementation
     */
    @Override
    public Map<String, T> getBulk(final String... keys) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Retrieves multiple objects from the cache using a collection.
     * This default implementation throws UnsupportedOperationException.
     * Subclasses that support bulk operations should override this method.
     *
     * @param keys the collection of cache keys
     * @return the map of cache keys to values (never returns normally in default implementation)
     * @throws UnsupportedOperationException always thrown by this default implementation
     */
    @Override
    public Map<String, T> getBulk(final Collection<String> keys) throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Flushes all data from all connected cache servers.
     * This default implementation throws UnsupportedOperationException.
     * Subclasses that support flush operations should override this method.
     *
     * Warning: This is a destructive operation that removes all data.
     *
     * @throws UnsupportedOperationException always thrown by this default implementation
     */
    @Override
    public void flushAll() throws UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    /**
     * Converts milliseconds to seconds for cache operations.
     * Most distributed caches use seconds for TTL, so this utility method
     * converts milliseconds (used by the Cache interface) to seconds.
     * The method rounds up to ensure the TTL is not shorter than requested.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * int seconds1 = toSeconds(1500);  // Returns 2 (1.5s rounds up)
     * int seconds2 = toSeconds(2000);  // Returns 2 (exactly 2s)
     * int seconds3 = toSeconds(999);   // Returns 1 (rounds up to 1s)
     * }</pre>
     *
     * @param liveTime the time-to-live in milliseconds
     * @return the time-to-live in seconds, rounded up
     * @throws IllegalArgumentException if the time value exceeds Integer.MAX_VALUE seconds
     */
    protected int toSeconds(final long liveTime) {
        final long seconds = (liveTime % 1000 == 0) ? (liveTime / 1000) : (liveTime / 1000) + 1;

        if (seconds > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Time value too large: " + liveTime + " ms (exceeds max integer seconds)");
        }

        return (int) seconds;
    }
}