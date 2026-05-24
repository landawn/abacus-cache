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

/**
 * A compatibility adapter for the ChronicleMap API name.
 * <p>
 * This class is now backed by {@link LocalCache} so existing code that references
 * {@code ChronicleMap} remains functional even when the optional Chronicle-Map
 * dependency is unavailable.
 * <p>
 * Note: this is not a true Chronicle-Map integration. The behavior matches
 * {@code LocalCache} semantics and exists to keep the public API usable.
 *
 * @param <K> the key type
 * @param <V> the value type
 *
 * @deprecated This is a compatibility wrapper. A dedicated Chronicle-Map adapter
 *             is not implemented yet.
 */
@Deprecated
public class ChronicleMap<K, V> extends LocalCache<K, V> {

    /**
     * Creates a ChronicleMap-compatible cache with default settings: capacity of 1024 entries,
     * an eviction scan delay of 60,000 ms (1 minute), and the default TTL/idle times defined by
     * {@link Cache#DEFAULT_LIVE_TIME} (3 hours) and {@link Cache#DEFAULT_MAX_IDLE_TIME} (30 minutes).
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, String> map = new ChronicleMap<>();
     * map.put("k", "v");
     * String value = map.getOrNull("k");
     * }</pre>
     */
    public ChronicleMap() {
        this(1024, 60_000L);
    }

    /**
     * Creates a ChronicleMap-compatible cache with the specified capacity and eviction delay,
     * using the default TTL/idle times defined by {@link Cache#DEFAULT_LIVE_TIME} (3 hours)
     * and {@link Cache#DEFAULT_MAX_IDLE_TIME} (30 minutes).
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, Integer> map = new ChronicleMap<>(1_024, 60_000L);
     * map.put("count", 42);
     * }</pre>
     *
     * @param capacity the maximum number of entries the cache can hold (must be positive)
     * @param evictDelay the delay in milliseconds between eviction runs (must be non-negative;
     *                   0 disables automatic eviction)
     * @throws IllegalArgumentException if capacity is not positive or evictDelay is negative
     */
    public ChronicleMap(final int capacity, final long evictDelay) {
        this(capacity, evictDelay, DEFAULT_LIVE_TIME, DEFAULT_MAX_IDLE_TIME);
    }

    /**
     * Creates a ChronicleMap-compatible cache with custom capacity, eviction delay, and default timings.
     * These defaults are used when entries are added via {@link #put(Object, Object)} without
     * explicit expiration; individual entries can override them by calling
     * {@link #put(Object, Object, long, long)}.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * Cache<String, String> map = new ChronicleMap<>(2_048, 30_000L, 3_600_000L, 300_000L);
     * map.put("session", "abc", 10_000L, 5_000L);
     * }</pre>
     *
     * @param capacity the maximum number of entries the cache can hold (must be positive)
     * @param evictDelay the delay in milliseconds between eviction runs (must be non-negative;
     *                   0 disables automatic eviction)
     * @param defaultLiveTime default TTL in milliseconds for entries added without explicit
     *                        expiration (0 or negative for no TTL expiration)
     * @param defaultMaxIdleTime default max idle time in milliseconds for entries added without
     *                           explicit expiration (0 or negative for no idle timeout)
     * @throws IllegalArgumentException if capacity is not positive or evictDelay is negative
     */
    public ChronicleMap(final int capacity, final long evictDelay, final long defaultLiveTime, final long defaultMaxIdleTime) {
        super(capacity, evictDelay, defaultLiveTime, defaultMaxIdleTime);
    }
}
