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
 * </p>
 * <p>
 * Note: this is not a true Chronicle-Map integration. The behavior matches
 * {@code LocalCache} semantics and exists to keep the public API usable.
 * </p>
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
     * Creates a ChronicleMap-compatible cache with default settings.
     *
     * <p><b>Usage Examples:</b></p>
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
     * Creates a ChronicleMap-compatible cache.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, Integer> map = new ChronicleMap<>(1_024, 60_000L);
     * map.put("count", 42);
     * }</pre>
     *
     * @param capacity cache capacity
     * @param evictDelay eviction scan delay in milliseconds
     */
    public ChronicleMap(final int capacity, final long evictDelay) {
        this(capacity, evictDelay, DEFAULT_LIVE_TIME, DEFAULT_MAX_IDLE_TIME);
    }

    /**
     * Creates a ChronicleMap-compatible cache with custom default timings.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * Cache<String, String> map = new ChronicleMap<>(2_048, 30_000L, 3_600_000L, 300_000L);
     * map.put("session", "abc", 10_000L, 5_000L);
     * }</pre>
     *
     * @param capacity cache capacity
     * @param evictDelay eviction scan delay in milliseconds
     * @param defaultLiveTime default TTL in milliseconds
     * @param defaultMaxIdleTime default max idle time in milliseconds
     */
    public ChronicleMap(final int capacity, final long evictDelay, final long defaultLiveTime, final long defaultMaxIdleTime) {
        super(capacity, evictDelay, defaultLiveTime, defaultMaxIdleTime);
    }
}
