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
 * A compatibility adapter that preserves the {@code ChronicleMap} class name.
 * <p>
 * This class is backed by {@link LocalCache} so existing code that references
 * {@code ChronicleMap} remains functional even when the optional Chronicle-Map
 * dependency is unavailable.
 * <p>
 * Note: this is not a true Chronicle-Map integration. The behavior matches
 * {@link LocalCache} semantics and exists solely to keep the public API usable.
 *
 * @param <K> the key type
 * @param <V> the value type
 *
 * @deprecated this is a compatibility wrapper; a dedicated Chronicle-Map adapter
 *             is not implemented yet
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
     * Cache<String, String> map = new ChronicleMap<>();   // capacity 1024, 60s eviction delay
     * map.stats().capacity();                             // returns 1024 (default capacity)
     * map.isClosed();                                     // returns false (freshly created)
     * map.put("k", "v");                                  // returns true (stored with default 3h TTL / 30min idle)
     * String value = map.getOrNull("k");                  // returns "v"
     * map.getOrNull("absent");                            // returns null (key not present)
     * map.getOrNull((String) null);                       // throws IllegalArgumentException (key must not be null)
     * }</pre>
     *
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
     * Cache<String, Integer> map = new ChronicleMap<>(1_024, 60_000L);   // capacity 1024, 60s eviction delay
     * map.stats().capacity();                                            // returns 1024 (capacity reflected in stats)
     * map.put("count", 42);                                              // returns true (uses default 3h TTL / 30min idle)
     * map.getOrNull("count");                                            // returns 42
     *
     * // An evictDelay of 0 is allowed and disables automatic background eviction.
     * Cache<String, Integer> noEvict = new ChronicleMap<>(64, 0L);   // valid
     *
     * // Edge cases (validated by the constructor):
     * new ChronicleMap<>(0, 60_000L);    // throws IllegalArgumentException (capacity must be positive)
     * new ChronicleMap<>(1_024, -1L);    // throws IllegalArgumentException (evictDelay must be non-negative)
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
     * // capacity 2048, 30s eviction delay, default 1h TTL, default 5min idle
     * Cache<String, String> map = new ChronicleMap<>(2_048, 30_000L, 3_600_000L, 300_000L);
     * map.stats().capacity();                       // returns 2048 (capacity reflected in stats)
     *
     * // Per-entry overrides win over the constructor defaults.
     * map.put("session", "abc", 10_000L, 5_000L);   // returns true (10s TTL, 5s idle for this entry)
     * map.getOrNull("session");                     // returns "abc"
     *
     * // A null value is accepted; getOrNull then returns null, indistinguishable from an absent key.
     * map.put("nullable", (String) null);           // returns true (uses the constructor defaults)
     * map.getOrNull("nullable");                    // returns null
     *
     * // Non-positive default times mean "no expiration" for entries added via put(key, value).
     * Cache<String, String> noExpire = new ChronicleMap<>(16, 0L, 0L, 0L);   // valid
     *
     * // Edge cases (validated by the constructor):
     * new ChronicleMap<>(0, 30_000L, 3_600_000L, 300_000L);      // throws IllegalArgumentException (capacity must be positive)
     * new ChronicleMap<>(2_048, -1L, 3_600_000L, 300_000L);      // throws IllegalArgumentException (evictDelay must be non-negative)
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
