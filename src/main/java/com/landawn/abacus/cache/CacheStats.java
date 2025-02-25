/*
 * Copyright 2025 Haiyang Li. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.landawn.abacus.cache;

/**
 * Part of this code is copied from <a href="https://github.com/ben-manes/caffeine">caffeine</a> under Apache License 2.0.
 *
 * <p>
 * A snapshot of the statistics of a {@link Cache} at a point in time.
 */
public record CacheStats(int capacity, int size, long putCount, long getCount, long hitCount, long missCount, long evictionCount, long maxMemory,
        long dataSize) {

}
