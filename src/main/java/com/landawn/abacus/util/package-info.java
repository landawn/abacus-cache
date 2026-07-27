/*
 * Copyright (c) 2015, Haiyang Li.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Cache-backed coordination utilities that build on the clients in
 * {@link com.landawn.abacus.cache}.
 *
 * <p>This package extends the {@code com.landawn.abacus.util} namespace of {@code abacus-common}
 * with the pieces that need a cache backend; the core utilities of that namespace, and the library's
 * API naming conventions, are documented in {@code abacus-common}. It currently holds a single
 * class:
 *
 * <ul>
 * <li>{@link com.landawn.abacus.util.MemcachedLock} — a cross-JVM lock built on Memcached's atomic
 *     {@code add} operation, using {@link com.landawn.abacus.cache.SpyMemcached} as its client. Each
 *     lock is an expiring lease: the TTL bounds how long a crashed holder can block others, and
 *     acquisition makes exactly one server attempt without polling or queuing.</li>
 * </ul>
 *
 * <p><b>&#9888;&#65039; Lease safety.</b> {@code MemcachedLock} is best-effort, not a
 * correctness-grade distributed lock. Release deletes the key without proving ownership, and
 * eviction, restart, failover, a global flush, or a holder pausing past the TTL can all put two
 * clients in the critical section at once. Use it for advisory coordination — deduplicating work,
 * damping stampedes — and choose a lock service with fencing tokens and atomic conditional release
 * where overlap could corrupt data.
 *
 * <p>Using this package requires the {@code provided}-scope SpyMemcached dependency on your runtime
 * classpath.
 *
 * @see com.landawn.abacus.util.MemcachedLock
 * @see com.landawn.abacus.cache.SpyMemcached
 */
package com.landawn.abacus.util;
