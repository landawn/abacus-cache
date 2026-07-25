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

package com.landawn.abacus.util;

import com.landawn.abacus.annotation.SuppressFBWarnings;
import com.landawn.abacus.cache.SpyMemcached;
import com.landawn.abacus.logging.Logger;
import com.landawn.abacus.logging.LoggerFactory;

import net.spy.memcached.util.StringUtils;

/**
 * A distributed lock implementation using Memcached as the coordination service.
 * This class provides a simple distributed locking mechanism that can be used to coordinate
 * access to shared resources across multiple JVMs or servers. It leverages Memcached's atomic
 * add operation so that, while the lock key remains present, only one client can acquire it.
 * The lease-safety limitations below explain why key loss or expiry can still create overlapping
 * holders.
 *
 * <p>Key features:
 * <ul>
 * <li>Best-effort lease exclusion across multiple processes while the lock key remains present</li>
 * <li>Automatic lock expiration to prevent deadlocks</li>
 * <li>Optional value storage with the lock</li>
 * <li>Non-retrying lock acquisition (one atomic server attempt; no polling or contention wait)</li>
 * </ul>
 *
 * <p>Implementation notes:
 * <ul>
 * <li>Uses Memcached's atomic add operation for lock acquisition</li>
 * <li>Lock expiration prevents permanent deadlocks if holder crashes</li>
 * <li>Not reentrant - same client cannot acquire lock twice</li>
 * <li>No queue or fairness guarantees - it is a simple expiring lease</li>
 * </ul>
 *
 * <p><b>&#9888;&#65039; Lease safety limitation:</b> this is a best-effort, expiring lease rather than a
 * correctness-grade distributed lock. The classic memcached text protocol used by the underlying
 * client has no atomic compare-and-delete operation (modern memcached offers one only via the meta
 * protocol, which SpyMemcached does not speak), so {@link #tryUnlock(Object)} deletes the key without
 * proving ownership. If a holder pauses longer than
 * the TTL, another client can acquire the expired key and the original holder can subsequently
 * delete that newer lease. Memcached eviction, restart, failover, or a global flush may also remove
 * the key before its TTL, allowing overlapping holders even without a pause. Do not use this class
 * where overlapping critical sections could corrupt data; prefer a coordination system with
 * ownership tokens/fencing and atomic conditional release.
 *
 * <p><b>No ownership-safe renewal:</b> this class intentionally has no lease-renewal method. Calling
 * {@code touch} or {@code replace} through {@link #client()} cannot atomically prove that the lease
 * still belongs to the caller; after expiry and reacquisition it could extend or overwrite another
 * holder's lease. Choose a TTL that covers the critical section, or use a lock service with atomic
 * token-checked renewal and fencing.
 *
 * <p>Example usage:
 * <pre>{@code
 * try (MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211")) {
 *     // Simple lock without value
 *     if (lock.tryLock("resource1", 30000)) { // 30-second TTL on the lock
 *         try {
 *             performExclusiveOperation();
 *         } finally {
 *             lock.unlockQuietly("resource1"); // operational release failures are logged
 *         }
 *     } else {
 *         System.out.println("Could not acquire lock");
 *     }
 *
 *     // Lock with associated diagnostic value
 *     String lockHolder = "server-1"; // stable diagnostic identifier for this process
 *     if (lock.tryLock("resource2", lockHolder, 60000)) {
 *         try {
 *             String currentHolder = lock.get("resource2");
 *             System.out.println("Lock held by: " + currentHolder);
 *         } finally {
 *             lock.unlockQuietly("resource2");
 *         }
 *     }
 * }
 * }</pre>
 *
 * <p><b>Thread Safety:</b> The base implementation is thread-safe and multiple threads can safely
 * call methods on the same instance. A subclass must keep any {@link #toKey(Object)} override
 * thread-safe and deterministic for that guarantee to hold. The lock itself is not reentrant.
 * {@link #close()} publishes the closed state before shutting down the client, so ordinary
 * operations begun after close starts fail with {@link IllegalStateException}
 * ({@link #unlockQuietly(Object)} instead reports {@code false}); an already in-flight operation
 * may still surface the underlying client's shutdown exception. Ordinary operations check this
 * terminal state before validating arguments or deriving a key, so a closed instance fails
 * consistently and does not invoke a subclass's {@link #toKey(Object)} override. The quiet-release
 * method deliberately validates its target and derived key first, then maps a closed state to
 * {@code false}; deterministic programming errors are not swallowed as transient release failures.
 *
 * @param <K> the type of lock identifiers used as keys (typically String)
 * @param <V> the type of optional metadata values associated with locks
 * @see SpyMemcached
 */
public class MemcachedLock<K, V> implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(MemcachedLock.class);

    private final SpyMemcached<V> mc;

    /** Published before client shutdown so operations begun during close fail deterministically. */
    private volatile boolean isClosed;

    /**
     * Creates a new MemcachedLock instance backed by the specified Memcached server(s).
     * Multiple {@code host:port} addresses may be separated by commas, whitespace, or both.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Both clients are closed when the block exits.
     * try (MemcachedLock<String, String> single = new MemcachedLock<>("localhost:11211");
     *         MemcachedLock<String, String> multi = new MemcachedLock<>("server1:11211,server2:11211")) {
     *     // acquire and release leases through single or multi
     * }
     * }</pre>
     *
     * @param serverUrl one or more {@code host:port} addresses separated by commas, whitespace,
     *                  or both; must not be null, empty, or blank
     * @throws IllegalArgumentException if serverUrl is null, empty, or blank, or contains no
     *         valid {@code host:port} addresses
     * @throws RuntimeException if {@code serverUrl} cannot be parsed (e.g., an unresolvable hostname)
     *         or local client/socket setup fails. Because connections are established asynchronously
     *         by the underlying SpyMemcached IO thread, a resolvable but unreachable or down server
     *         does <b>not</b> fail construction; operations against it fail later with timeouts.
     */
    public MemcachedLock(final String serverUrl) {
        N.checkArgNotBlank(serverUrl, "serverUrl");

        mc = new SpyMemcached<>(serverUrl);
    }

    /**
     * Attempts to acquire a lock on the specified target for the given duration.
     * This method stores a value-less marker (an empty byte array) as the lock value.
     * The lock will be automatically released after the specified live time expires, ensuring that
     * locks don't persist indefinitely if a holder crashes or fails to release them.
     *
     * <p>This is a non-spinning operation: it makes a single atomic attempt and never retries
     * or busy-waits. It does, however, block the calling thread for the duration of the one
     * Memcached round-trip (up to the configured SpyMemcached operation timeout). If the lock
     * is already held by another client, this method returns {@code false} without retrying. The requested
     * {@code liveTime} is converted to Memcached's second-granularity TTL before being forwarded
     * to the server, so sub-second values are rounded up to the next second.
     * The implementation uses Memcached's atomic add operation so only one client can acquire the
     * key while it remains present; expiry or key loss is subject to the lease limitations above.
     *
     * <p>Important considerations:
     * <ul>
     * <li>The lock is not reentrant - the same client cannot acquire the same lock twice</li>
     * <li>Choose an appropriate liveTime to balance between deadlock prevention and operational needs</li>
     * <li>Always make a best-effort release in a finally block so contenders need not wait for TTL expiry</li>
     * <li><b>Memcached TTL upper bound:</b> Memcached treats any TTL greater than 30 days
     *     (2,592,000 seconds = 2,592,000,000 ms) as an <em>absolute Unix timestamp</em>.
     *     The underlying {@link SpyMemcached} adapter converts longer relative {@code liveTime}
     *     values to absolute expiration timestamps before sending them to Memcached.</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * try (MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211")) {
     *     // Basic lock usage with 30-second TTL (30000 ms -> 30 s on the server)
     *     if (lock.tryLock("resource1", 30000)) {
     *         try {
     *             performOperation();
     *         } finally {
     *             lock.unlockQuietly("resource1"); // operational release failures are logged
     *         }
     *     } else {
     *         System.out.println("Failed to acquire lock - already held by another process");
     *     }
     * }
     * }</pre>
     *
     * @param target the target resource on which to acquire the lock (must not be null)
     * @param liveTime the time-to-live in milliseconds before the lock automatically expires. Must be
     *                 positive: a zero or non-expiring lock is disallowed because it would risk a
     *                 permanent deadlock if the holder died before releasing it. This is a deliberate
     *                 exception to the lenient TTL handling used by the cache {@code put} APIs. Converted
     *                 to whole seconds for Memcached, with fractional seconds rounded up; values whose
     *                 absolute expiration exceeds Memcached's signed 32-bit field are rejected.
     * @return {@code true} if the lock was successfully acquired, {@code false} if it's already held
     * @throws IllegalStateException if this lock client has been closed or is being closed
     * @throws IllegalArgumentException if target is null, liveTime is not positive or cannot be
     *         represented by Memcached's expiration field, or if the key
     *         derived from {@code target} (via {@code toKey}) is rejected by the memcached client —
     *         empty, longer than 250 bytes (UTF-8), or containing a space, CR, LF, or NUL byte. The default
     *         {@code toKey} uses the target's string form verbatim, so composite targets whose string
     *         representation is JSON-like (maps, beans) typically need a sanitizing {@code toKey} override
     * @throws RuntimeException if the Memcached operation fails. The lock state is then
     *         indeterminate: the {@code add} command may have reached the server even though its
     *         response was lost or timed out, in which case the lock IS held server-side (under this
     *         client's marker) until the TTL expires and no caller will ever {@code tryUnlock} it.
     *         Prefer short TTLs where this matters for availability.
     * @see #tryLock(Object, Object, long)
     * @see #tryUnlock(Object)
     */
    public boolean tryLock(final K target, final long liveTime) {
        return tryLock(target, null, liveTime);
    }

    /**
     * Attempts to acquire a lock on the specified target with an associated value.
     * This method allows storing additional information with the lock, such as the
     * identity of the lock holder or lock metadata. The lock will be automatically
     * released after the specified live time expires. The requested live time is converted to
     * whole seconds (rounded up if needed) before being sent to Memcached. The implementation uses
     * Memcached's atomic add operation (via mc.add) which only succeeds if the key
     * doesn't already exist, providing mutual exclusion while the lease key remains present.
     *
     * <p>The value can be retrieved using {@link #get(Object)} while the lock is held.
     * This is useful for debugging or for implementing more complex locking protocols
     * where knowing the lock holder is important.
     *
     * <p>Common use cases for the value parameter:
     * <ul>
     * <li>Storing the hostname or IP address of the lock holder</li>
     * <li>Recording the thread ID or process ID that acquired the lock</li>
     * <li>Storing a timestamp of when the lock was acquired</li>
     * <li>Adding contextual information for debugging distributed systems</li>
     * </ul>
     *
     * <p><b>Memcached TTL upper bound:</b> Memcached treats any TTL greater than 30 days
     * (2,592,000 seconds = 2,592,000,000 ms) as an <em>absolute Unix timestamp</em>.
     * The underlying {@link SpyMemcached} adapter converts longer relative {@code liveTime}
     * values to absolute expiration timestamps before sending them to Memcached.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * try (MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211")) {
     *     // Example 1: Store hostname with lock (60000 ms -> 60 s TTL)
     *     String lockHolder = "server-1"; // stable diagnostic identifier for this process
     *     if (lock.tryLock("resource1", lockHolder, 60000)) {
     *         try {
     *             System.out.println("Lock acquired by: " + lock.get("resource1"));
     *         } finally {
     *             lock.unlockQuietly("resource1");
     *         }
     *     } else {
     *         System.out.println("Lock is held by: " + lock.get("resource1"));
     *     }
     * }
     *
     * // Example 2: Store structured metadata (V must be a compatible type,
     * // e.g., MemcachedLock<String, Map<String, Object>>)
     * try (MemcachedLock<String, Map<String, Object>> metaLock = new MemcachedLock<>("localhost:11211")) {
     *     Map<String, Object> metadata = new HashMap<>();
     *     metadata.put("host", "server1");
     *     metadata.put("thread", Thread.currentThread().getName());
     *     metadata.put("timestamp", System.currentTimeMillis());
     *     if (metaLock.tryLock("resource2", metadata, 30000)) {
     *         try {
     *             Map<String, Object> current = metaLock.get("resource2"); // diagnostic snapshot
     *         } finally {
     *             metaLock.unlockQuietly("resource2");
     *         }
     *     }
     * }
     * }</pre>
     *
     * @param target the target resource on which to acquire the lock (must not be null)
     * @param value the value to associate with the lock (can be {@code null}; a {@code null}
     *              value is stored as the same value-less marker used by {@link #tryLock(Object, long)})
     * @param liveTime the time-to-live in milliseconds before the lock automatically expires (must be positive;
     *                 converted to whole seconds for Memcached, with fractional seconds rounded up;
     *                 rejected if its absolute expiration exceeds Memcached's signed 32-bit field)
     * @return {@code true} if the lock was successfully acquired, {@code false} if it's already held
     * @throws IllegalStateException if this lock client has been closed or is being closed
     * @throws IllegalArgumentException if target is null, liveTime is not positive or cannot be
     *         represented by Memcached's expiration field, or if the key
     *         derived from {@code target} (via {@code toKey}) is rejected by the memcached client —
     *         empty, longer than 250 bytes (UTF-8), or containing a space, CR, LF, or NUL byte. The default
     *         {@code toKey} uses the target's string form verbatim, so composite targets whose string
     *         representation is JSON-like (maps, beans) typically need a sanitizing {@code toKey} override
     * @throws RuntimeException if the Memcached operation fails. The lock state is then
     *         indeterminate: the {@code add} command may have reached the server even though its
     *         response was lost or timed out, in which case the lock IS held server-side (under this
     *         client's value) until the TTL expires and no caller will ever {@code tryUnlock} it.
     *         Prefer short TTLs where this matters for availability.
     * @see #tryLock(Object, long)
     * @see #get(Object)
     * @see #tryUnlock(Object)
     */
    @SuppressWarnings("unchecked")
    public boolean tryLock(final K target, final V value, final long liveTime) {
        assertOpen();
        N.checkArgNotNull(target, "target");
        N.checkArgPositive(liveTime, "liveTime");

        final String key = validatedKey(target);
        final V lockValue = value == null ? (V) N.EMPTY_BYTE_ARRAY : value;

        try {
            final boolean acquired = mc.add(key, lockValue, liveTime);

            if (logger.isDebugEnabled()) {
                logger.debug(acquired ? "Acquired lock for key: " + key + " (liveTime=" + liveTime + "ms)" : "Lock already held for key: " + key);
            }

            return acquired;
        } catch (final IllegalArgumentException e) {
            throw e;
        } catch (final Exception e) {
            if (logger.isWarnEnabled()) {
                logger.warn("Memcached lock acquisition failed (liveTime=" + liveTime + " ms); acquisition state may be indeterminate", e);
            }

            throw new RuntimeException("Failed to acquire lock for key: " + key, e);
        }
    }

    /**
     * Checks whether a lock is currently held on the specified target.
     * This method performs a read operation (mc.get) to determine lock status without
     * attempting to acquire or modify the lock. Returns true if a value exists for the
     * lock key, false otherwise.
     *
     * <p><b>&#9888;&#65039; Point-in-time observation:</b> Due to the distributed nature and timing, a lock could expire or be
     * acquired between checking and subsequent operations. This is a point-in-time check
     * and should not be relied upon for critical synchronization logic. Always use the
     * return value of {@link #tryLock(Object, long)} or {@link #tryLock(Object, Object, long)}
     * to determine if you successfully acquired the lock rather than checking first with
     * this method.
     *
     * <p>This method is primarily useful for:
     * <ul>
     * <li>Monitoring and diagnostics</li>
     * <li>Logging and alerting when resources are locked</li>
     * <li>Non-critical decision making where race conditions are acceptable</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * try (MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211")) {
     *
     * // Example 1: Check lock status for monitoring
     * if (lock.isLocked("resource1")) {                         // true if the key exists (even a value-less lock counts as held)
     *     System.out.println("Resource is currently locked");   // reached when isLocked() returned true
     * } else {                                                  // false: no key present
     *     System.out.println("Resource is available");          // reached when isLocked() returned false
     * }
     *
     * // Example 2: INCORRECT usage - race condition
     * if (!lock.isLocked("resource1")) {       // point-in-time check only; not atomic with the lock below
     *     // Lock could be acquired by another process here!
     *     boolean acquired = lock.tryLock("resource1", 30000); // another process may have raced in
     *     if (acquired) lock.unlockQuietly("resource1");       // cleanup; the pre-check was still incorrect
     * }
     *
     * // Example 3: CORRECT usage - atomic check
     * if (lock.tryLock("resource1", 30000)) {     // true: acquired atomically (single add-if-absent attempt)
     *     try {
     *         // Lock successfully acquired
     *     } finally {
     *         lock.unlockQuietly("resource1");    // operational release failures are logged
     *     }
     * }
     * }
     * }</pre>
     *
     * @param target the target resource whose lock status is to be checked (must not be null)
     * @return {@code true} if the lock is currently held, {@code false} otherwise
     * @throws IllegalStateException if this lock client has been closed or is being closed
     * @throws IllegalArgumentException if target is null, or if the key derived from {@code target}
     *         (via {@code toKey}) is rejected by the memcached client (empty, longer than 250 bytes,
     *         or containing a space, CR, LF, or NUL byte)
     * @throws RuntimeException if the Memcached operation fails
     * @see #tryLock(Object, long)
     * @see #tryLock(Object, Object, long)
     */
    public boolean isLocked(final K target) {
        assertOpen();
        N.checkArgNotNull(target, "target");

        return mc.get(validatedKey(target)) != null;
    }

    /**
     * Retrieves the value associated with a lock on the specified target.
     * If no lock exists (key not found), this method returns {@code null}.
     * If the lock stores an empty byte array (the default when using {@link #tryLock(Object, long)}),
     * {@code null} is returned for convenience to distinguish empty values from actual data.
     *
     * <p>This method is useful for:
     * <ul>
     * <li>Identifying which client holds a lock</li>
     * <li>Storing lock metadata or state information</li>
     * <li>Inspecting the claimed holder for diagnostics (not enforcing ownership)</li>
     * <li>Debugging distributed locking issues</li>
     * </ul>
     *
     * <p><b>&#9888;&#65039; Typed-value caveats:</b>
     * <ul>
     * <li>This method performs an unchecked cast from Object to V. Ensure the type parameter V
     * matches the actual type of the stored value to avoid ClassCastException at runtime.</li>
     * <li>The returned value represents a snapshot at the time of the call. The lock could
     * expire or be released immediately after retrieval.</li>
     * <li>Returns {@code null} if the lock doesn't exist or if it stores an empty byte array</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * try (MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211")) {
     *
     * // Example 1: Check who holds the lock
     * String holder = lock.get("resource1");   // the stored value, or null if absent / value-less lock
     * if (holder != null) {
     *     System.out.println("Lock held by: " + holder);            // reached when get() returned a non-null value
     * } else {                                                      // null: no lock, or a value-less tryLock(target, liveTime)
     *     System.out.println("No lock exists or value is empty");   // reached when get() returned null
     * }
     *
     * // Example 2: Inspect the claimed holder for diagnostics (not as an ownership proof)
     * String myId = "server-1"; // stable diagnostic identifier for this process
     * if (lock.tryLock("resource2", myId, 60000)) {       // true: acquired, myId stored as the value
     *     try {
     *         String observedHolder = lock.get("resource2");
     *         System.out.println("Claimed holder: " + observedHolder);
     *         performOperation();
     *     } finally {
     *         // Do not perform a read-before-delete here: it is racy and can itself mask a work failure.
     *         lock.unlockQuietly("resource2");
     *     }
     * }
     * }
     *
     * // Example 3: Retrieve structured metadata with a matching value type
     * try (MemcachedLock<String, Map<String, Object>> metaLock = new MemcachedLock<>("localhost:11211")) {
     *     Map<String, Object> lockInfo = metaLock.get("resource3"); // stored map, or null if absent
     *     if (lockInfo != null) {
     *         System.out.println("Locked by: " + lockInfo.get("host"));
     *         System.out.println("Thread: " + lockInfo.get("thread"));
     *     }
     * }
     * }</pre>
     *
     * @param target the target resource whose associated lock value is to be retrieved (must not be null)
     * @return the value associated with the lock, or {@code null} if the target is not locked
     *         or if the lock stores an empty byte array
     * @throws IllegalStateException if this lock client has been closed or is being closed
     * @throws IllegalArgumentException if target is null, or if the key derived from {@code target}
     *         (via {@code toKey}) is rejected by the memcached client (empty, longer than 250 bytes,
     *         or containing a space, CR, LF, or NUL byte)
     * @throws ClassCastException if the stored value is not compatible with {@code V}; because of generic
     *         type erasure this is typically surfaced at the call site rather than inside this method
     * @throws RuntimeException if the Memcached operation fails
     * @see #tryLock(Object, Object, long)
     * @see #isLocked(Object)
     */
    @SuppressWarnings("unchecked")
    public V get(final K target) {
        assertOpen();
        N.checkArgNotNull(target, "target");

        final Object value = mc.get(validatedKey(target));

        return (V) (value instanceof byte[] && ((byte[]) value).length == 0 ? null : value);
    }

    /**
     * Releases the lock on the specified target.
     * This method immediately removes the lock by deleting the key from Memcached (via mc.remove),
     * making the target available for other clients to acquire. It's important to always unlock
     * in a finally block to ensure locks are released even if exceptions occur.
     *
     * <p><b>&#9888;&#65039; No ownership verification:</b> This implementation deletes the key
     * unconditionally. Any client can release any lock, including one held by a different client.
     * If your TTL is short relative to how long the critical section may take (due to GC pauses,
     * scheduling, network jitter), the original holder's lock can expire and be reacquired by a
     * second client before the first client's unlock call runs — at which point the first client
     * deletes the second client's lock and a third client may immediately acquire it. A safe
     * "delete-if-mine" requires an atomic compare-and-delete primitive (e.g., Redis Lua scripts,
     * or memcached's meta-protocol {@code md <key> C<cas>}), which the classic text protocol used
     * by the underlying SpyMemcached client does not provide.
     *
     * <p>Implementing ownership verification by reading {@link #get(Object)} and comparing before
     * calling {@code tryUnlock} is <b>also racy</b> (the lock can expire between the read and the
     * delete) and only narrows the window — it does not eliminate it.
     *
     * <p>Best practices:
     * <ul>
     * <li>Release the lock in a finally block to ensure cleanup; in a finally block prefer
     *     {@link #unlockQuietly(Object)}, which logs and returns {@code false} on a communication
     *     error instead of throwing (a throwing {@code tryUnlock} in finally can mask the exception from
     *     the guarded code)</li>
     * <li>Do not rely on a read-before-delete ownership check for correctness; it remains racy</li>
     * <li>Don't assume tryUnlock() always succeeds - check the return value if needed</li>
     * <li>Be aware that locks can expire automatically, so tryUnlock() may return false</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * try (MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211")) {
     *     // Example 1: Inspect the explicit release result when no guarded exception can be masked.
     *     if (lock.tryLock("release-probe", 30000)) {
     *         boolean unlocked = lock.tryUnlock("release-probe"); // true: key deleted; false: already expired/removed
     *         if (!unlocked) {
     *             System.out.println("Lock may have already expired or been removed");   // reached when tryUnlock() returned false
     *         }
     *     }
     *
     *     // Example 2: In a failure-sensitive finally block, use the quiet counterpart.
     *     if (lock.tryLock("resource2", 60000)) {
     *         try {
     *             performOperation();
     *         } finally {
     *             lock.unlockQuietly("resource2");
     *         }
     *     }
     * }
     * }</pre>
     *
     * @param target the target resource whose lock is to be released (must not be null)
     * @return {@code true} if an entry was deleted from Memcached for this target (regardless of
     *         which client originally acquired the lock), {@code false} if no entry existed
     *         (e.g., the lock had already expired or was never acquired)
     * @throws IllegalStateException if this lock client has been closed or is being closed
     * @throws IllegalArgumentException if target is null, or if the key derived from {@code target}
     *         (via {@code toKey}) is rejected by the memcached client (empty, longer than 250 bytes,
     *         or containing a space, CR, LF, or NUL byte)
     * @throws RuntimeException if the Memcached operation fails
     * @see #unlockQuietly(Object)
     * @see #tryLock(Object, long)
     * @see #tryLock(Object, Object, long)
     */
    public boolean tryUnlock(final K target) {
        assertOpen();
        N.checkArgNotNull(target, "target");

        final String key = validatedKey(target);

        try {
            final boolean released = mc.remove(key);

            if (logger.isDebugEnabled()) {
                logger.debug(released ? "Released lock for key: " + key : "No lock to release for key: " + key);
            }

            return released;
        } catch (final IllegalArgumentException e) {
            throw e;
        } catch (final Exception e) {
            if (logger.isWarnEnabled()) {
                logger.warn("Memcached lock release failed; the lock may remain held until its lease expires", e);
            }

            throw new RuntimeException("Failed to release lock for key: " + key, e);
        }
    }

    /**
     * Releases the lock on the specified target without throwing when the Memcached operation fails.
     * This is the "quiet" counterpart of {@link #tryUnlock(Object)}, intended for use inside a
     * {@code finally} block: if releasing the lock fails because of a network or protocol error, this
     * method logs the failure at {@code WARN} level and returns {@code false} instead of throwing, so
     * a communication failure cannot mask an exception thrown by the guarded critical section.
     * Invalid arguments, an invalid derived key, or an exception thrown by {@code toKey} still
     * propagates; keep the target immutable and key derivation deterministic.
     *
     * <p>Like {@link #tryUnlock(Object)}, this method deletes the key unconditionally and performs
     * <b>no ownership verification</b> — see that method for the full discussion of the associated
     * race. The only behavioral difference is error handling: {@code tryUnlock} propagates a
     * communication failure as a {@link RuntimeException}, whereas this method swallows it and
     * returns {@code false}. A {@code null} target or a key rejected by the memcached client is
     * still rejected with {@link IllegalArgumentException} (exactly as in {@code tryUnlock}), because
     * those are deterministic programming errors rather than transient failures. With the intended
     * immutable target and deterministic {@code toKey} implementation, a key accepted by
     * {@code tryLock()} cannot become invalid at release time.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * try (MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211")) {
     *     if (lock.tryLock("resource1", 30000)) {                     // true: acquired
     *         try {
     *             performOperation();                                 // may throw; that exception must survive
     *         } finally {
     *             // Does not throw for a Memcached hiccup, so it cannot suppress the work exception.
     *             boolean released = lock.unlockQuietly("resource1");
     *             if (!released) {
     *                 // already absent, or the failed release was logged and the lease may expire later
     *                 System.out.println("Lock was not released by this call");
     *             }
     *         }
     *     }
     * }
     * }</pre>
     *
     * @param target the target resource whose lock is to be released (must not be null)
     * @return {@code true} if an entry was deleted from Memcached for this target; {@code false} if no
     *         entry existed (e.g., the lock had already expired or was never acquired) <i>or</i> if the
     *         release failed during the Memcached operation, including because this lock client has
     *         been closed (which is logged at {@code WARN})
     * @throws IllegalArgumentException if {@code target} is null, or if the key derived from {@code target}
     *         (via {@code toKey}) is rejected by the memcached client (empty, longer than 250 bytes,
     *         or containing a space, CR, LF, or NUL byte). The key is validated eagerly, so this is
     *         thrown even when the lock client has already been closed - a deterministic programming
     *         error is never downgraded to the quiet {@code false}
     * @see #tryUnlock(Object)
     * @see #tryLock(Object, long)
     */
    public boolean unlockQuietly(final K target) {
        N.checkArgNotNull(target, "target");

        // Validate before the quiet closed-state check. Otherwise an invalid key on a closed
        // instance would be silently downgraded to false with the swallowed IllegalStateException.
        final String key = validatedKey(target);

        try {
            assertOpen();

            final boolean released = mc.remove(key);

            if (logger.isDebugEnabled()) {
                logger.debug(released ? "Released lock for key: " + key : "No lock to release for key: " + key);
            }

            return released;
        } catch (final IllegalArgumentException e) {
            // A key the memcached client rejects is a deterministic programming error, not a
            // transient failure: no lock can exist under such a key (tryLock() would have failed the
            // same way), so swallowing it here would only disguise the bug as "already expired".
            throw e;
        } catch (final Exception e) {
            if (logger.isWarnEnabled()) {
                logger.warn("Quiet Memcached lock release failed; returning false and the lock may remain held until its lease expires", e);
            }

            return false;
        }
    }

    /**
     * Attempts to acquire a value-less lock on the specified target for the given duration.
     *
     * @param target the target resource on which to acquire the lock (must not be null)
     * @param liveTime the time-to-live in milliseconds before the lock automatically expires; must be positive
     * @return {@code true} if the lock was successfully acquired, {@code false} if it is already held
     * @deprecated renamed to {@link #tryLock(Object, long)} to reflect its single-attempt semantics:
     *             it performs one server round-trip but does not poll, retry, or wait for the current
     *             holder to release. This alias delegates to it and will be removed in a future release.
     */
    @Deprecated(since = "2.8.5", forRemoval = true)
    public boolean lock(final K target, final long liveTime) {
        return tryLock(target, liveTime);
    }

    /**
     * Attempts to acquire a lock on the specified target with an associated value.
     *
     * @param target the target resource on which to acquire the lock (must not be null)
     * @param value the value to associate with the lock (may be {@code null})
     * @param liveTime the time-to-live in milliseconds before the lock automatically expires; must be positive
     * @return {@code true} if the lock was successfully acquired, {@code false} if it is already held
     * @deprecated renamed to {@link #tryLock(Object, Object, long)} to reflect its single-attempt
     *             semantics: it performs one server round-trip but does not poll, retry, or wait for
     *             the current holder to release. This alias delegates to it and will be removed in a future release.
     */
    @Deprecated(since = "2.8.5", forRemoval = true)
    public boolean lock(final K target, final V value, final long liveTime) {
        return tryLock(target, value, liveTime);
    }

    /**
     * Releases the lock on the specified target.
     *
     * @param target the target resource whose lock is to be released (must not be null)
     * @return {@code true} if an entry was deleted for this target; {@code false} if no entry existed
     * @deprecated renamed to {@link #tryUnlock(Object)} to pair with {@link #tryLock(Object, long)};
     *             this alias delegates to it and will be removed in a future release.
     */
    @Deprecated(since = "2.8.5", forRemoval = true)
    public boolean unlock(final K target) {
        return tryUnlock(target);
    }

    /**
     * Releases the lock on the specified target without throwing when the Memcached operation fails.
     *
     * @param target the target resource whose lock is to be released (must not be null)
     * @return {@code true} if an entry was deleted for this target; {@code false} if no entry existed
     *         or the release failed (which is logged at {@code WARN})
     * @deprecated renamed to {@link #unlockQuietly(Object)}. The {@code try} prefix wrongly implied a
     *             non-blocking acquisition attempt, whereas this method's distinguishing behavior is
     *             swallowing communication errors on release. This alias delegates to it and will be
     *             removed in a future release.
     */
    @Deprecated(since = "2.8.5", forRemoval = true)
    public boolean tryUnlockQuietly(final K target) {
        return unlockQuietly(target);
    }

    /**
     * Converts a lock target to a Memcached key string.
     * This method can be overridden by subclasses to implement custom key
     * generation strategies, such as adding prefixes, namespaces, or applying
     * hashing algorithms for key normalization.
     *
     * <p>The default implementation uses {@link N#stringOf(Object)} to convert
     * the target to a string representation. Subclasses may override this to:
     * <ul>
     * <li>Add namespace prefixes to avoid key collisions</li>
     * <li>Apply hashing to long or complex keys</li>
     * <li>Enforce key naming conventions</li>
     * <li>Sanitize keys to comply with Memcached key restrictions</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Example 1: Custom implementation with namespace prefix
     * class NamespacedLock<V> extends MemcachedLock<String, V> {
     *     private final String namespace;
     *
     *     public NamespacedLock(String serverUrl, String namespace) {
     *         super(serverUrl);
     *         this.namespace = namespace;
     *     }
     *
     *     @Override
     *     protected String toKey(String target) {
     *         return "lock:" + namespace + ":" + target;
     *     }
     * }
     *
     * // Usage
     * try (NamespacedLock<String> namespacedLock = new NamespacedLock<>("localhost:11211", "myapp")) {
     *     if (namespacedLock.tryLock("resource1", 30000)) { // Key: "lock:myapp:resource1"
     *         namespacedLock.unlockQuietly("resource1");
     *     }
     * }
     *
     * // Example 2: Custom implementation that hashes long keys to stay under the Memcached limit.
     * class HashedLock<K, V> extends MemcachedLock<K, V> {
     *     HashedLock(String serverUrl) {
     *         super(serverUrl);
     *     }
     *
     *     @Override
     *     protected String toKey(K target) {
     *         String key = super.toKey(target); // null-check inherited from the base class
     *         if (("lock:" + key).getBytes(java.nio.charset.StandardCharsets.UTF_8).length > 250) {
     *             return "lock:hash:" + java.util.UUID.nameUUIDFromBytes(
     *                     key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
     *         }
     *         return "lock:" + key;
     *     }
     * }
     * }</pre>
     *
     * @param target the target object to be converted to a key string (must not be null)
     * @return the non-null string key to use in Memcached. The default implementation returns
     *         {@code N.stringOf(target)} without truncation; overrides must return a non-null key
     *         accepted by spymemcached (non-empty, at most 250 UTF-8 bytes, and containing no space,
     *         CR, LF, or NUL byte)
     * @throws IllegalArgumentException if {@code target} is {@code null}
     */
    protected String toKey(final K target) {
        // Match the Javadoc contract: a null target is a programming error and must be rejected.
        // Without this check, N.stringOf(null) returns null, and the null key would fail later
        // inside the memcached client with an unhelpful NPE instead of the documented
        // IllegalArgumentException.
        N.checkArgNotNull(target, "target");
        return N.stringOf(target);
    }

    /**
     * Derives and validates the exact key that will be sent to the ASCII-protocol client. Keeping
     * this check in the lock layer gives every operation identical failure behavior, rejects a
     * buggy {@code toKey} override that returns {@code null}, and lets {@link #unlockQuietly(Object)}
     * distinguish deterministic key errors from operational release failures.
     */
    private String validatedKey(final K target) {
        final String key = N.checkArgNotNull(toKey(target), "key returned by toKey");
        StringUtils.validateKey(key, false);
        return key;
    }

    /**
     * Returns the underlying SpyMemcached client used by this lock.
     * This method provides direct access to the Memcached client for advanced
     * operations or diagnostics. Use with caution as direct manipulation of
     * the client could interfere with lock operations.
     *
     * <p>Common uses include:
     * <ul>
     * <li>Checking connection status</li>
     * <li>Performing bulk operations</li>
     * <li>Accessing client statistics</li>
     * <li>Storing additional metadata alongside locks</li>
     * <li>Implementing custom caching logic independent of locking</li>
     * </ul>
     *
     * <p>Warning: Direct use of the client bypasses the lock abstraction. Be careful not to:
     * <ul>
     * <li>Call {@link SpyMemcached#disconnect()} directly. It bypasses this wrapper's closed-state
     *     publication; close the {@code MemcachedLock} instead</li>
     * <li>Delete lock keys using the client directly (use {@link #tryUnlock(Object)} instead)</li>
     * <li>Modify lock keys in ways that could break the locking protocol</li>
     * <li>Use {@code touch}/{@code replace} as lease renewal; neither operation proves ownership and
     *     either can affect a newer holder after this caller's lease expires</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * try (MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211")) {
     *     // Example 1: Access the underlying client for custom operations
     *     SpyMemcached<String> client = lock.client();       // never null; the same instance on every call
     *     client.put("custom:key", "value", 60000);          // returns true on success (60000 ms -> 60 s TTL)
     *
     *     // Example 2: Store metadata alongside lock
     *     String metadata = client.get("custom:key");        // returns "value" (or null if absent/expired)
     *     System.out.println("Metadata: " + metadata);       // prints: value
     *
     *     // Example 3: Perform bulk operations
     *     Map<String, String> data = new HashMap<>();
     *     data.put("data:key1", "value1");                   // seed local data map
     *     data.put("data:key2", "value2");                   // seed local data map
     *     // Note: Use different key prefix to avoid conflicts with lock keys
     *     for (Map.Entry<String, String> entry : data.entrySet()) {
     *         client.put(entry.getKey(), entry.getValue(), 300000); // 300000 ms -> 300 s TTL
     *     }
     * }
     * }</pre>
     *
     * @return the SpyMemcached client instance (never {@code null})
     */
    @SuppressFBWarnings("EI_EXPOSE_REP")
    public SpyMemcached<V> client() {
        return mc;
    }

    /**
     * Closes the underlying Memcached client and releases all associated resources.
     * This method calls mc.disconnect() to properly shut down the SpyMemcached client
     * and close network connections. After calling this method, the MemcachedLock instance
     * cannot be used anymore. This method is synchronized and idempotent: it marks the instance
     * closed before invoking {@code disconnect()}, and later calls are no-ops even if that first
     * disconnect attempt threw.
     *
     * <p><b>&#9888;&#65039; Outstanding leases survive close:</b>
     * <ul>
     * <li>Closing the lock does NOT automatically release any held locks</li>
     * <li>Absent eviction, restart, failover, or a global flush, leases remain until they expire
     *     or are explicitly unlocked</li>
     * <li>Always unlock resources before closing the lock instance</li>
     * <li>After close(), acquisition/status/value/release operations throw
     *     {@link IllegalStateException}; {@link #unlockQuietly(Object)} returns {@code false}</li>
     * </ul>
     *
     * <p>It's strongly recommended to use this class with try-with-resources to ensure proper cleanup.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * // Example 1: Recommended pattern with try-with-resources
     * try (MemcachedLock<String, String> autoLock = new MemcachedLock<>("localhost:11211")) {
     *     if (autoLock.tryLock("resource", 30000)) {
     *         try {
     *             // Critical section
     *             performOperation();                // your exclusive work runs here
     *         } finally {
     *             autoLock.unlockQuietly("resource"); // logs operational release failures
     *         }
     *     }
     * } // close() runs automatically: disconnects the client; does NOT release any still-held locks
     *
     * // Example 2: Manual close (not recommended)
     * MemcachedLock<String, String> manualLock = new MemcachedLock<>("localhost:11211");
     * try {
     *     if (manualLock.tryLock("resource", 30000)) {
     *         try {
     *             performOperation();                // your exclusive work runs here
     *         } finally {
     *             manualLock.unlockQuietly("resource");
     *         }
     *     }
     * } finally {
     *     manualLock.close();   // idempotent: disconnects the client; safe to call more than once
     * }
     *
     * // Example 3: Multiple locks with single client
     * try (MemcachedLock<String, String> multiLock = new MemcachedLock<>("localhost:11211")) {
     *     boolean lock1 = multiLock.tryLock("resource1", 30000);
     *     boolean lock2 = multiLock.tryLock("resource2", 30000);
     *
     *     try {
     *         if (lock1 && lock2) {
     *             // Both locks acquired
     *             performOperation();                      // your exclusive work runs here
     *         }
     *     } finally {
     *         if (lock1) multiLock.unlockQuietly("resource1");
     *         if (lock2) multiLock.unlockQuietly("resource2");
     *     }
     * } // close() runs automatically here
     * }</pre>
     *
     * @throws RuntimeException if shutting down the underlying client fails; the lock remains marked
     *         closed and later calls to {@code close()} are no-ops
     */
    @Override
    public synchronized void close() {
        if (isClosed) {
            return;
        }

        // Publish closure before shutting down the underlying client. This prevents a new lock
        // operation from entering a client whose IO threads/pools are concurrently being torn down.
        isClosed = true;
        mc.disconnect();
    }

    private void assertOpen() {
        if (isClosed) {
            throw new IllegalStateException("This MemcachedLock has been closed");
        }
    }
}
