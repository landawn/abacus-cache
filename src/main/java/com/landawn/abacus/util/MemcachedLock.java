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

/**
 * A distributed lock implementation using Memcached as the coordination service.
 * This class provides a simple distributed locking mechanism that can be used to coordinate
 * access to shared resources across multiple JVMs or servers. It leverages Memcached's atomic
 * add operation to ensure that only one client can hold a lock at a time.
 * 
 * <br><br>
 * Key features:
 * <ul>
 * <li>Distributed mutual exclusion across multiple processes</li>
 * <li>Automatic lock expiration to prevent deadlocks</li>
 * <li>Optional value storage with the lock</li>
 * <li>Non-blocking lock acquisition</li>
 * </ul>
 * 
 * <br>
 * Implementation notes:
 * <ul>
 * <li>Uses Memcached's atomic add operation for lock acquisition</li>
 * <li>Lock expiration prevents permanent deadlocks if holder crashes</li>
 * <li>Not reentrant - same client cannot acquire lock twice</li>
 * <li>No queue or fairness guarantees - it's a simple mutex</li>
 * </ul>
 * 
 * <br>
 * Example usage:
 * <pre>{@code
 * MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211");
 * 
 * // Simple lock without value
 * if (lock.lock("resource1", 30000)) { // 30 second timeout
 *     try {
 *         // Critical section - exclusive access to resource1
 *         performExclusiveOperation();
 *     } finally {
 *         lock.unlock("resource1");
 *     }
 * } else {
 *     // Lock is held by another process
 *     System.out.println("Could not acquire lock");
 * }
 * 
 * // Lock with associated value
 * String lockHolder = InetAddress.getLocalHost().getHostName();
 * if (lock.lock("resource2", lockHolder, 60000)) {
 *     // Lock acquired with holder information
 *     String currentHolder = lock.get("resource2");
 *     System.out.println("Lock held by: " + currentHolder);
 * }
 * }</pre>
 * 
 * <br>
 * Thread Safety: This class is thread-safe. Multiple threads can safely call methods
 * on the same instance. However, the lock itself is not reentrant.
 *
 * @param <K> the type of lock identifiers used as keys (typically String)
 * @param <V> the type of optional metadata values associated with locks
 * @see SpyMemcached
 */
public final class MemcachedLock<K, V> implements AutoCloseable {

    static final Logger logger = LoggerFactory.getLogger(MemcachedLock.class);

    private final SpyMemcached<V> mc;

    /**
     * Creates a new MemcachedLock instance connected to the specified Memcached server(s).
     * The server URL should be in the format "host1:port1,host2:port2" for multiple servers.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Single server
     * MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211");
     *
     * // Multiple servers
     * MemcachedLock<String, String> lock2 = new MemcachedLock<>("server1:11211,server2:11211");
     * }</pre>
     *
     * @param serverUrl the Memcached server URL(s) to connect to (must not be null)
     * @throws IllegalArgumentException if serverUrl is null or invalid
     * @throws RuntimeException if connection to the Memcached server(s) fails
     */
    public MemcachedLock(final String serverUrl) {
        mc = new SpyMemcached<>(serverUrl);
    }

    /**
     * Attempts to acquire a lock on the specified target for the given duration.
     * This method stores an empty byte array (N.EMPTY_BYTE_ARRAY cast to type V) as the lock value.
     * The lock will be automatically released after the specified live time expires, ensuring that
     * locks don't persist indefinitely if a holder crashes or fails to release them.
     *
     * <p>This is a non-blocking operation that returns immediately. If the lock is already
     * held by another client, this method returns {@code false} without waiting. The implementation
     * uses Memcached's atomic add operation to ensure only one client can acquire the lock.
     *
     * <p>Important considerations:
     * <ul>
     * <li>The lock is not reentrant - the same client cannot acquire the same lock twice</li>
     * <li>Choose an appropriate liveTime to balance between deadlock prevention and operational needs</li>
     * <li>Always release locks in a finally block to prevent resource leaks</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211");
     *
     * // Basic lock usage with 30-second timeout
     * if (lock.lock("resource1", 30000)) {
     *     try {
     *         // Critical section - perform exclusive operations
     *         performOperation();
     *     } finally {
     *         lock.unlock("resource1");
     *     }
     * } else {
     *     System.out.println("Failed to acquire lock - already held by another process");
     * }
     * }</pre>
     *
     * @param target the target resource on which to acquire the lock (must not be null)
     * @param liveTime the time-to-live in milliseconds before the lock automatically expires (must be positive)
     * @return {@code true} if the lock was successfully acquired, {@code false} if it's already held
     * @throws IllegalArgumentException if target is null or liveTime is not positive
     * @throws RuntimeException if a communication error occurs with Memcached
     * @see #lock(Object, Object, long)
     * @see #unlock(Object)
     */
    public boolean lock(final K target, final long liveTime) {
        if (target == null) {
            throw new IllegalArgumentException("Target cannot be null");
        }
        if (liveTime <= 0) {
            throw new IllegalArgumentException("Live time must be positive: " + liveTime);
        }

        return lock(target, (V) N.EMPTY_BYTE_ARRAY, liveTime);
    }

    /**
     * Attempts to acquire a lock on the specified target with an associated value.
     * This method allows storing additional information with the lock, such as the
     * identity of the lock holder or lock metadata. The lock will be automatically
     * released after the specified live time expires. The implementation uses
     * Memcached's atomic add operation (via mc.add) which only succeeds if the key
     * doesn't already exist, ensuring mutual exclusion.
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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211");
     *
     * // Example 1: Store hostname with lock
     * String lockHolder = InetAddress.getLocalHost().getHostName();
     * if (lock.lock("resource1", lockHolder, 60000)) {
     *     try {
     *         System.out.println("Lock acquired by: " + lock.get("resource1"));
     *         // Perform operations
     *     } finally {
     *         lock.unlock("resource1");
     *     }
     * } else {
     *     System.out.println("Lock is held by: " + lock.get("resource1"));
     * }
     *
     * // Example 2: Store structured metadata
     * Map<String, Object> metadata = new HashMap<>();
     * metadata.put("host", "server1");
     * metadata.put("thread", Thread.currentThread().getName());
     * metadata.put("timestamp", System.currentTimeMillis());
     * lock.lock("resource2", (V) metadata, 30000);
     * }</pre>
     *
     * @param target the target resource on which to acquire the lock (must not be null)
     * @param value the value to associate with the lock (can be {@code null})
     * @param liveTime the time-to-live in milliseconds before the lock automatically expires (must be positive)
     * @return {@code true} if the lock was successfully acquired, {@code false} if it's already held
     * @throws IllegalArgumentException if target is null or liveTime is not positive
     * @throws RuntimeException if a communication error occurs with Memcached
     * @see #get(Object)
     * @see #unlock(Object)
     */
    public boolean lock(final K target, final V value, final long liveTime) {
        if (target == null) {
            throw new IllegalArgumentException("Target cannot be null");
        }
        if (liveTime <= 0) {
            throw new IllegalArgumentException("Live time must be positive: " + liveTime);
        }

        final String key = toKey(target);

        try {
            return mc.add(key, value, liveTime);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to lock target with key: " + key, e);
        }
    }

    /**
     * Checks whether a lock is currently held on the specified target.
     * This method performs a read operation (mc.get) to determine lock status without
     * attempting to acquire or modify the lock. Returns true if a value exists for the
     * lock key, false otherwise.
     *
     * <p>Important: Due to the distributed nature and timing, a lock could expire or be
     * acquired between checking and subsequent operations. This is a point-in-time check
     * and should not be relied upon for critical synchronization logic. Always use the
     * return value of {@link #lock(Object, long)} or {@link #lock(Object, Object, long)}
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
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211");
     *
     * // Example 1: Check lock status for monitoring
     * if (lock.isLocked("resource1")) {
     *     System.out.println("Resource is currently locked");
     * } else {
     *     System.out.println("Resource is available");
     * }
     *
     * // Example 2: INCORRECT usage - race condition
     * if (!lock.isLocked("resource1")) {
     *     // Lock could be acquired by another process here!
     *     lock.lock("resource1", 30000);   // Might fail
     * }
     *
     * // Example 3: CORRECT usage - atomic check
     * if (lock.lock("resource1", 30000)) {
     *     try {
     *         // Lock successfully acquired
     *     } finally {
     *         lock.unlock("resource1");
     *     }
     * }
     * }</pre>
     *
     * @param target the target resource whose lock status is to be checked (must not be null)
     * @return {@code true} if the lock is currently held, {@code false} otherwise
     * @throws IllegalArgumentException if target is null
     * @throws RuntimeException if a communication error occurs with Memcached
     * @see #lock(Object, long)
     * @see #lock(Object, Object, long)
     */
    public boolean isLocked(final K target) {
        if (target == null) {
            throw new IllegalArgumentException("Target cannot be null");
        }

        return mc.get(toKey(target)) != null;
    }

    /**
     * Retrieves the value associated with a lock on the specified target.
     * If no lock exists (key not found), this method returns {@code null}.
     * If the lock stores an empty byte array (the default when using {@link #lock(Object, long)}),
     * {@code null} is returned for convenience to distinguish empty values from actual data.
     *
     * <p>This method is useful for:
     * <ul>
     * <li>Identifying which client holds a lock</li>
     * <li>Storing lock metadata or state information</li>
     * <li>Implementing lock ownership verification</li>
     * <li>Debugging distributed locking issues</li>
     * </ul>
     *
     * <p>Important considerations:
     * <ul>
     * <li>This method performs an unchecked cast from Object to V. Ensure the type parameter V
     * matches the actual type of the stored value to avoid ClassCastException at runtime.</li>
     * <li>The returned value represents a snapshot at the time of the call. The lock could
     * expire or be released immediately after retrieval.</li>
     * <li>Returns {@code null} if the lock doesn't exist or if it stores an empty byte array</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211");
     *
     * // Example 1: Check who holds the lock
     * String holder = lock.get("resource1");
     * if (holder != null) {
     *     System.out.println("Lock held by: " + holder);
     * } else {
     *     System.out.println("No lock exists or value is empty");
     * }
     *
     * // Example 2: Verify lock ownership before unlocking
     * String myId = InetAddress.getLocalHost().getHostName();
     * if (lock.lock("resource2", myId, 60000)) {
     *     try {
     *         // Perform operations
     *     } finally {
     *         // Verify we still own the lock before unlocking
     *         if (myId.equals(lock.get("resource2"))) {
     *             lock.unlock("resource2");
     *         } else {
     *             System.out.println("Lock ownership changed - not unlocking");
     *         }
     *     }
     * }
     *
     * // Example 3: Retrieve structured metadata
     * Map<String, Object> lockInfo = (Map<String, Object>) lock.get("resource3");
     * if (lockInfo != null) {
     *     System.out.println("Locked by: " + lockInfo.get("host"));
     *     System.out.println("Thread: " + lockInfo.get("thread"));
     * }
     * }</pre>
     *
     * @param target the target resource whose associated lock value is to be retrieved (must not be null)
     * @return the value associated with the lock, or {@code null} if not locked or stores an empty byte array
     * @throws IllegalArgumentException if target is null
     * @throws ClassCastException if V doesn't match the actual stored value type
     * @throws RuntimeException if a communication error occurs with Memcached
     * @see #lock(Object, Object, long)
     * @see #isLocked(Object)
     */
    @SuppressWarnings("unchecked")
    public V get(final K target) {
        if (target == null) {
            throw new IllegalArgumentException("Target cannot be null");
        }

        final Object value = mc.get(toKey(target));

        return (V) (value instanceof byte[] && ((byte[]) value).length == 0 ? null : value);
    }

    /**
     * Releases the lock on the specified target.
     * This method immediately removes the lock by deleting the key from Memcached (via mc.delete),
     * making the target available for other clients to acquire. It's important to always unlock
     * in a finally block to ensure locks are released even if exceptions occur.
     *
     * <p>Important: This implementation does not verify lock ownership. Any client can
     * unlock any lock, even if they don't hold it. For applications requiring ownership
     * verification, implement additional logic using the value stored with the lock (e.g.,
     * check if {@link #get(Object)} returns your identifier before unlocking).
     *
     * <p>Best practices:
     * <ul>
     * <li>Always call unlock() in a finally block to ensure cleanup</li>
     * <li>Consider implementing ownership verification in critical applications</li>
     * <li>Don't assume unlock() always succeeds - check the return value if needed</li>
     * <li>Be aware that locks can expire automatically, so unlock() may return false</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211");
     *
     * // Example 1: Basic unlock in finally block
     * if (lock.lock("resource1", 30000)) {
     *     try {
     *         performOperation();
     *     } finally {
     *         boolean unlocked = lock.unlock("resource1");
     *         if (!unlocked) {
     *             System.out.println("Lock may have already expired or been removed");
     *         }
     *     }
     * }
     *
     * // Example 2: Unlock with ownership verification
     * String myId = "server-1";
     * if (lock.lock("resource2", myId, 60000)) {
     *     try {
     *         performOperation();
     *     } finally {
     *         // Only unlock if we still own it
     *         if (myId.equals(lock.get("resource2"))) {
     *             lock.unlock("resource2");
     *         } else {
     *             System.out.println("Lock no longer owned by us - skipping unlock");
     *         }
     *     }
     * }
     * }</pre>
     *
     * @param target the target resource whose lock is to be released (must not be null)
     * @return {@code true} if the lock was successfully removed, {@code false} if it didn't exist
     * @throws IllegalArgumentException if target is null
     * @throws RuntimeException if a communication error occurs with Memcached
     * @see #lock(Object, long)
     * @see #lock(Object, Object, long)
     */
    public boolean unlock(final K target) {
        if (target == null) {
            throw new IllegalArgumentException("Target cannot be null");
        }

        try {
            return mc.delete(toKey(target));
        } catch (final Exception e) {
            throw new RuntimeException("Failed to unlock with key: " + target, e);
        }
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
     * <p><b>Usage Examples:</b></p>
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
     * NamespacedLock<String> lock = new NamespacedLock<>("localhost:11211", "myapp");
     * lock.lock("resource1", 30000);   // Key in Memcached: "lock:myapp:resource1"
     *
     * // Example 2: Custom implementation with MD5 hashing for long keys
     * class HashedLock<K, V> extends MemcachedLock<K, V> {
     *     @Override
     *     protected String toKey(K target) {
     *         String key = N.stringOf(target);
     *         if (key.length() > 200) { // Memcached key limit is 250 bytes
     *             return "lock:hash:" + N.md5(key);
     *         }
     *         return "lock:" + key;
     *     }
     * }
     * }</pre>
     *
     * @param target the target object to be converted to a key string (must not be null)
     * @return the string key to use in Memcached (should not exceed 250 bytes)
     */
    protected String toKey(final K target) {
        return N.stringOf(target);
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
     * <li>Delete lock keys using the client directly (use {@link #unlock(Object)} instead)</li>
     * <li>Modify lock keys in ways that could break the locking protocol</li>
     * <li>Use conflicting TTL values that could cause unexpected behavior</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211");
     *
     * // Example 1: Access the underlying client for custom operations
     * SpyMemcached<String> client = lock.client();
     * client.set("custom:key", "value", 60000);
     *
     * // Example 2: Store metadata alongside lock
     * String metadata = client.get("custom:key");
     * System.out.println("Metadata: " + metadata);
     *
     * // Example 3: Perform bulk operations
     * Map<String, String> data = new HashMap<>();
     * data.put("data:key1", "value1");
     * data.put("data:key2", "value2");
     * // Note: Use different key prefix to avoid conflicts with lock keys
     * for (Map.Entry<String, String> entry : data.entrySet()) {
     *     client.set(entry.getKey(), entry.getValue(), 300000);
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
     * cannot be used anymore. This method is idempotent - calling it multiple times has no
     * additional effect because disconnect() handles multiple calls gracefully.
     *
     * <p>Important notes:
     * <ul>
     * <li>Closing the lock does NOT automatically release any held locks</li>
     * <li>Locks will remain in Memcached until they expire or are explicitly unlocked</li>
     * <li>Always unlock resources before closing the lock instance</li>
     * <li>After close(), any method calls will likely throw exceptions</li>
     * </ul>
     *
     * <p>It's strongly recommended to use this class with try-with-resources to ensure proper cleanup:
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Example 1: Recommended pattern with try-with-resources
     * try (MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211")) {
     *     if (lock.lock("resource", 30000)) {
     *         try {
     *             // Critical section
     *             performOperation();
     *         } finally {
     *             lock.unlock("resource");
     *         }
     *     }
     * } // Automatically closed, resources released
     *
     * // Example 2: Manual close (not recommended)
     * MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211");
     * try {
     *     if (lock.lock("resource", 30000)) {
     *         try {
     *             performOperation();
     *         } finally {
     *             lock.unlock("resource");
     *         }
     *     }
     * } finally {
     *     lock.close();   // Ensure close is called
     * }
     *
     * // Example 3: Multiple locks with single client
     * try (MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211")) {
     *     boolean lock1 = lock.lock("resource1", 30000);
     *     boolean lock2 = lock.lock("resource2", 30000);
     *
     *     try {
     *         if (lock1 && lock2) {
     *             // Both locks acquired
     *             performOperation();
     *         }
     *     } finally {
     *         if (lock1) lock.unlock("resource1");
     *         if (lock2) lock.unlock("resource2");
     *     }
     * }
     * }</pre>
     */
    @Override
    public void close() {
        if (mc != null) {
            mc.disconnect();
        }
    }
}