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
 * Thread safety: This class is thread-safe. Multiple threads can safely call methods
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
     * @param serverUrl the Memcached server URL(s) to connect to
     */
    public MemcachedLock(final String serverUrl) {
        mc = new SpyMemcached<>(serverUrl);
    }

    /**
     * Attempts to acquire a lock on the specified target for the given duration.
     * This method stores an empty byte array as the lock value. The lock will be
     * automatically released after the specified live time expires.
     * 
     * <br><br>
     * This is a non-blocking operation that returns immediately. If the lock is already
     * held by another client, this method returns false without waiting.
     *
     * @param target the target resource to lock
     * @param liveTime the duration in milliseconds before the lock automatically expires
     * @return true if the lock was successfully acquired, false if it's already held
     * @throws RuntimeException if a communication error occurs with Memcached
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
     * released after the specified live time expires.
     * 
     * <br><br>
     * The value can be retrieved using {@link #get(Object)} while the lock is held.
     * This is useful for debugging or for implementing more complex locking protocols.
     *
     * @param target the target resource to lock
     * @param value the value to associate with the lock (can be null)
     * @param liveTime the duration in milliseconds before the lock automatically expires
     * @return true if the lock was successfully acquired, false if it's already held
     * @throws RuntimeException if a communication error occurs with Memcached
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
     * This method performs a read operation to determine lock status without
     * attempting to acquire or modify the lock.
     * 
     * <br><br>
     * Note: Due to the distributed nature and timing, a lock could expire or be
     * acquired between checking and subsequent operations.
     *
     * @param target the target resource to check
     * @return true if the lock is currently held, false otherwise
     */
    public boolean isLocked(final K target) {
        if (target == null) {
            throw new IllegalArgumentException("Target cannot be null");
        }

        return mc.get(toKey(target)) != null;
    }

    /**
     * Retrieves the value associated with a lock on the specified target.
     * If the lock was acquired with just a live time (no value), this method
     * returns null. If the lock stores an empty byte array (default), null is
     * also returned for convenience.
     *
     * <br><br>
     * This method is useful for:
     * <ul>
     * <li>Identifying which client holds a lock</li>
     * <li>Storing lock metadata or state information</li>
     * <li>Implementing lock ownership verification</li>
     * </ul>
     *
     * <br><br>
     * WARNING: This method performs an unchecked cast. Ensure the type parameter V
     * matches the actual type of the stored value to avoid ClassCastException.
     *
     * @param target the target resource whose lock value to retrieve
     * @return the value associated with the lock, or null if not locked or no value stored
     * @throws ClassCastException if V doesn't match the actual stored value type
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
     * This method immediately removes the lock, making the target available for
     * other clients to acquire. It's important to always unlock in a finally block
     * to ensure locks are released even if exceptions occur.
     * 
     * <br><br>
     * Note: This implementation does not verify lock ownership. Any client can
     * unlock any lock. For ownership verification, implement additional logic using
     * the value stored with the lock.
     *
     * @param target the target resource to unlock
     * @return true if the lock was successfully removed, false if it didn't exist
     * @throws RuntimeException if a communication error occurs with Memcached
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
     * generation strategies, such as adding prefixes or namespaces.
     * 
     * <br><br>
     * The default implementation uses {@link N#stringOf(Object)} to convert
     * the target to a string representation.
     *
     * @param target the target object to convert
     * @return the string key to use in Memcached
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
     * <br><br>
     * Common uses include:
     * <ul>
     * <li>Checking connection status</li>
     * <li>Performing bulk operations</li>
     * <li>Accessing client statistics</li>
     * </ul>
     *
     * @return the SpyMemcached client instance
     */
    @SuppressFBWarnings("EI_EXPOSE_REP")
    public SpyMemcached<V> client() {
        return mc;
    }

    /**
     * Closes the underlying Memcached client and releases all associated resources.
     * After calling this method, the MemcachedLock instance cannot be used anymore.
     * This method is idempotent - calling it multiple times has no additional effect.
     *
     * <br><br>
     * It's recommended to use this class with try-with-resources to ensure proper cleanup:
     * <pre>{@code
     * try (MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211")) {
     *     if (lock.lock("resource", 30000)) {
     *         // Use the lock
     *         lock.unlock("resource");
     *     }
     * } // Automatically closed
     * }</pre>
     */
    @Override
    public void close() {
        if (mc != null) {
            mc.disconnect();
        }
    }
}