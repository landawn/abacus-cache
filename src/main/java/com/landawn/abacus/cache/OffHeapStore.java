/*
 * Copyright (c) 2025, Haiyang Li.
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

package com.landawn.abacus.cache;

/**
 * Interface for persistent storage backing an off-heap cache.
 * Defines the contract for disk-based storage that can be used as a spillover
 * mechanism when off-heap memory is full. Implementations may use various
 * storage technologies such as memory-mapped files, embedded databases, or
 * custom file formats.
 *
 * <p>
 * Key characteristics:
 * <ul>
 * <li>Stores raw byte arrays associated with keys.</li>
 * <li>Should handle concurrent access safely.</li>
 * <li>Performance should be optimized for cache spillover scenarios.</li>
 * <li>May implement compression or other optimizations.</li>
 * </ul>
 *
 * <p>
 * Implementation considerations:
 * <ul>
 * <li>Thread safety - implementations must handle concurrent operations safely.</li>
 * <li>Persistence - data should survive JVM restarts if required.</li>
 * <li>Performance - optimize for cache access patterns (frequent reads).</li>
 * <li>Resource management - handle file handles and disk space efficiently.</li>
 * <li>Error handling - use {@code null} / {@code false} for an ordinary miss or inability to
 *     complete an operation. Implementations may propagate unexpected runtime failures; the owning
 *     cache does not silently translate such exceptions into misses.</li>
 * <li>Argument validation - the cache always passes non-null, already-validated keys and values, so
 *     implementations need not null-check arguments; behavior on a {@code null} key or value is left
 *     to the implementation.</li>
 * </ul>
 *
 * <p><b>Usage Examples:</b>
 * <pre>{@code
 * public class FileBasedOffHeapStore<K> implements OffHeapStore<K> {
 *     private final Path storageDir;
 *     private final ConcurrentHashMap<K, Path> keyToFile = new ConcurrentHashMap<>();
 *
 *     public FileBasedOffHeapStore(Path storageDir) throws IOException {
 *         this.storageDir = storageDir;
 *         Files.createDirectories(storageDir);
 *     }
 *
 *     public byte[] get(K key) {
 *         Path file = keyToFile.get(key);
 *         if (file != null && Files.exists(file)) {
 *             try {
 *                 return Files.readAllBytes(file);
 *             } catch (IOException e) {
 *                 // Log error and return null
 *                 return null;
 *             }
 *         }
 *         return null;
 *     }
 *
 *     public boolean put(K key, byte[] value) {
 *         try {
 *             // Use a unique file name per put; deriving it from key.hashCode() would let two
 *             // distinct keys with the same hash code overwrite each other's data.
 *             Path file = storageDir.resolve(UUID.randomUUID() + ".cache");
 *             Files.write(file, value);
 *             Path previous = keyToFile.put(key, file);
 *             if (previous != null) {
 *                 try {
 *                     Files.deleteIfExists(previous); // best-effort cleanup of superseded bytes
 *                 } catch (IOException ignored) {
 *                     // The replacement is already committed; report it as successful.
 *                 }
 *             }
 *             return true;
 *         } catch (IOException e) {
 *             // Log error and return false
 *             return false;
 *         }
 *     }
 *
 *     public boolean remove(K key) {
 *         Path file = keyToFile.get(key);
 *         if (file == null) {
 *             return false;
 *         }
 *         try {
 *             if (!Files.deleteIfExists(file)) {
 *                 // The bytes vanished externally (crash residue, manual cleanup): drop the
 *                 // stale mapping too, so later calls do not keep reporting a phantom entry.
 *                 keyToFile.remove(key, file);
 *                 return false;
 *             }
 *             return keyToFile.remove(key, file);
 *         } catch (IOException e) {
 *             // Keep the mapping so a later remove can retry cleanup.
 *             return false;
 *         }
 *     }
 * }
 * }</pre>
 *
 * @param <K> the type of keys used to identify and retrieve stored values
 * @see OffHeapCache
 * @see ForeignMemoryOffHeapCache
 * @see AbstractOffHeapCache
 */
public interface OffHeapStore<K> extends AutoCloseable {

    /**
     * Retrieves the byte array associated with the specified key.
     * Returns {@code null} if the key is not found or the implementation cannot complete an ordinary
     * retrieval. Unexpected runtime failures may be propagated to the caller.
     * Implementations may return either a defensive copy or an internally retained array. The
     * owning cache treats the returned array as store-owned and makes its own copy before exposing
     * bytes to a caller or custom deserializer.
     *
     * <p><b>Thread Safety:</b>
     * Implementations of this method must be thread-safe and support concurrent
     * access from multiple threads.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * OffHeapStore<String> store = new FileBasedOffHeapStore<>(Paths.get("/tmp/cache"));
     * byte[] data = store.get("user:123");
     * if (data != null) {
     *     User user = deserialize(data);
     *     System.out.println("User loaded from disk");
     * } else {
     *     System.out.println("User not found");
     * }
     * }</pre>
     *
     * <p><b>Round-trip contract:</b> the returned array must contain exactly the bytes passed to
     * {@code put(key, value)} — same content and same length. Any compression or encoding applied by
     * the implementation must be fully transparent (decoded before returning). The cache treats a
     * length mismatch between the returned array and the size it recorded at put time as data
     * corruption and fails the read with an exception rather than a cache miss.
     *
     * @param key the key whose associated value is to be retrieved; must not be {@code null}
     * @return the stored byte array, or {@code null} if not found or the retrieval cannot be completed normally
     */
    byte[] get(K key);

    /**
     * Stores a byte array under the specified key.
     * If a value already exists for the key, it is replaced.
     * Implementations should consider making a defensive copy of the byte array
     * to prevent external modifications, though this behavior is implementation-specific.
     *
     * <p><b>Failure atomicity:</b> Returning {@code false} or throwing must leave any value that
     * was previously associated with {@code key} unchanged. The owning cache keeps the prior
     * entry's metadata installed across a failed replacement; a store that partially overwrites
     * the bytes before reporting failure would therefore make that metadata describe the wrong
     * payload. Implementations should write to temporary storage and atomically publish/rename
     * it, or otherwise roll back a failed replacement.
     *
     * <p><b>Thread Safety:</b>
     * Implementations of this method must be thread-safe and support concurrent
     * access from multiple threads.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * OffHeapStore<String> store = new FileBasedOffHeapStore<>(Paths.get("/tmp/cache"));
     * User user = new User("John", 30);
     * byte[] serializedData = serialize(user);
     * boolean success = store.put("user:123", serializedData);
     * if (success) {
     *     System.out.println("Data stored to disk successfully");
     * } else {
     *     System.out.println("Failed to store data");
     * }
     * }</pre>
     *
     * @param key the key with which the specified value is to be associated; must not be {@code null}
     * @param value the byte array value to store; must not be {@code null}
     * @return {@code true} if the value was successfully stored, {@code false} if it was not stored
     *         and any previous mapping remains unchanged
     */
    boolean put(K key, byte[] value);

    /**
     * Removes the value associated with the specified key.
     * Returns {@code true} if a value was removed, {@code false} if the key was not found
     * or if the implementation cannot complete an ordinary removal. Unexpected runtime failures
     * may be propagated to the caller. It is safe to call this method for a
     * non-existent key.
     *
     * <p><b>Thread Safety:</b>
     * Implementations of this method must be thread-safe and support concurrent
     * access from multiple threads.
     *
     * <p><b>Usage Examples:</b>
     * <pre>{@code
     * OffHeapStore<String> store = new FileBasedOffHeapStore<>(Paths.get("/tmp/cache"));
     * if (store.remove("user:123")) {
     *     System.out.println("User data removed from disk");
     * } else {
     *     System.out.println("User data not found or removal failed");
     * }
     * }</pre>
     *
     * @param key the key whose associated value is to be removed; must not be {@code null}
     * @return {@code true} if a value was removed, {@code false} otherwise
     */
    boolean remove(K key);

    /**
     * Releases any OS resources held by this store (file handles, memory-mapped regions, embedded
     * database connections, etc.). Called by the owning off-heap cache when the cache itself is
     * closed; may also be called directly by users (this interface extends {@link AutoCloseable},
     * so a store can be used in try-with-resources).
     *
     * <p>This {@code default} implementation does nothing, which is appropriate for stores that hold
     * no resources requiring explicit release. Implementations that open files, mmap regions, or
     * connections should override it. The method is expected to be idempotent (safe to call more
     * than once). During normal cache shutdown an unchecked exception raised here propagates from
     * the cache's {@code close()} (or is attached as a suppressed exception when an earlier cleanup
     * step already failed); the cache's other cleanup steps still run. If cache construction fails
     * after acquiring the store, a store-close failure is attached to the construction failure as a
     * suppressed exception.
     */
    @Override
    default void close() {
        // No-op by default; stores holding OS resources should override.
    }

}
