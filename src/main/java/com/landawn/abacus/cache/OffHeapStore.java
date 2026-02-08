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
 * This interface defines the contract for disk-based storage that can be used
 * as a spillover mechanism when off-heap memory is full. Implementations might
 * use various storage technologies such as memory-mapped files, embedded databases,
 * or custom file formats.
 *
 * <p>
 * Key characteristics:
 * <ul>
 * <li>Stores raw byte arrays associated with keys</li>
 * <li>Should handle concurrent access safely</li>
 * <li>Performance should be optimized for cache spillover scenarios</li>
 * <li>May implement compression or other optimizations</li>
 * </ul>
 *
 * <p>
 * Implementation considerations:
 * <ul>
 * <li>Thread safety - implementations must handle concurrent operations safely</li>
 * <li>Persistence - data should survive JVM restarts if required</li>
 * <li>Performance - optimize for cache access patterns (frequent reads)</li>
 * <li>Resource management - handle file handles and disk space efficiently</li>
 * <li>Error handling - return null or false on failures rather than throwing exceptions</li>
 * </ul>
 *
 * <p><b>Usage Examples:</b></p>
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
 *             Path file = storageDir.resolve(key.hashCode() + ".cache");
 *             Files.write(file, value);
 *             keyToFile.put(key, file);
 *             return true;
 *         } catch (IOException e) {
 *             // Log error and return false
 *             return false;
 *         }
 *     }
 *
 *     public boolean remove(K key) {
 *         Path file = keyToFile.remove(key);
 *         if (file != null) {
 *             try {
 *                 Files.deleteIfExists(file);
 *                 return true;
 *             } catch (IOException e) {
 *                 // Log error and return false
 *                 return false;
 *             }
 *         }
 *         return false;
 *     }
 * }
 * }</pre>
 *
 * @param <K> the type of keys used to identify and retrieve stored values
 * @see OffHeapCache
 * @see OffHeapCache25
 * @see AbstractOffHeapCache
 */
public interface OffHeapStore<K> {

    /**
     * Retrieves the byte array associated with the specified key.
     * Returns {@code null} if the key is not found or if an error occurs during retrieval.
     * Implementations should consider returning a defensive copy to prevent external
     * modifications, though this is implementation-specific.
     *
     * <p><b>Thread Safety:</b></p>
     * This method must be thread-safe and support concurrent access from multiple threads.
     *
     * <p><b>Usage Examples:</b></p>
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
     * @param key the key whose associated value is to be retrieved; must not be {@code null}
     * @return the stored byte array, or {@code null} if not found or an error occurs
     */
    byte[] get(K key);

    /**
     * Stores a byte array with the specified key.
     * If a value already exists for the key, it should be replaced.
     * Implementations should consider making a defensive copy of the byte array
     * if necessary to prevent external modifications, though this is implementation-specific.
     *
     * <p><b>Thread Safety:</b></p>
     * This method must be thread-safe and support concurrent access from multiple threads.
     *
     * <p><b>Usage Examples:</b></p>
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
     * @return {@code true} if the value was successfully stored, {@code false} otherwise
     */
    boolean put(K key, byte[] value);

    /**
     * Removes the value associated with the specified key.
     * Returns {@code true} if a value was removed, {@code false} if the key was not found
     * or if an error occurred during removal. It is safe to call this method for a non-existent key.
     *
     * <p><b>Thread Safety:</b></p>
     * This method must be thread-safe and support concurrent access from multiple threads.
     *
     * <p><b>Usage Examples:</b></p>
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

}