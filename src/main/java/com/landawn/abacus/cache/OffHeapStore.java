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
 * <br><br>
 * Key characteristics:
 * <ul>
 * <li>Stores raw byte arrays associated with keys</li>
 * <li>Should handle concurrent access safely</li>
 * <li>Performance should be optimized for cache spillover scenarios</li>
 * <li>May implement compression or other optimizations</li>
 * </ul>
 * 
 * <br>
 * Implementation considerations:
 * <ul>
 * <li>Thread safety - implementations should handle concurrent operations</li>
 * <li>Persistence - data should survive JVM restarts if required</li>
 * <li>Performance - optimize for cache access patterns (frequent reads)</li>
 * <li>Resource management - handle file handles and disk space efficiently</li>
 * </ul>
 * 
 * <br>
 * Example implementation:
 * <pre>{@code
 * public class FileBasedOffHeapStore<K> implements OffHeapStore<K> {
 *     private final Path storageDir;
 *     private final ConcurrentHashMap<K, Path> keyToFile = new ConcurrentHashMap<>();
 *     
 *     public byte[] get(K key) {
 *         Path file = keyToFile.get(key);
 *         if (file != null && Files.exists(file)) {
 *             try {
 *                 return Files.readAllBytes(file);
 *             } catch (IOException e) {
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
 * @see AbstractOffHeapCache
 */
public interface OffHeapStore<K> {

    /**
     * Retrieves the byte array associated with the given key.
     * Returns null if the key is not found or if an error occurs.
     * The returned byte array should be a copy to prevent external modifications.
     *
     * @param key the key to look up
     * @return the stored byte array, or null if not found
     */
    byte[] get(K key);

    /**
     * Stores a byte array with the associated key.
     * If a value already exists for the key, it should be replaced.
     * The implementation should make a defensive copy of the byte array
     * if necessary to prevent external modifications.
     *
     * @param key the key to associate with the value
     * @param value the byte array to store
     * @return true if the value was successfully stored, false otherwise
     */
    boolean put(K key, byte[] value);

    /**
     * Removes the value associated with the given key.
     * Returns true if a value was removed, false if the key was not found
     * or if an error occurred during removal.
     *
     * @param key the key whose value should be removed
     * @return true if a value was removed, false otherwise
     */
    boolean remove(K key);

}