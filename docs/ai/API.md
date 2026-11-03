# abacus-cache API Index (v2.9.2)
- Build: f6638b6be378c930c232396441f24113951a77fa
- Java: 25
- Generated: 2026-11-03

## Packages
- com.landawn.abacus.cache — The Abacus caching framework: one Cache interface over in-memory, off-heap, third-party, and distributed cache backends.
- com.landawn.abacus.util — Cache-backed coordination utilities that build on the clients in `com.landawn.abacus.cache`.

## com.landawn.abacus.cache
### Class AbstractCache (com.landawn.abacus.cache.AbstractCache)
Abstract base class for all cache implementations providing common functionality.

**Thread-safety:** unspecified
**Nullability:** unspecified
**Implements:** `Cache<K, V>`

#### Public Constructors
- (none)

#### Public Static Methods
- (none)

#### Public Instance Methods
##### get(...) -> Optional<V>
- **Signature:** `public Optional<V> get(final K key) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** This base implementation delegates to `#getOrNull(Object)` and wraps the result via `Optional#ofNullable(Object)`.
- **Documentation:** inherits documentation from the overridden member
- **Contract:**
  - Consequently, if a concrete cache permits a key to be explicitly mapped to `null`, this method returns an empty optional for that mapping.
  - Such a mapping is distinguishable from absence only if the concrete implementation's `#containsKey(Object)` contract says so.
  - Subclasses that need different semantics should override `#getOrNull(Object)` rather than this method.
- **Parameters:**
  - `key` (`K`) — the cache key to look up; null-handling is implementation-defined (most implementations reject null)
- **Returns:** an Optional containing the non-null value returned by `getOrNull`, whether cached or loaded, or an empty Optional when that method returns `null`
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if `key` is `null` and the implementation rejects `null` keys, as all built-in implementations do, or the key cannot be encoded for its backend
  - `java.lang.RuntimeException` — if the delegated cache operation propagates a backend, serialization, or loading failure
- **Examples:**
  ```java
  Cache<String, String> cache = new LocalCache<>(100, 0);
  cache.put("k", "v");                       // seed an entry

  Optional<String> hit = cache.get("k");
  hit.isPresent();                           // returns true
  hit.get();                                 // returns "v"

  // Edge: missing key yields an empty Optional, not null.
  Optional<String> miss = cache.get("absent");
  miss.isPresent();                          // returns false
  miss.orElse("fallback");                   // returns "fallback"
  ```
- **See also:** `#getOrNull(Object)`, `#asyncGet(Object)`
##### put(...) -> boolean
- **Signature:** `public boolean put(final K key, final V value) throws IllegalStateException, IllegalArgumentException, RuntimeException, StackOverflowError`
- **Summary:** This base implementation delegates to `#put(Object, Object, long, long)` using the `defaultLiveTime` and `defaultMaxIdleTime` configured at construction time.
- **Documentation:** inherits documentation from the overridden member
- **Contract:**
  - If the operation succeeds and the key already exists, its value is replaced and its expiration is reset.
  - Behavior: A successful write overwrites any existing entry with the same key A successful write resets the implementation's applicable expiration counters May trigger eviction of other entries if cache capacity is reached May return `false` when storage is rejected; backend failures may also throw Usage Examples:
- **Parameters:**
  - `key` (`K`) — the cache key to store the value under; null-handling is implementation-defined (most implementations reject null)
  - `value` (`V`) — the value to cache; null-handling is implementation-defined
- **Returns:** `true` if the implementation reports success, `false` if storage was rejected. Success does not guarantee retention: eviction, expiration, concurrent changes, or a backend resilience policy may discard the entry. Backend failures may throw an exception
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if `key` or `value` is `null` and the implementation rejects that `null` argument; all built-in implementations reject a `null` key, and all except `DistributedCache` (which leaves `null` values to its client) also reject a `null` value. The implementation may also reject a key or configured default expiration that its backend cannot encode
  - `java.lang.RuntimeException` — if the delegated cache operation propagates a backend write, serialization, or cache callback failure
  - `java.lang.StackOverflowError` — if Kryo serialization in the delegated put (a Kryo-backed distributed client, or an off-heap cache's default serializer) encounters a cycle made only of collections, maps, or arrays in `value`
- **Examples:**
  ```java
  Cache<String, String> cache = new LocalCache<>(100, 0);

  // Stores using the construction-time defaults (3h live, 30min idle by default).
  boolean stored = cache.put("k", "v");      // returns true
  cache.get("k").get();                      // returns "v"

  // Re-putting the same key overwrites the existing value.
  cache.put("k", "v2");                      // returns true
  cache.get("k").get();                      // returns "v2"
  ```
- **See also:** `#put(Object, Object, long, long)`, `#asyncPut(Object, Object)`
##### asyncGet(...) -> ContinuableFuture<Optional<V>>
- **Signature:** `public ContinuableFuture<Optional<V>> asyncGet(final K key) throws IllegalStateException, RejectedExecutionException`
- **Summary:** This base implementation submits a call to `#get(Object)` on `#asyncExecutor`.
- **Documentation:** inherits documentation from the overridden member
- **Parameters:**
  - `key` (`K`) — the cache key to look up; null-handling is implementation-defined (most implementations reject null)
- **Returns:** a ContinuableFuture that completes with the Optional returned by `#get(Object)`, including any loaded value. The future completes exceptionally if the underlying call throws.
- **Throws:**
  - `java.lang.IllegalStateException` — if the shared `#asyncExecutor` has been explicitly shut down
  - `java.util.concurrent.RejectedExecutionException` — if its backing thread pool rejects the task, including during JVM shutdown
- **Examples:**
  ```java
  Cache<String, String> cache = new LocalCache<>(100, 0);
  cache.put("k", "v");                       // seed an entry

  ContinuableFuture<Optional<String>> f = cache.asyncGet("k");
  Optional<String> hit = f.get();            // throws InterruptedException, ExecutionException
  hit.isPresent();                           // returns true
  hit.get();                                 // returns "v"

  // Edge: missing key resolves to an empty Optional.
  cache.asyncGet("absent").get().isPresent();  // returns false
  ```
- **See also:** `#get(Object)`, `#asyncGetOrNull(Object)`
##### asyncGetOrNull(...) -> ContinuableFuture<V>
- **Signature:** `public ContinuableFuture<V> asyncGetOrNull(final K key) throws IllegalStateException, RejectedExecutionException`
- **Summary:** This base implementation submits a call to `#getOrNull(Object)` on `#asyncExecutor`.
- **Documentation:** inherits documentation from the overridden member
- **Parameters:**
  - `key` (`K`) — the cache key to look up; null-handling is implementation-defined (most implementations reject null)
- **Returns:** a ContinuableFuture that completes with the value returned by `#getOrNull(Object)`, including any loaded value or `null`. The future completes exceptionally if the underlying call throws.
- **Throws:**
  - `java.lang.IllegalStateException` — if the shared `#asyncExecutor` has been explicitly shut down
  - `java.util.concurrent.RejectedExecutionException` — if its backing thread pool rejects the task, including during JVM shutdown
- **Examples:**
  ```java
  Cache<String, String> cache = new LocalCache<>(100, 0);
  cache.put("k", "v");                       // seed an entry

  ContinuableFuture<String> f = cache.asyncGetOrNull("k");
  f.get();                                   // returns "v"; throws InterruptedException, ExecutionException

  // Edge: missing key resolves to null (no Optional wrapper).
  cache.asyncGetOrNull("absent").get();      // returns null
  ```
- **See also:** `#getOrNull(Object)`, `#asyncGet(Object)`
##### asyncPut(...) -> ContinuableFuture<Boolean>
- **Signature:** `public ContinuableFuture<Boolean> asyncPut(final K key, final V value) throws IllegalStateException, RejectedExecutionException`
- **Summary:** This base implementation submits a call to `#put(Object, Object)` on `#asyncExecutor`.
- **Documentation:** inherits documentation from the overridden member
- **Parameters:**
  - `key` (`K`) — the cache key to store the value under; null-handling is implementation-defined (most implementations reject null)
  - `value` (`V`) — the value to cache; null-handling is implementation-defined
- **Returns:** a ContinuableFuture that completes with `true` on success, `false` otherwise. The future completes exceptionally if the underlying `put` call throws.
- **Throws:**
  - `java.lang.IllegalStateException` — if the shared `#asyncExecutor` has been explicitly shut down
  - `java.util.concurrent.RejectedExecutionException` — if its backing thread pool rejects the task, including during JVM shutdown
- **Examples:**
  ```java
  Cache<String, String> cache = new LocalCache<>(100, 0);

  // Stores asynchronously using the construction-time default expiration.
  ContinuableFuture<Boolean> f = cache.asyncPut("k", "v");
  f.get();                                   // returns true; throws InterruptedException, ExecutionException
  cache.getOrNull("k");                      // returns "v"

  // Overwriting an existing key also completes with true.
  cache.asyncPut("k", "v2").get();           // returns true
  cache.getOrNull("k");                      // returns "v2"
  ```
- **See also:** `#put(Object, Object)`, `#asyncPut(Object, Object, long, long)`
- **Signature:** `public ContinuableFuture<Boolean> asyncPut(final K key, final V value, final long liveTime, final long maxIdleTime) throws IllegalStateException, RejectedExecutionException`
- **Summary:** This base implementation submits a call to `#put(Object, Object, long, long)` on `#asyncExecutor`.
- **Documentation:** inherits documentation from the overridden member
- **Parameters:**
  - `key` (`K`) — the cache key to store the value under; null-handling is implementation-defined (most implementations reject null)
  - `value` (`V`) — the value to cache; null-handling is implementation-defined
  - `liveTime` (`long`) — the requested time-to-live in milliseconds from insertion; where per-entry TTL is supported, a value `<= 0` means no per-entry TTL
  - `maxIdleTime` (`long`) — the maximum idle time in milliseconds since last access; a value `<= 0` means no idle timeout where supported; this parameter may be ignored by the implementation
- **Returns:** a ContinuableFuture that completes with `true` on success, `false` otherwise. The future completes exceptionally if the underlying `put` call throws.
- **Throws:**
  - `java.lang.IllegalStateException` — if the shared `#asyncExecutor` has been explicitly shut down
  - `java.util.concurrent.RejectedExecutionException` — if its backing thread pool rejects the task, including during JVM shutdown
- **Examples:**
  ```java
  Cache<String, String> cache = new LocalCache<>(100, 0);

  // Stores asynchronously with an explicit 5s live time and 5s idle time.
  ContinuableFuture<Boolean> f = cache.asyncPut("k", "v", 5000L, 5000L);
  f.get();                                   // returns true; throws InterruptedException, ExecutionException
  cache.getOrNull("k");                      // returns "v"

  // Edge: liveTime <= 0 means no expiration by TTL.
  cache.asyncPut("forever", "v", 0L, 0L).get();  // returns true
  cache.getOrNull("forever");                    // returns "v"
  ```
- **See also:** `#put(Object, Object, long, long)`, `#asyncPut(Object, Object)`
##### asyncRemove(...) -> ContinuableFuture<Void>
- **Signature:** `public ContinuableFuture<Void> asyncRemove(final K key) throws IllegalStateException, RejectedExecutionException`
- **Summary:** This base implementation submits a call to `#remove(Object)` on `#asyncExecutor` and completes the returned future with a `null` result when the removal finishes.
- **Documentation:** inherits documentation from the overridden member
- **Contract:**
  - This base implementation submits a call to `#remove(Object)` on `#asyncExecutor` and completes the returned future with a `null` result when the removal finishes.
- **Parameters:**
  - `key` (`K`) — the cache key to remove; null-handling is implementation-defined (most implementations reject null)
- **Returns:** a ContinuableFuture that completes (with a `null` result) when the removal has finished. The future completes exceptionally if the underlying `remove` call throws.
- **Throws:**
  - `java.lang.IllegalStateException` — if the shared `#asyncExecutor` has been explicitly shut down
  - `java.util.concurrent.RejectedExecutionException` — if its backing thread pool rejects the task, including during JVM shutdown
- **Examples:**
  ```java
  Cache<String, String> cache = new LocalCache<>(100, 0);
  cache.put("k", "v");                       // seed an entry

  ContinuableFuture<Void> f = cache.asyncRemove("k");
  f.get();                                   // returns null (Void); throws InterruptedException, ExecutionException
  cache.getOrNull("k");                      // returns null (entry removed)

  // Edge: removing an absent key still completes normally with a null result.
  cache.asyncRemove("absent").get();         // returns null
  ```
- **See also:** `#remove(Object)`, `#asyncPut(Object, Object)`
##### asyncContainsKey(...) -> ContinuableFuture<Boolean>
- **Signature:** `public ContinuableFuture<Boolean> asyncContainsKey(final K key) throws IllegalStateException, RejectedExecutionException`
- **Summary:** This base implementation submits a call to `#containsKey(Object)` on `#asyncExecutor`.
- **Documentation:** inherits documentation from the overridden member
- **Parameters:**
  - `key` (`K`) — the cache key to check for; null-handling is implementation-defined (most implementations reject null)
- **Returns:** a ContinuableFuture that completes with the result of `#containsKey(Object)`. The future completes exceptionally if the underlying call throws.
- **Throws:**
  - `java.lang.IllegalStateException` — if the shared `#asyncExecutor` has been explicitly shut down
  - `java.util.concurrent.RejectedExecutionException` — if its backing thread pool rejects the task, including during JVM shutdown
- **Examples:**
  ```java
  Cache<String, String> cache = new LocalCache<>(100, 0);
  cache.put("k", "v");                       // seed an entry

  ContinuableFuture<Boolean> f = cache.asyncContainsKey("k");
  f.get();                                   // returns true; throws InterruptedException, ExecutionException

  // Edge: key never stored resolves to false.
  cache.asyncContainsKey("absent").get();    // returns false
  ```
- **See also:** `#containsKey(Object)`, `#asyncGet(Object)`
##### getProperties(...) -> Properties<String, Object>
- **Signature:** `public Properties<String, Object> getProperties()`
- **Summary:** This base implementation returns the `#properties` instance held by this cache.
- **Documentation:** inherits documentation from the overridden member
- **Contract:**
  - Iteration over `keySet()`, `values()`, or `entrySet()` is not safe during concurrent mutation because the synchronized backing map's mutex is not exposed; call `Properties#copy()` first when a stable traversal is required.
- **Parameters:**
  - (none)
- **Returns:** the properties container for this cache; never `null`
- **Examples:**
  ```java
  Cache<String, String> cache = new LocalCache<>(100, 0);

  Properties<String, Object> props = cache.getProperties();
  props.isEmpty();                           // returns true (no properties set yet)

  // The same backing instance is returned on each call and reflects later writes.
  cache.setProperty("region", "us-east");    // returns null (no previous mapping)
  boolean same = cache.getProperties() == props;   // true (same instance)
  cache.getProperties().get("region");       // returns "us-east"
  ```
- **See also:** `#getProperty(String)`, `#setProperty(String, Object)`
##### getProperty(...) -> T
- **Signature:** `@SuppressWarnings("unchecked") public <T> T getProperty(final String propName)`
- **Summary:** This base implementation looks up the value in `#properties` and returns it via an unchecked cast to `T`.
- **Documentation:** inherits documentation from the overridden member
- **Parameters:**
  - `propName` (`String`) — the property name to look up
- **Returns:** the property value cast to `T`, or `null` if no such property is set
- **Examples:**
  ```java
  Cache<String, String> cache = new LocalCache<>(100, 0);
  cache.setProperty("ttlSeconds", 60);       // returns null (no previous mapping)

  Integer ttl = cache.getProperty("ttlSeconds");
  ttl.intValue();                            // returns 60

  // Edge: an unset property returns null (regardless of the inferred type T).
  String missing = cache.getProperty("absent");  // returns null
  ```
- **See also:** `#getProperties()`, `#setProperty(String, Object)`
##### setProperty(...) -> T
- **Signature:** `@SuppressWarnings("unchecked") public <T> T setProperty(final String propName, final Object propValue) throws IllegalStateException`
- **Summary:** This base implementation writes the value to `#properties` and returns the previous value via an unchecked cast to `T`.
- **Documentation:** inherits documentation from the overridden member
- **Contract:**
  - The previous value (if any) is returned as an unchecked cast to `T`; any `ClassCastException` surfaces at the call site, not inside this method.
- **Parameters:**
  - `propName` (`String`) — the property name to set
  - `propValue` (`Object`) — the property value to set (may be `null`)
- **Returns:** the previous value associated with this property name, or `null` if there was none
- **Throws:**
  - `java.lang.IllegalStateException` — if this cache has been closed
- **Examples:**
  ```java
  Cache<String, String> cache = new LocalCache<>(100, 0);

  // First write for a name has no previous mapping.
  String prev1 = cache.setProperty("name", "alpha");  // returns null
  cache.getProperty("name");                          // returns "alpha"

  // Re-setting the same name returns the value it replaced.
  String prev2 = cache.setProperty("name", "beta");   // returns "alpha"
  cache.getProperty("name");                          // returns "beta"
  ```
- **See also:** `#getProperty(String)`, `#removeProperty(String)`
##### removeProperty(...) -> T
- **Signature:** `@SuppressWarnings("unchecked") public <T> T removeProperty(final String propName) throws IllegalStateException`
- **Summary:** This base implementation removes the entry from `#properties` and returns the removed value via an unchecked cast to `T`.
- **Documentation:** inherits documentation from the overridden member
- **Parameters:**
  - `propName` (`String`) — the property name to remove
- **Returns:** the previous value associated with this property name, or `null` if there was none
- **Throws:**
  - `java.lang.IllegalStateException` — if this cache has been closed
- **Examples:**
  ```java
  Cache<String, String> cache = new LocalCache<>(100, 0);
  cache.setProperty("foo", "bar");           // returns null (no previous mapping)

  String removed = cache.removeProperty("foo");
  assert "bar".equals(removed);
  cache.getProperty("foo");                  // returns null (entry removed)

  // Edge: removing a name that was never set returns null.
  cache.removeProperty("never-set");         // returns null
  ```
- **See also:** `#getProperty(String)`, `#setProperty(String, Object)`

### Class AbstractDistributedCacheClient (com.landawn.abacus.cache.AbstractDistributedCacheClient)
Abstract base class for distributed cache client implementations.

**Thread-safety:** conditional
**Nullability:** unspecified
**Implements:** `DistributedCacheClient<T>`

#### Public Constructors
- (none)

#### Public Static Methods
- (none)

#### Public Instance Methods
##### serverUrl(...) -> String
- **Signature:** `public String serverUrl()`
- **Summary:** Returns the server URL(s) this client is configured to use.
- **Parameters:**
  - (none)
- **Returns:** the server URL(s) for this client, never `null` or blank
- **Examples:**
  ```java
  String url = client.serverUrl();            // client is the application's shared cache client
  System.out.println("Connected to: " + url); // Output: "Connected to: localhost:11211"
  ```
##### getBulk(...) -> Map<String, T>
- **Signature:** `public Map<String, T> getBulk(final String... keys) throws UnsupportedOperationException`
- **Summary:** Optional bulk retrieval using varargs.
- **Contract:**
  - Subclasses should override this method if the underlying cache system supports bulk retrieval operations (such as Memcached's getMulti or Redis's MGET).
  - If the underlying system does not provide native bulk operations, subclasses may choose to implement this by performing multiple individual get operations, though this defeats the performance benefit.
  - When to Override: The underlying cache system provides native bulk retrieval operations Performance optimization is needed for multi-key retrieval scenarios Your use case frequently requires retrieving multiple related cache entries Thread Safety: Implementations must be thread-safe and handle concurrent access safely across distributed cache clients.
- **Parameters:**
  - `keys` (`String...`) — the cache keys to retrieve values for (variable number of String arguments), must not be `null` or contain `null` elements
- **Returns:** a map of found key-value pairs, never `null` (may be empty if no keys are found). This describes the contract of overriding implementations; the base-class default never returns normally and always throws `UnsupportedOperationException`.
- **Throws:**
  - `java.lang.UnsupportedOperationException` — always; this base implementation does not support the operation
- **Examples:**
  ```java
  // The shared application client below must override getBulk (SpyMemcached does).
  AbstractDistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
  Map<String, User> users = client.getBulk("user:123", "user:456", "user:789");   // map of found keys; missing keys are absent
  users.forEach((key, user) -> System.out.println(key + ": " + user.getName()));  // prints one line per found entry

  // Handling missing keys: the returned map only contains keys that were actually present.
  String[] requestedKeys = {"user:1", "user:2", "user:3"};                                          // setup: 3 requested keys
  Map<String, User> result = client.getBulk(requestedKeys);                                         // size() <= 3 (only found keys)
  System.out.println("Retrieved " + result.size() + " out of " + requestedKeys.length + " users");  // prints e.g. "Retrieved 2 out of 3 users"

  // Without a subclass override, the base-class default does not validate keys and simply throws:
  AbstractDistributedCacheClient<User> plain = new MyDistributedCache<>("localhost:11211");  // does not override getBulk
  try {
      plain.getBulk("user:123");                                                   // throws UnsupportedOperationException (default impl)
  } catch (UnsupportedOperationException expected) {
      // This implementation has no bulk operation.
  }
  ```
- **See also:** `#getBulk(Collection)`
- **Signature:** `public Map<String, T> getBulk(final Collection<String> keys) throws UnsupportedOperationException`
- **Summary:** Optional bulk retrieval using a collection.
- **Contract:**
  - Subclasses should override this method if the underlying cache system supports bulk retrieval operations (such as Memcached's getMulti or Redis's MGET).
  - If the underlying system does not provide native bulk operations, subclasses may choose to implement this by performing multiple individual get operations, though this defeats the performance benefit.
  - When to Override: The underlying cache system provides native bulk retrieval operations Performance optimization is needed for multi-key retrieval scenarios Your use case frequently requires retrieving multiple related cache entries You need to accept dynamically generated key collections (Lists, Sets, etc.) Implementation Note: If you override this method, you should typically also override `#getBulk(String...)` and potentially implement it by delegating to this method using `Arrays.asList(keys)` for consistency.
  - Thread Safety: Implementations must be thread-safe and handle concurrent access safely across distributed cache clients.
- **Parameters:**
  - `keys` (`Collection<String>`) — the collection of cache keys to retrieve values for, must not be `null` or contain `null` elements
- **Returns:** a map of found key-value pairs, never `null` (may be empty if no keys are found). This describes the contract of overriding implementations; the base-class default never returns normally and always throws `UnsupportedOperationException`.
- **Throws:**
  - `java.lang.UnsupportedOperationException` — always; this base implementation does not support the operation
- **Examples:**
  ```java
  // The shared application client below must override getBulk (SpyMemcached does).
  AbstractDistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
  List<String> userKeys = Arrays.asList("user:123", "user:456", "user:789");       // setup: 3 keys to look up
  Map<String, User> users = client.getBulk(userKeys);                              // map of found keys; missing keys are absent
  System.out.println("Retrieved " + users.size() + " users");                      // prints e.g. "Retrieved 2 users"

  // Using with dynamically generated keys
  List<Integer> userIds = Arrays.asList(101, 102, 103);                            // setup: source ids
  Set<String> generatedKeys = userIds.stream()
          .map(id -> "user:" + id)
          .collect(Collectors.toSet());                                            // -> {"user:101", "user:102", "user:103"}
  Map<String, User> generatedUsers = client.getBulk(generatedKeys);                // map of found keys; missing keys are absent

  // Without a subclass override, the base-class default does not validate keys and simply throws:
  AbstractDistributedCacheClient<User> plain = new MyDistributedCache<>("localhost:11211");  // does not override getBulk
  try {
      plain.getBulk(userKeys);                                                     // throws UnsupportedOperationException (default impl)
  } catch (UnsupportedOperationException expected) {
      // This implementation has no bulk operation.
  }
  ```
- **See also:** `#getBulk(String...)`
##### flushAll(...) -> void
- **Signature:** `public void flushAll() throws UnsupportedOperationException`
- **Summary:** Requests removal of cached data in the scope defined by the concrete implementation.
- **Contract:**
  - Subclasses should override this method if the underlying cache system supports flush/clear operations (such as Memcached's flush_all or Redis's FLUSHALL/FLUSHDB).
  - When to Override: The underlying cache system provides flush/clear operations You need to support testing scenarios that require a clean cache state Administrative operations require the ability to clear all cached data Implementation Note: An override must document its backend-specific scope.
  - The operation should be idempotent (safe to call multiple times).
  - Thread Safety: Implementations must be thread-safe.
- **Parameters:**
  - (none)
- **Throws:**
  - `java.lang.UnsupportedOperationException` — always; this base implementation does not support the operation
- **Examples:**
  ```java
  // WARNING: a subclass override may remove every entry in its documented backend scope.
  // SpyMemcached, for example, requests a full flush from every configured server.

  // Without a subclass override, the base-class default simply throws:
  AbstractDistributedCacheClient<User> client = new MyDistributedCache<>("localhost:11211");  // does not override flushAll()
  try {
      client.flushAll();                                          // throws UnsupportedOperationException (default impl)
  } catch (UnsupportedOperationException expected) {
      // This implementation has no flush operation.
  }

  // Safe usage in testing
  @AfterEach
  public void cleanupCache() {
      try {
          cacheClient.flushAll();                                 // throws UnsupportedOperationException if unsupported
      } catch (UnsupportedOperationException e) {
          // Cache implementation doesn't support flush
      }
  }
  ```

### Interface Cache (com.landawn.abacus.cache.Cache)
The core interface for all cache implementations in the Abacus framework.

**Thread-safety:** unspecified
**Nullability:** unspecified

#### Public Constructors
- (none)

#### Public Fields
- `public static final long DEFAULT_LIVE_TIME` — Default time-to-live for cache entries: 3 hours (in milliseconds).
  - **Value:** `3 * 60 * 60 * 1000L`
- `public static final long DEFAULT_MAX_IDLE_TIME` — Default maximum idle time for cache entries: 30 minutes (in milliseconds).
  - **Value:** `30 * 60 * 1000L`

#### Public Static Methods
- (none)

#### Public Instance Methods
##### get(...) -> Optional<V>
- **Signature:** `Optional<V> get(final K key) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Retrieves a value from the cache wrapped in an Optional.
- **Contract:**
  - Wraps the result of `#getOrNull(Object)` in an `Optional`, including a value supplied by a read-through cache loader when the implementation supports loading.
  - Behavior: Returns `Optional.empty()` when no value is available after any implementation-specific loading An absent or expired entry may trigger a cache loader; a loaded value is wrapped in the Optional Also returns `Optional.empty()` for a present entry explicitly mapped to `null` when the implementation permits null values.
- **Parameters:**
  - `key` (`K`) — the cache key to look up; null-handling is implementation-defined (most implementations reject null)
- **Returns:** an Optional containing the non-null value returned by `getOrNull`, whether cached or loaded, or an empty Optional when that method returns `null`
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if `key` is `null` and the implementation rejects `null` keys, as all built-in implementations do, or the key cannot be encoded for its backend
  - `java.lang.RuntimeException` — if the implementation propagates a backend read, deserialization, or cache-loader failure
- **Examples:**
  ```java
  Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);

  // Basic retrieval with functional approach
  Optional<User> user = cache.get("user:123");
  user.ifPresent(u -> System.out.println("Found: " + u.getName()));

  // With default value
  User result = cache.get("user:123").orElse(defaultUser);

  // Chaining operations
  String userName = cache.get("user:123")
      .map(User::getName)
      .orElse("Unknown");
  ```
- **See also:** `#getOrNull(Object)`, `#asyncGet(Object)`
##### getOrNull(...) -> V
- **Signature:** `V getOrNull(final K key) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Retrieves a value from the cache directly, returning `null` when no value is available.
- **Contract:**
  - Behavior: Returns `null` when a missing or expired entry is not replaced by a loaded value Also returns `null` for a stored null value or a suppressed backend failure where supported May update the last-access time for idle-timeout tracking (implementation-specific) A missing key alone is not an error, but loading or backend access may fail as documented below Usage Examples:
- **Parameters:**
  - `key` (`K`) — the cache key to look up; null-handling is implementation-defined (most implementations reject null)
- **Returns:** the cached or loaded value, or `null` when no value is available; implementations that suppress backend failures may also return `null` for those failures
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if `key` is `null` and the implementation rejects `null` keys, as all built-in implementations do, or the key cannot be encoded for its backend
  - `java.lang.RuntimeException` — if the implementation propagates a backend read, deserialization, or cache-loader failure
- **Examples:**
  ```java
  Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);

  // Basic retrieval with null check
  User user = cache.getOrNull("user:123");
  if (user != null) {
      System.out.println(user.getName());
  }

  // Cache-aside with database fallback
  User result = cache.getOrNull("user:123");
  if (result == null) {
      result = loadFromDatabase("user:123");
      cache.put("user:123", result);
  }
  ```
- **See also:** `#get(Object)`, `#asyncGetOrNull(Object)`
##### put(...) -> boolean
- **Signature:** `boolean put(final K key, final V value) throws IllegalStateException, IllegalArgumentException, RuntimeException, StackOverflowError`
- **Summary:** Stores a key-value pair in the cache using the implementation's default expiration settings.
- **Contract:**
  - If the operation succeeds and the key already exists, its value is replaced and its expiration is reset.
  - Behavior: A successful write overwrites any existing entry with the same key A successful write resets the implementation's applicable expiration counters May trigger eviction of other entries if cache capacity is reached May return `false` when storage is rejected; backend failures may also throw Usage Examples:
- **Parameters:**
  - `key` (`K`) — the cache key to store the value under; null-handling is implementation-defined (most implementations reject null)
  - `value` (`V`) — the value to cache; null-handling is implementation-defined
- **Returns:** `true` if the implementation reports success, `false` if storage was rejected. Success does not guarantee retention: eviction, expiration, concurrent changes, or a backend resilience policy may discard the entry. Backend failures may throw an exception
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if `key` or `value` is `null` and the implementation rejects that `null` argument; all built-in implementations reject a `null` key, and all except `DistributedCache` (which leaves `null` values to its client) also reject a `null` value. The implementation may also reject a key or configured default expiration that its backend cannot encode
  - `java.lang.RuntimeException` — if the implementation propagates a backend write, serialization, or cache callback failure
  - `java.lang.StackOverflowError` — if Kryo serialization (used by the Kryo-backed distributed clients and, by default, the off-heap caches) encounters a cycle made only of collections, maps, or arrays in `value`
- **Examples:**
  ```java
  Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);

  // Basic put operation
  User user = loadFromDatabase("user:123");
  boolean success = cache.put("user:123", user);
  if (success) {
      System.out.println("User cached successfully");
  }

  // Update existing entry
  User updatedUser = loadFromDatabase("user:123");
  cache.put("user:123", updatedUser);   // Replaces previous value

  // Cache-aside pattern
  User cachedUser = cache.getOrNull("user:123");
  if (cachedUser == null) {
      cachedUser = loadFromDatabase("user:123");
      cache.put("user:123", cachedUser);
  }
  ```
- **See also:** `#put(Object, Object, long, long)`, `#asyncPut(Object, Object)`
- **Signature:** `boolean put(final K key, final V value, long liveTime, long maxIdleTime) throws IllegalStateException, IllegalArgumentException, RuntimeException, StackOverflowError`
- **Summary:** Stores a key-value pair in the cache with custom expiration settings.
- **Contract:**
  - If the key already exists, its value and expiration settings are replaced.
  - The entry is evicted when either the TTL elapses (measured from insertion) or the idle time is exceeded (measured from last access), whichever happens first.
  - Entry is removed if not accessed within this duration.
  - If both are set, the entry expires when either condition is met (whichever comes first).
- **Parameters:**
  - `key` (`K`) — the cache key to store the value under; null-handling is implementation-defined (most implementations reject null)
  - `value` (`V`) — the value to cache; null-handling is implementation-defined
  - `liveTime` (`long`) — the requested time-to-live in milliseconds from insertion; where per-entry TTL is supported, a value `<= 0` means no per-entry TTL
  - `maxIdleTime` (`long`) — the maximum idle time in milliseconds since last access; a value `<= 0` means no idle timeout where supported; this parameter may be ignored by the implementation
- **Returns:** `true` if the implementation reports success, `false` if storage was rejected. Success does not guarantee retention: eviction, expiration, concurrent changes, or a backend resilience policy may discard the entry. Backend failures may throw an exception
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if `key` or `value` is `null` and the implementation rejects that `null` argument; all built-in implementations reject a `null` key, and all except `DistributedCache` (which leaves `null` values to its client) also reject a `null` value. Implementations may also reject a key or `liveTime` their backend cannot encode (e.g. `DistributedCache` with a `liveTime` beyond the client's expiration range)
  - `java.lang.RuntimeException` — if the implementation propagates a backend write, serialization, or cache callback failure
  - `java.lang.StackOverflowError` — if Kryo serialization (used by the Kryo-backed distributed clients and, by default, the off-heap caches) encounters a cycle made only of collections, maps, or arrays in `value`
- **Examples:**
  ```java
  Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);

  // Session with 1 hour TTL, 30 minute idle timeout
  cache.put("session:abc", session, 3600000, 1800000);

  // Temporary data with 5 second TTL, no idle timeout
  cache.put("temp:data", data, 5000, 0);

  // Short-lived data that expires quickly if unused
  cache.put("otp:token", token, 300000, 60000);   // 5min TTL, 1min idle
  ```
- **See also:** `#put(Object, Object)`, `#asyncPut(Object, Object, long, long)`
##### remove(...) -> void
- **Signature:** `void remove(final K key) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Removes an entry from the cache.
- **Contract:**
  - Behavior: Removes the entry if the key exists Does nothing if the key does not exist (no error) Safe to call multiple times with the same key Usage Examples:
- **Parameters:**
  - `key` (`K`) — the cache key to remove; null-handling is implementation-defined (most implementations reject null)
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if `key` is `null` and the implementation rejects `null` keys, as all built-in implementations do, or the key cannot be encoded for its backend
  - `java.lang.RuntimeException` — if the implementation propagates a backend deletion or cache-writer failure
- **Examples:**
  ```java
  Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);

  // Basic removal
  cache.remove("user:123");   // Removes if exists, no error if not

  // Invalidate on update failure
  boolean updated = updateUser(userId);
  if (!updated) {
      cache.remove("user:" + userId);
  }
  ```
- **See also:** `#clear()`, `#containsKey(Object)`, `#asyncRemove(Object)`
##### containsKey(...) -> boolean
- **Signature:** `boolean containsKey(final K key) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Returns whether the cache contains an entry for the specified key.
- **Contract:**
  - Behavior: Returns `true` if the implementation currently considers the key present Returns `false` if the key does not exist Whether the call updates access time / LRU ordering / idle counters is implementation-defined Whether expired-but-not-yet-evicted entries are visible here is implementation-defined Whether the call is side-effect-free is implementation-defined: some implementations (e.g.
- **Parameters:**
  - `key` (`K`) — the cache key to check for; null-handling is implementation-defined (most implementations reject null)
- **Returns:** `true` if the implementation currently considers an entry for the key present; visibility of expired or null-valued entries is implementation-specific
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if `key` is `null` and the implementation rejects `null` keys, as all built-in implementations do, or the key cannot be encoded for its backend
  - `java.lang.RuntimeException` — if the implementation propagates a backend lookup, deserialization, or cache-loader failure
- **Examples:**
  ```java
  Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);

  if (cache.containsKey("user:123")) {
      System.out.println("User is cached");
  }

  if (!cache.containsKey("config:settings")) {
      cache.put("config:settings", loadConfigFromFile());
  }
  ```
- **See also:** `#get(Object)`, `#asyncContainsKey(Object)`
##### asyncGet(...) -> ContinuableFuture<Optional<V>>
- **Signature:** `ContinuableFuture<Optional<V>> asyncGet(final K key) throws IllegalStateException, RejectedExecutionException`
- **Summary:** Asynchronously retrieves a value from the cache wrapped in an Optional.
- **Parameters:**
  - `key` (`K`) — the cache key to look up; null-handling is implementation-defined (most implementations reject null)
- **Returns:** a ContinuableFuture that completes with the Optional returned by `#get(Object)`, including any loaded value. The future completes exceptionally if the underlying call throws.
- **Throws:**
  - `java.lang.IllegalStateException` — if the implementation's asynchronous executor has been shut down before submission
  - `java.util.concurrent.RejectedExecutionException` — if the executor rejects the retrieval task
- **Examples:**
  ```java
  Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);

  cache.asyncGet("user:123")
       .thenRunAsync(opt -> opt.ifPresent(u -> System.out.println("Found: " + u.getName())));

  // Block and transform the result
  String name = cache.asyncGet("user:123")
       .get()                                 // throws InterruptedException, ExecutionException
       .map(User::getName)
       .orElse("Unknown");
  ```
- **See also:** `#get(Object)`, `#asyncGetOrNull(Object)`
##### asyncGetOrNull(...) -> ContinuableFuture<V>
- **Signature:** `ContinuableFuture<V> asyncGetOrNull(final K key) throws IllegalStateException, RejectedExecutionException`
- **Summary:** Asynchronously retrieves a value from the cache, returning `null` on a miss.
- **Parameters:**
  - `key` (`K`) — the cache key to look up; null-handling is implementation-defined (most implementations reject null)
- **Returns:** a ContinuableFuture that completes with the value returned by `#getOrNull(Object)`, including any loaded value or `null`. The future completes exceptionally if the underlying call throws.
- **Throws:**
  - `java.lang.IllegalStateException` — if the implementation's asynchronous executor has been shut down before submission
  - `java.util.concurrent.RejectedExecutionException` — if the executor rejects the retrieval task
- **Examples:**
  ```java
  Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);

  cache.asyncGetOrNull("user:123")
       .thenRunAsync(user -> {
           if (user != null) {
               process(user);
           }
       });
  ```
- **See also:** `#getOrNull(Object)`, `#asyncGet(Object)`
##### asyncPut(...) -> ContinuableFuture<Boolean>
- **Signature:** `ContinuableFuture<Boolean> asyncPut(final K key, final V value) throws IllegalStateException, RejectedExecutionException`
- **Summary:** Asynchronously stores a key-value pair using the implementation's default expiration settings.
- **Parameters:**
  - `key` (`K`) — the cache key to store the value under; null-handling is implementation-defined (most implementations reject null)
  - `value` (`V`) — the value to cache; null-handling is implementation-defined
- **Returns:** a ContinuableFuture that completes with `true` on success, `false` otherwise. The future completes exceptionally if the underlying `put` call throws.
- **Throws:**
  - `java.lang.IllegalStateException` — if the implementation's asynchronous executor has been shut down before submission
  - `java.util.concurrent.RejectedExecutionException` — if the executor rejects the storage task
- **Examples:**
  ```java
  Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);

  cache.asyncPut("user:123", user)
       .thenRunAsync(success -> {
           if (success) {
               log("User cached successfully");
           }
       });
  ```
- **See also:** `#put(Object, Object)`, `#asyncPut(Object, Object, long, long)`
- **Signature:** `ContinuableFuture<Boolean> asyncPut(final K key, final V value, long liveTime, long maxIdleTime) throws IllegalStateException, RejectedExecutionException`
- **Summary:** Asynchronously stores a key-value pair with custom expiration settings.
- **Parameters:**
  - `key` (`K`) — the cache key to store the value under; null-handling is implementation-defined (most implementations reject null)
  - `value` (`V`) — the value to cache; null-handling is implementation-defined
  - `liveTime` (`long`) — the requested time-to-live in milliseconds from insertion; where per-entry TTL is supported, a value `<= 0` means no per-entry TTL
  - `maxIdleTime` (`long`) — the maximum idle time in milliseconds since last access; a value `<= 0` means no idle timeout where supported; this parameter may be ignored by the implementation
- **Returns:** a ContinuableFuture that completes with `true` on success, `false` otherwise. The future completes exceptionally if the underlying `put` call throws.
- **Throws:**
  - `java.lang.IllegalStateException` — if the implementation's asynchronous executor has been shut down before submission
  - `java.util.concurrent.RejectedExecutionException` — if the executor rejects the storage task
- **Examples:**
  ```java
  Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);

  // Session with 1 hour TTL, 30 minute idle timeout
  cache.asyncPut("session:abc", session, 3600000, 1800000)
       .thenRunAsync(success -> log("Session cached: " + success));

  // Short-lived temporary data. Inspect the result: a no-argument continuation would
  // also run when the put is rejected (the future completes normally with false).
  cache.asyncPut("temp:data", data, 5000, 0)
       .thenRunAsync(stored -> log(stored ? "Temporary data cached for 5 seconds" : "Temporary data rejected"));
  ```
- **See also:** `#put(Object, Object, long, long)`, `#asyncPut(Object, Object)`
##### asyncRemove(...) -> ContinuableFuture<Void>
- **Signature:** `ContinuableFuture<Void> asyncRemove(final K key) throws IllegalStateException, RejectedExecutionException`
- **Summary:** Asynchronously removes an entry from the cache.
- **Parameters:**
  - `key` (`K`) — the cache key to remove; null-handling is implementation-defined (most implementations reject null)
- **Returns:** a ContinuableFuture that completes (with a `null` result) when the removal has finished. The future completes exceptionally if the underlying `remove` call throws.
- **Throws:**
  - `java.lang.IllegalStateException` — if the implementation's asynchronous executor has been shut down before submission
  - `java.util.concurrent.RejectedExecutionException` — if the executor rejects the removal task
- **Examples:**
  ```java
  Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);

  cache.asyncRemove("user:123")
       .thenRunAsync(() -> log("User removed from cache"));
  ```
- **See also:** `#remove(Object)`, `#asyncPut(Object, Object)`
##### asyncContainsKey(...) -> ContinuableFuture<Boolean>
- **Signature:** `ContinuableFuture<Boolean> asyncContainsKey(final K key) throws IllegalStateException, RejectedExecutionException`
- **Summary:** Asynchronously checks if the cache contains a specific key.
- **Parameters:**
  - `key` (`K`) — the cache key to check for; null-handling is implementation-defined (most implementations reject null)
- **Returns:** a ContinuableFuture that completes with the result of `#containsKey(Object)`. The future completes exceptionally if the underlying call throws.
- **Throws:**
  - `java.lang.IllegalStateException` — if the implementation's asynchronous executor has been shut down before submission
  - `java.util.concurrent.RejectedExecutionException` — if the executor rejects the lookup task
- **Examples:**
  ```java
  Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);

  cache.asyncContainsKey("user:123")
       .thenRunAsync(exists -> log("User exists in cache: " + exists));
  ```
- **See also:** `#containsKey(Object)`, `#asyncGet(Object)`
##### keySet(...) -> Set<K>
- **Signature:** `Set<K> keySet() throws IllegalStateException, UnsupportedOperationException`
- **Summary:** Returns a set of the keys currently in the cache.
- **Parameters:**
  - (none)
- **Returns:** a set of cache keys; whether expired entries are included is implementation-defined
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.UnsupportedOperationException` — if the operation is not supported by this implementation (e.g., `DistributedCache`, `CaffeineCache`, and `Ehcache` always throw it)
- **Examples:**
  ```java
  Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);

  Set<String> keys = cache.keySet();
  keys.forEach(key -> System.out.println("Cached key: " + key));

  // The returned Set may be a snapshot, an unmodifiable set, or a live view. Copy it before
  // traversing while invalidating entries, and call remove() on the cache itself.
  Set.copyOf(cache.keySet()).stream()
      .filter(key -> key.startsWith("temp:"))
      .forEach(cache::remove);
  ```
- **See also:** `#size()`, `#containsKey(Object)`
##### size(...) -> int
- **Signature:** `int size() throws IllegalStateException, UnsupportedOperationException`
- **Summary:** Returns the number of entries currently in the cache.
- **Parameters:**
  - (none)
- **Returns:** the number of cache entries (may be an estimate depending on implementation)
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.UnsupportedOperationException` — if the operation is not supported by this implementation (e.g., `DistributedCache` and `Ehcache` always throw it)
- **Examples:**
  ```java
  Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);

  int count = cache.size();
  System.out.println("Cache contains " + count + " entries");

  if (cache.size() > threshold) {
      cache.clear();
  }
  ```
- **See also:** `#keySet()`, `#clear()`
##### clear(...) -> void
- **Signature:** `void clear() throws IllegalStateException, UnsupportedOperationException, RuntimeException`
- **Summary:** Removes all entries in the implementation-defined scope of this cache.
- **Parameters:**
  - (none)
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.UnsupportedOperationException` — if the operation is not supported by this implementation (e.g., some distributed cache backends)
  - `java.lang.RuntimeException` — if the implementation propagates a backend flush or entry-removal failure
- **Examples:**
  ```java
  Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);

  cache.clear();
  System.out.println("Cache size after clear: " + cache.size());   // typically 0

  if (configChanged()) {
      cache.clear();
      log("Cache cleared due to configuration change");
  }
  ```
- **See also:** `#remove(Object)`, `#size()`
##### close(...) -> void
- **Signature:** `void close() throws IllegalStateException, RuntimeException`
- **Summary:** Explicitly shuts down this cache before the end of its normal owner-managed lifecycle.
- **Contract:**
  - This operation is intended for early retirement of a cache instance, such as when replacing a dynamically configured cache or stopping the component that owns it.
  - After shutdown, the cache cannot be reopened; subsequent data operations throw `IllegalStateException` as documented on each method (a custom implementation may instead behave as if empty).
  - Typical resource cleanup: Releases cached entries and associated memory Stops background eviction threads (if any) Closes connections to remote cache servers (for distributed caches) Releases file handles or other system resources Usage Examples:
- **Parameters:**
  - (none)
- **Throws:**
  - `java.lang.IllegalStateException` — if the implementation rejects a close invoked reentrantly from a cache operation or callback
  - `java.lang.RuntimeException` — if the implementation propagates a failure while releasing cache storage or backend connections
- **Examples:**
  ```java
  Cache<String, User> cache = cacheRegistry.get("users");
  // ... the owning component uses the cache over its normal lifetime ...

  // Explicit early shutdown when the owner retires this cache instance.
  if (cacheRegistry.remove("users", cache)) {
      cache.close();
  }
  ```
- **See also:** `#isClosed()`
##### isClosed(...) -> boolean
- **Signature:** `boolean isClosed()`
- **Summary:** Returns whether the cache has entered its closed state.
- **Parameters:**
  - (none)
- **Returns:** `true` if the cache is closed, `false` otherwise
- **Examples:**
  ```java
  Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);

  if (cache.isClosed()) {
      System.out.println("Cache has already been retired");
  } else {
      cache.put("key", value);
  }
  ```
- **See also:** `#close()`
##### getProperties(...) -> Properties<String, Object>
- **Signature:** `Properties<String, Object> getProperties()`
- **Summary:** Returns the mutable property bag for this cache instance.
- **Contract:**
  - Implementations based on `AbstractCache` synchronize individual map operations and provide `Properties#copy()` for stable traversal, but their live-view iterators still must not race with mutation.
- **Parameters:**
  - (none)
- **Returns:** the properties container for this cache; never `null`
- **Examples:**
  ```java
  Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);

  Properties<String, Object> props = cache.getProperties();
  props.put("description", "User cache for active sessions");
  props.put("region", "us-west-2");

  String description = (String) cache.getProperties().get("description");
  ```
- **See also:** `#getProperty(String)`, `#setProperty(String, Object)`
##### getProperty(...) -> T
- **Signature:** `<T> T getProperty(String propName)`
- **Summary:** Retrieves a property value by name.
- **Parameters:**
  - `propName` (`String`) — the property name to look up
- **Returns:** the property value cast to `T`, or `null` if no such property is set
- **Examples:**
  ```java
  Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
  cache.setProperty("description", "User cache");
  cache.setProperty("maxRetries", 3);

  String description = cache.getProperty("description");
  Integer retries = cache.getProperty("maxRetries");

  String unknown = cache.getProperty("nonExistent");   // null
  ```
- **See also:** `#getProperties()`, `#setProperty(String, Object)`
##### setProperty(...) -> T
- **Signature:** `<T> T setProperty(String propName, Object propValue) throws IllegalStateException`
- **Summary:** Sets a property value.
- **Contract:**
  - The previous value (if any) is returned as an unchecked cast to `T`; any `ClassCastException` surfaces at the call site, not inside this method.
- **Parameters:**
  - `propName` (`String`) — the property name to set
  - `propValue` (`Object`) — the property value to set (may be `null`)
- **Returns:** the previous value associated with this property name, or `null` if there was none
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed (property reads remain usable; direct mutation through the map returned by `#getProperties()` is not routed through this lifecycle check)
- **Examples:**
  ```java
  Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);

  cache.setProperty("description", "User cache for session data");
  cache.setProperty("maxRetries", 3);

  // Update returns the old value
  String oldDescription = cache.setProperty("description", "Updated description");
  // oldDescription == "User cache for session data"

  // First set returns null
  String prev = cache.setProperty("newProperty", "value");   // prev is null
  ```
- **See also:** `#getProperty(String)`, `#removeProperty(String)`
##### removeProperty(...) -> T
- **Signature:** `<T> T removeProperty(String propName) throws IllegalStateException`
- **Summary:** Removes a property by name.
- **Parameters:**
  - `propName` (`String`) — the property name to remove
- **Returns:** the previous value associated with this property name, or `null` if there was none
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed (property reads remain usable; direct mutation through the map returned by `#getProperties()` is not routed through this lifecycle check)
- **Examples:**
  ```java
  Cache<String, User> cache = CacheFactory.createLocalCache(1000, 60000);
  cache.setProperty("description", "User cache");

  String oldValue = cache.removeProperty("description");   // "User cache"
  String notFound = cache.removeProperty("nonExistent");   // null
  ```
- **See also:** `#getProperty(String)`, `#setProperty(String, Object)`

### Class CacheFactory (com.landawn.abacus.cache.CacheFactory)
Factory class for creating various types of cache implementations.

**Thread-safety:** unspecified
**Nullability:** unspecified

#### Public Constructors
- (none)

#### Public Static Methods
##### createLocalCache(...) -> LocalCache<K, V>
- **Signature:** `public static <K, V> LocalCache<K, V> createLocalCache(final int capacity, final long evictDelay) throws IllegalArgumentException, RejectedExecutionException, IllegalStateException`
- **Summary:** Creates a new LocalCache with the specified capacity and eviction delay.
- **Contract:**
  - A value of 0 disables the periodic eviction scan; entries can still be lazily evicted when accessed.
- **Parameters:**
  - `capacity` (`int`) — the maximum number of entries the cache can hold (must be positive)
  - `evictDelay` (`long`) — the delay in milliseconds between eviction runs (0 to disable periodic eviction, must be non-negative)
- **Returns:** a new LocalCache instance with the specified configuration
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `capacity` is not positive or `evictDelay` is negative
  - `java.util.concurrent.RejectedExecutionException` — if `evictDelay` is positive and the pool's maintenance scheduler rejects the eviction task
  - `java.lang.IllegalStateException` — if the JVM is already shutting down when the underlying pool registers its shutdown hook
- **Examples:**
  ```java
  // Create cache with 1000 entries capacity, checking for expired entries every minute
  LocalCache<String, User> cache = CacheFactory.createLocalCache(1000, 60000); // returns a non-null LocalCache
  cache.put("user:123", user);                                                 // returns true (entry stored)
  User retrieved = cache.getOrNull("user:123");                                // returns the stored user (null if absent/expired)

  // Edge cases (validated by the underlying constructor):
  CacheFactory.createLocalCache(0, 60000);      // throws IllegalArgumentException (capacity must be positive)
  CacheFactory.createLocalCache(1000, -1);      // throws IllegalArgumentException (evictDelay must be non-negative)
  ```
- **See also:** `#createLocalCache(int, long, long, long)`, `#createLocalCache(KeyedObjectPool, long, long)`
- **Signature:** `public static <K, V> LocalCache<K, V> createLocalCache(final int capacity, final long evictDelay, final long defaultLiveTime, final long defaultMaxIdleTime) throws IllegalArgumentException, RejectedExecutionException, IllegalStateException`
- **Summary:** Creates a new LocalCache with fully customized parameters.
- **Parameters:**
  - `capacity` (`int`) — the maximum number of entries the cache can hold (must be positive)
  - `evictDelay` (`long`) — the delay in milliseconds between eviction runs (0 to disable periodic eviction, must be non-negative)
  - `defaultLiveTime` (`long`) — the default time-to-live in milliseconds for entries added without explicit TTL (0 or negative for no expiration)
  - `defaultMaxIdleTime` (`long`) — the default maximum idle time in milliseconds for entries added without explicit idle time (0 or negative for no idle timeout)
- **Returns:** a new LocalCache instance with the specified configuration
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `capacity` is not positive or `evictDelay` is negative
  - `java.util.concurrent.RejectedExecutionException` — if `evictDelay` is positive and the pool's maintenance scheduler rejects the eviction task
  - `java.lang.IllegalStateException` — if the JVM is already shutting down when the underlying pool registers its shutdown hook
- **Examples:**
  ```java
  // Create session cache: 500 capacity, check every 30s, expire after 30min or 15min idle
  LocalCache<String, Session> cache = CacheFactory.createLocalCache(
      500,                                // capacity: 500 entries max
      30000,                              // evictDelay: check every 30 seconds
      1800000,                            // defaultLiveTime: 30 minutes TTL
      900000                              // defaultMaxIdleTime: 15 minutes idle timeout
  );                                      // returns a non-null LocalCache
  cache.put("session:abc123", session);   // returns true; uses default TTL and idle time

  // Edge case: capacity must be positive (TTL/idle of 0 are accepted as "no expiration")
  CacheFactory.createLocalCache(0, 30000, 1800000, 900000);   // throws IllegalArgumentException (capacity must be positive)
  ```
- **See also:** `#createLocalCache(int, long)`, `#createLocalCache(KeyedObjectPool, long, long)`
- **Signature:** `public static <K, V> LocalCache<K, V> createLocalCache(final KeyedObjectPool<K, PoolableAdapter<V>> pool, final long defaultLiveTime, final long defaultMaxIdleTime) throws IllegalArgumentException`
- **Summary:** Creates a new LocalCache with a custom KeyedObjectPool.
- **Contract:**
  - The provided pool must be configured to handle PoolableAdapter objects and will be used directly by the cache for all entry storage and retrieval operations.
- **Parameters:**
  - `pool` (`KeyedObjectPool<K, PoolableAdapter<V>>`) — the pre-configured KeyedObjectPool to use for managing cache entries (must not be null)
  - `defaultLiveTime` (`long`) — the default time-to-live in milliseconds for entries added without explicit TTL (0 or negative for no expiration)
  - `defaultMaxIdleTime` (`long`) — the default maximum idle time in milliseconds for entries added without explicit idle time (0 or negative for no idle timeout)
- **Returns:** a new LocalCache instance configured with the specified pool
- **Throws:**
  - `java.lang.IllegalArgumentException` — if pool is null
- **Examples:**
  ```java
  // Create custom pool with specific configuration
  KeyedObjectPool<String, PoolableAdapter<User>> customPool =
      PoolFactory.createKeyedObjectPool(1000, 60000);   // capacity 1000, 60s eviction delay

  // Create cache using the custom pool
  LocalCache<String, User> cache = CacheFactory.createLocalCache(
      customPool,              // custom pool implementation (leads, so it cannot be confused
                               //   positionally with the capacity-first overloads)
      3600000,                 // defaultLiveTime: 1 hour
      1800000                  // defaultMaxIdleTime: 30 minutes
  );                           // returns a non-null LocalCache backed by customPool
  cache.put("user:123", user); // returns true (entry stored in customPool)

  // Edge case: a null pool is rejected
  CacheFactory.createLocalCache((KeyedObjectPool<String, PoolableAdapter<User>>) null, 3600000L, 1800000L);
      // throws IllegalArgumentException (pool must not be null)
  ```
- **See also:** `#createLocalCache(int, long)`, `#createLocalCache(int, long, long, long)`
##### createOffHeapCache(...) -> OffHeapCache<K, V>
- **Signature:** `public static <K, V> OffHeapCache<K, V> createOffHeapCache(final int capacityInMB) throws IllegalArgumentException, OutOfMemoryError, RejectedExecutionException, IllegalStateException`
- **Summary:** Creates a new `OffHeapCache` with the specified off-heap capacity, using default eviction delay and the framework default TTL (`Cache#DEFAULT_LIVE_TIME`) and idle time (`Cache#DEFAULT_MAX_IDLE_TIME`).
- **Parameters:**
  - `capacityInMB` (`int`) — the total off-heap memory to allocate, in megabytes (must be positive)
- **Returns:** a new OffHeapCache instance with the specified capacity
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `capacityInMB` is not positive
  - `java.lang.OutOfMemoryError` — if the native allocation cannot be reserved
  - `java.util.concurrent.RejectedExecutionException` — if the maintenance scheduler rejects the eviction task (this overload always schedules one, using the default eviction delay)
  - `java.lang.IllegalStateException` — if shutdown-hook registration is attempted during JVM shutdown
- **See also:** `#createOffHeapCache(int, long)`, `#createOffHeapCache(int, long, long, long)`, `OffHeapCache#builder()`
- **Signature:** `public static <K, V> OffHeapCache<K, V> createOffHeapCache(final int capacityInMB, final long evictDelay) throws IllegalArgumentException, OutOfMemoryError, RejectedExecutionException, IllegalStateException`
- **Summary:** Creates a new `OffHeapCache` with the specified off-heap capacity and eviction delay, using the framework default TTL (`Cache#DEFAULT_LIVE_TIME`) and idle time (`Cache#DEFAULT_MAX_IDLE_TIME`).
- **Parameters:**
  - `capacityInMB` (`int`) — the total off-heap memory to allocate, in megabytes (must be positive)
  - `evictDelay` (`long`) — the delay in milliseconds between eviction runs; `0` or a negative value disables periodic eviction
- **Returns:** a new OffHeapCache instance with the specified configuration
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `capacityInMB` is not positive
  - `java.lang.OutOfMemoryError` — if the native allocation cannot be reserved
  - `java.util.concurrent.RejectedExecutionException` — if `evictDelay` is positive and the maintenance scheduler rejects its task
  - `java.lang.IllegalStateException` — if shutdown-hook registration is attempted during JVM shutdown
- **See also:** `#createOffHeapCache(int)`, `#createOffHeapCache(int, long, long, long)`
- **Signature:** `public static <K, V> OffHeapCache<K, V> createOffHeapCache(final int capacityInMB, final long evictDelay, final long defaultLiveTime, final long defaultMaxIdleTime) throws IllegalArgumentException, OutOfMemoryError, RejectedExecutionException, IllegalStateException`
- **Summary:** Creates a new `OffHeapCache` with fully customized off-heap capacity, eviction delay, and default expiration behavior.
- **Parameters:**
  - `capacityInMB` (`int`) — the total off-heap memory to allocate, in megabytes (must be positive)
  - `evictDelay` (`long`) — the delay in milliseconds between eviction runs; `0` or a negative value disables periodic eviction
  - `defaultLiveTime` (`long`) — the default time-to-live in milliseconds for entries added without explicit TTL (0 or negative for no expiration)
  - `defaultMaxIdleTime` (`long`) — the default maximum idle time in milliseconds for entries added without explicit idle time (0 or negative for no idle timeout)
- **Returns:** a new OffHeapCache instance with the specified configuration
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `capacityInMB` is not positive
  - `java.lang.OutOfMemoryError` — if the native allocation cannot be reserved
  - `java.util.concurrent.RejectedExecutionException` — if `evictDelay` is positive and the maintenance scheduler rejects its task
  - `java.lang.IllegalStateException` — if shutdown-hook registration is attempted during JVM shutdown
- **See also:** `#createOffHeapCache(int)`, `#createOffHeapCache(int, long)`
##### createCaffeineCache(...) -> CaffeineCache<K, V>
- **Signature:** `public static <K, V> CaffeineCache<K, V> createCaffeineCache(final com.github.benmanes.caffeine.cache.Cache<K, V> caffeineCache) throws IllegalArgumentException`
- **Summary:** Wraps a pre-configured Caffeine cache as a framework `Cache` via `CaffeineCache`.
- **Parameters:**
  - `caffeineCache` (`com.github.benmanes.caffeine.cache.Cache<K, V>`) — the underlying Caffeine cache instance to wrap (must not be null)
- **Returns:** a new CaffeineCache wrapping the provided Caffeine instance
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `caffeineCache` is null
##### createEhcache(...) -> Ehcache<K, V>
- **Signature:** `public static <K, V> Ehcache<K, V> createEhcache(final org.ehcache.Cache<K, V> ehcache) throws IllegalArgumentException`
- **Summary:** Wraps a pre-configured Ehcache 3.x cache as a framework `Cache` via `Ehcache`.
- **Parameters:**
  - `ehcache` (`org.ehcache.Cache<K, V>`) — the underlying Ehcache 3.x cache instance to wrap (must not be null)
- **Returns:** a new Ehcache wrapper around the provided Ehcache instance
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `ehcache` is null
##### createDistributedCache(...) -> DistributedCache<K, V>
- **Signature:** `public static <K, V> DistributedCache<K, V> createDistributedCache(final DistributedCacheClient<V> client) throws IllegalArgumentException`
- **Summary:** Creates a DistributedCache wrapper for a distributed cache client.
- **Parameters:**
  - `client` (`DistributedCacheClient<V>`) — the distributed cache client to wrap (must not be null)
- **Returns:** a new DistributedCache instance wrapping the provided client
- **Throws:**
  - `java.lang.IllegalArgumentException` — if client is null
- **Examples:**
  ```java
  // Create Memcached client and wrap it
  SpyMemcached<User> memcachedClient = new SpyMemcached<>("localhost:11211", 5000);
  DistributedCache<String, User> cache = CacheFactory.createDistributedCache(memcachedClient);   // returns a non-null DistributedCache

  // Use the cache
  cache.put("user:123", user, 3600000, 0);      // returns true (sent to the wrapped client)
  User retrieved = cache.getOrNull("user:123"); // returns the value, or null if absent/circuit-open

  // Edge case: a null client is rejected
  CacheFactory.createDistributedCache((DistributedCacheClient<User>) null);   // throws IllegalArgumentException (client must not be null)
  ```
- **See also:** `#createDistributedCache(DistributedCacheClient, String)`, `#createDistributedCache(DistributedCacheClient, String, int, long)`, `#createCache(String)`
- **Signature:** `public static <K, V> DistributedCache<K, V> createDistributedCache(final DistributedCacheClient<V> client, final String keyPrefix) throws IllegalArgumentException`
- **Summary:** Creates a DistributedCache with a key prefix for namespace isolation.
- **Parameters:**
  - `client` (`DistributedCacheClient<V>`) — the distributed cache client to wrap (must not be null)
  - `keyPrefix` (`String`) — the key prefix to prepend to all keys (can be empty string or null for no prefix)
- **Returns:** a new DistributedCache instance wrapping the provided client with the specified key prefix
- **Throws:**
  - `java.lang.IllegalArgumentException` — if client is null, or if keyPrefix contains a non-printable-ASCII character, a space, or a control character
- **Examples:**
  ```java
  // Create Redis client
  JRedis<Session> redisClient = new JRedis<>("localhost:6379", 3000);

  // Create cache with namespace prefix
  DistributedCache<String, Session> cache =
      CacheFactory.createDistributedCache(redisClient, "myapp:sessions:");   // returns a non-null DistributedCache

  // Keys are automatically prefixed and Base64-encoded
  cache.put("user123", session, 3600000, 0);    // returns true
  // Actual cache key: "myapp:sessions:" + Base64("user123")
  Session s = cache.getOrNull("user123");        // returns the value, or null if absent/circuit-open

  // Pass null or an empty string as the second argument when an unprefixed cache is desired.

  // Edge case: a null client is rejected
  CacheFactory.createDistributedCache((DistributedCacheClient<Session>) null, "myapp:");   // throws IllegalArgumentException (client must not be null)
  ```
- **See also:** `#createDistributedCache(DistributedCacheClient)`, `#createDistributedCache(DistributedCacheClient, String, int, long)`, `#createCache(String)`
- **Signature:** `public static <K, V> DistributedCache<K, V> createDistributedCache(final DistributedCacheClient<V> client, final String keyPrefix, final int maxFailuresBeforeCircuitOpen, final long retryDelay) throws IllegalArgumentException`
- **Summary:** Creates a DistributedCache with custom circuit breaker configuration.
- **Contract:**
  - This method allows fine-tuning of the circuit breaker pattern for distributed cache read operations, which protects against cascading failures when the distributed cache becomes unavailable.
  - The circuit breaker pattern works as follows: When consecutive failures reach `maxFailuresBeforeCircuitOpen`, the circuit opens (`0` means the first recorded failure) and read operations return `null` immediately without attempting cache access After `retryDelay` milliseconds since the last failure, ALL subsequent reads attempt the cache again (there is no half-open single-probe gate; size the delay accordingly if the backend is sensitive to retry bursts) Successful reads reset the failure counter and close the circuit; writes do not alter it Usage Examples:
- **Parameters:**
  - `client` (`DistributedCacheClient<V>`) — the distributed cache client to wrap (must not be null)
  - `keyPrefix` (`String`) — the key prefix to prepend to all keys (can be empty string or null for no prefix)
  - `maxFailuresBeforeCircuitOpen` (`int`) — the maximum number of consecutive failures before the circuit breaker opens (must be non-negative); `0` opens it after the first recorded backend failure
  - `retryDelay` (`long`) — the delay in milliseconds before attempting a retry after the circuit breaker opens (must be non-negative)
- **Returns:** a new DistributedCache instance with custom circuit breaker configuration
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `client` is `null`, `keyPrefix` contains a non-printable-ASCII character, a space, or a control character, `maxFailuresBeforeCircuitOpen` is negative, or `retryDelay` is negative
- **Examples:**
  ```java
  // Create Redis client
  JRedis<User> redisClient = new JRedis<>("localhost:6379", 3000);

  // Create cache with custom circuit breaker configuration
  DistributedCache<String, User> cache = CacheFactory.createDistributedCache(
      redisClient,
      "app:",    // Key prefix for namespace isolation
      50,        // Open circuit after 50 consecutive failures
      2000       // Wait 2 seconds before attempting retry after circuit opens
  );             // returns a non-null DistributedCache

  // Circuit breaker protects against cascading failures on reads
  User user = new User();
  cache.put("user:123", user, 3600000, 0);         // returns true
  User cached = cache.getOrNull("user:123");       // returns the value, or null if absent/circuit-open

  // Edge cases (validated by the underlying constructor):
  CacheFactory.createDistributedCache(redisClient, "app:", -1, 2000);   // throws IllegalArgumentException (maxFailuresBeforeCircuitOpen must be non-negative)
  CacheFactory.createDistributedCache(redisClient, "app:", 50, -1);     // throws IllegalArgumentException (retryDelay must be non-negative)
  ```
- **See also:** `#createDistributedCache(DistributedCacheClient)`, `#createDistributedCache(DistributedCacheClient, String)`, `#createCache(String)`
##### createCache(...) -> Cache<K, V>
- **Signature:** `public static <K, V> Cache<K, V> createCache(final String provider) throws IllegalArgumentException, UncheckedIOException, RuntimeException`
- **Summary:** Creates a cache instance from a string specification.
- **Contract:**
  - An all-digit key prefix must use the explicit three-parameter layout, e.g.
- **Parameters:**
  - `provider` (`String`) — the cache provider specification string in format "ClassName(param1,param2,...)" (must not be null or empty)
- **Returns:** a new Cache instance configured according to the specification
- **Throws:**
  - `java.lang.IllegalArgumentException` — if the provider string is null or empty, cannot be parsed, or has an empty class name; for the built-in providers (Memcached/Redis/RedisCluster), also if it has no parameters, has an empty server URL, specifies an unsupported or (for RedisCluster) ambiguous parameter layout or an additional RedisCluster seed node that is not a `host:port` endpoint, specifies a timeout that is not an optional sign followed by decimal digits, does not fit in a `long`, or is not positive, if the client constructor rejects the server URL (no valid server address or, for Memcached, a hostname that cannot be resolved) or the timeout (a Redis/RedisCluster timeout above `Integer#MAX_VALUE`), or if the key prefix contains a non-printable-ASCII character, a space, or a control character (the already-constructed client is disconnected first); for custom classes, also if the class cannot be found under the given name (checked against this library's classloader, then the thread context classloader; this includes a `NoClassDefFoundError` raised while loading it, e.g. for a name differing only in letter case on a case-insensitive file system, which is kept as the cause or as a suppressed exception; any other `LinkageError` while loading, such as `UnsupportedClassVersionError` or `ClassFormatError`, means the class exists but cannot be used and propagates unchanged), does not implement `Cache`, is an interface or abstract class, or declares no constructor matching the specified parameters. A candidate class is loaded without running its static initializer until after this type check. A custom cache class with a no-arg constructor may be specified without parameters, e.g. `"com.example.MyCache()"`
  - `com.landawn.abacus.exception.UncheckedIOException` — if creating the Memcached client's selector or sockets fails
  - `java.lang.RuntimeException` — if Redis client-pool setup, RedisCluster seed resolution, or initial topology discovery fails, or if reflective construction cannot access the custom cache's matching constructor or that constructor throws. A `RuntimeException` thrown by that constructor propagates as is; a checked exception or an `Error` (including a `LinkageError` raised while the constructor body runs) arrives wrapped in a `RuntimeException` whose cause is the original
  - `java.lang.IllegalStateException` — for the Redis and RedisCluster providers, if the optional Kryo dependency (required by the Jedis cache clients) is not on the classpath or cannot be initialized
  - `java.lang.ExceptionInInitializerError` — if the custom cache class's static initializer, which first runs when the validated class is instantiated, throws (later attempts in the same JVM then fail with `NoClassDefFoundError`); unlike an Error thrown by the constructor body, it propagates unwrapped
- **Examples:**
  ```java
  // Memcached with single server and default timeout; the result is a DistributedCache
  Cache<String, User> cache1 = CacheFactory.createCache("Memcached(localhost:11211)");   // returns a non-null DistributedCache

  // Redis with key prefix and custom 5-second timeout
  Cache<String, Session> cache2 = CacheFactory.createCache(
      "Redis(localhost:6379,app:cache:,5000)"
  );                                            // returns a non-null DistributedCache

  // Multiple Memcached servers (space-separated in serverUrl)
  Cache<String, Object> cache3 = CacheFactory.createCache(
      "Memcached(host1:11211 host2:11211,myprefix:,3000)"
  );                                            // returns a non-null DistributedCache

  // Provider name matching is case-insensitive
  Cache<String, Object> cache4 = CacheFactory.createCache("memcached(localhost:11211)");   // returns a non-null DistributedCache

  // Custom cache implementation (fully qualified class name implementing Cache)
  Cache<String, Object> cache5 = CacheFactory.createCache(
      "com.mycompany.CustomCache(param1,param2)"
  );                                            // returns an instance of the named Cache class
  // Edge cases (all throw IllegalArgumentException):
  CacheFactory.createCache(null);                                 // "Provider specification cannot be null or empty"
  CacheFactory.createCache("");                                   // "Provider specification cannot be null or empty"
  CacheFactory.createCache("Memcached()");                        // "missing parameters" (the parser yields an empty parameter list)
  CacheFactory.createCache("Memcached(localhost,p:,0)");          // non-positive timeout rejected
  CacheFactory.createCache("Memcached(localhost,p:,abc)");        // "Invalid timeout parameter: abc"
  CacheFactory.createCache("Memcached(localhost,p:,0x1F4)");      // "Invalid timeout parameter" (decimal digits only)
  CacheFactory.createCache("RedisCluster(h1:7000,h2:7000,3000)"); // "Ambiguous RedisCluster parameters" (endpoint-shaped token before the timeout)
  CacheFactory.createCache("Memcached(a,b,1000,extra)");          // "Unsupported parameters" (more than 3)
  CacheFactory.createCache("Memcached(localhost,app:");           // unbalanced parenthesis -> "Malformed type attribute: missing closing ')'"
  CacheFactory.createCache("com.example.NoSuchCache(host)");      // "Cannot find class: com.example.NoSuchCache"
  CacheFactory.createCache("java.lang.String(host)");             // "Custom cache class must implement Cache"
  ```
- **See also:** `#createDistributedCache(DistributedCacheClient)`, `#createDistributedCache(DistributedCacheClient, String)`, `#createLocalCache(int, long)`

#### Public Instance Methods
- (none)

### Record CacheStats (com.landawn.abacus.cache.CacheStats)
An immutable snapshot of cache statistics at a specific point in time.

**Thread-safety:** thread-safe
**Nullability:** unspecified

#### Record Components
- `capacity` (`int`) - the capacity reported by the cache adapter; depending on the backing cache this may be an entry limit, another policy limit, or `0` when unbounded/unreported. Must be non-negative.
- `size` (`int`) - the current number of entries stored in the cache at the time of this snapshot. This count may transiently include expired entries that have not yet been evicted. Must be non-negative.
- `putCount` (`long`) - the cumulative put count reported by the adapter. Must be non-negative.
- `getCount` (`long`) - the cumulative get count reported by the adapter. Must be non-negative. Normally equals `hitCount + missCount`, but the relationship is not enforced because the counters are sampled non-atomically.
- `hitCount` (`long`) - the number of get operations that successfully found and returned a cached value. Must be non-negative. Under normal conditions this does not exceed `getCount`, but the relationship is not enforced because the counters are sampled non-atomically.
- `missCount` (`long`) - the number of get operations that did not find a cached value (cache miss). Must be non-negative. Under normal conditions this does not exceed `getCount`, but the relationship is not enforced because the counters are sampled non-atomically.
- `evictionCount` (`long`) - the total number of entries that have been evicted from the cache due to capacity constraints or expiration policies (TTL or idle timeout). Must be non-negative.
- `maxMemory` (`long`) - the maximum memory in bytes allocated for the cache. A value of `-1` indicates that no memory limit is configured or that the underlying cache does not track memory usage. Must be `-1` or non-negative.
- `dataSize` (`long`) - the total size in bytes of all data currently stored in the cache at the time of this snapshot. Should not exceed `maxMemory` if memory limits are enforced; a value of `-1` indicates that the underlying cache does not track memory usage. Must be `-1` or non-negative.

#### Public Constructors
- `public CacheStats(int capacity, int size, long putCount, long getCount, long hitCount, long missCount, long evictionCount, long maxMemory, long dataSize)` — Compact constructor that enforces the documented non-negative invariant on every component except `maxMemory` and `dataSize`, which additionally allow `-1` as a sentinel meaning "not tracked by the underlying cache implementation".
  - **Throws:**
    - `java.lang.IllegalArgumentException` — if any non-memory component is negative, or if `maxMemory` or `dataSize` is less than `-1`

#### Public Static Methods
- (none)

#### Public Instance Methods
##### hitRate(...) -> double
- **Signature:** `public double hitRate()`
- **Summary:** Returns the cache hit rate as a fraction in `[0.0, 1.0]`.
- **Parameters:**
  - (none)
- **Returns:** the ratio of hits to total get requests, or `0.0` when no get requests have been recorded (i.e. `hitCount + missCount == 0`)
- **See also:** `#missRate()`
##### missRate(...) -> double
- **Signature:** `public double missRate()`
- **Summary:** Returns the cache miss rate as a fraction in `[0.0, 1.0]`.
- **Parameters:**
  - (none)
- **Returns:** the ratio of misses to total get requests, or `0.0` when no get requests have been recorded (i.e. `hitCount + missCount == 0`)
- **See also:** `#hitRate()`
##### capacity(...) -> int
- **Signature:** `public int capacity()`
- **Summary:** the capacity reported by the cache adapter; depending on the backing cache this may be an entry limit, another policy limit, or `0` when unbounded/unreported. Must be non-negative.
- **Parameters:**
  - (none)
- **Returns:** the capacity reported by the cache adapter; depending on the backing cache this may be an entry limit, another policy limit, or `0` when unbounded/unreported. Must be non-negative.
##### size(...) -> int
- **Signature:** `public int size()`
- **Summary:** the current number of entries stored in the cache at the time of this snapshot. This count may transiently include expired entries that have not yet been evicted. Must be non-negative.
- **Parameters:**
  - (none)
- **Returns:** the current number of entries stored in the cache at the time of this snapshot. This count may transiently include expired entries that have not yet been evicted. Must be non-negative.
##### putCount(...) -> long
- **Signature:** `public long putCount()`
- **Summary:** the cumulative put count reported by the adapter. Must be non-negative.
- **Parameters:**
  - (none)
- **Returns:** the cumulative put count reported by the adapter. Must be non-negative.
##### getCount(...) -> long
- **Signature:** `public long getCount()`
- **Summary:** the cumulative get count reported by the adapter. Must be non-negative. Normally equals `hitCount + missCount`, but the relationship is not enforced because the counters are sampled non-atomically.
- **Parameters:**
  - (none)
- **Returns:** the cumulative get count reported by the adapter. Must be non-negative. Normally equals `hitCount + missCount`, but the relationship is not enforced because the counters are sampled non-atomically.
##### hitCount(...) -> long
- **Signature:** `public long hitCount()`
- **Summary:** the number of get operations that successfully found and returned a cached value. Must be non-negative. Under normal conditions this does not exceed `getCount`, but the relationship is not enforced because the counters are sampled non-atomically.
- **Parameters:**
  - (none)
- **Returns:** the number of get operations that successfully found and returned a cached value. Must be non-negative. Under normal conditions this does not exceed `getCount`, but the relationship is not enforced because the counters are sampled non-atomically.
##### missCount(...) -> long
- **Signature:** `public long missCount()`
- **Summary:** the number of get operations that did not find a cached value (cache miss). Must be non-negative. Under normal conditions this does not exceed `getCount`, but the relationship is not enforced because the counters are sampled non-atomically.
- **Parameters:**
  - (none)
- **Returns:** the number of get operations that did not find a cached value (cache miss). Must be non-negative. Under normal conditions this does not exceed `getCount`, but the relationship is not enforced because the counters are sampled non-atomically.
##### evictionCount(...) -> long
- **Signature:** `public long evictionCount()`
- **Summary:** the total number of entries that have been evicted from the cache due to capacity constraints or expiration policies (TTL or idle timeout). Must be non-negative.
- **Parameters:**
  - (none)
- **Returns:** the total number of entries that have been evicted from the cache due to capacity constraints or expiration policies (TTL or idle timeout). Must be non-negative.
##### maxMemory(...) -> long
- **Signature:** `public long maxMemory()`
- **Summary:** the maximum memory in bytes allocated for the cache. A value of `-1` indicates that no memory limit is configured or that the underlying cache does not track memory usage. Must be `-1` or non-negative.
- **Parameters:**
  - (none)
- **Returns:** the maximum memory in bytes allocated for the cache. A value of `-1` indicates that no memory limit is configured or that the underlying cache does not track memory usage. Must be `-1` or non-negative.
##### dataSize(...) -> long
- **Signature:** `public long dataSize()`
- **Summary:** the total size in bytes of all data currently stored in the cache at the time of this snapshot. Should not exceed `maxMemory` if memory limits are enforced; a value of `-1` indicates that the underlying cache does not track memory usage. Must be `-1` or non-negative.
- **Parameters:**
  - (none)
- **Returns:** the total size in bytes of all data currently stored in the cache at the time of this snapshot. Should not exceed `maxMemory` if memory limits are enforced; a value of `-1` indicates that the underlying cache does not track memory usage. Must be `-1` or non-negative.
##### equals(...) -> boolean
- **Signature:** `public final boolean equals(Object o)`
- **Parameters:**
  - `o` (`Object`)
- **Returns:** unspecified
##### hashCode(...) -> int
- **Signature:** `public final int hashCode()`
- **Parameters:**
  - (none)
- **Returns:** unspecified
##### toString(...) -> String
- **Signature:** `public final String toString()`
- **Parameters:**
  - (none)
- **Returns:** unspecified

### Class CaffeineCache (com.landawn.abacus.cache.CaffeineCache)
A wrapper implementation that adapts a Caffeine cache to the Abacus Cache interface.

**Thread-safety:** unspecified
**Nullability:** unspecified
**Extends:** `AbstractCache<K, V>`

#### Public Constructors
- `public CaffeineCache(final Cache<K, V> cache) throws IllegalArgumentException` — Creates a new CaffeineCache wrapper instance.
  - **Throws:**
    - `java.lang.IllegalArgumentException` — if `cache` is `null`
  ```java
  import com.github.benmanes.caffeine.cache.Cache;
  import com.github.benmanes.caffeine.cache.Caffeine;
  import java.util.concurrent.TimeUnit;

  Cache<String, User> caffeine = Caffeine.newBuilder()
      .maximumSize(1000)
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build();                                                        // build the pre-configured Caffeine instance
  CaffeineCache<String, User> cache = new CaffeineCache<>(caffeine);   // wrap it (cache.isClosed() == false)

  new CaffeineCache<String, User>((Cache<String, User>) null);   // throws IllegalArgumentException (null cache)
  ```

#### Public Static Methods
- (none)

#### Public Instance Methods
##### getOrNull(...) -> V
- **Signature:** `public V getOrNull(final K key) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Retrieves a value from the cache by its key.
- **Contract:**
  - The operation may update the entry's access time depending on the eviction policy configured when the Caffeine cache was built.
- **Parameters:**
  - `key` (`K`) — the cache key whose associated value is to be returned (must not be `null`)
- **Returns:** the value associated with the specified key, or `null` if not found, expired, or evicted
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if `key` is `null`
  - `java.lang.RuntimeException` — if the configured ticker or variable-expiration callback fails while reading the entry
- **Examples:**
  ```java
  CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
  cache.put("user:123", user, 0, 0);                 // seed one entry
  User retrieved = cache.getOrNull("user:123");      // returns the stored user (hit)
  if (retrieved != null) {
      System.out.println("Found user: " + retrieved.getName());   // prints "Found user: ..."
  } else {
      System.out.println("Cache miss - entry not found or evicted");   // not reached in this scenario (hit)
  }

  User absent = cache.getOrNull("user:999");         // returns null (miss; getIfPresent does not load)
  cache.getOrNull((String) null);                    // throws IllegalArgumentException (null key)
  ```
##### put(...) -> boolean
- **Signature:** `@SuppressWarnings("unused") public boolean put(final K key, final V value, final long liveTime, final long maxIdleTime) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Stores a key-value pair in the cache.
- **Contract:**
  - If the key already exists, its value will be replaced atomically.
  - All entries use the cache-wide expiration settings configured when building the Caffeine instance.
  - If this operation races with `#close()`, it removes its own late write when that mapping is still current and then throws `IllegalStateException`; a newer mapping with a different value instance written directly through the caller-owned Caffeine cache is left completely untouched - the closed-state cleanup probes it quietly, without recording statistics or refreshing its expiration metadata.
- **Parameters:**
  - `key` (`K`) — the cache key with which the specified value is to be associated (must not be `null`)
  - `value` (`V`) — the cache value to be associated with the specified key (must not be `null`; this wrapper rejects `null` values with `IllegalArgumentException`)
  - `liveTime` (`long`) — the time-to-live in milliseconds (ignored; configure expiration via the Caffeine builder)
  - `maxIdleTime` (`long`) — the maximum idle time in milliseconds (ignored; configure expiration via the Caffeine builder)
- **Returns:** `true` whenever the call returns normally. This reports that Caffeine accepted the write, not that the entry is retained: a size- or weight-bounded cache may evict it at once (for example, with `maximumSize(0)` or an entry heavier than `maximumWeight`)
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed, including when `#close()` runs concurrently with this put (a late write that is still current is removed before the exception is thrown)
  - `java.lang.IllegalArgumentException` — if `key` or `value` is `null`, or the configured weigher returns a negative weight
  - `java.lang.RuntimeException` — if the configured ticker, weigher, or variable-expiration callback fails while storing the entry
- **Examples:**
  ```java
  // Configure expiration at cache creation time
  Cache<String, User> caffeine = Caffeine.newBuilder()
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build();                                            // expiry lives on the builder, not per-entry
  CaffeineCache<String, User> cache = new CaffeineCache<>(caffeine);

  // liveTime and maxIdleTime parameters are ignored
  boolean stored = cache.put("user:123", user, 0, 0);     // returns true
  cache.put("user:456", anotherUser, 3600000, 1800000);   // returns true; time params still ignored
  cache.put("user:123", updatedUser, 0, 0);               // returns true; replaces existing value atomically

  // null key or null value are both rejected
  cache.put((String) null, user, 0, 0);                   // throws IllegalArgumentException (null key)
  cache.put("user:789", (User) null, 0, 0);               // throws IllegalArgumentException (null value)

  // All entries expire based on the cache-level configuration (10 minutes)
  ```
##### remove(...) -> void
- **Signature:** `public void remove(final K key) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Removes the mapping for a key from the cache if it is present.
- **Contract:**
  - If the key is not found, the operation completes silently without throwing an exception.
- **Parameters:**
  - `key` (`K`) — the cache key whose mapping is to be removed from the cache (must not be `null`)
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if `key` is `null`
  - `java.lang.RuntimeException` — if the configured ticker fails while invalidating the entry
- **Examples:**
  ```java
  CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
  cache.put("user:123", user, 0, 0);              // seed one entry
  cache.remove("user:123");                       // invalidates the mapping
  User retrieved = cache.getOrNull("user:123");   // returns null (entry gone)

  // Removing non-existent key is safe
  cache.remove("user:999");                       // no error, silent no-op
  cache.remove((String) null);                    // throws IllegalArgumentException (null key)
  ```
##### containsKey(...) -> boolean
- **Signature:** `public boolean containsKey(final K key) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Checks if the cache contains a mapping for the specified key.
- **Contract:**
  - When it finds a logically expired or reference-collected entry, Caffeine schedules its maintenance task on the cache's configured executor (which runs on the calling thread for a same-thread executor such as `Runnable::run`).
- **Parameters:**
  - `key` (`K`) — the cache key whose presence in the cache is to be tested (must not be `null`)
- **Returns:** `true` if the underlying `asMap()` view holds a live (non-expired) mapping for the key; `false` otherwise
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if `key` is `null`
  - `java.lang.RuntimeException` — if the configured ticker fails while checking the entry's expiration
- **Examples:**
  ```java
  CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
  cache.put("user:123", user, 0, 0);              // seed one entry
  if (cache.containsKey("user:123")) {            // returns true (mapping present)
      System.out.println("User exists in cache"); // this branch runs
  } else {
      System.out.println("User not in cache or has expired");   // not reached in this scenario (present)
  }

  boolean absent = cache.containsKey("user:999"); // returns false (no mapping)
  cache.containsKey((String) null);               // throws IllegalArgumentException (null key)
  ```
##### keySet(...) -> Set<K>
- **Signature:** `public Set<K> keySet() throws UnsupportedOperationException`
- **Summary:** Unsupported by this implementation; always throws `UnsupportedOperationException`.
- **Contract:**
  - If key iteration is required, use the underlying Caffeine instance directly or a different cache implementation (e.g., `LocalCache`).
- **Parameters:**
  - (none)
- **Returns:** this method never returns normally; it always throws
- **Throws:**
  - `java.lang.UnsupportedOperationException` — always, because this wrapper does not support key enumeration
- **Examples:**
  ```java
  CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
  try {
      Set<String> keys = cache.keySet();   // throws UnsupportedOperationException (always, even when open)
  } catch (UnsupportedOperationException e) {
      System.out.println("Key iteration is not supported in CaffeineCache");   // this branch runs
      // Use a different cache implementation if key iteration is required
  }
  ```
##### size(...) -> int
- **Signature:** `public int size() throws IllegalStateException`
- **Summary:** Returns the estimated number of entries in the cache.
- **Contract:**
  - If the underlying `long` estimate exceeds `Integer#MAX_VALUE`, it is clamped to `Integer.MAX_VALUE`.
- **Parameters:**
  - (none)
- **Returns:** the estimated number of cache entries
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
- **Examples:**
  ```java
  CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
  int empty = cache.size();                                                 // returns 0 (no entries yet)
  cache.put("user:123", user1, 0, 0);                                       // seed first entry
  cache.put("user:456", user2, 0, 0);                                       // seed second entry
  int count = cache.size();                                                 // returns approximately 2 (estimate, never negative)
  System.out.println("Cache contains approximately " + count + " entries"); // prints "...approximately 2..."
  ```
##### clear(...) -> void
- **Signature:** `public void clear() throws IllegalStateException, RuntimeException`
- **Summary:** Removes all entries from the cache.
- **Parameters:**
  - (none)
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.RuntimeException` — if the configured ticker fails while invalidating entries
- **Examples:**
  ```java
  CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
  cache.put("user:123", user1, 0, 0);                   // seed first entry
  cache.put("user:456", user2, 0, 0);                   // seed second entry
  System.out.println("Before clear: " + cache.size());  // prints approximately 2
  cache.clear();                                        // removes all cached entries
  System.out.println("After clear: " + cache.size());   // prints approximately 0
  User gone = cache.getOrNull("user:123");              // returns null (all entries invalidated)
  ```
##### close(...) -> void
- **Signature:** `public void close() throws RuntimeException`
- **Summary:** Closes this cache wrapper and invalidates entries currently present in the supplied Caffeine cache.
- **Contract:**
  - A repeated call returns immediately, even if that first invalidation is still running.
- **Parameters:**
  - (none)
- **Throws:**
  - `java.lang.RuntimeException` — if the configured ticker fails while invalidating entries; the wrapper remains closed
- **Examples:**
  ```java
  CaffeineCache<String, User> applicationCache = new CaffeineCache<>(caffeineInstance);
  applicationCache.put("user:123", user, 0, 0);             // seed one entry
  User retrieved = applicationCache.getOrNull("user:123");  // returns the stored user

  // Deliberately retire the application wrapper early, after all users have stopped.
  applicationCache.close();       // marks the wrapper closed and invalidates delegate entries
  applicationCache.close();       // idempotent no-op

  // After closing, operations throw IllegalStateException
  try {
      applicationCache.getOrNull("user:123");   // throws IllegalStateException
  } catch (IllegalStateException e) {
      System.out.println("Cache is closed");   // this branch runs
  }
  ```
##### isClosed(...) -> boolean
- **Signature:** `public boolean isClosed()`
- **Summary:** Returns whether this cache has been closed.
- **Contract:**
  - Returns `true` if `#close()` has been called on this cache.
- **Parameters:**
  - (none)
- **Returns:** `true` if `#close()` has been called; `false` if the cache is still operational
- **Examples:**
  ```java
  CaffeineCache<String, User> cache = new CaffeineCache<>(caffeineInstance);
  if (!cache.isClosed()) {                 // evaluates to true on a fresh cache, so this branch runs
      cache.put("user:123", user, 0, 0);   // seed one entry
  } else {
      System.out.println("Cache is closed, cannot perform operation");   // not reached on a fresh cache
  }

  cache.close();                       // deliberately retire the application wrapper early
  boolean closed = cache.isClosed();   // returns true
  ```
##### stats(...) -> CacheStats
- **Signature:** `public CacheStats stats()`
- **Summary:** Returns a snapshot of this cache's statistics in the framework's `CacheStats` form.
- **Contract:**
  - Hit/miss/eviction counts come from Caffeine and are only meaningful if the underlying cache was built with `recordStats()` (otherwise they are zero).
  - `capacity` is the Caffeine eviction maximum when an eviction bound is configured — the maximum entry count for a size-bounded cache, or the maximum total weight for a weight-bounded cache (a weight, not an entry count) — clamped to `Integer#MAX_VALUE` when the configured maximum exceeds it (possible for either bound, because Caffeine accepts `long` maximums).
  - It is `0` when the cache is unbounded, or when it is explicitly bounded at zero (`maximumSize(0)`).
- **Parameters:**
  - (none)
- **Returns:** a framework `CacheStats` snapshot at the time of invocation; never `null`
- **Examples:**
  ```java
  CaffeineCache<String, User> cache = new CaffeineCache<>(caffeine);   // caffeine built with recordStats()
  cache.put("user:123", user, 0, 0);                                   // seed one entry
  cache.getOrNull("user:123");                                         // hit
  cache.getOrNull("user:999");                                         // miss

  CacheStats stats = cache.stats();
  System.out.println("Hit rate: " + stats.hitRate());     // [0.0, 1.0]
  System.out.println("Hits: " + stats.hitCount());        // >= 1 here
  System.out.println("Puts: " + stats.putCount());        // >= 1 here
  ```
- **See also:** `#caffeineStats()`, `LocalCache#stats()`
##### caffeineStats(...) -> com.github.benmanes.caffeine.cache.stats.CacheStats
- **Signature:** `public com.github.benmanes.caffeine.cache.stats.CacheStats caffeineStats()`
- **Summary:** Returns the underlying Caffeine-native statistics object, exposing metrics that the framework `CacheStats` does not carry (load count, load success/failure, average load penalty, etc.).
- **Contract:**
  - Statistics are only meaningful if the underlying cache was built with `recordStats()`; otherwise all counters are zero.
- **Parameters:**
  - (none)
- **Returns:** a snapshot of the Caffeine-native statistics (all zeros if recordStats() was not enabled)
- **Examples:**
  ```java
  com.github.benmanes.caffeine.cache.stats.CacheStats nativeStats = cache.caffeineStats();
  System.out.println("Average load penalty: " + nativeStats.averageLoadPenalty() + " ns");
  System.out.println("Load success count: " + nativeStats.loadSuccessCount());
  ```
- **See also:** `#stats()`, `com.github.benmanes.caffeine.cache.Cache#stats()`

### Class ChronicleMap (com.landawn.abacus.cache.ChronicleMap)
A compatibility adapter that preserves the `ChronicleMap` class name.

**Deprecated:** yes (since 2.8.1); marked for removal — this is a compatibility wrapper, not a real Chronicle-Map adapter; its name collides with the unrelated OpenHFT `net.openhft.chronicle.map.ChronicleMap` and its behavior is plain `LocalCache`. Use `LocalCache` (or `CacheFactory#createLocalCache(int, long)`) directly instead. Scheduled for removal.
**Thread-safety:** unspecified
**Nullability:** unspecified
**Extends:** `LocalCache<K, V>`

#### Public Constructors
- `public ChronicleMap() throws RejectedExecutionException, IllegalStateException` — Creates a ChronicleMap-compatible cache with default settings: capacity of 1024 entries, an eviction scan delay of 60,000 ms (1 minute), and the default TTL/idle times defined by `Cache#DEFAULT_LIVE_TIME` (3 hours) and `Cache#DEFAULT_MAX_IDLE_TIME` (30 minutes).
  - **Throws:**
    - `java.util.concurrent.RejectedExecutionException` — if the pool's maintenance scheduler rejects the eviction task
    - `java.lang.IllegalStateException` — if the JVM is already shutting down when the underlying pool registers its shutdown hook
  ```java
  ChronicleMap<String, String> map = new ChronicleMap<>();   // capacity 1024, 60s eviction delay
  map.stats().capacity();                                    // returns 1024 (default capacity)
  map.isClosed();                                            // returns false (freshly created)
  map.put("k", "v");                                         // returns true (stored with default 3h TTL / 30min idle)
  String value = map.getOrNull("k");                         // returns "v"
  map.getOrNull("absent");                                   // returns null (key not present)
  map.getOrNull((String) null);                              // throws IllegalArgumentException (key must not be null)
  map.close();                                               // stops this cache's background eviction task
  ```
- `public ChronicleMap(final int capacity, final long evictDelay) throws IllegalArgumentException, RejectedExecutionException, IllegalStateException` — Creates a ChronicleMap-compatible cache with the specified capacity and eviction delay, using the default TTL/idle times defined by `Cache#DEFAULT_LIVE_TIME` (3 hours) and `Cache#DEFAULT_MAX_IDLE_TIME` (30 minutes).
  - **Throws:**
    - `java.lang.IllegalArgumentException` — if capacity is not positive or evictDelay is negative
    - `java.util.concurrent.RejectedExecutionException` — if `evictDelay` is positive and the pool's maintenance scheduler rejects the eviction task
    - `java.lang.IllegalStateException` — if the JVM is already shutting down when the underlying pool registers its shutdown hook
  ```java
  ChronicleMap<String, Integer> map = new ChronicleMap<>(1_024, 60_000L);   // capacity 1024, 60s eviction delay
  map.stats().capacity();                                                   // returns 1024 (capacity reflected in stats)
  map.put("count", 42);                                                     // returns true (uses default 3h TTL / 30min idle)
  map.getOrNull("count");                                                   // returns 42

  // An evictDelay of 0 is allowed and disables only the background expiry scan
  // (capacity eviction and on-access expiry still apply).
  Cache<String, Integer> noEvict = new ChronicleMap<>(64, 0L);   // valid
  noEvict.close();                                               // release cache resources
  map.close();                                                   // stop its background eviction task

  // Edge cases (validated by the constructor):
  new ChronicleMap<>(0, 60_000L);    // throws IllegalArgumentException (capacity must be positive)
  new ChronicleMap<>(1_024, -1L);    // throws IllegalArgumentException (evictDelay must be non-negative)
  ```
- `public ChronicleMap(final int capacity, final long evictDelay, final long defaultLiveTime, final long defaultMaxIdleTime) throws IllegalArgumentException, RejectedExecutionException, IllegalStateException` — Creates a ChronicleMap-compatible cache with custom capacity, eviction delay, and default timings.
  - **Throws:**
    - `java.lang.IllegalArgumentException` — if capacity is not positive or evictDelay is negative
    - `java.util.concurrent.RejectedExecutionException` — if `evictDelay` is positive and the pool's maintenance scheduler rejects the eviction task
    - `java.lang.IllegalStateException` — if the JVM is already shutting down when the underlying pool registers its shutdown hook
  ```java
  // capacity 2048, 30s eviction delay, 1h TTL, 5min idle (the per-cache defaults for put(key, value))
  ChronicleMap<String, String> map = new ChronicleMap<>(2_048, 30_000L, 3_600_000L, 300_000L);
  map.stats().capacity();                       // returns 2048 (capacity reflected in stats)

  // Per-entry overrides win over the constructor defaults.
  map.put("session", "abc", 10_000L, 5_000L);   // returns true (10s TTL, 5s idle for this entry)
  map.getOrNull("session");                     // returns "abc"

  // A null value is rejected (inherited LocalCache behavior).
  map.put("nullable", (String) null);           // throws IllegalArgumentException (value must not be null)

  // Non-positive default times mean "no expiration" for entries added via put(key, value).
  Cache<String, String> noExpire = new ChronicleMap<>(16, 0L, 0L, 0L);   // valid
  noExpire.close();                                                      // release cache resources
  map.close();                                                           // stop its background eviction task

  // Edge cases (validated by the constructor):
  new ChronicleMap<>(0, 30_000L, 3_600_000L, 300_000L);      // throws IllegalArgumentException (capacity must be positive)
  new ChronicleMap<>(2_048, -1L, 3_600_000L, 300_000L);      // throws IllegalArgumentException (evictDelay must be non-negative)
  ```

#### Public Static Methods
- (none)

#### Public Instance Methods
- (none)

### Class DistributedCache (com.landawn.abacus.cache.DistributedCache)
A wrapper cache implementation that provides a standardized Cache interface for distributed cache clients.

**Thread-safety:** thread-safe
**Nullability:** unspecified
**Extends:** `AbstractCache<K, V>`

#### Public Constructors
- (none)

#### Public Static Methods
- (none)

#### Public Instance Methods
##### getOrNull(...) -> V
- **Signature:** `public V getOrNull(final K key) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Retrieves a value from the distributed cache by its key.
- **Contract:**
  - This method implements circuit breaker pattern to protect against cascading failures when the distributed cache becomes unavailable.
  - Callers that require availability diagnostics must monitor the breaker warning and backend health separately.
  - Open (Failing Fast): When at least one failure has been recorded, the recorded failure count is at least `maxFailuresBeforeCircuitOpen`, AND the time since the last failure is less than `retryDelay` milliseconds (so a `retryDelay` of `0` never fails fast), this method returns `null` immediately without attempting cache access.
  - A failure counts as interrupt-caused only while the calling thread's interrupt status is still set and its causes or suppressed exceptions (searched transitively: Jedis reports a failed connect attempt, and a Redis Cluster's random-node fallback reports each node's failed pool wait, only as suppressed exceptions) include an `InterruptedException`, `java.nio.channels.ClosedByInterruptException`, an `java.io.InterruptedIOException` other than `java.net.SocketTimeoutException` (a socket timeout is a backend failure), or the `java.net.SocketException` with the message `"Closed by interrupt"` that `java.net.Socket` throws when an interrupt closes a virtual thread's blocking socket (as it does for a bundled Jedis client's read or connect on a virtual thread); any other failure on an interrupted thread is still counted.
  - The bundled Jedis clients re-assert the interrupt status that Jedis clears when an interrupt ends its connection-pool wait, so such a failure qualifies as well.
  - An `Error` thrown by the client is not caught: it propagates and leaves the breaker state unchanged A read that was already in flight when `#close()` began and then fails (typically because close disconnects the client) is reported as a miss without touching the breaker state The closed-to-open transition is logged once at WARN level Thread Safety: This method is thread-safe.
- **Parameters:**
  - `key` (`K`) — the cache key, must not be `null`
- **Returns:** the cached value, or `null` if not found, expired, evicted, circuit breaker is open, or on error
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if the key is null or its string representation is null or contains an unpaired UTF-16 surrogate (validated locally by the null check and `#generateKey(Object)` before the circuit breaker is consulted, so these are thrown even while the circuit is open), or if the underlying client rejects the generated cache key (e.g. it exceeds memcached's 250-character key limit after prefixing and Base64 expansion); such validation errors are rethrown and do not affect the circuit breaker state
  - `java.lang.RuntimeException` — if converting `key` to a string or an overridden `generateKey` fails before the backend read; these failures are not handled by the circuit breaker
- **Examples:**
  ```java
  DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
  DistributedCache<String, User> cache = CacheFactory.createDistributedCache(client, "myapp:", 100, 1000);

  // Basic retrieval with null check
  User user = cache.getOrNull("user:123");          // returns the cached User, or null on miss
  if (user != null) {
      System.out.println("Found: " + user.getName());   // prints when present
  } else {
      // Could be: not found, expired, evicted, circuit open, or network error
      System.out.println("Cache miss - loading from database");   // prints on null result
      user = loadFromDatabase("user:123");
      cache.put("user:123", user, 3600000, 0);      // returns true on successful store
  }

  // Cache-aside pattern with circuit breaker protection
  User getUser(String userId) {
      User user = cache.getOrNull("user:" + userId);   // returns cached value or null
      if (user == null) {
          // Fallback to database (could be cache miss or circuit breaker open)
          user = database.findUser(userId);
          if (user != null) {
              cache.put("user:" + userId, user, 3600000, 0);   // returns true; re-populates cache
          }
      }
      return user;
  }
  ```
- **See also:** `#generateKey(Object)`, `DistributedCacheClient#get(String)`
##### put(...) -> boolean
- **Signature:** `public boolean put(final K key, final V value, final long liveTime, final long maxIdleTime) throws IllegalStateException, IllegalArgumentException, RuntimeException, StackOverflowError`
- **Summary:** Stores a key-value pair in the distributed cache with custom expiration settings.
- **Contract:**
  - If the key already exists, its value and expiration time will be replaced.
  - If you need idle timeout functionality, consider using `LocalCache` instead.
- **Parameters:**
  - `key` (`K`) — the cache key, must not be `null`
  - `value` (`V`) — the value to cache (null handling depends on underlying cache client implementation)
  - `liveTime` (`long`) — the time-to-live in milliseconds (0 or negative for no expiration)
  - `maxIdleTime` (`long`) — the maximum idle time in milliseconds (IGNORED - not supported by distributed caches)
- **Returns:** `true` if the operation was successful, `false` otherwise (as reported by the underlying cache client)
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if the key is null, its string representation is null or contains an unpaired UTF-16 surrogate, if the underlying client rejects the generated cache key (e.g. it exceeds memcached's 250-character key limit after prefixing and Base64 expansion), if the underlying client rejects `value` (the bundled `SpyMemcached` client rejects a `null` value while spymemcached's stock `SerializingTranscoder` is active, and an encoded value larger than the transcoder's maximum item size), or if `liveTime` is too large for the underlying client's expiration encoding (the bundled `SpyMemcached` client rejects a `liveTime` whose absolute expiration would exceed epoch second `2^31-1` / January 2038, as well as any `liveTime` exceeding `Integer#MAX_VALUE` seconds / ~68 years)
  - `java.lang.RuntimeException` — if key conversion fails, if a network error or timeout occurs, if the underlying client cannot encode `value`, or if the server rejects `liveTime` (the bundled Redis clients pass the millisecond `liveTime` directly to Redis, which may reject an extreme value that cannot be represented as an absolute expiration); propagated from the underlying cache client
  - `java.lang.StackOverflowError` — if a client using the bundled Kryo transcoder attempts to serialize a cyclic collection, map, or object array in `value`
  - `java.lang.UnsupportedOperationException` — if the underlying client implements neither `DistributedCacheClient#put(String, Object, long)` nor its deprecated `set` alias
- **Examples:**
  ```java
  DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
  DistributedCache<String, User> cache = CacheFactory.createDistributedCache(client, "myapp:");

  // Cache with 1 hour TTL (maxIdleTime ignored)
  User user = new User("John");
  boolean success = cache.put("user:123", user, 3600000, 0);   // returns true on success
  if (success) {
      System.out.println("User cached successfully");   // prints when put returned true
  } else {
      System.out.println("Failed to cache user (underlying client reported failure, e.g. cache full / write rejected)");   // prints when put returned false
  }

  // 30 minute TTL; the 30 minute maxIdleTime argument is ignored
  User guest = new User("Guest");
  cache.put("user:guest", guest, 1800000, 1800000);   // returns true; idle time ignored

  // No expiration (permanent until manually deleted or evicted)
  User admin = loadAdminUser();
  cache.put("user:admin", admin, 0, 0);             // returns true; liveTime 0 = no expiration

  // Update existing cache entry with new TTL
  User updated = cache.getOrNull("user:123");       // returns the previously cached User
  if (updated != null) {
      updated.setLastLogin(System.currentTimeMillis());   // mutate the loaded value before re-caching
      cache.put("user:123", updated, 7200000, 0);   // returns true; overwrites with 2 hour TTL
  }
  ```
- **See also:** `#generateKey(Object)`, `DistributedCacheClient#put(String, Object, long)`
##### remove(...) -> void
- **Signature:** `public void remove(final K key) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Removes a key-value pair from the distributed cache.
- **Contract:**
  - Behavior: Removes the entry if the key exists Silently succeeds if the key does not exist (no error) Returns void (does not indicate success/failure or key existence) Safe to call multiple times with the same key (idempotent) Network errors or timeouts typically propagate as a `RuntimeException` from the underlying client Does not implement circuit breaker logic Thread Safety: This method is thread-safe and can be called concurrently from multiple threads.
- **Parameters:**
  - `key` (`K`) — the cache key, must not be `null`
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if the key is null, its string representation is null or contains an unpaired UTF-16 surrogate, or the underlying client rejects the generated key (for example, because it exceeds Memcached's key-length limit)
  - `java.lang.RuntimeException` — if key conversion fails, or if the underlying client's removal operation fails because of a network error or timeout
  - `java.lang.UnsupportedOperationException` — if the underlying client implements neither `DistributedCacheClient#remove(String)` nor its deprecated `delete` alias
- **Examples:**
  ```java
  DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
  DistributedCache<String, User> cache = CacheFactory.createDistributedCache(client, "myapp:");

  // Basic removal
  cache.remove("user:123");   // removes if present, no-op if absent; returns void

  // Safe to call multiple times (idempotent)
  cache.remove("user:123");   // removes the entry
  cache.remove("user:123");   // no-op; no exception thrown

  // Cache invalidation on update (write-through pattern)
  void updateUser(User user) {
      database.save(user);                    // persist the authoritative copy first
      cache.remove("user:" + user.getId());   // invalidates cache; returns void
  }

  // Batch removal
  List<String> userIds = Arrays.asList("123", "456", "789");
  userIds.forEach(id -> cache.remove("user:" + id));   // removes each key in turn

  // Remove on business logic event
  void logoutUser(String sessionId) {
      cache.remove("session:" + sessionId);   // evicts the session entry; returns void
      database.updateLastLogout(sessionId);   // record the logout in the backing store
  }
  ```
- **See also:** `#clear()`, `#generateKey(Object)`, `DistributedCacheClient#remove(String)`
##### containsKey(...) -> boolean
- **Signature:** `public boolean containsKey(final K key) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Checks if the cache contains a specific key with a non-null value.
- **Contract:**
  - Implementation Note: This method calls `getOrNull(key)!= null`, which means it: Performs a full GET operation (not a lightweight existence check) May return `false` if circuit breaker is open May return `false` on network errors (exceptions are caught) Returns `false` for expired entries Returns `false` if the cached value is null Subject to circuit breaker state (same as `#getOrNull(Object)`) Thread Safety: This method is thread-safe and inherits the thread-safety guarantees of `#getOrNull(Object)`.
  - Performance Consideration: Since this method performs a full GET operation, if you need the value afterward, it's more efficient to call `#getOrNull(Object)` directly and check for null rather than calling `containsKey()` followed by `getOrNull()`.
- **Parameters:**
  - `key` (`K`) — the cache key, must not be `null`
- **Returns:** `true` if the key exists and has a non-null value, `false` otherwise, on error, or when circuit breaker is open
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if the key or its string representation is invalid, as described by `#getOrNull(Object)`
  - `java.lang.RuntimeException` — if converting `key` to a string or an overridden `generateKey` fails before the backend read, as described by `#getOrNull(Object)`
- **Examples:**
  ```java
  DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
  DistributedCache<String, User> cache = CacheFactory.createDistributedCache(client, "myapp:");

  // Basic existence check
  if (cache.containsKey("user:123")) {              // returns true only if value present and non-null
      System.out.println("User exists in cache");   // prints when containsKey returned true
  } else {
      System.out.println("User not in cache (or circuit breaker open)");   // prints when containsKey returned false
  }

  // Conditional caching
  if (!cache.containsKey("user:admin")) {           // returns false when absent
      User admin = loadAdminUser();
      cache.put("user:admin", admin, 0, 0);         // returns true; populates the missing entry
  }

  // INEFFICIENT - performs GET twice:
  if (cache.containsKey("user:123")) {              // first GET (returns true)
      User user = cache.getOrNull("user:123");      // second GET operation!
      processUser(user);                            // consume the fetched value
  }

  // EFFICIENT - performs GET once:
  User user = cache.getOrNull("user:123");          // single GET; returns value or null
  if (user != null) {
      processUser(user);                            // consume the fetched value
  }
  ```
- **See also:** `#getOrNull(Object)`
##### keySet(...) -> Set<K>
- **Signature:** `public Set<K> keySet() throws UnsupportedOperationException`
- **Summary:** Returns the set of keys in the cache.
- **Parameters:**
  - (none)
- **Returns:** this method never returns normally; it always throws
- **Throws:**
  - `java.lang.UnsupportedOperationException` — always thrown for distributed caches
- **Examples:**
  ```java
  DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
  DistributedCache<String, User> cache = CacheFactory.createDistributedCache(client, "myapp:");

  // keySet() is never supported, even on an empty distributed cache.
  try {
      Set<String> keys = cache.keySet();           // throws UnsupportedOperationException
  } catch (UnsupportedOperationException e) {
      // Use containsKey(key) to test a specific key instead of enumerating keys.
      boolean present = cache.containsKey("user:123");   // returns false (nothing cached yet)
  }
  ```
- **See also:** `#size()`, `#containsKey(Object)`
##### size(...) -> int
- **Signature:** `public int size() throws UnsupportedOperationException`
- **Summary:** Returns the number of entries in the cache.
- **Parameters:**
  - (none)
- **Returns:** this method never returns normally; it always throws
- **Throws:**
  - `java.lang.UnsupportedOperationException` — always thrown for distributed caches
- **Examples:**
  ```java
  DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
  DistributedCache<String, User> cache = CacheFactory.createDistributedCache(client, "myapp:");

  // size() is never supported; it throws even after a successful put.
  cache.put("user:123", new User("John"), 3600000, 0);   // returns true
  try {
      int count = cache.size();                    // throws UnsupportedOperationException
  } catch (UnsupportedOperationException e) {
      // No entry count is available; probe individual keys with containsKey(key) instead.
      boolean present = cache.containsKey("user:123");   // returns true (just cached above)
  }
  ```
- **See also:** `#keySet()`, `#containsKey(Object)`
##### clear(...) -> void
- **Signature:** `public void clear() throws IllegalStateException, UnsupportedOperationException, RuntimeException`
- **Summary:** Requests a flush of the scope defined by the underlying `DistributedCacheClient`.
- **Contract:**
  - ⚠️ Potentially global destructive impact: With the bundled clients, flushes ALL data in their documented server/cluster scope (not just keys with this instance's prefix) Can affect all applications sharing that backend scope Cannot be undone - flushed cached data is permanently lost May cause sudden load spike on backend systems as all caches become cold simultaneously Should typically only be used in testing or with dedicated cache servers Does not reset the circuit breaker state Thread Safety: This method is thread-safe.
- **Parameters:**
  - (none)
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed, or if the underlying client reports the flush as unsuccessful (the bundled `SpyMemcached` client does this; note that with multiple servers its success flag is a last-writer-wins aggregate)
  - `java.lang.UnsupportedOperationException` — if the underlying client does not support `flushAll` (the `AbstractDistributedCacheClient` base class throws this by default)
  - `java.lang.RuntimeException` — if a network error or timeout occurs (propagated from the underlying cache client)
- **Examples:**
  ```java
  DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
  DistributedCache<String, User> cache = CacheFactory.createDistributedCache(client, "myapp:");

  // WARNING: This can flush ALL data from every configured cache server!
  // Not just "myapp:" prefixed keys, but EVERYTHING!
  cache.clear();   // requests the underlying client's full flush scope; returns void

  // Safe usage in testing (with dedicated test cache server)
  // @AfterEach
  public void cleanupCache() {
      if (isTestEnvironment()) {
          testCache.clear();   // OK - requests the dedicated servers' full flush scope
      }
  }

  // Production usage - AVOID or require explicit confirmation
  public void clearProductionCache(String confirmationToken) {
      if (!"CONFIRM_FLUSH_ALL_PRODUCTION".equals(confirmationToken)) {
          throw new IllegalArgumentException("Invalid confirmation");   // throws on bad token
      }
      logger.warn("DANGEROUS: Flushing ALL cache data from production servers");   // audit warning before flush
      cache.clear();   // affects all apps sharing these servers; returns void
      auditLog.record("CACHE_FLUSH_ALL", getCurrentUser());   // records the destructive action
  }
  ```
- **See also:** `DistributedCacheClient#flushAll()`, `#remove(Object)`
##### close(...) -> void
- **Signature:** `public synchronized void close()`
- **Summary:** Permanently shuts down this cache wrapper and disconnects its owned client from all distributed cache servers.
- **Contract:**
  - Cache wrappers and their pooled clients should normally be retained and shared; do not call this method after each request or cache operation.
  - Note that the bundled `SpyMemcached` client runs a non-daemon I/O thread, so an application that relies on the JVM exiting once its own threads finish (rather than on `System.exit` or a signal) must close the cache for the JVM to exit.
  - An operation already in flight when `close()` runs is not excluded: it may complete normally, or surface the underlying client's own exception (e.g., a "shutting down" error) instead of `IllegalStateException`.
  - ⚠️ Best-effort disconnect: if the underlying client's disconnect throws an exception, this method logs it and still leaves this wrapper permanently closed; client-side connections or threads may therefore require external diagnosis.
- **Parameters:**
  - (none)
- **Examples:**
  ```java
  // Optional lifecycle callback for an application-scoped cache
  @PreDestroy
  public void shutdownCache() {
      applicationCache.close(); // also disconnects the client owned by this wrapper
  }

  // Lifecycle test: close is idempotent and terminal
  cache.close();              // marks the cache closed and disconnects its owned client
  cache.close();              // no-op
  cache.getOrNull("key");     // throws IllegalStateException (cache is closed)
  ```
- **See also:** `DistributedCacheClient#disconnect()`, `#isClosed()`, `#assertNotClosed()`
##### isClosed(...) -> boolean
- **Signature:** `public boolean isClosed()`
- **Summary:** Checks if the cache has been closed.
- **Contract:**
  - Once a cache is closed via `#close()`, it cannot be reopened and should not be used.
  - Behavior: Returns `true` if `#close()` has been called, `false` otherwise Thread-safe and can be called from multiple threads concurrently Does not throw exceptions even if the cache is in an error state Once true, will always remain true (caches cannot be reopened) Reads the volatile `isClosed` field, ensuring visibility across threads Thread Safety: This method is thread-safe.
- **Parameters:**
  - (none)
- **Returns:** `true` if `#close()` has been called, `false` otherwise
- **Examples:**
  ```java
  DistributedCacheClient<User> client = new SpyMemcached<>("localhost:11211");
  DistributedCache<String, User> cache = CacheFactory.createDistributedCache(client, "myapp:");

  // Check before operations
  if (!cache.isClosed()) {                          // returns false while the cache is open
      cache.put("user:123", user, 3600000, 0);      // returns true
  } else {
      logger.warn("Cache is closed, skipping cache operation");   // logged when isClosed() is true
  }

  // Verify state at application shutdown
  cache.close();                                    // terminally disconnects the owned client
  System.out.println("Is closed: " + cache.isClosed());   // isClosed() returns true

  // Safe guard in long-running operations
  public void processUsers(List<User> users) {
      for (User user : users) {
          if (cache.isClosed()) {                   // returns true once close() has been called
              logger.warn("Cache closed during processing");   // logged and loop aborted
              break;
          }
          cache.put("user:" + user.getId(), user, 3600000, 0);   // returns true while open
      }
  }

  // Conditional cleanup in shutdown hooks
  public void shutdown() {
      if (!cache.isClosed()) {                      // returns false if not yet closed
          cache.close();                            // disconnects; isClosed() becomes true
      }
  }
  ```
- **See also:** `#close()`

### Interface DistributedCacheClient (com.landawn.abacus.cache.DistributedCacheClient)
Interface for distributed cache client implementations.

**Thread-safety:** unspecified
**Nullability:** unspecified

#### Public Constructors
- (none)

#### Public Fields
- `public static final long DEFAULT_TIMEOUT` — Default timeout for network operations in milliseconds (1000ms).
  - **Value:** `1000`
- `public static final String MEMCACHED` — Constant identifier for Memcached client type.
  - **Value:** `"Memcached"`
- `public static final String REDIS` — Constant identifier for Redis client type.
  - **Value:** `"Redis"`
- `public static final String REDIS_CLUSTER` — Constant identifier for Redis Cluster client type.
  - **Value:** `"RedisCluster"`

#### Public Static Methods
- (none)

#### Public Instance Methods
##### serverUrl(...) -> String
- **Signature:** `String serverUrl()`
- **Summary:** Returns the server URL(s) this client is configured to use.
- **Parameters:**
  - (none)
- **Returns:** the server URL(s), never `null` or blank
- **Examples:**
  ```java
  String url = client.serverUrl(); // client is the application's shared cache client
  System.out.println("Connected to: " + url);
  ```
##### get(...) -> T
- **Signature:** `T get(String key) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Retrieves a value from the cache by its key.
- **Parameters:**
  - `key` (`String`) — the cache key, must not be `null`
- **Returns:** the cached value, or `null` if not found, expired, evicted, or stored as `null` (where the implementation supports `null` values)
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, or violates an implementation-specific key restriction
  - `java.lang.RuntimeException` — if submitting or executing the backend operation fails, the response wait times out or is interrupted, or a returned value cannot be decoded (when applicable)
- **Examples:**
  ```java
  // Simple get operation
  User cachedUser = client.get("user:123");
  if (cachedUser != null) {
      System.out.println("Found user: " + cachedUser.getName());
  } else {
      System.out.println("User not found in cache");
  }

  // Get with fallback to database
  User user = client.get("user:123");
  if (user == null) {
      user = database.findUser(123);
      client.put("user:123", user, 3600000);
  }
  ```
##### getBulk(...) -> Map<String, T>
- **Signature:** `Map<String, T> getBulk(String... keys) throws UnsupportedOperationException, IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Retrieves multiple values from the cache.
- **Contract:**
  - Implementations may batch requests by server or issue individual gets when backend topology requires it.
- **Parameters:**
  - `keys` (`String...`) — the cache keys to retrieve; a supporting implementation requires a non-null array with no null elements
- **Returns:** a map of found key-value pairs, never `null` (may be empty if no keys are found)
- **Throws:**
  - `java.lang.UnsupportedOperationException` — if the implementation does not support bulk retrieval (the `AbstractDistributedCacheClient` base class throws this by default)
  - `java.lang.IllegalStateException` — if bulk retrieval is supported and this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if bulk retrieval is supported and `keys` is `null`, contains a null element, or contains a key rejected by the implementation
  - `java.lang.RuntimeException` — if submitting or executing the backend operation fails, the response wait times out or is interrupted, or a returned value cannot be decoded (when applicable)
- **Examples:**
  ```java
  // Basic bulk get
  Map<String, User> users = client.getBulk("user:123", "user:456", "user:789");
  users.forEach((key, user) -> System.out.println(key + ": " + user.getName()));

  // Bulk get with missing key handling
  Map<String, User> admins = client.getBulk("admin:1", "admin:2", "admin:3");
  System.out.println("Found " + admins.size() + " out of 3 admins");

  // Identify missing keys
  String[] requestedKeys = {"user:1", "user:2", "user:3"};
  Map<String, User> found = client.getBulk(requestedKeys);
  Arrays.stream(requestedKeys)
        .filter(key -> !found.containsKey(key))
        .forEach(key -> System.out.println("Missing: " + key));
  ```
- **Signature:** `Map<String, T> getBulk(Collection<String> keys) throws UnsupportedOperationException, IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Retrieves multiple values from the cache.
- **Contract:**
  - Implementations may batch requests by server or issue individual gets when backend topology requires it.
- **Parameters:**
  - `keys` (`Collection<String>`) — the cache keys to retrieve; a supporting implementation requires a non-null collection with no null elements
- **Returns:** a map of found key-value pairs, never `null` (may be empty if no keys are found)
- **Throws:**
  - `java.lang.UnsupportedOperationException` — if the implementation does not support bulk retrieval (the `AbstractDistributedCacheClient` base class throws this by default)
  - `java.lang.IllegalStateException` — if bulk retrieval is supported and this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if bulk retrieval is supported and `keys` is `null`, contains a null element, or contains a key rejected by the implementation
  - `java.lang.RuntimeException` — if submitting or executing the backend operation fails, the response wait times out or is interrupted, or a returned value cannot be decoded (when applicable)
- **Examples:**
  ```java
  // Using a List
  List<String> userKeys = Arrays.asList("user:123", "user:456", "user:789");
  Map<String, User> users = client.getBulk(userKeys);

  // Using a Set (useful when keys come from various sources)
  Set<String> keySet = new HashSet<>(Arrays.asList("admin:1", "admin:2"));
  Map<String, User> admins = client.getBulk(keySet);

  // Dynamically built key collection
  List<Integer> userIds = List.of(101, 102, 103);
  List<String> keys = userIds.stream()
                             .map(id -> "user:" + id)
                             .collect(Collectors.toList());
  Map<String, User> dynamicUsers = client.getBulk(keys);
  ```
##### put(...) -> boolean
- **Signature:** `@SuppressWarnings("deprecation") default boolean put(final String key, final T value, final long liveTime) throws UnsupportedOperationException, IllegalStateException, IllegalArgumentException, RuntimeException, StackOverflowError`
- **Summary:** Stores a key-value pair in the cache with a specified time-to-live.
- **Contract:**
  - If the key already exists, its value will be replaced.
  - When multiple clients set the same key concurrently, the last write wins.
  - Implementation compatibility: the default implementation delegates to the legacy `#set(String, Object, long)` name when the concrete client overrides it.
  - A concrete client must provide a non-recursive implementation of either this method (recommended) or `set`.
  - The cycle guard is per-thread and per-client, not per-key: while this default is delegating to `set`, any further call on the same thread that reaches a compatibility default of the pair for this client — even for a different key — is treated as a delegation cycle and also fails fast, so an overriding `set` must not call back into `this.put(...)`.
- **Parameters:**
  - `key` (`String`) — the cache key, must not be `null`
  - `value` (`T`) — the value to cache; may be `null` if supported by the implementation. Support for `null` values is implementation-defined: the bundled Redis clients store it as an empty payload, whereas a Memcached client using spymemcached's stock `SerializingTranscoder` rejects it with `IllegalArgumentException`
  - `liveTime` (`long`) — the time-to-live in milliseconds (`0` or negative for no expiration)
- **Returns:** `true` if the operation was successful, `false` otherwise
- **Throws:**
  - `java.lang.UnsupportedOperationException` — if the concrete client supplies neither a non-recursive `put` nor a non-recursive `set` implementation
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, or violates an implementation-specific key restriction; if `value` is `null` and the implementation rejects null values; or if `liveTime` is too large for the implementation's expiration representation (see above)
  - `java.lang.RuntimeException` — if `value` cannot be encoded by the implementation (including a `null` value it does not support), a backend request fails or is cancelled, or the response wait times out or is interrupted
  - `java.lang.StackOverflowError` — if a Kryo-backed implementation encounters a cycle made only of collections, maps, or arrays in `value`
- **Examples:**
  ```java
  // Cache with 1 hour TTL
  User user = new User("John", "john@example.com");
  boolean success = client.put("user:123", user, 3600000);
  if (success) {
      System.out.println("User cached successfully");
  }

  // Cache with a 30 minute TTL
  User guest = new User("Guest", "guest@example.com");
  client.put("user:guest", guest, 1800000);

  // Cache with no expiration
  User admin = loadAdminUser();
  client.put("user:admin", admin, 0);   // No expiration

  // Updating existing value
  User existing = client.get("user:456");
  if (existing != null) {
      existing.setEmail("new@example.com");
      client.put("user:456", existing, 7200000);   // 2 hour TTL
  }
  ```
##### set(...) -> boolean
- **Signature:** `@Deprecated(since = "2.8.5", forRemoval = false) default boolean set(final String key, final T value, final long liveTime) throws UnsupportedOperationException, IllegalStateException, IllegalArgumentException, RuntimeException, StackOverflowError`
- **Summary:** Legacy name for `#put(String, Object, long)`.
- **Deprecated:** yes (since 2.8.5) — Use `#put(String, Object, long)`. Retained for source and binary compatibility with clients compiled against version 2.8.4 and earlier.
- **Contract:**
  - Every concrete client must provide a non-recursive implementation of at least one member of the pair; otherwise this method fails fast with `UnsupportedOperationException`.
  - The cycle guard is per-thread and per-client, not per-key: while this default is delegating to `put`, any further call on the same thread that reaches a compatibility default of the pair for this client — even for a different key — is treated as a delegation cycle and also fails fast, so an overriding `put` must not call back into `this.set(...)`.
- **Parameters:**
  - `key` (`String`) — the cache key, must not be `null`
  - `value` (`T`) — the value to cache; may be `null` if supported by the implementation (see `#put(String, Object, long)`)
  - `liveTime` (`long`) — the time-to-live in milliseconds (`0` or negative for no expiration)
- **Returns:** `true` if the operation was successful, `false` otherwise
- **Throws:**
  - `java.lang.UnsupportedOperationException` — if the concrete client supplies neither a non-recursive `put` nor a non-recursive `set` implementation
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, or violates an implementation-specific key restriction; if `value` is `null` and the implementation rejects null values; or if `liveTime` is too large for the implementation's expiration representation (see `#put(String, Object, long)`)
  - `java.lang.RuntimeException` — if `value` cannot be encoded by the implementation (including a `null` value it does not support), a backend request fails or is cancelled, or the response wait times out or is interrupted
  - `java.lang.StackOverflowError` — if a Kryo-backed implementation encounters a cycle made only of collections, maps, or arrays in `value`
##### remove(...) -> boolean
- **Signature:** `@SuppressWarnings("deprecation") default boolean remove(final String key) throws UnsupportedOperationException, IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Removes a key-value pair from the cache.
- **Contract:**
  - Returns `false` when the key did not exist.
  - Redis (`JRedis`/`JRedisCluster`): Returns `true` when the key existed and was removed, `false` when the key did not exist (based on the count returned by the DEL command).
  - Implementation compatibility: the default implementation delegates to the legacy `#delete(String)` name when the concrete client overrides it.
  - A concrete client must provide a non-recursive implementation of either this method (recommended) or `delete`.
  - The cycle guard is per-thread and per-client, not per-key: while this default is delegating to `delete`, any further call on the same thread that reaches a compatibility default of the pair for this client — even for a different key — is treated as a delegation cycle and also fails fast, so an overriding `delete` must not call back into `this.remove(...)`.
- **Parameters:**
  - `key` (`String`) — the cache key, must not be `null`
- **Returns:** `true` if the key existed and was removed; `false` if the key did not exist when the command was issued (see the per-implementation notes above for exact semantics). A network error or timeout is thrown rather than reported as a `false` return.
- **Throws:**
  - `java.lang.UnsupportedOperationException` — if the concrete client supplies neither a non-recursive `remove` nor a non-recursive `delete` implementation
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, or violates an implementation-specific key restriction
  - `java.lang.RuntimeException` — if submitting or executing the backend operation fails, the response wait times out or is interrupted
- **Examples:**
  ```java
  // Simple delete
  boolean success = client.remove("user:123");
  System.out.println("Key existed and was removed: " + success);

  // Delete after update
  User user = client.get("user:456");
  if (user != null && user.isInactive()) {
      client.remove("user:456");
  }

  // Delete multiple keys
  String[] keysToDelete = {"session:1", "session:2", "session:3"};
  Arrays.stream(keysToDelete).forEach(client::remove);

  // Invalidate cache on entity update
  void updateUser(User user) {
      database.save(user);
      client.remove("user:" + user.getId());   // Invalidate cache
  }
  ```
##### delete(...) -> boolean
- **Signature:** `@Deprecated(since = "2.8.5", forRemoval = false) default boolean delete(final String key) throws UnsupportedOperationException, IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Legacy name for `#remove(String)`.
- **Deprecated:** yes (since 2.8.5) — Use `#remove(String)`. Retained for source and binary compatibility with clients compiled against version 2.8.4 and earlier.
- **Contract:**
  - Every concrete client must provide a non-recursive implementation of at least one member of the pair; otherwise this method fails fast with `UnsupportedOperationException`.
  - The cycle guard is per-thread and per-client, not per-key: while this default is delegating to `remove`, any further call on the same thread that reaches a compatibility default of the pair for this client — even for a different key — is treated as a delegation cycle and also fails fast, so an overriding `remove` must not call back into `this.delete(...)`.
- **Parameters:**
  - `key` (`String`) — the cache key, must not be `null`
- **Returns:** `true` if the key existed and was removed; `false` otherwise
- **Throws:**
  - `java.lang.UnsupportedOperationException` — if the concrete client supplies neither a non-recursive `remove` nor a non-recursive `delete` implementation
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, or violates an implementation-specific key restriction
  - `java.lang.RuntimeException` — if submitting or executing the backend operation fails, the response wait times out or is interrupted
##### incr(...) -> long
- **Signature:** `long incr(String key) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Atomically increments a numeric value by 1.
- **Contract:**
  - Implementation-specific behavior when key doesn't exist: Memcached (SpyMemcached): Returns -1 if key doesn't exist Redis (`JRedis`/`JRedisCluster`): Creates key with value 1 (initializes to 0, then increments) This operation is atomic and thread-safe across all distributed cache clients.
- **Parameters:**
  - `key` (`String`) — the cache key, must not be `null`
- **Returns:** the value after increment. For non-existent keys: Memcached returns -1 (no auto-initialization); Redis creates the key (effective value after increment is 1).
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, or violates an implementation-specific key restriction
  - `java.lang.RuntimeException` — if a backend request fails or is cancelled, the response wait times out or is interrupted, the key holds a value that is not a valid integer counter, or the result exceeds the backend's supported counter range
- **Examples:**
  ```java
  // Simple counter
  long pageViews = client.incr("page:views");
  System.out.println("Page views: " + pageViews);

  // Request counter with Redis (auto-initializes)
  long requestCount = client.incr("api:requests");

  // Memcached returns -1 for a counter that was never seeded. Do NOT seed it with put(...):
  // the bundled SpyMemcached transcoder serializes the value, and native incr/decr cannot
  // mutate those bytes. Seed Memcached counters through a path that writes a plain
  // ASCII-decimal value (see the SpyMemcached implementation's counter handling). Redis, by
  // contrast, auto-initializes the counter on the first incr.
  long count = client.incr("counter:visits");   // -1 on Memcached if the counter was never seeded

  // Rate limiting. Redis creates the counter on the first incr WITHOUT an expiration, so expire
  // or reset it per time window out of band, or a user who once exceeds the limit stays limited.
  // On Memcached an unseeded counter returns -1 and never trips this check.
  String key = "rate:limit:" + userId;
  long attempts = client.incr(key);
  if (attempts > MAX_ATTEMPTS) {
      throw new RateLimitException("Too many requests");
  }
  ```
- **Signature:** `long incr(String key, long delta) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Atomically increments a numeric value by a specified amount.
- **Contract:**
  - Implementation-specific behavior when key doesn't exist: Memcached (SpyMemcached): Returns -1 if key doesn't exist Redis (`JRedis`/`JRedisCluster`): Creates key with delta value (initializes to 0, then increments by delta) This operation is atomic and thread-safe across all distributed cache clients.
- **Parameters:**
  - `key` (`String`) — the cache key, must not be `null`
  - `delta` (`long`) — the increment amount, must be non-negative
- **Returns:** the value after increment. For non-existent keys: Memcached returns -1 (no auto-initialization); Redis creates the key (effective value after increment is `delta`).
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, or violates an implementation-specific key restriction, or if `delta` is negative
  - `java.lang.RuntimeException` — if a backend request fails or is cancelled, the response wait times out or is interrupted, the key holds a value that is not a valid integer counter, or the result exceeds the backend's supported counter range
- **Examples:**
  ```java
  // On Memcached each counter below must already be seeded: an absent key returns -1 and is
  // not created, so, for example, the quota check at the end would never fire.

  // Game score increment
  long score = client.incr("player:score", 10);
  System.out.println("New score: " + score);

  // Batch processing counter
  long processed = client.incr("batch:processed", 100);

  // Points system
  int points = calculatePoints(action);
  long totalPoints = client.incr("user:points:" + userId, points);

  // Bandwidth tracking
  long bytesTransferred = client.incr("bandwidth:today", fileSize);
  if (bytesTransferred > QUOTA) {
      logger.warn("Bandwidth quota exceeded");
  }
  ```
##### decr(...) -> long
- **Signature:** `long decr(String key) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Atomically decrements a numeric value by 1.
- **Contract:**
  - Implementation-specific behavior when key doesn't exist: Memcached (SpyMemcached): Returns -1 if key doesn't exist.
- **Parameters:**
  - `key` (`String`) — the cache key, must not be `null`
- **Returns:** the value after decrement. Memcached clamps at 0 (values cannot go negative) and returns -1 if the key doesn't exist; Redis allows negative values and creates non-existent keys (effective value after decrement is -1).
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, or violates an implementation-specific key restriction
  - `java.lang.RuntimeException` — if a backend request fails or is cancelled, the response wait times out or is interrupted, the key holds a value that is not a valid integer counter, or the result exceeds the backend's supported counter range
- **Examples:**
  ```java
  // decr returns the value AFTER the decrement, so with a counter seeded at exactly the number
  // of available units, the caller that takes the last unit also gets 0 - the same result as an
  // exhausted counter clamped at 0 on Memcached. The examples below therefore ASSUME the caller
  // seeded each counter at (available units + 1) beforehand, so that a result > 0 means a unit
  // was taken. This interface has no seeding method; for example:
  //   memcached.incr("api:tokens:" + userId, 0, units + 1);   // SpyMemcached: seeds an absent key
  //   redis.incr("api:tokens:" + userId, units + 1);          // Redis: creates the counter at units + 1
  // (The Redis form is not idempotent: run it once, when the counter is created.)

  // Token bucket rate limiting
  long remainingTokens = client.decr("api:tokens:" + userId);
  if (remainingTokens <= 0) {
      throw new RateLimitException("Rate limit exceeded");
  }

  // Inventory management: a result <= 0 means no unit was taken, on both backends. Do not
  // read -1 as "never seeded": that holds only on Memcached, which clamps an existing counter
  // at 0 and returns -1 solely for an absent key. Redis keeps decrementing a depleted
  // counter (0, then -1, -2, ...) and creates an absent one at -1.
  long stock = client.decr("product:stock:123");
  if (stock <= 0) {
      throw new OutOfStockException();   // depleted (or, on Memcached with -1, never seeded)
  }

  // Download counter: seed the counter at (limit + 1) so that "> 0" cleanly means "allowed".
  // Decrementing an exhausted (0) counter stays 0 on Memcached (clamped), so it remains denied.
  long remaining = client.decr("downloads:remaining:" + userId);
  if (remaining > 0) {
      processDownload();
  }
  ```
- **Signature:** `long decr(String key, long delta) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Atomically decrements a numeric value by a specified amount.
- **Contract:**
  - Implementation-specific behavior when key doesn't exist: Memcached (SpyMemcached): Returns -1 if key doesn't exist.
- **Parameters:**
  - `key` (`String`) — the cache key, must not be `null`
  - `delta` (`long`) — the decrement amount, must be non-negative
- **Returns:** the value after decrement. Memcached clamps at 0 (values cannot go negative) and returns -1 if the key doesn't exist; Redis allows negative values and creates non-existent keys (effective value after decrement is `-delta`).
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, or violates an implementation-specific key restriction, or if `delta` is negative
  - `java.lang.RuntimeException` — if a backend request fails or is cancelled, the response wait times out or is interrupted, the key holds a value that is not a valid integer counter, or the result exceeds the backend's supported counter range
- **Examples:**
  ```java
  // Bulk inventory decrement
  long inventory = client.decr("product:stock:456", 5);
  System.out.println("Remaining inventory: " + inventory);

  // API quota management. Note the guards below are written for Redis semantics (negative
  // results allowed). On Memcached an existing counter is clamped at 0, so a multi-unit
  // decrement CANNOT detect "not enough quota left" this way - and the "< 0" guard would
  // fire for a never-seeded key (-1 sentinel), misreporting "counter absent" as "quota
  // exceeded". Seed the counter and treat 0 as exhausted instead (see the single-decrement
  // example on decr(String)).
  int requestCost = calculateCost(request);
  long quotaRemaining = client.decr("quota:" + apiKey, requestCost);   // Redis-style guard below
  if (quotaRemaining < 0) {
      throw new QuotaExceededException();
  }

  // Reservation system (Redis semantics: a negative result reveals over-reservation)
  long availableSeats = client.decr("event:seats:789", numberOfTickets);
  if (availableSeats < 0) {
      // Revert the decrement
      client.incr("event:seats:789", numberOfTickets);
      throw new NotEnoughSeatsException();
  }

  // Resource pool management: seed the counter at (poolSize + 1) and use a strict "> 0"
  // guard - it is portable across both backends. On Memcached, decrementing an exhausted (0)
  // counter stays 0 (clamped), so a ">= 0" guard would hand out connections forever once
  // the pool hit zero.
  long availableConnections = client.decr("pool:connections", 1);
  if (availableConnections > 0) {
      return acquireConnection();
  }
  ```
##### flushAll(...) -> void
- **Signature:** `void flushAll() throws UnsupportedOperationException, IllegalStateException, RuntimeException`
- **Summary:** Requests removal of cached data in the implementation-defined backend scope.
- **Parameters:**
  - (none)
- **Throws:**
  - `java.lang.UnsupportedOperationException` — if the implementation does not support flushing all entries (the `AbstractDistributedCacheClient` base class throws this by default)
  - `java.lang.IllegalStateException` — if flushing is supported and this client has been disconnected or is being disconnected
  - `java.lang.RuntimeException` — if submitting or executing the backend operation fails, the response wait times out or is interrupted
- **Examples:**
  ```java
  // WARNING: This may remove every entry in the implementation-defined backend scope!
  client.flushAll();
  System.out.println("Configured cache scope cleared");

  // Safe usage in testing
  @AfterEach
  public void cleanupCache() {
      if (isTestEnvironment()) {
          cacheClient.flushAll();
      }
  }

  // Production usage with confirmation
  public void clearCache(String confirmationToken) {
      if (!"CONFIRM_FLUSH_ALL".equals(confirmationToken)) {
          throw new IllegalArgumentException("Invalid confirmation");
      }
      logger.warn("Flushing all cache data");
      client.flushAll();
      auditLog.record("CACHE_FLUSH_ALL", user);
  }

  // Application reset
  public void resetApplication() {
      client.flushAll();
      database.resetToDefaults();
      logger.info("Application reset complete");
  }
  ```
##### disconnect(...) -> void
- **Signature:** `void disconnect()`
- **Summary:** Disconnects from all cache servers and releases resources.
- **Contract:**
  - Implementations commonly pool connections, so callers should normally retain and share a client and invoke this method only when that owner is permanently shutting down, not after an individual request or cache operation.
  - Implementations should make repeated calls harmless; consult the concrete implementation for exact idempotence and failure behavior.
  - Some implementations keep the JVM alive until disconnected (the bundled `SpyMemcached` runs a non-daemon I/O thread), so an application that relies on the JVM exiting once its own threads finish must call it.
  - This method is thread-safe, but once called, no other operations should be attempted on this client instance.
- **Parameters:**
  - (none)
- **Examples:**
  ```java
  // Optional application shutdown hook for a shared client
  Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Shutting down cache client");
      cacheClient.disconnect();
  }));

  // Spring Bean destruction
  @PreDestroy
  public void cleanup() {
      if (cacheClient != null) {
          cacheClient.disconnect();
      }
  }

  // Explicit component lifecycle callback
  public void shutdownCacheClient() {
      logger.info("Disconnecting from cache servers");
      client.disconnect();
      logger.info("Disconnection complete");
  }
  ```

### Class Ehcache (com.landawn.abacus.cache.Ehcache)
A wrapper implementation that adapts Ehcache 3.x to the Abacus Cache interface.

**Thread-safety:** unspecified
**Nullability:** unspecified
**Extends:** `AbstractCache<K, V>`

#### Public Constructors
- `public Ehcache(final Cache<K, V> cache) throws IllegalArgumentException` — Creates a new Ehcache wrapper instance.
  - **Throws:**
    - `java.lang.IllegalArgumentException` — if `cache` is `null`
  ```java
  CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .withCache("userCache",
          CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, User.class,
              ResourcePoolsBuilder.heap(1000)))
      .build(true);   // build the cache manager
  Cache<String, User> ehcache = cacheManager.getCache("userCache", String.class, User.class);

  Ehcache<String, User> cache = new Ehcache<>(ehcache);   // wraps the Ehcache instance
  boolean closed = cache.isClosed();                      // returns false (freshly created)

  new Ehcache<>((Cache<String, User>) null);              // throws IllegalArgumentException (null cache)
  ```

#### Public Static Methods
- (none)

#### Public Instance Methods
##### getOrNull(...) -> V
- **Signature:** `public V getOrNull(final K key) throws IllegalStateException, IllegalArgumentException, ClassCastException, CacheLoadingException, RuntimeException`
- **Summary:** Retrieves a value from the cache by its key.
- **Contract:**
  - This method may trigger a cache loader if configured in the underlying Ehcache.
- **Parameters:**
  - `key` (`K`) — the cache key whose associated value is to be returned (must not be `null`)
- **Returns:** the value associated with the specified key, or `null` if not found, expired, or evicted
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed, or if the underlying Ehcache instance is not available (for example, it was removed from or closed with its `CacheManager`)
  - `java.lang.IllegalArgumentException` — if `key` is `null`
  - `java.lang.ClassCastException` — if `key` or a loaded value does not match the underlying cache's configured types
  - `org.ehcache.spi.loaderwriter.CacheLoadingException` — if the cache loader fails
  - `java.lang.RuntimeException` — if a configured copier or resilience strategy fails while retrieving the entry
- **Examples:**
  ```java
  Ehcache<String, User> cache = new Ehcache<>(ehcache);
  cache.put("userId123", user, 0, 0);          // returns true; seeds an entry
  User found = cache.getOrNull("userId123");   // returns the stored user
  User missing = cache.getOrNull("absent");    // returns null if no loader supplies a value

  cache.getOrNull(null);                        // throws IllegalArgumentException (null key)
  ```
##### put(...) -> boolean
- **Signature:** `public boolean put(final K key, final V value, final long liveTime, final long maxIdleTime) throws IllegalStateException, IllegalArgumentException, ClassCastException, CacheWritingException, RuntimeException`
- **Summary:** Stores a key-value pair in the cache.
- **Contract:**
  - If the key already exists, its value will be replaced.
- **Parameters:**
  - `key` (`K`) — the cache key with which the specified value is to be associated (must not be `null`)
  - `value` (`V`) — the cache value to be associated with the specified key (must not be `null`; this wrapper rejects `null` values with `IllegalArgumentException`)
  - `liveTime` (`long`) — the time-to-live in milliseconds (ignored; configure expiration via the Ehcache builder)
  - `maxIdleTime` (`long`) — the maximum idle time in milliseconds (ignored; configure expiration via the Ehcache builder)
- **Returns:** `true` whenever the call returns normally. This reports that Ehcache accepted the operation, not that the value is durably stored: under Ehcache's default (robust) resilience strategy, an internal store failure (`StoreAccessException`) is routed to the resilience strategy and the call still returns normally, so `true` can be reported while the mapping was not actually retained. `true` is likewise reported when the configured `ExpiryPolicy` returns a zero creation/update duration, or when a byte-sized heap tier rejects the value as too large; in both cases Ehcache discards the new mapping and any previous one. This wrapper cannot detect these cases; if a store failure matters, configure a custom `ResilienceStrategy` on the Ehcache instance
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed, or if the underlying Ehcache instance is not available (for example, it was removed from or closed with its `CacheManager`)
  - `java.lang.IllegalArgumentException` — if `key` or `value` is `null`
  - `java.lang.ClassCastException` — if `key` or `value` does not match the underlying cache's configured types
  - `org.ehcache.spi.loaderwriter.CacheWritingException` — if the cache writer fails
  - `java.lang.RuntimeException` — if a configured copier or resilience strategy fails while storing the entry
- **Examples:**
  ```java
  Ehcache<String, User> cache = new Ehcache<>(ehcache);
  User user = new User("John", "john@example.com");
  // Note: liveTime and maxIdleTime are ignored by Ehcache (expiration is cache-level config)
  boolean stored = cache.put("userId123", user, 0, 0);   // returns true
  cache.put("userId123", new User("Jane"), 0, 0);        // returns true; replaces the existing value

  cache.put(null, user, 0, 0);                            // throws IllegalArgumentException (null key)
  cache.put("userId123", null, 0, 0);                     // throws IllegalArgumentException (null value)
  ```
##### remove(...) -> void
- **Signature:** `public void remove(final K key) throws IllegalStateException, IllegalArgumentException, ClassCastException, CacheWritingException, RuntimeException`
- **Summary:** Removes the mapping for a key from the cache if it is present.
- **Contract:**
  - If a cache writer is configured, a delete for that key is still issued to it (synchronously with write-through; queued and delivered later with write-behind).
- **Parameters:**
  - `key` (`K`) — the cache key whose mapping is to be removed from the cache (must not be `null`)
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed, or if the underlying Ehcache instance is not available (for example, it was removed from or closed with its `CacheManager`)
  - `java.lang.IllegalArgumentException` — if `key` is `null`
  - `java.lang.ClassCastException` — if `key` does not match the underlying cache's configured key type
  - `org.ehcache.spi.loaderwriter.CacheWritingException` — if the cache writer fails
  - `java.lang.RuntimeException` — if a configured copier or resilience strategy fails while removing the entry
- **Examples:**
  ```java
  Ehcache<String, User> cache = new Ehcache<>(ehcache);
  cache.put("userId123", user, 0, 0);   // returns true; seeds an entry
  cache.remove("userId123");            // entry removed; getOrNull("userId123") now returns null
  cache.remove("absent");               // idempotent: removing a missing key is a no-op (no exception)

  cache.remove(null);             // throws IllegalArgumentException (null key)
  ```
##### containsKey(...) -> boolean
- **Signature:** `public boolean containsKey(final K key) throws IllegalStateException, IllegalArgumentException, ClassCastException, RuntimeException`
- **Summary:** Checks if the cache contains a mapping for the specified key.
- **Parameters:**
  - `key` (`K`) — the cache key whose presence in the cache is to be tested (must not be `null`)
- **Returns:** `true` if the cache currently has a live (non-expired) mapping for the specified key; `false` otherwise. An entry that has passed its expiry is reported as absent, and the check does not count as an access for time-to-idle purposes, so it does not extend the entry's lifetime.
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed, or if the underlying Ehcache instance is not available (for example, it was removed from or closed with its `CacheManager`)
  - `java.lang.IllegalArgumentException` — if `key` is `null`
  - `java.lang.ClassCastException` — if `key` does not match the underlying cache's configured key type
  - `java.lang.RuntimeException` — if a configured copier or resilience strategy fails while checking the entry
- **Examples:**
  ```java
  Ehcache<String, User> cache = new Ehcache<>(ehcache);
  cache.put("userId123", user, 0, 0);                 // returns true; seeds an entry
  boolean present = cache.containsKey("userId123");   // returns true
  boolean absent = cache.containsKey("missing");      // returns false

  cache.containsKey(null);                            // throws IllegalArgumentException (null key)
  ```
##### putIfAbsent(...) -> V
- **Signature:** `public V putIfAbsent(final K key, final V value) throws IllegalStateException, IllegalArgumentException, ClassCastException, CacheLoadingException, CacheWritingException, RuntimeException`
- **Summary:** Atomically puts a value if the key is not already present.
- **Contract:**
  - If the key already exists, the existing value is returned unchanged.
- **Parameters:**
  - `key` (`K`) — the cache key with which the specified value is to be associated (must not be `null`)
  - `value` (`V`) — the cache value to be associated with the specified key (must not be `null`; this wrapper rejects `null` values with `IllegalArgumentException`)
- **Returns:** the previous value associated with the specified key, or `null` if there was no live mapping. `null` does not by itself prove that `value` was retained: as with `#put(Object, Object, long, long)`, a zero creation duration from the configured `ExpiryPolicy`, or a store failure handled by Ehcache's default (robust) resilience strategy, also yields `null` without retaining the value
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed, or if the underlying Ehcache instance is not available (for example, it was removed from or closed with its `CacheManager`)
  - `java.lang.IllegalArgumentException` — if `key` or `value` is `null`
  - `java.lang.ClassCastException` — if `key`, `value`, or a loaded value does not match the underlying cache's configured types
  - `org.ehcache.spi.loaderwriter.CacheLoadingException` — if the cache loader fails
  - `org.ehcache.spi.loaderwriter.CacheWritingException` — if the cache writer fails
  - `java.lang.RuntimeException` — if a configured copier or resilience strategy fails while conditionally storing the entry
- **Examples:**
  ```java
  Ehcache<String, User> cache = new Ehcache<>(ehcache);

  // First insert: key absent, value is stored and null is returned
  User previous = cache.putIfAbsent("user:123", newUser);   // returns null; newUser is stored

  // Second insert for the same key: existing value returned, cache left unchanged
  User existing = cache.putIfAbsent("user:123", otherUser); // returns newUser; cache unchanged
  cache.getOrNull("user:123");                              // returns newUser (not otherUser)

  cache.putIfAbsent(null, newUser);                         // throws IllegalArgumentException (null key)
  cache.putIfAbsent("user:123", null);                      // throws IllegalArgumentException (null value)
  ```
##### getAll(...) -> Map<K, V>
- **Signature:** `public Map<K, V> getAll(final Set<? extends K> keys) throws IllegalStateException, IllegalArgumentException, ClassCastException, BulkCacheLoadingException, RuntimeException`
- **Summary:** Retrieves multiple values from the cache in a single bulk call.
- **Contract:**
  - ⚠️ Partial result: If bulk loading fails, some requested keys may already have succeeded.
- **Parameters:**
  - `keys` (`Set<? extends K>`) — the set of cache keys to retrieve; must not be `null` and must not contain a `null` element
- **Returns:** a map keyed by the requested keys, with each value being the cached value or `null` if the key is not present in the cache (subject to the cache-loader semantics described above). Treat it as read-only: for an empty `keys` set, Ehcache returns an immutable empty map
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed, or if the underlying Ehcache instance is not available (for example, it was removed from or closed with its `CacheManager`)
  - `java.lang.IllegalArgumentException` — if `keys` is `null` or contains a `null` element
  - `java.lang.ClassCastException` — if a key in `keys` or a loaded value does not match the underlying cache's configured types
  - `org.ehcache.spi.loaderwriter.BulkCacheLoadingException` — if the bulk cache loader fails
  - `java.lang.RuntimeException` — if a configured copier or resilience strategy fails while retrieving the entries
- **Examples:**
  ```java
  Ehcache<String, User> cache = new Ehcache<>(ehcache);
  cache.put("user:1", alice, 0, 0);   // returns true; seeds an entry
  cache.put("user:2", bob, 0, 0);     // returns true; seeds an entry

  Set<String> keys = Set.of("user:1", "user:2", "user:3");
  Map<String, User> results = cache.getAll(keys);   // size 3: an entry for every requested key
  results.get("user:1");                            // returns alice
  results.get("user:3");                            // returns null (requested but absent; no loader)
  results.containsKey("user:3");                    // returns true (key present, mapped to a null value)

  cache.getAll(null);                               // throws IllegalArgumentException (null keys)
  ```
##### putAll(...) -> void
- **Signature:** `public void putAll(final Map<? extends K, ? extends V> entries) throws IllegalStateException, IllegalArgumentException, ClassCastException, BulkCacheWritingException, RuntimeException`
- **Summary:** Stores multiple key-value pairs in the cache in a single bulk call.
- **Parameters:**
  - `entries` (`Map<? extends K, ? extends V>`) — the map of key-value pairs to store; must not be `null` and must not contain a `null` key or `null` value
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed, or if the underlying Ehcache instance is not available (for example, it was removed from or closed with its `CacheManager`)
  - `java.lang.IllegalArgumentException` — if `entries` is `null` or contains a `null` key or `null` value
  - `java.lang.ClassCastException` — if a key or value in `entries` does not match the underlying cache's configured types
  - `org.ehcache.spi.loaderwriter.BulkCacheWritingException` — if the bulk cache writer fails
  - `java.lang.RuntimeException` — if a configured copier or resilience strategy fails while storing the entries
- **Examples:**
  ```java
  Ehcache<String, User> cache = new Ehcache<>(ehcache);
  Map<String, User> users = new HashMap<>();
  users.put("user:1", new User("John"));   // add to the batch map
  users.put("user:2", new User("Jane"));   // add to the batch map
  cache.putAll(users);                     // both entries stored (any existing keys are overwritten)
  cache.getOrNull("user:1");               // returns the John user

  cache.putAll(null);             // throws IllegalArgumentException (null entries)
  ```
##### removeAll(...) -> void
- **Signature:** `public void removeAll(final Set<? extends K> keys) throws IllegalStateException, IllegalArgumentException, ClassCastException, BulkCacheWritingException, RuntimeException`
- **Summary:** Removes multiple keys from the cache in a single bulk call.
- **Parameters:**
  - `keys` (`Set<? extends K>`) — the set of cache keys to remove; must not be `null` and must not contain a `null` element
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed, or if the underlying Ehcache instance is not available (for example, it was removed from or closed with its `CacheManager`)
  - `java.lang.IllegalArgumentException` — if `keys` is `null` or contains a `null` element
  - `java.lang.ClassCastException` — if a key in `keys` does not match the underlying cache's configured key type
  - `org.ehcache.spi.loaderwriter.BulkCacheWritingException` — if the bulk cache writer fails
  - `java.lang.RuntimeException` — if a configured copier or resilience strategy fails while removing the entries
- **Examples:**
  ```java
  Ehcache<String, User> cache = new Ehcache<>(ehcache);
  cache.put("user:1", alice, 0, 0);   // returns true; seeds an entry
  cache.put("user:2", bob, 0, 0);     // returns true; seeds an entry

  Set<String> keysToRemove = Set.of("user:1", "user:2");
  cache.removeAll(keysToRemove);                 // both entries removed
  cache.getOrNull("user:1");                     // returns null
  cache.removeAll(Set.of("absent"));             // idempotent: non-existent keys are ignored (no exception)

  cache.removeAll(null);                         // throws IllegalArgumentException (null keys)
  ```
##### keySet(...) -> Set<K>
- **Signature:** `public Set<K> keySet() throws UnsupportedOperationException`
- **Summary:** Returns the set of keys in the cache.
- **Contract:**
  - An external key set must also track expiration and eviction to reflect the cache accurately.
- **Parameters:**
  - (none)
- **Returns:** this method never returns normally; it always throws
- **Throws:**
  - `java.lang.UnsupportedOperationException` — always thrown, because materializing a key set would require a full scan of all Ehcache storage tiers
- **Performance:** Why Not Supported: Ehcache 3.x only offers entry iteration (`Iterable<Cache.Entry>`), which would require a full O(n) scan of all storage tiers to materialize a key set Multi-tier storage makes key enumeration prohibitively expensive Memory overhead of materializing all keys could exceed available heap For disk/distributed tiers, full scans would severely impact performance Inconsistent with Ehcache's design philosophy of avoiding full cache scans Alternatives: Use the underlying Ehcache entry iterator when a scan is acceptable, or a cache implementation that supports key iteration.
- **Examples:**
  ```java
  Ehcache<String, User> cache = new Ehcache<>(ehcache);
  try {
      cache.keySet();   // throws UnsupportedOperationException (the wrapper avoids a full scan)
  } catch (UnsupportedOperationException expected) {
      // Fall back to application-maintained key tracking if enumeration is required.
  }

  // Alternative: maintain the key set yourself if you must enumerate keys
  Set<String> trackedKeys = new java.util.HashSet<>();
  cache.put("user:123", user, 0, 0);   // returns true
  trackedKeys.add("user:123");         // record the key in the external set
  ```
##### size(...) -> int
- **Signature:** `public int size() throws UnsupportedOperationException`
- **Summary:** Returns the number of entries in the cache.
- **Contract:**
  - An application-maintained counter must account for replacements, expiration, eviction, removals, and failed writes; incrementing it for each put does not measure cache size.
- **Parameters:**
  - (none)
- **Returns:** this method never returns normally; it always throws
- **Throws:**
  - `java.lang.UnsupportedOperationException` — always thrown, because the underlying Ehcache 3.x API does not provide size reporting
- **Examples:**
  ```java
  Ehcache<String, User> cache = new Ehcache<>(ehcache);
  try {
      cache.size();   // throws UnsupportedOperationException (Ehcache 3.x has no size API)
  } catch (UnsupportedOperationException expected) {
      // Fall back to an application-maintained count if one is required.
  }

  // Alternative: maintain the entry count yourself with an atomic counter
  java.util.concurrent.atomic.AtomicInteger counter = new java.util.concurrent.atomic.AtomicInteger(0);
  cache.put("user:123", user, 0, 0);   // returns true
  counter.incrementAndGet();           // tracked size is now 1
  ```
##### clear(...) -> void
- **Signature:** `public void clear() throws IllegalStateException, RuntimeException`
- **Summary:** Removes all entries from the cache.
- **Parameters:**
  - (none)
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed, or if the underlying Ehcache instance is not available (for example, it was removed from or closed with its `CacheManager`)
  - `java.lang.RuntimeException` — if the configured resilience strategy fails while handling a store failure during clearing
- **Examples:**
  ```java
  Ehcache<String, User> cache = new Ehcache<>(ehcache);
  cache.put("userId123", user, 0, 0);  // returns true; seeds an entry
  cache.clear();                       // removes all entries from the cache
  cache.getOrNull("userId123");        // returns null
  ```
##### close(...) -> void
- **Signature:** `public synchronized void close()`
- **Summary:** Closes this wrapper.
- **Parameters:**
  - (none)
- **Examples:**
  ```java
  Ehcache<String, User> applicationCache = new Ehcache<>(ehcache);
  applicationCache.put("userId123", user, 0, 0);   // returns true

  // Deliberately retire the wrapper early, after all application users have stopped.
  applicationCache.close();                 // does not dispose the manager-owned Ehcache
  applicationCache.close();                 // idempotent no-op
  applicationCache.getOrNull("userId123");  // throws IllegalStateException (wrapper closed)
  ```
##### isClosed(...) -> boolean
- **Signature:** `public boolean isClosed()`
- **Summary:** Checks if the cache has been closed.
- **Contract:**
  - Returns `true` if `#close()` has been called on this cache, `false` otherwise.
  - This method can be used to verify cache state before performing operations, though most operations will throw `IllegalStateException` if the cache is closed.
- **Parameters:**
  - (none)
- **Returns:** `true` if `#close()` has been called on this cache; `false` if the cache is still operational
- **Examples:**
  ```java
  Ehcache<String, User> cache = new Ehcache<>(ehcache);
  boolean closed = cache.isClosed();   // returns false (freshly created)
  if (!cache.isClosed()) {
      User user = cache.getOrNull("userId123");   // safe to use while the cache is open
  }

  cache.close();                       // deliberately retire the application wrapper early
  closed = cache.isClosed();           // returns true
  ```

### Class ForeignMemoryOffHeapCache (com.landawn.abacus.cache.ForeignMemoryOffHeapCache)
A modern off-heap cache implementation using Java's Foreign Function & Memory API.

**Thread-safety:** unspecified
**Nullability:** unspecified
**Extends:** `AbstractCache<K, V>`

#### Public Constructors
- (none)

#### Public Static Methods
##### builder(...) -> Builder<K, V>
- **Signature:** `public static <K, V> Builder<K, V> builder()`
- **Summary:** Creates a new builder for constructing `ForeignMemoryOffHeapCache` instances.
- **Parameters:**
  - (none)
- **Returns:** a new `Builder` instance initialized with default configuration values
- **Examples:**
  ```java
  // Basic configuration
  ForeignMemoryOffHeapCache<String, Data> cache = ForeignMemoryOffHeapCache.<String, Data>builder()
      .capacityInMB(100)             // 100MB off-heap (= 100 * 1048576 bytes); required, must be positive
      .evictDelay(60000)             // scan for expired entries every 60s
      .defaultLiveTime(3600000)      // default 1h TTL
      .defaultMaxIdleTime(1800000)   // default 30min idle timeout
      .build();                      // allocates native memory now; returns a ForeignMemoryOffHeapCache

  // Advanced configuration with disk spillover
  // OffHeapStore is an interface; supply your own implementation (e.g., a file-backed store).
  OffHeapStore<String> diskStore = myFileBackedOffHeapStore(Paths.get("/tmp/cache"));
  ForeignMemoryOffHeapCache<String, byte[]> advancedCache = ForeignMemoryOffHeapCache.<String, byte[]>builder()
      .capacityInMB(200)             // 200MB off-heap
      .maxBlockSizeInBytes(16384)    // 16KB max block; larger values split across blocks
      .evictDelay(30000)             // scan every 30s
      .vacatingFactor(0.3f)          // evict 30% of entries (LRU first) under memory pressure
      .offHeapStore(diskStore)       // spill to disk when memory is full
      .statsTimeOnDisk(true)         // track disk read/write timing
      .build();                      // returns an application-scoped cache with disk spillover enabled
  ```
- **See also:** `Builder`, `Builder#build()`

#### Public Instance Methods
##### getOrNull(...) -> V
- **Signature:** `public V getOrNull(final K key) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Retrieves the value associated with the specified key, or `null` if no live mapping exists.
- **Contract:**
  - For a disk-backed entry, the serialized bytes are fetched from the `OffHeapStore` and the entry may be promoted back into memory when the configured promotion predicate accepts it.
  - A read that found live bytes can still return its value if the entry expires while deserialization or promotion is running.
- **Parameters:**
  - `key` (`K`) — the key whose associated value is to be returned; must not be `null`
- **Returns:** the cached value, or `null` if the key is not present, the entry has expired, the disk-backed entry's bytes are missing from the store, or the entry found by the lookup was removed or replaced (by a same-key put or a promotion back into memory) concurrently, even if the key stayed mapped throughout
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed, or if a value cannot be reconstructed because the fetched size no longer matches the recorded size (data corruption), or because a configured deserializer returned `null`
  - `java.lang.IllegalArgumentException` — if `key` is `null`
  - `java.lang.RuntimeException` — if the backing-store read, value deserialization, promotion predicate, or native-memory copy throws an unchecked exception
##### put(...) -> boolean
- **Signature:** `public boolean put(final K key, final V value, final long liveTime, final long maxIdleTime) throws IllegalStateException, IllegalArgumentException, RuntimeException, StackOverflowError, RejectedExecutionException`
- **Summary:** Stores a key-value pair with the specified expiration settings.
- **Contract:**
  - The value is serialized and placed in off-heap memory; when memory is unavailable (or the `storeSelector` directs it), the value is written to the configured `OffHeapStore` instead.
  - Values that could not be placed even in an otherwise empty cache — because the serialized form exceeds the entire off-heap capacity, or because its slot-rounded chunks need more 1 MB segments than the region has — never attempt the in-memory placement (no amount of vacating could make them fit) and go straight to the disk fallback, if any; such a doomed put schedules no vacate pass and leaves the in-memory cache contents untouched.
  - Key requirements: keys must implement `hashCode`, `equals`, and, for a `Comparable` key, `compareTo` consistently and without throwing.
  - The internal index also calls them while serving other keys' operations (`compareTo` when a bin of colliding keys is converted to a tree), so such a failure can surface from a put of another key.
  - If that removal fails (the key's lookup fails again, or the store's `remove` throws), the bytes, unreachable through the cache, are removed by a later `#clear()` or by `#close()` (if the store's `remove` fails during `close()`, that failure is logged and the bytes stay).
  - Until then the cache keeps a reference to each such key, and logs a warning when their number first reaches 1,024 and again each time it doubles; the count restarts once a `clear()` has removed them all.
- **Parameters:**
  - `key` (`K`) — the key; must not be `null`
  - `value` (`V`) — the value; must not be `null`
  - `liveTime` (`long`) — the TTL in milliseconds; `<= 0` means no expiration
  - `maxIdleTime` (`long`) — the maximum idle time in milliseconds; `<= 0` means no idle limit
- **Returns:** `true` if the value was stored (in memory or on disk); `false` otherwise
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed, or memory pressure triggers submission of a vacate task after the shared async executor has been shut down
  - `java.lang.IllegalArgumentException` — if `key` or `value` is `null`, or if `storeSelector` returns `null` or a value outside 0..2
  - `java.lang.RuntimeException` — if serialization, the store selector, the native-memory copy, or the backing-store write throws an unchecked exception, or if the key's `hashCode`, `equals`, or `compareTo` (or a colliding key's `compareTo`) throws one while the value is being mapped
  - `java.lang.StackOverflowError` — if the default Kryo serializer encounters a cycle made only of collections, maps, or arrays in `value`; the cache is left unchanged
  - `java.util.concurrent.RejectedExecutionException` — if the put fails under memory pressure and the shared executor rejects the vacate task
##### remove(...) -> void
- **Signature:** `public void remove(final K key) throws IllegalStateException, IllegalArgumentException`
- **Summary:** Removes the cache entry associated with the specified key, if present, releasing its memory slots or disk bytes.
- **Contract:**
  - The mapping is removed even if the `OffHeapStore` fails to remove the disk bytes; such a failure is logged rather than propagated.
- **Parameters:**
  - `key` (`K`) — the key whose mapping is to be removed; must not be `null`
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if `key` is `null`
##### containsKey(...) -> boolean
- **Signature:** `public boolean containsKey(final K key) throws IllegalStateException, IllegalArgumentException`
- **Summary:** Returns whether the cache contains a live (non-expired) mapping for the specified key.
- **Contract:**
  - Like `#getOrNull(Object)`, it can also report `false` when the entry it finds is being replaced concurrently (by a same-key put or a promotion back into memory), even though the key stays mapped throughout.
- **Parameters:**
  - `key` (`K`) — the key to test; must not be `null`
- **Returns:** `true` if a live mapping exists
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if `key` is `null`
##### keySet(...) -> Set<K>
- **Signature:** `public Set<K> keySet() throws IllegalStateException`
- **Summary:** Returns a snapshot of all keys currently held in the cache, including keys of entries spilled to disk.
- **Parameters:**
  - (none)
- **Returns:** a snapshot of the cache keys; never `null`
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
##### size(...) -> int
- **Signature:** `public int size() throws IllegalStateException`
- **Summary:** Returns the number of entries currently held in the cache (including disk-backed entries, and entries that have expired but not yet been swept).
- **Parameters:**
  - (none)
- **Returns:** the current entry count
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
##### clear(...) -> void
- **Signature:** `public void clear() throws IllegalStateException`
- **Summary:** Removes all entries, releasing their memory slots and disk bytes, and reclaims the now-empty segments for reuse by any slot size.
- **Contract:**
  - The retry runs even when removing the entries throws; that failure is then rethrown, with any retry failure attached to it as suppressed.
- **Parameters:**
  - (none)
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
##### isClosed(...) -> boolean
- **Signature:** `public boolean isClosed()`
- **Summary:** Returns whether this cache has been closed.
- **Parameters:**
  - (none)
- **Returns:** `true` if the cache is closed
##### close(...) -> void
- **Signature:** `public void close() throws IllegalStateException, RuntimeException`
- **Summary:** Deliberately decommissions this application-scoped cache before JVM termination.
- **Contract:**
  - It cancels the maintenance task, removes the JVM shutdown hook, frees every entry (removing disk-backed entries' bytes from the store, along with any bytes a failed disk put left behind), deallocates the native region, and closes the configured `OffHeapStore`, if any.
  - If the cache remains active for the application lifetime, the registered shutdown hook invokes this method automatically during JVM shutdown.
  - Every entry and cleanup step is attempted even when an earlier one fails.
  - The entry index is cleared even if an entry cannot be released.
  - Reentrant callback restriction: a serializer, store selector, or other callback invoked from an operation holding this cache's lifecycle read lock must not close the cache synchronously (a read-to-write lock upgrade is unsupported); such a call fails immediately with `IllegalStateException`.
- **Parameters:**
  - (none)
- **Throws:**
  - `java.lang.IllegalStateException` — if the current thread attempts to close the cache while it is executing a cache operation or callback
  - `java.lang.RuntimeException` — if canceling maintenance, removing the shutdown hook, releasing an entry, deallocating native memory, or closing the configured store throws an unchecked exception; later distinct cleanup failures are suppressed on the first failure
##### stats(...) -> OffHeapCacheStats
- **Signature:** `public OffHeapCacheStats stats() throws IllegalStateException, IllegalArgumentException`
- **Summary:** Returns a sampled, point-in-time snapshot of cache statistics: entry counts, hit/miss and eviction counters, memory and disk usage, disk I/O timing aggregates, and per-segment slot occupancy.
- **Parameters:**
  - (none)
- **Returns:** the statistics snapshot; never `null`
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if an accumulated counter has overflowed into a negative value, or an overflowed disk-timing accumulator produces invalid timing statistics

### Class Builder (com.landawn.abacus.cache.ForeignMemoryOffHeapCache.Builder)
Fluent builder for creating `ForeignMemoryOffHeapCache` instances with custom configuration.

**Thread-safety:** thread-safe
**Nullability:** unspecified

#### Public Constructors
- `public Builder()` — Creates a new builder instance with default configuration values.
  ```java
  ForeignMemoryOffHeapCache.Builder<String, Data> builder = ForeignMemoryOffHeapCache.builder();   // new builder, defaults applied
  builder.capacityInMB(100)                                                                        // each fluent setter returns the same builder instance
         .evictDelay(60000)
         .defaultLiveTime(3600000);                                  // mutates builder in place; result is the same builder reference
  ForeignMemoryOffHeapCache<String, Data> cache = builder.build();   // allocates native memory for an application-scoped cache
  ```

#### Public Static Methods
- (none)

#### Public Instance Methods
##### serializer(...) -> varies by overload
- **Signature:** `public Builder<K, V> serializer(final BiConsumer<? super V, ByteArrayOutputStream> serializer) throws IllegalArgumentException`
- **Summary:** Sets the serializer used to convert values to byte streams.
- **Parameters:**
  - `serializer` (`BiConsumer<? super V, ByteArrayOutputStream>`) — the serializer to use
- **Returns:** this builder
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `serializer` is `null`
- **Signature:** `public BiConsumer<? super V, ByteArrayOutputStream> serializer()`
- **Summary:** Custom serializer for converting values to byte streams.
- **Contract:**
  - The serializer receives the value and a `ByteArrayOutputStream`, and should write the complete serialized form to the stream.
  - Default: `null` (uses Kryo if available, otherwise JSON)
- **Parameters:**
  - (none)
- **Returns:** Custom serializer for converting values to byte streams.
##### deserializer(...) -> varies by overload
- **Signature:** `public Builder<K, V> deserializer(final BiFunction<byte[], Type<V>, ? extends V> deserializer) throws IllegalArgumentException`
- **Summary:** Sets the deserializer used to reconstruct values from byte arrays.
- **Parameters:**
  - `deserializer` (`BiFunction<byte[], Type<V>, ? extends V>`) — the deserializer to use
- **Returns:** this builder
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `deserializer` is `null`
- **Signature:** `public BiFunction<byte[], Type<V>, ? extends V> deserializer()`
- **Summary:** Custom deserializer for converting byte arrays back to values.
- **Contract:**
  - The function receives the byte array and the value `Type`, and should return the deserialized value instance.
  - When a disk read may be promoted to off-heap memory, the cache preserves a separate pristine copy for that promotion.
  - Default: `null` (uses Kryo if available, otherwise JSON)
- **Parameters:**
  - (none)
- **Returns:** Custom deserializer for converting byte arrays back to values.
##### testerForLoadingItemFromDiskToMemory(...) -> varies by overload
- **Signature:** `public Builder<K, V> testerForLoadingItemFromDiskToMemory(final TriPredicate<ActivityPrint, Integer, Long> testerForLoadingItemFromDiskToMemory) throws IllegalArgumentException`
- **Summary:** Sets the predicate used to decide whether an item should be loaded from disk to memory.
- **Parameters:**
  - `testerForLoadingItemFromDiskToMemory` (`TriPredicate<ActivityPrint, Integer, Long>`) — the predicate to use
- **Returns:** this builder
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `testerForLoadingItemFromDiskToMemory` is `null`
- **Signature:** `public TriPredicate<ActivityPrint, Integer, Long> testerForLoadingItemFromDiskToMemory()`
- **Summary:** Predicate to determine when to load items from disk back to memory based on access patterns.
- **Contract:**
  - Only applies when `#offHeapStore` is configured.
- **Parameters:**
  - (none)
- **Returns:** Predicate to determine when to load items from disk back to memory based on access patterns.
##### storeSelector(...) -> varies by overload
- **Signature:** `public Builder<K, V> storeSelector(final TriFunction<K, V, Integer, Integer> storeSelector) throws IllegalArgumentException`
- **Summary:** Sets the function used to choose the storage location for a put operation.
- **Parameters:**
  - `storeSelector` (`TriFunction<K, V, Integer, Integer>`) — the storage selector to use
- **Returns:** this builder
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `storeSelector` is `null`
- **Signature:** `public TriFunction<K, V, Integer, Integer> storeSelector()`
- **Summary:** Function to determine storage location for each put operation.
- **Contract:**
  - The function receives the key, value, and size (in bytes), and should return: 0 - default routing (try memory first, fallback to disk if memory unavailable) 1 - memory only (never store to disk, operation fails if memory unavailable) 2 - disk only (always store to disk via `#offHeapStore`, skip memory) The selector is evaluated for every put.
- **Parameters:**
  - (none)
- **Returns:** Function to determine storage location for each put operation.
##### build(...) -> ForeignMemoryOffHeapCache<K, V>
- **Signature:** `public ForeignMemoryOffHeapCache<K, V> build() throws IllegalArgumentException, OutOfMemoryError, RejectedExecutionException, IllegalStateException`
- **Summary:** Builds and returns a new `ForeignMemoryOffHeapCache` configured with the current builder settings.
- **Contract:**
  - If `maxBlockSizeInBytes` is `0`, the default block size (8192) is applied.
- **Parameters:**
  - (none)
- **Returns:** a new `ForeignMemoryOffHeapCache` instance configured with the builder settings
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `capacityInMB` is not positive, if `maxBlockSizeInBytes` is non-zero and outside \[1024, 1048576\] (a value of 0 is replaced with the default 8192), or if `vacatingFactor` is NaN or outside \[0.0, 1.0\]
  - `java.lang.OutOfMemoryError` — if native memory allocation fails, or if the Java heap cannot hold the cache's per-megabyte segment bookkeeping (possible for a very large `capacityInMB`)
  - `java.util.concurrent.RejectedExecutionException` — if `evictDelay` is positive and the maintenance scheduler rejects its task
  - `java.lang.IllegalStateException` — if the JVM is already shutting down when the cache registers its shutdown hook
- **Examples:**
  ```java
  // Basic cache with required fields
  ForeignMemoryOffHeapCache<String, Data> cache = ForeignMemoryOffHeapCache.<String, Data>builder()
      .capacityInMB(200)                     // 200MB off-heap (required, must be positive)
      .evictDelay(60000)                     // scan for expired entries every 60s
      .vacatingFactor(0.3f)                  // evict 30% of entries (LRU first) under memory pressure
      .build();                              // application-scoped cache; native memory now allocated
  cache.put("key", data);                    // serializes + copies the value off-heap; returns true on success
  Data retrieved = cache.getOrNull("key");   // returns the cached value, or null if absent/expired

  // Advanced cache with disk spillover
  OffHeapStore<String> store = myOffHeapStoreImplementation;   // your disk-backed store
  ForeignMemoryOffHeapCache<String, Data> advancedCache = ForeignMemoryOffHeapCache.<String, Data>builder()
      .capacityInMB(100)            // 100MB off-heap
      .maxBlockSizeInBytes(16384)   // 16KB max block; larger values split across blocks
      .evictDelay(30000)            // scan every 30s
      .defaultLiveTime(3600000)     // default 1h TTL
      .defaultMaxIdleTime(1800000)  // default 30min idle timeout
      .vacatingFactor(0.25f)        // evict 25% of entries (LRU first) on vacate
      .offHeapStore(store)          // spill to disk when memory is full
      .statsTimeOnDisk(true)        // track disk read/write timing
      .build();                     // returns an application-scoped cache with disk spillover enabled

  // build() validates eagerly:
  ForeignMemoryOffHeapCache.<String, Data>builder().build();                                            // throws IllegalArgumentException (capacityInMB not set / 0)
  ForeignMemoryOffHeapCache.<String, Data>builder().capacityInMB(8).maxBlockSizeInBytes(512).build();   // throws IllegalArgumentException (maxBlockSizeInBytes < 1024)
  ForeignMemoryOffHeapCache<String, Data> defaultBlock = ForeignMemoryOffHeapCache.<String, Data>builder()
      .capacityInMB(8).maxBlockSizeInBytes(0).build();                                // ok: 0 is replaced with the default 8192
  ```
##### capacityInMB(...) -> varies by overload
- **Signature:** `public int capacityInMB()`
- **Summary:** The total off-heap memory capacity in megabytes.
- **Contract:**
  - This is a required field and must be set to a positive value before calling `#build()`.
  - Default: 0 (must be set to a positive value)
- **Parameters:**
  - (none)
- **Returns:** The total off-heap memory capacity in megabytes.
- **Signature:** `public Builder capacityInMB(int capacityInMB)`
- **Summary:** The total off-heap memory capacity in megabytes.
- **Contract:**
  - This is a required field and must be set to a positive value before calling `#build()`.
  - Default: 0 (must be set to a positive value)
- **Parameters:**
  - `capacityInMB` (`int`) — The total off-heap memory capacity in megabytes.
- **Returns:** unspecified
##### maxBlockSizeInBytes(...) -> varies by overload
- **Signature:** `public int maxBlockSizeInBytes()`
- **Summary:** Maximum size of a single memory block in bytes.
- **Contract:**
  - Must be `0` (replaced with the default 8192 by `#build()`) or between 1024 and `SEGMENT_SIZE` (1,048,576), and is rounded up to the nearest multiple of `MIN_BLOCK_SIZE` (64 bytes).
  - Larger blocks reduce the number of slots needed for large values; each segment is dedicated to a single slot size and may have an unused tail when that size does not divide it.
- **Parameters:**
  - (none)
- **Returns:** Maximum size of a single memory block in bytes.
- **Signature:** `public Builder maxBlockSizeInBytes(int maxBlockSizeInBytes)`
- **Summary:** Maximum size of a single memory block in bytes.
- **Contract:**
  - Must be `0` (replaced with the default 8192 by `#build()`) or between 1024 and `SEGMENT_SIZE` (1,048,576), and is rounded up to the nearest multiple of `MIN_BLOCK_SIZE` (64 bytes).
  - Larger blocks reduce the number of slots needed for large values; each segment is dedicated to a single slot size and may have an unused tail when that size does not divide it.
- **Parameters:**
  - `maxBlockSizeInBytes` (`int`) — Maximum size of a single memory block in bytes.
- **Returns:** unspecified
##### evictDelay(...) -> varies by overload
- **Signature:** `public long evictDelay()`
- **Summary:** Delay between eviction runs in milliseconds.
- **Parameters:**
  - (none)
- **Returns:** Delay between eviction runs in milliseconds.
- **Signature:** `public Builder evictDelay(long evictDelay)`
- **Summary:** Delay between eviction runs in milliseconds.
- **Parameters:**
  - `evictDelay` (`long`) — Delay between eviction runs in milliseconds.
- **Returns:** unspecified
##### defaultLiveTime(...) -> varies by overload
- **Signature:** `public long defaultLiveTime()`
- **Summary:** Default time-to-live for cache entries in milliseconds.
- **Parameters:**
  - (none)
- **Returns:** Default time-to-live for cache entries in milliseconds.
- **Signature:** `public Builder defaultLiveTime(long defaultLiveTime)`
- **Summary:** Default time-to-live for cache entries in milliseconds.
- **Parameters:**
  - `defaultLiveTime` (`long`) — Default time-to-live for cache entries in milliseconds.
- **Returns:** unspecified
##### defaultMaxIdleTime(...) -> varies by overload
- **Signature:** `public long defaultMaxIdleTime()`
- **Summary:** Default maximum idle time for cache entries in milliseconds.
- **Parameters:**
  - (none)
- **Returns:** Default maximum idle time for cache entries in milliseconds.
- **Signature:** `public Builder defaultMaxIdleTime(long defaultMaxIdleTime)`
- **Summary:** Default maximum idle time for cache entries in milliseconds.
- **Parameters:**
  - `defaultMaxIdleTime` (`long`) — Default maximum idle time for cache entries in milliseconds.
- **Returns:** unspecified
##### vacatingFactor(...) -> varies by overload
- **Signature:** `public float vacatingFactor()`
- **Summary:** Factor in `[0.0, 1.0]` controlling how aggressive a vacate is.
- **Contract:**
  - Vacate is triggered when off-heap memory cannot satisfy a put and no disk store absorbs the value; this fraction of the memory-resident entries (at least one) is then evicted (LRU first) to free space.
  - It does NOT control when vacating starts.
- **Parameters:**
  - (none)
- **Returns:** Factor in `[0.0, 1.0]` controlling how aggressive a vacate is.
- **Signature:** `public Builder vacatingFactor(float vacatingFactor)`
- **Summary:** Factor in `[0.0, 1.0]` controlling how aggressive a vacate is.
- **Contract:**
  - Vacate is triggered when off-heap memory cannot satisfy a put and no disk store absorbs the value; this fraction of the memory-resident entries (at least one) is then evicted (LRU first) to free space.
  - It does NOT control when vacating starts.
- **Parameters:**
  - `vacatingFactor` (`float`) — Factor in `[0.0, 1.0]` controlling how aggressive a vacate is.
- **Returns:** unspecified
##### offHeapStore(...) -> varies by overload
- **Signature:** `public OffHeapStore<K> offHeapStore()`
- **Summary:** Optional disk store for spillover when memory is full.
- **Contract:**
  - When configured, values that don't fit in memory can be stored to disk instead of being rejected.
- **Parameters:**
  - (none)
- **Returns:** Optional disk store for spillover when memory is full.
- **Signature:** `public Builder offHeapStore(OffHeapStore<K> offHeapStore)`
- **Summary:** Optional disk store for spillover when memory is full.
- **Contract:**
  - When configured, values that don't fit in memory can be stored to disk instead of being rejected.
- **Parameters:**
  - `offHeapStore` (`OffHeapStore<K>`) — Optional disk store for spillover when memory is full.
- **Returns:** unspecified
##### statsTimeOnDisk(...) -> varies by overload
- **Signature:** `public boolean statsTimeOnDisk()`
- **Summary:** Whether to collect detailed disk I/O timing statistics.
- **Contract:**
  - When `true`, tracks min/max/average read and write times to disk.
  - Only applies when `#offHeapStore` is configured.
- **Parameters:**
  - (none)
- **Returns:** Whether to collect detailed disk I/O timing statistics.
- **Signature:** `public Builder statsTimeOnDisk(boolean statsTimeOnDisk)`
- **Summary:** Whether to collect detailed disk I/O timing statistics.
- **Contract:**
  - When `true`, tracks min/max/average read and write times to disk.
  - Only applies when `#offHeapStore` is configured.
- **Parameters:**
  - `statsTimeOnDisk` (`boolean`) — Whether to collect detailed disk I/O timing statistics.
- **Returns:** unspecified
##### equals(...) -> boolean
- **Signature:** `public boolean equals(Object other)`
- **Parameters:**
  - `other` (`Object`)
- **Returns:** unspecified
##### hashCode(...) -> int
- **Signature:** `public int hashCode()`
- **Parameters:**
  - (none)
- **Returns:** unspecified
##### toString(...) -> String
- **Signature:** `public String toString()`
- **Parameters:**
  - (none)
- **Returns:** unspecified

### Class JRedis (com.landawn.abacus.cache.JRedis)
A Redis-based distributed cache client implementation using Jedis with client-side sharding.

**Thread-safety:** thread-safe
**Nullability:** unspecified
**Extends:** `AbstractDistributedCacheClient<T>`

#### Public Constructors
- `public JRedis(final String serverUrl) throws IllegalArgumentException, RuntimeException` — Creates a new JRedis instance with the default timeout.
  - **Throws:**
    - `java.lang.IllegalArgumentException` — if `serverUrl` is `null`, empty, blank, or contains no valid server addresses
    - `java.lang.RuntimeException` — if a shard's client pool cannot be constructed. Note that an unresolvable hostname does NOT fail construction: the failure surfaces on the first operation routed to that shard.
    - `java.lang.IllegalStateException` — if the optional Kryo dependency is not on the classpath or cannot be initialized (checked before any shard client is built)
  ```java
  // Single Redis instance
  JRedis<User> cache = new JRedis<>("localhost:6379");           // uses DEFAULT_TIMEOUT
  String url = cache.serverUrl();                                // returns "localhost:6379" (preserved verbatim)

  // Multiple Redis instances for sharding
  JRedis<User> sharded = new JRedis<>("localhost:6379,localhost:6380,localhost:6381");

  // Use the cache
  User user = new User("Alice", "alice@example.com");
  cache.put("user:123", user, 3600000);                         // returns true on success
  // Retain and share cache/sharded; optionally disconnect them during application shutdown.

  // Negative: null serverUrl is rejected up-front
  JRedis<User> bad = new JRedis<>((String) null);               // throws IllegalArgumentException

  // Negative: blank serverUrl is rejected (checkArgNotBlank)
  JRedis<User> blank = new JRedis<>("   ");                     // throws IllegalArgumentException
  ```
- `public JRedis(final String serverUrl, final long timeout) throws IllegalArgumentException, RuntimeException` — Creates a new JRedis instance with a specified timeout.
  - **Throws:**
    - `java.lang.IllegalArgumentException` — if `serverUrl` is `null`, empty, blank, or contains no valid server addresses, or if `timeout` is not positive or exceeds `Integer#MAX_VALUE`
    - `java.lang.RuntimeException` — if a shard's client pool cannot be constructed. Note that an unresolvable hostname does NOT fail construction: the failure surfaces on the first operation routed to that shard.
    - `java.lang.IllegalStateException` — if the optional Kryo dependency is not on the classpath or cannot be initialized (checked after `serverUrl` is found non-blank and before any shard client is built)
  ```java
  // Cache with 5 second timeout
  JRedis<Data> cache = new JRedis<>("redis1:6379,redis2:6379", 5000);   // timeout in milliseconds
  String url = cache.serverUrl();                                       // returns "redis1:6379,redis2:6379"

  // Use the cache
  Data data = new Data("value");
  cache.put("key", data, 7200000);                                      // Cache for 2 hours; returns true
  Data retrieved = cache.get("key");                                    // the cached Data, or null

  // Negative: a non-positive timeout is rejected (checkArgPositive)
  JRedis<Data> zero = new JRedis<>("localhost:6379", 0);              // throws IllegalArgumentException

  // Negative: a timeout above Integer.MAX_VALUE ms cannot fit the int Jedis API
  JRedis<Data> tooBig = new JRedis<>("localhost:6379", Integer.MAX_VALUE + 1L);   // throws IllegalArgumentException
  ```

#### Public Static Methods
- (none)

#### Public Instance Methods
##### flushAll(...) -> void
- **Signature:** `public void flushAll() throws IllegalStateException, RuntimeException`
- **Summary:** Removes all keys from all connected standalone Redis instances.
- **Contract:**
  - If a FLUSHALL on one shard fails, the remaining shards are still attempted.
  - If other applications share the same Redis instances, their data will also be deleted.
- **Parameters:**
  - (none)
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.RuntimeException` — if acquiring a shard connection or executing FLUSHALL fails; the first such exception is rethrown after all remaining shards have been attempted, with later failures attached as suppressed exceptions
- **See also:** `#disconnect()`
##### get(...) -> T
- **Signature:** `public T get(final String key) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Retrieves a value from the cache by its key.
- **Contract:**
  - If the key does not exist or has expired, `null` is returned.
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be retrieved. Must not be `null`.
- **Returns:** the cached value, or `null` if not found, expired, evicted, or stored as `null`
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null` or contains an unpaired UTF-16 surrogate
  - `java.lang.RuntimeException` — if acquiring a Redis connection or executing GET fails, Redis rejects the command, or the returned bytes cannot be decoded by the configured Kryo serializers
- **Performance:** The operation is O(1) time complexity.
- **Examples:**
  ```java
  User user = cache.get("user:123");                            // the cached User, or null if absent

  // Edge: a missing or expired key returns null (not an exception)
  User missing = cache.get("does:not:exist");                   // returns null

  // Negative: a null key is rejected
  User bad = cache.get(null);                                   // throws IllegalArgumentException

  // Negative: a counter key created by incr()/decr() cannot be deserialized by get()
  cache.incr("page:views");
  cache.get("page:views");                                      // throws KryoException (decode failure)
  ```
- **See also:** `#put(String, Object, long)`, `#remove(String)`
##### getBulk(...) -> Map<String, T>
- **Signature:** `public Map<String, T> getBulk(final String... keys) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Retrieves multiple values from the cache, returning a map of only the keys that were found.
- **Parameters:**
  - `keys` (`String...`) — the cache keys to retrieve; must not be `null` or contain `null` elements
- **Returns:** a map of the found key-value pairs, never `null` (empty if no keys are found)
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `keys` is `null`, contains a `null` element, or contains a key with an unpaired UTF-16 surrogate
  - `java.lang.RuntimeException` — if acquiring a Redis connection or executing GET fails, Redis rejects the command, or the returned bytes cannot be decoded by the configured Kryo serializers
- **Examples:**
  ```java
  // "user:1" exists, "user:2" is missing
  Map<String, User> found = cache.getBulk("user:1", "user:2");   // size == 1; contains only "user:1"

  // Edge: no arguments -> empty (never null) map; no GET is issued
  Map<String, User> none = cache.getBulk();                      // returns an empty map

  // Negative: a null element throws; no GET is issued for any key
  cache.getBulk("user:1", null, "user:3");                      // throws IllegalArgumentException
  ```
- **See also:** `#get(String)`, `#getBulk(Collection)`
- **Signature:** `public Map<String, T> getBulk(final Collection<String> keys) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Retrieves multiple values from the cache, returning a map of only the keys that were found.
- **Parameters:**
  - `keys` (`Collection<String>`) — the collection of cache keys to retrieve; must not be `null` or contain `null` elements
- **Returns:** a map of the found key-value pairs, never `null` (empty if no keys are found)
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `keys` is `null`, contains a `null` element, or contains a key with an unpaired UTF-16 surrogate
  - `java.lang.RuntimeException` — if acquiring a Redis connection or executing GET fails, Redis rejects the command, or the returned bytes cannot be decoded by the configured Kryo serializers
- **Examples:**
  ```java
  List<String> ids = List.of("user:1", "user:2", "user:3");
  Map<String, User> users = cache.getBulk(ids);                  // contains only the keys that exist

  // Edge: empty collection -> empty (never null) map; no GET is issued
  Map<String, User> none = cache.getBulk(List.of());            // returns an empty map

  // Negative: a null collection throws (validated before any network call)
  cache.getBulk((Collection<String>) null);                     // throws IllegalArgumentException
  ```
- **See also:** `#get(String)`, `#getBulk(String...)`
##### put(...) -> boolean
- **Signature:** `public boolean put(final String key, final T value, final long liveTime) throws IllegalStateException, IllegalArgumentException, RuntimeException, StackOverflowError`
- **Summary:** Stores a key-value pair in the cache with a specified time-to-live.
- **Contract:**
  - If the key already exists, its value and TTL will be replaced.
  - Redis-specific behavior: When `liveTime` is positive, this operation uses the Redis SET command with the `PX` option, which atomically sets both the value and a millisecond-precision expiration time — the requested `liveTime` is passed unchanged, with no client-side rounding to seconds.
  - When `liveTime` is 0 or negative, a plain SET command is used without expiration, meaning the key will persist until explicitly deleted.
  - If the key already exists, the previous value and TTL are completely replaced.
- **Parameters:**
  - `key` (`String`) — the cache key with which the specified value is to be associated. Must not be `null`.
  - `value` (`T`) — the value to cache. May be `null` (stored as empty byte array).
  - `liveTime` (`long`) — the time-to-live in milliseconds. Positive values are passed unchanged to SET... PX (subject to Redis's representable expiration range). 0 or negative means no expiration (plain SET).
- **Returns:** `true` when Redis acknowledges the write with "OK" (the normal success reply). An unconditional `SET` either replies "OK" or fails with a `JedisException`, so in practice this returns `true` on success and throws on failure rather than returning `false`
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null` or contains an unpaired UTF-16 surrogate
  - `java.lang.RuntimeException` — if serialization of `value` fails, a network error or timeout occurs, or Redis rejects `liveTime` as an out-of-range expiration
  - `java.lang.StackOverflowError` — if `value`'s object graph contains a cycle made only of collections, maps, or arrays (see the class documentation)
- **Examples:**
  ```java
  // Cache with 1 hour TTL (3600000 ms -> SET ... PX 3600000)
  boolean success = cache.put("user:123", user, 3600000);       // returns true when Redis replies "OK"

  // Edge: liveTime <= 0 means NO expiration -> plain SET (no PX)
  cache.put("forever", user, 0);                                // uses SET; key never auto-expires

  // Edge: a null value is allowed; it is stored as an empty byte array
  cache.put("empty:key", (User) null, 3600000);                // returns true; get(...) later returns null

  // Negative: a null key is rejected
  cache.put(null, user, 60000);                                // throws IllegalArgumentException
  ```
- **See also:** `#get(String)`, `#remove(String)`
##### remove(...) -> boolean
- **Signature:** `public boolean remove(final String key) throws IllegalStateException, IllegalArgumentException, JedisException`
- **Summary:** Removes the mapping for a key from the cache if it is present.
- **Contract:**
  - The return value reflects Redis's DEL semantics (which returns the number of keys removed): `true` when the key existed and was removed, `false` when the key did not exist at the time the command was issued.
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be removed. Must not be `null`.
- **Returns:** `true` if Redis reported at least one key was actually removed; `false` if the key did not exist at the time the `DEL` command was issued
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null` or contains an unpaired UTF-16 surrogate
  - `redis.clients.jedis.exceptions.JedisException` — if acquiring a Redis connection or executing DEL fails, including a timeout or a server response rejecting the command
- **Performance:** The operation is O(1) time complexity.
- **Examples:**
  ```java
  cache.put("user:123", user, 60000);                          // key now present
  boolean removed = cache.remove("user:123");                  // returns true (DEL count == 1)

  // Edge: deleting a key that does not exist returns false (DEL count == 0)
  boolean wasThere = cache.remove("never:set");                // returns false

  // Negative: a null key is rejected
  cache.remove(null);                                          // throws IllegalArgumentException
  ```
- **See also:** `#get(String)`, `#put(String, Object, long)`
##### incr(...) -> long
- **Signature:** `public long incr(final String key) throws IllegalStateException, IllegalArgumentException, JedisException`
- **Summary:** Atomically increments a numeric value by 1.
- **Contract:**
  - If the key doesn't exist, it will be automatically created with value 1 (Redis initializes to 0, then increments by 1).
  - If the key is not an integer or the result exceeds Redis's signed 64-bit range, an error occurs.
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be incremented. Must not be `null`.
- **Returns:** the value after increment (will be 1 if the key did not exist before)
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null` or contains an unpaired UTF-16 surrogate
  - `redis.clients.jedis.exceptions.JedisException` — if acquiring a Redis connection or executing the counter command fails, including a timeout, a server rejection, a non-integer value at `key`, or a result outside Redis's signed 64-bit integer range
- **Performance:** The operation is O(1) time complexity.
- **Examples:**
  ```java
  long pageViews = cache.incr("page:views");                   // returns 1 the first time, 2 the second, ...

  // Negative: a null key is rejected
  cache.incr(null);                                           // throws IllegalArgumentException

  // Negative: incrementing a non-integer value fails on the server
  cache.put("name", "Alice", 0);
  cache.incr("name");                                        // throws JedisDataException (a JedisException)
  ```
- **See also:** `#incr(String, long)`, `#decr(String)`
- **Signature:** `public long incr(final String key, final long delta) throws IllegalStateException, IllegalArgumentException, JedisException`
- **Summary:** Atomically increments a numeric value by a specified amount.
- **Contract:**
  - If the key doesn't exist, it will be automatically created with the delta value.
  - If the key is not an integer or the result exceeds Redis's signed 64-bit range, an error occurs.
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be incremented. Must not be `null`.
  - `delta` (`long`) — the increment amount; must be non-negative. Although the Redis INCRBY command itself supports negative deltas, this implementation rejects them with an `IllegalArgumentException` for portability across cache backends (e.g. SpyMemcached, which also rejects negative deltas).
- **Returns:** the value after increment (will be equal to delta if the key did not exist before)
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, or `delta` is negative
  - `redis.clients.jedis.exceptions.JedisException` — if acquiring a Redis connection or executing the counter command fails, including a timeout, a server rejection, a non-integer value at `key`, or a result outside Redis's signed 64-bit integer range
- **Performance:** The operation is O(1) time complexity.
- **Examples:**
  ```java
  long score = cache.incr("player:score:123", 10);            // returns 10 the first time

  // Edge: a delta of 0 returns the current value unchanged
  long current = cache.incr("player:score:123", 0);

  // Negative: a negative delta is rejected by this wrapper (for cross-backend portability)
  cache.incr("counter", -1);                                 // throws IllegalArgumentException
  ```
- **See also:** `#incr(String)`, `#decr(String, long)`
##### decr(...) -> long
- **Signature:** `public long decr(final String key) throws IllegalStateException, IllegalArgumentException, JedisException`
- **Summary:** Atomically decrements a numeric value by 1.
- **Contract:**
  - If the key doesn't exist, it will be automatically created with value -1.
  - If the key is not an integer or the result exceeds Redis's signed 64-bit range, an error occurs.
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be decremented. Must not be `null`.
- **Returns:** the value after decrement (can be negative in Redis, will be -1 if the key did not exist before)
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null` or contains an unpaired UTF-16 surrogate
  - `redis.clients.jedis.exceptions.JedisException` — if acquiring a Redis connection or executing the counter command fails, including a timeout, a server rejection, a non-integer value at `key`, or a result outside Redis's signed 64-bit integer range
- **Performance:** The operation is O(1) time complexity.
- **Examples:**
  ```java
  long stock = cache.decr("product:stock:123");                // value after decrement

  // Edge: a fresh key auto-initializes to 0 then -1 (Redis allows negatives)
  long first = cache.decr("brand:new:counter");               // returns -1 the first time

  // Negative: a null key is rejected
  cache.decr(null);                                          // throws IllegalArgumentException
  ```
- **See also:** `#decr(String, long)`, `#incr(String)`
- **Signature:** `public long decr(final String key, final long delta) throws IllegalStateException, IllegalArgumentException, JedisException`
- **Summary:** Atomically decrements a numeric value by a specified amount.
- **Contract:**
  - If the key doesn't exist, it will be automatically created with the negative delta value.
  - If the key is not an integer or the result exceeds Redis's signed 64-bit range, an error occurs.
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be decremented. Must not be `null`.
  - `delta` (`long`) — the decrement amount; must be non-negative. Although the Redis DECRBY command itself supports negative deltas, this implementation rejects them with an `IllegalArgumentException` for portability across cache backends (e.g. SpyMemcached, which also rejects negative deltas).
- **Returns:** the value after decrement (can be negative in Redis, will be equal to `-delta` if the key did not exist before)
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, or `delta` is negative
  - `redis.clients.jedis.exceptions.JedisException` — if acquiring a Redis connection or executing the counter command fails, including a timeout, a server rejection, a non-integer value at `key`, or a result outside Redis's signed 64-bit integer range
- **Performance:** The operation is O(1) time complexity.
- **Examples:**
  ```java
  long inventory = cache.decr("product:stock:456", 5);        // value after subtracting 5

  // Edge: a delta of 0 returns the current value unchanged
  long current = cache.decr("quota:abc", 0);

  // Negative: a negative delta is rejected by this wrapper
  cache.decr("counter", -1);                                 // throws IllegalArgumentException
  ```
- **See also:** `#decr(String)`, `#incr(String, long)`
##### disconnect(...) -> void
- **Signature:** `public final synchronized void disconnect() throws Error`
- **Summary:** Disconnects from all Redis instances and releases resources.
- **Contract:**
  - Thread Safety: This method is thread-safe, but once called, no other operations should be attempted on this client instance from any thread.
- **Parameters:**
  - (none)
- **Throws:**
  - `java.lang.Error` — if closing an underlying client raises an error; standalone shards are all attempted before the first error is rethrown
- **See also:** `#flushAll()`

### Class JRedisCluster (com.landawn.abacus.cache.JRedisCluster)
A Redis-based distributed cache client implementation backed by a Redis Cluster.

**Thread-safety:** thread-safe
**Nullability:** unspecified
**Extends:** `AbstractDistributedCacheClient<T>`

#### Public Constructors
- `public JRedisCluster(final String serverUrl) throws IllegalArgumentException, RuntimeException` — Creates a new JRedisCluster instance with the default timeout.
  - **Throws:**
    - `java.lang.IllegalArgumentException` — if `serverUrl` is `null`, empty, blank, or contains no valid server addresses
    - `java.lang.RuntimeException` — if seed resolution, initial topology discovery, or client construction fails
    - `java.lang.IllegalStateException` — if the optional Kryo dependency is not on the classpath or cannot be initialized (checked before the cluster client is built)
  ```java
  // A single seed node is enough to discover the whole cluster
  JRedisCluster<User> cache = new JRedisCluster<>("10.0.0.1:7000");        // uses DEFAULT_TIMEOUT
  String url = cache.serverUrl();                                          // returns "10.0.0.1:7000" (verbatim)

  // Several seed nodes for resilient bootstrapping
  JRedisCluster<User> ha = new JRedisCluster<>("10.0.0.1:7000,10.0.0.2:7000,10.0.0.3:7000");
  // Retain and share cache/ha; optionally disconnect them during application shutdown.

  // Negative: null serverUrl is rejected up-front
  JRedisCluster<User> bad = new JRedisCluster<>((String) null);           // throws IllegalArgumentException

  // Negative: blank serverUrl is rejected (checkArgNotBlank)
  JRedisCluster<User> blank = new JRedisCluster<>("   ");                  // throws IllegalArgumentException
  ```
- `public JRedisCluster(final String serverUrl, final long timeout) throws IllegalArgumentException, RuntimeException` — Creates a new JRedisCluster instance with a specified timeout.
  - **Throws:**
    - `java.lang.IllegalArgumentException` — if `serverUrl` is `null`, empty, blank, or contains no valid server addresses, or if `timeout` is not positive or exceeds `Integer#MAX_VALUE`
    - `java.lang.RuntimeException` — if seed resolution, initial topology discovery, or client construction fails
    - `java.lang.IllegalStateException` — if the optional Kryo dependency is not on the classpath or cannot be initialized (checked after `serverUrl` is found non-blank and before the cluster client is built)
  ```java
  // Cluster cache with a 5 second timeout
  JRedisCluster<Data> cache = new JRedisCluster<>("10.0.0.1:7000,10.0.0.2:7000", 5000);
  String url = cache.serverUrl();                                          // returns the verbatim seed list

  // Use the cache
  Data data = new Data("value");
  cache.put("key", data, 7200000);                                        // Cache for 2 hours; returns true
  Data retrieved = cache.get("key");                                      // the cached Data, or null

  // Negative: a non-positive timeout is rejected (checkArgPositive)
  JRedisCluster<Data> zero = new JRedisCluster<>("10.0.0.1:7000", 0);     // throws IllegalArgumentException

  // Negative: a timeout above Integer.MAX_VALUE ms cannot fit the int Jedis API
  JRedisCluster<Data> tooBig = new JRedisCluster<>("10.0.0.1:7000", Integer.MAX_VALUE + 1L);   // throws IllegalArgumentException
  ```

#### Public Static Methods
- (none)

#### Public Instance Methods
##### flushAll(...) -> void
- **Signature:** `public void flushAll() throws IllegalStateException, JedisException`
- **Summary:** Requests removal of all keys from every primary in the Redis Cluster.
- **Contract:**
  - If one or more masters fail, Jedis raises a broadcast exception after attempting every master, so a single failing node does not prevent the others from being flushed.
  - If other applications share the cluster, their data is also deleted.
- **Parameters:**
  - (none)
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `redis.clients.jedis.exceptions.JedisException` — if flushing one or more cluster nodes fails (typically a `JedisBroadcastException` reporting the per-node outcomes)
- **See also:** `#disconnect()`
##### get(...) -> T
- **Signature:** `public T get(final String key) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Retrieves a value from the cache by its key.
- **Contract:**
  - If the key does not exist or has expired, `null` is returned.
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be retrieved. Must not be `null`.
- **Returns:** the cached value, or `null` if not found, expired, evicted, or stored as `null`
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null` or contains an unpaired UTF-16 surrogate
  - `java.lang.RuntimeException` — if acquiring a Redis connection or executing GET fails, Redis rejects the command, or the returned bytes cannot be decoded by the configured Kryo serializers
- **Performance:** The operation is O(1) time complexity.
- **Examples:**
  ```java
  User user = cache.get("user:123");                            // the cached User, or null if absent

  // Edge: a missing or expired key returns null (not an exception)
  User missing = cache.get("does:not:exist");                   // returns null

  // Negative: a null key is rejected
  User bad = cache.get(null);                                   // throws IllegalArgumentException

  // Negative: a counter key created by incr()/decr() cannot be deserialized by get()
  cache.incr("page:views");
  cache.get("page:views");                                      // throws KryoException (decode failure)
  ```
- **See also:** `#put(String, Object, long)`, `#remove(String)`
##### getBulk(...) -> Map<String, T>
- **Signature:** `public Map<String, T> getBulk(final String... keys) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Retrieves multiple values from the cache, returning a map of only the keys that were found.
- **Parameters:**
  - `keys` (`String...`) — the cache keys to retrieve; must not be `null` or contain `null` elements
- **Returns:** a map of the found key-value pairs, never `null` (empty if no keys are found)
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `keys` is `null`, contains a `null` element, or contains a key with an unpaired UTF-16 surrogate
  - `java.lang.RuntimeException` — if acquiring a Redis connection or executing GET fails, Redis rejects the command, or the returned bytes cannot be decoded by the configured Kryo serializers
- **Examples:**
  ```java
  // "user:1" exists, "user:2" is missing
  Map<String, User> found = cache.getBulk("user:1", "user:2");   // size == 1; contains only "user:1"

  // Edge: no arguments -> empty (never null) map; no GET is issued
  Map<String, User> none = cache.getBulk();                      // returns an empty map

  // Negative: a null element throws; no GET is issued for any key
  cache.getBulk("user:1", null, "user:3");                      // throws IllegalArgumentException
  ```
- **See also:** `#get(String)`, `#getBulk(Collection)`
- **Signature:** `public Map<String, T> getBulk(final Collection<String> keys) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Retrieves multiple values from the cache, returning a map of only the keys that were found.
- **Parameters:**
  - `keys` (`Collection<String>`) — the collection of cache keys to retrieve; must not be `null` or contain `null` elements
- **Returns:** a map of the found key-value pairs, never `null` (empty if no keys are found)
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `keys` is `null`, contains a `null` element, or contains a key with an unpaired UTF-16 surrogate
  - `java.lang.RuntimeException` — if acquiring a Redis connection or executing GET fails, Redis rejects the command, or the returned bytes cannot be decoded by the configured Kryo serializers
- **Examples:**
  ```java
  List<String> ids = List.of("user:1", "user:2", "user:3");
  Map<String, User> users = cache.getBulk(ids);                  // contains only the keys that exist

  // Edge: empty collection -> empty (never null) map; no GET is issued
  Map<String, User> none = cache.getBulk(List.of());            // returns an empty map

  // Negative: a null collection throws (validated before any network call)
  cache.getBulk((Collection<String>) null);                     // throws IllegalArgumentException
  ```
- **See also:** `#get(String)`, `#getBulk(String...)`
##### put(...) -> boolean
- **Signature:** `public boolean put(final String key, final T value, final long liveTime) throws IllegalStateException, IllegalArgumentException, RuntimeException, StackOverflowError`
- **Summary:** Stores a key-value pair in the cache with a specified time-to-live.
- **Contract:**
  - If the key already exists, its value and TTL will be replaced.
  - Redis-specific behavior: When `liveTime` is positive, this operation uses the Redis SET command with the `PX` option, which atomically sets both the value and a millisecond-precision expiration time — the requested `liveTime` is passed unchanged, with no client-side rounding to seconds.
  - When `liveTime` is 0 or negative, a plain SET command is used without expiration, meaning the key will persist until explicitly deleted.
  - If the key already exists, the previous value and TTL are completely replaced.
- **Parameters:**
  - `key` (`String`) — the cache key with which the specified value is to be associated. Must not be `null`.
  - `value` (`T`) — the value to cache. May be `null` (stored as empty byte array).
  - `liveTime` (`long`) — the time-to-live in milliseconds. Positive values are passed unchanged to SET... PX (subject to Redis's representable expiration range). 0 or negative means no expiration (plain SET).
- **Returns:** `true` when Redis acknowledges the write with "OK" (the normal success reply). An unconditional `SET` either replies "OK" or fails with a `JedisException`, so in practice this returns `true` on success and throws on failure rather than returning `false`
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null` or contains an unpaired UTF-16 surrogate
  - `java.lang.RuntimeException` — if serialization of `value` fails, a network error or timeout occurs, or Redis rejects `liveTime` as an out-of-range expiration
  - `java.lang.StackOverflowError` — if `value`'s object graph contains a cycle made only of collections, maps, or arrays (see the class documentation)
- **Examples:**
  ```java
  // Cache with 1 hour TTL (3600000 ms -> SET ... PX 3600000)
  boolean success = cache.put("user:123", user, 3600000);       // returns true when Redis replies "OK"

  // Edge: liveTime <= 0 means NO expiration -> plain SET (no PX)
  cache.put("forever", user, 0);                                // uses SET; key never auto-expires

  // Edge: a null value is allowed; it is stored as an empty byte array
  cache.put("empty:key", (User) null, 3600000);                // returns true; get(...) later returns null

  // Negative: a null key is rejected
  cache.put(null, user, 60000);                                // throws IllegalArgumentException
  ```
- **See also:** `#get(String)`, `#remove(String)`
##### remove(...) -> boolean
- **Signature:** `public boolean remove(final String key) throws IllegalStateException, IllegalArgumentException, JedisException`
- **Summary:** Removes the mapping for a key from the cache if it is present.
- **Contract:**
  - The return value reflects Redis's DEL semantics (which returns the number of keys removed): `true` when the key existed and was removed, `false` when the key did not exist at the time the command was issued.
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be removed. Must not be `null`.
- **Returns:** `true` if Redis reported at least one key was actually removed; `false` if the key did not exist at the time the `DEL` command was issued
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null` or contains an unpaired UTF-16 surrogate
  - `redis.clients.jedis.exceptions.JedisException` — if acquiring a Redis connection or executing DEL fails, including a timeout or a server response rejecting the command
- **Performance:** The operation is O(1) time complexity.
- **Examples:**
  ```java
  cache.put("user:123", user, 60000);                          // key now present
  boolean removed = cache.remove("user:123");                  // returns true (DEL count == 1)

  // Edge: deleting a key that does not exist returns false (DEL count == 0)
  boolean wasThere = cache.remove("never:set");                // returns false

  // Negative: a null key is rejected
  cache.remove(null);                                          // throws IllegalArgumentException
  ```
- **See also:** `#get(String)`, `#put(String, Object, long)`
##### incr(...) -> long
- **Signature:** `public long incr(final String key) throws IllegalStateException, IllegalArgumentException, JedisException`
- **Summary:** Atomically increments a numeric value by 1.
- **Contract:**
  - If the key doesn't exist, it will be automatically created with value 1 (Redis initializes to 0, then increments by 1).
  - If the key is not an integer or the result exceeds Redis's signed 64-bit range, an error occurs.
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be incremented. Must not be `null`.
- **Returns:** the value after increment (will be 1 if the key did not exist before)
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null` or contains an unpaired UTF-16 surrogate
  - `redis.clients.jedis.exceptions.JedisException` — if acquiring a Redis connection or executing the counter command fails, including a timeout, a server rejection, a non-integer value at `key`, or a result outside Redis's signed 64-bit integer range
- **Performance:** The operation is O(1) time complexity.
- **Examples:**
  ```java
  long pageViews = cache.incr("page:views");                   // returns 1 the first time, 2 the second, ...

  // Negative: a null key is rejected
  cache.incr(null);                                           // throws IllegalArgumentException

  // Negative: incrementing a non-integer value fails on the server
  cache.put("name", "Alice", 0);
  cache.incr("name");                                        // throws JedisDataException (a JedisException)
  ```
- **See also:** `#incr(String, long)`, `#decr(String)`
- **Signature:** `public long incr(final String key, final long delta) throws IllegalStateException, IllegalArgumentException, JedisException`
- **Summary:** Atomically increments a numeric value by a specified amount.
- **Contract:**
  - If the key doesn't exist, it will be automatically created with the delta value.
  - If the key is not an integer or the result exceeds Redis's signed 64-bit range, an error occurs.
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be incremented. Must not be `null`.
  - `delta` (`long`) — the increment amount; must be non-negative. Although the Redis INCRBY command itself supports negative deltas, this implementation rejects them with an `IllegalArgumentException` for portability across cache backends (e.g. SpyMemcached, which also rejects negative deltas).
- **Returns:** the value after increment (will be equal to delta if the key did not exist before)
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, or `delta` is negative
  - `redis.clients.jedis.exceptions.JedisException` — if acquiring a Redis connection or executing the counter command fails, including a timeout, a server rejection, a non-integer value at `key`, or a result outside Redis's signed 64-bit integer range
- **Performance:** The operation is O(1) time complexity.
- **Examples:**
  ```java
  long score = cache.incr("player:score:123", 10);            // returns 10 the first time

  // Edge: a delta of 0 returns the current value unchanged
  long current = cache.incr("player:score:123", 0);

  // Negative: a negative delta is rejected by this wrapper (for cross-backend portability)
  cache.incr("counter", -1);                                 // throws IllegalArgumentException
  ```
- **See also:** `#incr(String)`, `#decr(String, long)`
##### decr(...) -> long
- **Signature:** `public long decr(final String key) throws IllegalStateException, IllegalArgumentException, JedisException`
- **Summary:** Atomically decrements a numeric value by 1.
- **Contract:**
  - If the key doesn't exist, it will be automatically created with value -1.
  - If the key is not an integer or the result exceeds Redis's signed 64-bit range, an error occurs.
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be decremented. Must not be `null`.
- **Returns:** the value after decrement (can be negative in Redis, will be -1 if the key did not exist before)
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null` or contains an unpaired UTF-16 surrogate
  - `redis.clients.jedis.exceptions.JedisException` — if acquiring a Redis connection or executing the counter command fails, including a timeout, a server rejection, a non-integer value at `key`, or a result outside Redis's signed 64-bit integer range
- **Performance:** The operation is O(1) time complexity.
- **Examples:**
  ```java
  long stock = cache.decr("product:stock:123");                // value after decrement

  // Edge: a fresh key auto-initializes to 0 then -1 (Redis allows negatives)
  long first = cache.decr("brand:new:counter");               // returns -1 the first time

  // Negative: a null key is rejected
  cache.decr(null);                                          // throws IllegalArgumentException
  ```
- **See also:** `#decr(String, long)`, `#incr(String)`
- **Signature:** `public long decr(final String key, final long delta) throws IllegalStateException, IllegalArgumentException, JedisException`
- **Summary:** Atomically decrements a numeric value by a specified amount.
- **Contract:**
  - If the key doesn't exist, it will be automatically created with the negative delta value.
  - If the key is not an integer or the result exceeds Redis's signed 64-bit range, an error occurs.
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be decremented. Must not be `null`.
  - `delta` (`long`) — the decrement amount; must be non-negative. Although the Redis DECRBY command itself supports negative deltas, this implementation rejects them with an `IllegalArgumentException` for portability across cache backends (e.g. SpyMemcached, which also rejects negative deltas).
- **Returns:** the value after decrement (can be negative in Redis, will be equal to `-delta` if the key did not exist before)
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, or `delta` is negative
  - `redis.clients.jedis.exceptions.JedisException` — if acquiring a Redis connection or executing the counter command fails, including a timeout, a server rejection, a non-integer value at `key`, or a result outside Redis's signed 64-bit integer range
- **Performance:** The operation is O(1) time complexity.
- **Examples:**
  ```java
  long inventory = cache.decr("product:stock:456", 5);        // value after subtracting 5

  // Edge: a delta of 0 returns the current value unchanged
  long current = cache.decr("quota:abc", 0);

  // Negative: a negative delta is rejected by this wrapper
  cache.decr("counter", -1);                                 // throws IllegalArgumentException
  ```
- **See also:** `#decr(String)`, `#incr(String, long)`
##### disconnect(...) -> void
- **Signature:** `public final synchronized void disconnect() throws Error`
- **Summary:** Disconnects from all Redis instances and releases resources.
- **Contract:**
  - Thread Safety: This method is thread-safe, but once called, no other operations should be attempted on this client instance from any thread.
- **Parameters:**
  - (none)
- **Throws:**
  - `java.lang.Error` — if closing an underlying client raises an error; standalone shards are all attempted before the first error is rethrown
- **See also:** `#flushAll()`

### Class KryoTranscoder (com.landawn.abacus.cache.KryoTranscoder)
A Memcached `Transcoder` implementation that uses Kryo for serialization.

**Thread-safety:** thread-safe
**Nullability:** unspecified
**Implements:** `Transcoder<T>`

#### Public Constructors
- `public KryoTranscoder() throws IllegalStateException` — Creates a new `KryoTranscoder` configured with the default maximum size.
  - **Throws:**
    - `java.lang.IllegalStateException` — if the optional Kryo dependency is unavailable
  ```java
  // Create transcoder with the default max size
  KryoTranscoder<String> transcoder = new KryoTranscoder<>();
  int max = transcoder.getMaxSize();   // CachedData.MAX_SIZE

  // Encode then decode round-trips the value
  CachedData encoded = transcoder.encode("Alice");
  String decoded = transcoder.decode(encoded);   // "Alice" -- equal to the encoded value
  ```
- `public KryoTranscoder(final int maxSize) throws IllegalArgumentException, IllegalStateException` — Creates a new `KryoTranscoder` with the specified maximum size.
  - **Throws:**
    - `java.lang.IllegalArgumentException` — if `maxSize` is not positive
    - `java.lang.IllegalStateException` — if the optional Kryo dependency is unavailable
  ```java
  // Create transcoder with a 1MB max size
  KryoTranscoder<String> transcoder = new KryoTranscoder<>(1024 * 1024);
  int max = transcoder.getMaxSize();   // 1048576 -- the value passed in

  // A non-positive maxSize is rejected
  new KryoTranscoder<String>(0);    // throws IllegalArgumentException
  new KryoTranscoder<String>(-1);   // throws IllegalArgumentException

  // Use with a custom ConnectionFactory
  ConnectionFactory connFactory = new DefaultConnectionFactory() {
      @Override
      public Transcoder<Object> getDefaultTranscoder() {
          return new KryoTranscoder<>(512 * 1024);   // 512KB limit
      }
  };
  ```
- `public KryoTranscoder(final KryoParser kryoParser) throws IllegalArgumentException` — Creates a new `KryoTranscoder` with the default maximum size (`CachedData#MAX_SIZE`) and a caller-supplied `KryoParser`.
  - **Throws:**
    - `java.lang.IllegalArgumentException` — if `kryoParser` is `null`
- `public KryoTranscoder(final int maxSize, final KryoParser kryoParser) throws IllegalArgumentException` — Creates a new `KryoTranscoder` with the specified maximum size and a caller-supplied `KryoParser`.
  - **Throws:**
    - `java.lang.IllegalArgumentException` — if `maxSize` is not positive or `kryoParser` is `null`
  ```java
  KryoParser parser = ParserFactory.createKryoParser();   // configure / register classes as needed
  KryoTranscoder<String> transcoder = new KryoTranscoder<>(512 * 1024, parser);
  ```

#### Public Static Methods
- (none)

#### Public Instance Methods
##### asyncDecode(...) -> boolean
- **Signature:** `public boolean asyncDecode(final CachedData d)`
- **Summary:** Indicates whether this transcoder supports asynchronous decoding.
- **Contract:**
  - This method is called by SpyMemcached to determine whether the decode operation should be scheduled on a worker thread.
- **Parameters:**
  - `d` (`CachedData`) — the cached data whose asynchronous-decode capability is being queried (ignored; may be `null`)
- **Returns:** `false` always, indicating synchronous decoding only
- **Examples:**
  ```java
  KryoTranscoder<String> transcoder = new KryoTranscoder<>();
  CachedData data = transcoder.encode("Bob");

  // Async decoding is never supported, regardless of the argument
  boolean supportsAsync = transcoder.asyncDecode(data);   // false
  boolean forNull = transcoder.asyncDecode(null);         // false (the argument is ignored)

  // Decoding is therefore performed synchronously
  String decoded = transcoder.decode(data);   // "Bob"
  ```
##### encode(...) -> CachedData
- **Signature:** `public CachedData encode(final T o) throws RuntimeException, StackOverflowError, IllegalArgumentException`
- **Summary:** Encodes an object to cached data using Kryo serialization.
- **Contract:**
  - If the serialized payload exceeds the configured maximum size, an `IllegalArgumentException` is thrown.
  - The maximum size is enforced while writing the payload, so an oversized value need not be fully serialized or copied before rejection; the exception message therefore reports the size reached when serialization stopped, a lower bound of the full encoded size.
  - If a serializer catches the rejection and then fails in another way, that failure is attached to the `IllegalArgumentException` as a suppressed exception.
- **Parameters:**
  - `o` (`T`) — the object to encode and serialize (may be `null`)
- **Returns:** a `CachedData` containing the serialized bytes and metadata; never `null`
- **Throws:**
  - `java.lang.RuntimeException` — if a Kryo serializer cannot encode `o`; normally a `KryoException`, including for cycles through object fields and for a serializer that fails with an I/O cause (for example, Kryo's `JavaSerializer` meeting a non-serializable field)
  - `java.lang.StackOverflowError` — if the object graph contains a cycle made only of collections, maps, or arrays (their serializers do not wrap the overflow)
  - `java.lang.IllegalArgumentException` — if the serialized size exceeds the configured `maxSize`, or if the parser's Kryo registrations assign one registration ID to two different classes
- **Examples:**
  ```java
  KryoTranscoder<String> transcoder = new KryoTranscoder<>();

  // Encode a simple object
  CachedData cached = transcoder.encode("John");
  int flags = cached.getFlags();              // 0 (this transcoder never sets flags)
  byte[] serialized = cached.getData();       // non-empty Kryo payload (serialized.length > 0)
  String back = transcoder.decode(cached);    // "John" -- round-trips equal

  // Encode null is supported: a non-null CachedData is produced...
  CachedData nullData = transcoder.encode(null);   // never null
  String none = transcoder.decode(nullData);       // null -- decodes back to null

  // Encoding past the configured maxSize is rejected
  KryoTranscoder<String> tiny = new KryoTranscoder<>(8);
  tiny.encode("payload larger than 8 bytes once Kryo-encoded");   // throws IllegalArgumentException
  ```
- **See also:** `#decode(CachedData)`, `CachedData`
##### decode(...) -> T
- **Signature:** `public T decode(final CachedData d) throws RuntimeException`
- **Summary:** Decodes cached data back into an object using Kryo deserialization.
- **Contract:**
  - ⚠️ Class compatibility: The class definitions of the objects being deserialized must be available on the classpath.
  - If the class structure has changed between encoding and decoding (e.g., fields added or removed), deserialization may fail or produce unexpected results.
  - A type may encode successfully yet fail to decode if it lacks a no-argument constructor and no configured serializer provides its instantiation.
  - Custom serializers and class registrations must match those used to encode the entry.
- **Parameters:**
  - `d` (`CachedData`) — the cached data to decode and deserialize; if `null`, `null` is returned
- **Returns:** the deserialized object of type `T`, or `null` if `d` is `null`, its data is empty, or `null` was originally encoded
- **Throws:**
  - `java.lang.RuntimeException` — if the payload is corrupt, truncated, uses an unavailable or incompatible class, or was not written with compatible Kryo serializers (for example, a counter's ASCII digits); normally a `KryoException`. A bare `IncompatibleClassChangeError` or an `InstantiationError` raised while decoding is converted to a `KryoException` with that error as its cause: foreign bytes and an incompatibly changed application class can raise the same error, and its type cannot tell them apart. The other `IncompatibleClassChangeError` subclasses (such as `NoSuchMethodError`), which only an application class incompatibility raises, propagate unchanged.
  - `java.lang.IllegalArgumentException` — if the parser's Kryo registrations assign one registration ID to two different classes
- **Examples:**
  ```java
  KryoTranscoder<String> transcoder = new KryoTranscoder<>();

  // Basic encode-decode cycle
  CachedData cached = transcoder.encode("Alice");
  String decoded = transcoder.decode(cached);   // "Alice" -- equal to the encoded value

  // A null CachedData decodes to null
  String fromNull = transcoder.decode((CachedData) null);   // null

  // Zero-length data also decodes to null (the empty-data short-circuit)
  CachedData empty = new CachedData(0, new byte[0], 100);
  String fromEmpty = transcoder.decode(empty);   // null

  // A value that was encoded as null decodes back to null
  CachedData nullCached = transcoder.encode(null);
  String nullDecoded = transcoder.decode(nullCached);   // null
  ```
- **See also:** `#encode(Object)`, `CachedData#getData()`
##### getMaxSize(...) -> int
- **Signature:** `public int getMaxSize()`
- **Summary:** Returns the maximum allowed size, in bytes, for cached objects.
- **Parameters:**
  - (none)
- **Returns:** the maximum size, in bytes, for objects that can be cached
- **Examples:**
  ```java
  KryoTranscoder<String> transcoder = new KryoTranscoder<>(1024 * 1024);

  // Query the configured max size
  int maxSize = transcoder.getMaxSize();   // 1048576 (1MB) -- the value passed to the constructor

  // A default transcoder reports CachedData.MAX_SIZE
  int defaultMax = new KryoTranscoder<String>().getMaxSize();   // CachedData.MAX_SIZE

  // Compare transcoders
  KryoTranscoder<String> smallTranscoder = new KryoTranscoder<>(512 * 1024);
  KryoTranscoder<String> largeTranscoder = new KryoTranscoder<>(2 * 1024 * 1024);
  int small = smallTranscoder.getMaxSize();   // 524288 (512KB)
  int large = largeTranscoder.getMaxSize();   // 2097152 (2MB)
  int diff = large - small;                   // 1572864
  ```
- **See also:** `#encode(Object)`, `CachedData#MAX_SIZE`

### Class LocalCache (com.landawn.abacus.cache.LocalCache)
A thread-safe, in-memory cache implementation with automatic eviction and expiration support.

**Thread-safety:** thread-safe
**Nullability:** unspecified
**Extends:** `AbstractCache<K, V>`

#### Public Constructors
- `public LocalCache(final int capacity, final long evictDelay) throws IllegalArgumentException, RejectedExecutionException, IllegalStateException` — Creates a new LocalCache with specified capacity and eviction delay.
  - **Throws:**
    - `java.lang.IllegalArgumentException` — if capacity is not positive or evictDelay is negative
    - `java.util.concurrent.RejectedExecutionException` — if `evictDelay > 0` and the shared eviction executor rejects the periodic task
    - `java.lang.IllegalStateException` — if the JVM is already shutting down when the underlying pool registers its shutdown hook
  ```java
  // Create cache with 1000 max entries, check every 60 seconds for expired entries
  LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
  cache.stats().capacity();          // returns 1000
  User user = new User("John", 30);
  cache.put("user:123", user);       // returns true; uses 3h TTL, 30min idle by default

  // Retrieve the cached user
  User cached = cache.getOrNull("user:123");   // returns the stored user (not null)
  if (cached != null) {
      System.out.println("Found user: " + cached);   // prints "Found user: ..."
  }

  // Edge cases (validated by the constructor):
  new LocalCache<>(0, 60000);        // throws IllegalArgumentException (capacity must be positive)
  new LocalCache<>(1000, -1);        // throws IllegalArgumentException (evictDelay must be non-negative)
  ```
- `public LocalCache(final int capacity, final long evictDelay, final long defaultLiveTime, final long defaultMaxIdleTime) throws IllegalArgumentException, RejectedExecutionException, IllegalStateException` — Creates a new LocalCache with fully customized parameters.
  - **Throws:**
    - `java.lang.IllegalArgumentException` — if capacity is not positive or evictDelay is negative
    - `java.util.concurrent.RejectedExecutionException` — if `evictDelay > 0` and the shared eviction executor rejects the periodic task
    - `java.lang.IllegalStateException` — if the JVM is already shutting down when the underlying pool registers its shutdown hook
  ```java
  // Create cache with custom settings: 5000 capacity, 30s eviction check, 2h TTL, 30min idle
  LocalCache<String, Data> cache = new LocalCache<>(5000, 30000, 7200000, 1800000);
  cache.stats().capacity();          // returns 5000

  // Entry added without explicit times uses defaults (2h TTL, 30min idle)
  cache.put("data:1", data1);        // returns true

  // Override defaults for specific entry (10 second TTL, no idle timeout)
  cache.put("data:2", data2, 10000, 0);   // returns true

  // Edge cases (validated by the constructor):
  new LocalCache<>(0, 30000, 7200000, 1800000);    // throws IllegalArgumentException (capacity must be positive)
  new LocalCache<>(5000, -1, 7200000, 1800000);    // throws IllegalArgumentException (evictDelay must be non-negative)
  ```
- `public LocalCache(final long defaultLiveTime, final long defaultMaxIdleTime, final KeyedObjectPool<K, PoolableAdapter<V>> pool) throws IllegalArgumentException` — Creates a new LocalCache with a custom KeyedObjectPool.
  - **Throws:**
    - `java.lang.IllegalArgumentException` — if `pool` is `null`
  ```java
  // Create a custom pool with specific configuration
  KeyedObjectPool<String, PoolableAdapter<User>> customPool =
      PoolFactory.createKeyedObjectPool(1000, 60000);   // capacity 1000, 60s eviction delay

  // Create cache with 1 hour default TTL and 30 minute idle time
  LocalCache<String, User> cache = new LocalCache<>(3600000, 1800000, customPool);

  // Use the cache normally
  cache.put("user:123", user);                    // returns true
  User retrieved = cache.getOrNull("user:123");   // returns the stored user

  // Edge: a null pool is rejected.
  new LocalCache<>(3600000L, 1800000L, null);   // throws IllegalArgumentException (pool must not be null)
  ```

#### Public Static Methods
- (none)

#### Public Instance Methods
##### getOrNull(...) -> V
- **Signature:** `public V getOrNull(final K key) throws IllegalStateException, IllegalArgumentException`
- **Summary:** Retrieves a value from the cache by its key.
- **Contract:**
  - If the entry has expired (TTL exceeded or idle timeout reached) or been evicted, `null` will be returned; an expired entry is reclaimed immediately when accessed by this method, or otherwise during the next background eviction cycle (if `evictDelay > 0`).
- **Parameters:**
  - `key` (`K`) — the cache key whose associated value is to be returned (must not be null)
- **Returns:** the value associated with the specified key, or `null` if the key is not found, has expired, or has been evicted
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if `key` is `null`
- **Examples:**
  ```java
  LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
  cache.put("user:123", new User("John"));       // seed an entry; returns true

  // Retrieve from cache
  User user = cache.getOrNull("user:123");       // returns the stored User (and resets its idle timer)
  if (user != null) {
      System.out.println("Found: " + user.getName());   // prints "Found: John"
  } else {
      System.out.println("Cache miss - need to load from database");   // not reached here (entry present)
  }

  cache.getOrNull("absent");                     // returns null (key not present)
  cache.getOrNull(null);                         // throws IllegalArgumentException (key must not be null)
  ```
##### put(...) -> boolean
- **Signature:** `public boolean put(final K key, final V value, final long liveTime, final long maxIdleTime) throws IllegalStateException, IllegalArgumentException`
- **Summary:** Stores a key-value pair in the cache with custom expiration settings.
- **Contract:**
  - If the key already exists, its value and expiration settings will be replaced.
  - The entry will be evicted when either the TTL expires (time since creation) or the idle time is exceeded (time since last access, e.g.
  - If both are 0 or negative, the entry will only be evicted when the cache reaches capacity and needs to make room for new entries.
  - If the pool then rejects the new entry (for example a memory limit configured on a custom pool, or the entry expiring by the time the pool re-checks it under its lock), this method returns `false` and the previous mapping is no longer present.
  - By contrast, if the new entry is rejected up front because it is already expired at the pool's initial check (only possible with a millisecond-scale `liveTime` or `maxIdleTime`, both measured from the moment the entry is created), any previous mapping is retained.
- **Parameters:**
  - `key` (`K`) — the cache key with which the specified value is to be associated (must not be null)
  - `value` (`V`) — the cache value to be associated with the specified key (must not be null)
  - `liveTime` (`long`) — the time-to-live in milliseconds from entry creation (0 or negative for no TTL expiration)
  - `maxIdleTime` (`long`) — the maximum idle time in milliseconds since last access (0 or negative for no idle timeout)
- **Returns:** `true` if the entry was successfully stored; `false` if the underlying pool rejected the entry
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if `key` or `value` is `null`
- **Examples:**
  ```java
  LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
  User user = new User("John");

  // Store with 1 hour TTL and 30 minute idle timeout
  boolean success = cache.put("user:123", user, 3600000, 1800000);   // returns true
  if (success) {
      System.out.println("Entry cached successfully");   // prints this branch
  } else {
      // Not reached here. A full default pool evicts older entries to admit a new key, so false
      // means the pool rejected the entry (e.g. it had already expired), not "cache is full".
      System.out.println("Entry rejected by the pool");
  }

  // Temporary data with 5 second TTL, no idle timeout
  User tempUser = new User("Temp");
  cache.put("temp:data", tempUser, 5000, 0);     // returns true (maxIdleTime <= 0 means "no idle timeout")

  // Permanent entry (no TTL or idle timeout, evicted only when cache is full)
  User admin = new User("Admin");
  cache.put("user:admin", admin, 0, 0);          // returns true (both <= 0 means "no expiration")

  // A null value is rejected, consistent with the other in-memory caches (CaffeineCache, Ehcache, OffHeapCache).
  cache.put("nullable", (User) null, 1000, 1000);   // throws IllegalArgumentException (value must not be null)

  cache.put(null, user, 1000, 1000);             // throws IllegalArgumentException (key must not be null)
  ```
##### remove(...) -> void
- **Signature:** `public void remove(final K key) throws IllegalStateException, IllegalArgumentException`
- **Summary:** Removes the mapping for a key from the cache if it is present.
- **Contract:**
  - If the key is not found, the operation completes silently without throwing an exception.
- **Parameters:**
  - `key` (`K`) — the cache key whose mapping is to be removed from the cache (must not be null)
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if `key` is `null`
- **Examples:**
  ```java
  LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
  cache.put("user:123", user);                  // seed an entry; returns true

  // Remove the entry
  cache.remove("user:123");                     // entry gone afterwards
  User removed = cache.getOrNull("user:123");   // returns null
  cache.containsKey("user:123");                // returns false

  // Removing again is safe and has no effect
  cache.remove("user:123");                     // no error, silent no-op

  cache.remove(null);                           // throws IllegalArgumentException (key must not be null)
  ```
##### containsKey(...) -> boolean
- **Signature:** `public boolean containsKey(final K key) throws IllegalStateException, IllegalArgumentException`
- **Summary:** Checks if the cache contains a mapping for the specified key.
- **Contract:**
  - Returns `false` if the entry has expired or been evicted.
- **Parameters:**
  - `key` (`K`) — the cache key whose presence in the cache is to be tested (must not be null)
- **Returns:** `true` if the cache contains a mapping for the specified key and it has not expired; `false` otherwise
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if `key` is `null`
- **Examples:**
  ```java
  LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
  cache.put("user:123", user);                  // seed an entry; returns true

  cache.containsKey("user:123");                // returns true
  cache.containsKey("absent");                  // returns false

  if (cache.containsKey("user:123")) {
      System.out.println("User exists in cache");      // prints this branch
      User cachedUser = cache.getOrNull("user:123");   // returns the stored user
  } else {
      System.out.println("User not in cache - loading from database");   // not reached here (key present)
      User loadedUser = loadFromDatabase("user:123");
      cache.put("user:123", loadedUser);   // would re-cache on a miss
  }

  cache.containsKey(null);                      // throws IllegalArgumentException (key must not be null)
  ```
##### keySet(...) -> Set<K>
- **Signature:** `public Set<K> keySet() throws IllegalStateException`
- **Summary:** Returns a set of all keys currently in the cache.
- **Contract:**
  - Be aware that accessing values for returned keys may still return `null` if entries have expired between obtaining the key set and accessing the values.
- **Parameters:**
  - (none)
- **Returns:** a set containing all cache keys (including potentially expired entries that haven't been evicted yet)
- **Throws:**
  - `java.lang.IllegalStateException` — if the underlying pool has been closed
- **Examples:**
  ```java
  LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
  cache.put("user:1", user1);   // returns true
  cache.put("user:2", user2);   // returns true
  cache.put("user:3", user3);   // returns true

  // Get all keys and iterate
  Set<String> keys = cache.keySet();
  keys.size();                                                     // returns 3
  System.out.println("Cache contains " + keys.size() + " keys");   // prints "Cache contains 3 keys"
  for (String key : keys) {
      User user = cache.getOrNull(key);
      if (user != null) {                                                  // Entry may have expired
          System.out.println("Key: " + key + ", User: " + user.getName()); // prints one line per live entry
      }
  }

  // The set is a snapshot: later cache changes are not reflected in it.
  cache.put("user:4", user4);                   // returns true
  keys.size();                                  // still returns 3 (snapshot taken earlier)

  // Filter keys by pattern
  keys.stream()
      .filter(key -> key.startsWith("user:"))   // all 3 keys match
      .forEach(key -> System.out.println("User key: " + key));   // prints each "user:" key
  ```
##### size(...) -> int
- **Signature:** `public int size() throws IllegalStateException`
- **Summary:** Returns the current number of entries in the cache.
- **Parameters:**
  - (none)
- **Returns:** the number of entries currently in the cache (including potentially expired entries that haven't been evicted yet)
- **Throws:**
  - `java.lang.IllegalStateException` — if the underlying pool has been closed
- **Examples:**
  ```java
  LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
  cache.put("user:1", user1);   // returns true
  cache.put("user:2", user2);   // returns true
  cache.put("user:3", user3);   // returns true

  int count = cache.size();                                     // returns 3
  System.out.println("Cache contains " + count + " entries");   // prints "Cache contains 3 entries"

  // Check cache utilization
  CacheStats stats = cache.stats();
  stats.capacity();                                                                   // returns 1000
  double utilization = (double) cache.size() / stats.capacity() * 100;                // 0.3
  System.out.println("Cache utilization: " + String.format("%.2f%%", utilization));   // prints "Cache utilization: 0.30%"
  ```
##### clear(...) -> void
- **Signature:** `public void clear() throws IllegalStateException`
- **Summary:** Removes all entries from the cache immediately.
- **Parameters:**
  - (none)
- **Throws:**
  - `java.lang.IllegalStateException` — if the underlying pool has been closed
- **Examples:**
  ```java
  LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
  cache.put("user:1", user1);   // returns true
  cache.put("user:2", user2);   // returns true
  cache.put("user:3", user3);   // returns true

  cache.size();                                          // returns 3
  System.out.println("Before clear: " + cache.size());   // prints "Before clear: 3"
  cache.clear();                                         // removes all cached entries
  cache.size();                                          // returns 0
  System.out.println("After clear: " + cache.size());    // prints "After clear: 0"

  // Common use case: invalidate cache after data update
  updateDatabase();   // persist changes first (application-supplied helper)
  cache.clear();      // invalidate all cached users
  ```
##### stats(...) -> CacheStats
- **Signature:** `public CacheStats stats() throws IllegalStateException, IllegalArgumentException`
- **Summary:** Returns comprehensive statistics about cache performance and usage.
- **Contract:**
  - Key metrics like hit rate can help determine if the cache capacity and TTL settings are appropriate for your workload.
  - by its capacity or memory limit or because the entry had already expired, are not counted) getCount - Total number of get operations (hits + misses) hitCount - Number of successful cache hits (entry found and not expired) missCount - Number of cache misses (entry not found or expired) evictionCount - Number of entries evicted (due to capacity, TTL, or idle timeout) maxMemory - The pool's memory limit, or `-1` if none is configured (always `-1` for the default pool) dataSize - The pool's measured data size, or `-1` if the pool does not measure entries (always `-1` for the default pool) Thread Safety: This method is thread-safe and can be called concurrently.
- **Parameters:**
  - (none)
- **Returns:** a snapshot of current cache statistics including capacity, size, hit/miss counts, and eviction metrics
- **Throws:**
  - `java.lang.IllegalStateException` — if the underlying pool has been closed
  - `java.lang.IllegalArgumentException` — if the underlying pool cannot construct a valid statistics snapshot, for example because an operation counter overflowed or its memory accounting is inconsistent
- **Examples:**
  ```java
  LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
  cache.put("a", user);          // putCount becomes 1
  cache.getOrNull("a");          // hit; getCount/hitCount each become 1
  cache.getOrNull("missing");    // miss; getCount becomes 2, missCount becomes 1

  CacheStats stats = cache.stats();
  stats.capacity();              // returns 1000

  // Calculate and display hit rate
  double hitRate = stats.hitRate();                                            // 0.5
  System.out.println("Hit rate: " + String.format("%.2f%%", hitRate * 100));   // prints "Hit rate: 50.00%"

  // Display cache utilization
  System.out.println("Cache size: " + stats.size() + "/" + stats.capacity());   // prints "Cache size: 1/1000"
  double utilization = (double) stats.size() / stats.capacity() * 100;          // 0.1
  System.out.println("Utilization: " + String.format("%.2f%%", utilization));   // prints "Utilization: 0.10%"

  // Display operation counts
  System.out.println("Total get operations: " + stats.getCount());   // prints "Total get operations: 2"
  System.out.println("Total put operations: " + stats.putCount());   // prints "Total put operations: 1"
  System.out.println("Evictions: " + stats.evictionCount());         // prints "Evictions: 0"

  // Monitor cache effectiveness
  if (hitRate < 0.5) {                                                                       // false here (hitRate == 0.5)
      System.out.println("Warning: Low hit rate - consider increasing cache size or TTL");   // not reached here
  }

  // Note: calling stats() after close() throws IllegalStateException.
  ```
- **See also:** `CacheStats`, `PoolStats`
##### close(...) -> void
- **Signature:** `public synchronized void close()`
- **Summary:** Closes the cache and releases all associated resources.
- **Contract:**
  - Local caches are commonly application-scoped; call this method only when deliberately decommissioning one before process termination and no component will use it again.
  - ⚠️ Discarding without closing: that shutdown hook (and, when `evictDelay > 0`, the pool's scheduled eviction task) keeps the pool strongly reachable, so if a cache is merely dropped, its pool and every cached entry stay in memory until JVM exit.
  - Close every cache instance created dynamically (for example one per tenant or per reconfiguration) when it is retired.
- **Parameters:**
  - (none)
- **Examples:**
  ```java
  LocalCache<String, User> applicationCache = new LocalCache<>(1000, 60000);
  applicationCache.put("user:123", user);                  // returns true
  User cached = applicationCache.getOrNull("user:123");    // returns the stored user

  // Deliberately retire the application cache early, after all users have stopped.
  applicationCache.close();       // also stops and closes the owned pool
  applicationCache.isClosed();    // returns true

  // After close, query methods such as size()/keySet()/stats() throw IllegalStateException.
  applicationCache.size();        // throws IllegalStateException (pool is closed)

  applicationCache.close();       // idempotent no-op after the first call
  ```
##### isClosed(...) -> boolean
- **Signature:** `public boolean isClosed()`
- **Summary:** Checks whether the underlying pool has been closed, either through `#close()`, by the pool's own JVM shutdown hook during JVM shutdown, or directly by code that supplied a custom pool.
- **Contract:**
  - Use this method to verify whether the cache is still operational before performing operations, especially in long-running applications or when sharing cache instances across components.
  - Once closed, the cache cannot be reopened - a new instance must be created.
- **Parameters:**
  - (none)
- **Returns:** `true` if the underlying pool is closed; `false` otherwise
- **Examples:**
  ```java
  LocalCache<String, User> cache = new LocalCache<>(1000, 60000);
  cache.isClosed();              // returns false (freshly created)

  // Check before performing operations
  if (!cache.isClosed()) {
      cache.put("key", value);   // executed: returns true
  } else {
      System.out.println("Cache is closed, cannot perform operation");   // not reached here (cache open)
  }

  // Pattern for conditional cache usage
  String userId = "user:123";
  User user = new User();
  if (!cache.isClosed()) {
      cache.put(userId, user);   // store only while the cache is open
  } else {
      System.out.println("Cache unavailable, skipping caching");   // taken once the cache is closed
  }

  // Verify state after deliberate early decommissioning
  cache.close();   // explicitly retires the application cache (idempotent)
  System.out.println("Cache closed: " + cache.isClosed());   // prints "Cache closed: true"
  ```

### Class OffHeapCache (com.landawn.abacus.cache.OffHeapCache)
A high-performance off-heap cache implementation using sun.misc.Unsafe for direct memory management.

**Thread-safety:** unspecified
**Nullability:** unspecified
**Extends:** `AbstractCache<K, V>`

#### Public Constructors
- (none)

#### Public Static Methods
##### builder(...) -> Builder<K, V>
- **Signature:** `public static <K, V> Builder<K, V> builder()`
- **Summary:** Creates a new builder for constructing `OffHeapCache` instances.
- **Parameters:**
  - (none)
- **Returns:** a new `Builder` instance initialized with default configuration values
- **Examples:**
  ```java
  // Each call returns a fresh Builder pre-populated with default values
  OffHeapCache.Builder<String, Data> b = OffHeapCache.builder(); // never null; a new instance each call

  // Basic configuration
  OffHeapCache<String, Data> cache = OffHeapCache.<String, Data>builder()
      .capacityInMB(100)             // required; must be positive or build() throws IAE
      .evictDelay(60000)             // scan for expired entries every 60s
      .defaultLiveTime(3600000)      // 1h TTL
      .defaultMaxIdleTime(1800000)   // 30min idle timeout
      .build();                      // returns a ready-to-use OffHeapCache (native memory allocated)

  // Advanced configuration with disk spillover
  // OffHeapStore is an interface; supply your own implementation (e.g., a file-backed store).
  OffHeapStore<String> diskStore = myFileBackedOffHeapStore(Paths.get("/tmp/cache"));
  OffHeapCache<String, byte[]> advancedCache = OffHeapCache.<String, byte[]>builder()
      .capacityInMB(200)
      .maxBlockSizeInBytes(16384)    // values larger than this are split across multiple slots
      .evictDelay(30000)
      .vacatingFactor(0.3f)          // evict 30% of entries (LRU first) under memory pressure
      .offHeapStore(diskStore)       // spill overflow entries to disk
      .statsTimeOnDisk(true)
      .build();                      // returns an application-scoped OffHeapCache
  ```
- **See also:** `Builder`, `Builder#build()`

#### Public Instance Methods
##### getOrNull(...) -> V
- **Signature:** `public V getOrNull(final K key) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Retrieves the value associated with the specified key, or `null` if no live mapping exists.
- **Contract:**
  - For a disk-backed entry, the serialized bytes are fetched from the `OffHeapStore` and the entry may be promoted back into memory when the configured promotion predicate accepts it.
  - A read that found live bytes can still return its value if the entry expires while deserialization or promotion is running.
- **Parameters:**
  - `key` (`K`) — the key whose associated value is to be returned; must not be `null`
- **Returns:** the cached value, or `null` if the key is not present, the entry has expired, the disk-backed entry's bytes are missing from the store, or the entry found by the lookup was removed or replaced (by a same-key put or a promotion back into memory) concurrently, even if the key stayed mapped throughout
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed, or if a value cannot be reconstructed because the fetched size no longer matches the recorded size (data corruption), or because a configured deserializer returned `null`
  - `java.lang.IllegalArgumentException` — if `key` is `null`
  - `java.lang.RuntimeException` — if the backing-store read, value deserialization, promotion predicate, or native-memory copy throws an unchecked exception
##### put(...) -> boolean
- **Signature:** `public boolean put(final K key, final V value, final long liveTime, final long maxIdleTime) throws IllegalStateException, IllegalArgumentException, RuntimeException, StackOverflowError, RejectedExecutionException`
- **Summary:** Stores a key-value pair with the specified expiration settings.
- **Contract:**
  - The value is serialized and placed in off-heap memory; when memory is unavailable (or the `storeSelector` directs it), the value is written to the configured `OffHeapStore` instead.
  - Values that could not be placed even in an otherwise empty cache — because the serialized form exceeds the entire off-heap capacity, or because its slot-rounded chunks need more 1 MB segments than the region has — never attempt the in-memory placement (no amount of vacating could make them fit) and go straight to the disk fallback, if any; such a doomed put schedules no vacate pass and leaves the in-memory cache contents untouched.
  - Key requirements: keys must implement `hashCode`, `equals`, and, for a `Comparable` key, `compareTo` consistently and without throwing.
  - The internal index also calls them while serving other keys' operations (`compareTo` when a bin of colliding keys is converted to a tree), so such a failure can surface from a put of another key.
  - If that removal fails (the key's lookup fails again, or the store's `remove` throws), the bytes, unreachable through the cache, are removed by a later `#clear()` or by `#close()` (if the store's `remove` fails during `close()`, that failure is logged and the bytes stay).
  - Until then the cache keeps a reference to each such key, and logs a warning when their number first reaches 1,024 and again each time it doubles; the count restarts once a `clear()` has removed them all.
- **Parameters:**
  - `key` (`K`) — the key; must not be `null`
  - `value` (`V`) — the value; must not be `null`
  - `liveTime` (`long`) — the TTL in milliseconds; `<= 0` means no expiration
  - `maxIdleTime` (`long`) — the maximum idle time in milliseconds; `<= 0` means no idle limit
- **Returns:** `true` if the value was stored (in memory or on disk); `false` otherwise
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed, or memory pressure triggers submission of a vacate task after the shared async executor has been shut down
  - `java.lang.IllegalArgumentException` — if `key` or `value` is `null`, or if `storeSelector` returns `null` or a value outside 0..2
  - `java.lang.RuntimeException` — if serialization, the store selector, the native-memory copy, or the backing-store write throws an unchecked exception, or if the key's `hashCode`, `equals`, or `compareTo` (or a colliding key's `compareTo`) throws one while the value is being mapped
  - `java.lang.StackOverflowError` — if the default Kryo serializer encounters a cycle made only of collections, maps, or arrays in `value`; the cache is left unchanged
  - `java.util.concurrent.RejectedExecutionException` — if the put fails under memory pressure and the shared executor rejects the vacate task
##### remove(...) -> void
- **Signature:** `public void remove(final K key) throws IllegalStateException, IllegalArgumentException`
- **Summary:** Removes the cache entry associated with the specified key, if present, releasing its memory slots or disk bytes.
- **Contract:**
  - The mapping is removed even if the `OffHeapStore` fails to remove the disk bytes; such a failure is logged rather than propagated.
- **Parameters:**
  - `key` (`K`) — the key whose mapping is to be removed; must not be `null`
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if `key` is `null`
##### containsKey(...) -> boolean
- **Signature:** `public boolean containsKey(final K key) throws IllegalStateException, IllegalArgumentException`
- **Summary:** Returns whether the cache contains a live (non-expired) mapping for the specified key.
- **Contract:**
  - Like `#getOrNull(Object)`, it can also report `false` when the entry it finds is being replaced concurrently (by a same-key put or a promotion back into memory), even though the key stays mapped throughout.
- **Parameters:**
  - `key` (`K`) — the key to test; must not be `null`
- **Returns:** `true` if a live mapping exists
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if `key` is `null`
##### keySet(...) -> Set<K>
- **Signature:** `public Set<K> keySet() throws IllegalStateException`
- **Summary:** Returns a snapshot of all keys currently held in the cache, including keys of entries spilled to disk.
- **Parameters:**
  - (none)
- **Returns:** a snapshot of the cache keys; never `null`
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
##### size(...) -> int
- **Signature:** `public int size() throws IllegalStateException`
- **Summary:** Returns the number of entries currently held in the cache (including disk-backed entries, and entries that have expired but not yet been swept).
- **Parameters:**
  - (none)
- **Returns:** the current entry count
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
##### clear(...) -> void
- **Signature:** `public void clear() throws IllegalStateException`
- **Summary:** Removes all entries, releasing their memory slots and disk bytes, and reclaims the now-empty segments for reuse by any slot size.
- **Contract:**
  - The retry runs even when removing the entries throws; that failure is then rethrown, with any retry failure attached to it as suppressed.
- **Parameters:**
  - (none)
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
##### isClosed(...) -> boolean
- **Signature:** `public boolean isClosed()`
- **Summary:** Returns whether this cache has been closed.
- **Parameters:**
  - (none)
- **Returns:** `true` if the cache is closed
##### close(...) -> void
- **Signature:** `public void close() throws IllegalStateException, RuntimeException`
- **Summary:** Deliberately decommissions this application-scoped cache before JVM termination.
- **Contract:**
  - It cancels the maintenance task, removes the JVM shutdown hook, frees every entry (removing disk-backed entries' bytes from the store, along with any bytes a failed disk put left behind), deallocates the native region, and closes the configured `OffHeapStore`, if any.
  - If the cache remains active for the application lifetime, the registered shutdown hook invokes this method automatically during JVM shutdown.
  - Every entry and cleanup step is attempted even when an earlier one fails.
  - The entry index is cleared even if an entry cannot be released.
  - Reentrant callback restriction: a serializer, store selector, or other callback invoked from an operation holding this cache's lifecycle read lock must not close the cache synchronously (a read-to-write lock upgrade is unsupported); such a call fails immediately with `IllegalStateException`.
- **Parameters:**
  - (none)
- **Throws:**
  - `java.lang.IllegalStateException` — if the current thread attempts to close the cache while it is executing a cache operation or callback
  - `java.lang.RuntimeException` — if canceling maintenance, removing the shutdown hook, releasing an entry, deallocating native memory, or closing the configured store throws an unchecked exception; later distinct cleanup failures are suppressed on the first failure
##### stats(...) -> OffHeapCacheStats
- **Signature:** `public OffHeapCacheStats stats() throws IllegalStateException, IllegalArgumentException`
- **Summary:** Returns a sampled, point-in-time snapshot of cache statistics: entry counts, hit/miss and eviction counters, memory and disk usage, disk I/O timing aggregates, and per-segment slot occupancy.
- **Parameters:**
  - (none)
- **Returns:** the statistics snapshot; never `null`
- **Throws:**
  - `java.lang.IllegalStateException` — if the cache has been closed
  - `java.lang.IllegalArgumentException` — if an accumulated counter has overflowed into a negative value, or an overflowed disk-timing accumulator produces invalid timing statistics

### Class Builder (com.landawn.abacus.cache.OffHeapCache.Builder)
Builder class for creating OffHeapCache instances with custom configuration.

**Thread-safety:** thread-safe
**Nullability:** unspecified

#### Public Constructors
- `public Builder()` — Creates a new builder instance with default configuration values.
  ```java
  // Prefer the factory; OffHeapCache.builder() simply returns new Builder()
  OffHeapCache.Builder<String, Data> builder = OffHeapCache.builder(); // never null

  // Setters are fluent (chain = true): each returns this same builder instance
  builder.capacityInMB(100)      // required; positive
         .evictDelay(60000)
         .defaultLiveTime(3600000); // returns the same builder reference

  OffHeapCache<String, Data> cache = builder.build(); // allocates native memory for an application-scoped cache
  ```

#### Public Static Methods
- (none)

#### Public Instance Methods
##### serializer(...) -> varies by overload
- **Signature:** `public Builder<K, V> serializer(final BiConsumer<? super V, ByteArrayOutputStream> serializer) throws IllegalArgumentException`
- **Summary:** Sets the serializer used to convert values to byte streams.
- **Parameters:**
  - `serializer` (`BiConsumer<? super V, ByteArrayOutputStream>`) — the serializer to use
- **Returns:** this builder
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `serializer` is `null`
- **Signature:** `public BiConsumer<? super V, ByteArrayOutputStream> serializer()`
- **Summary:** Custom serializer for converting values to byte streams.
- **Contract:**
  - The serializer receives the value and a ByteArrayOutputStream, and should write the complete serialized form to the stream.
  - Default: `null` (uses Kryo if available, otherwise JSON)
- **Parameters:**
  - (none)
- **Returns:** Custom serializer for converting values to byte streams.
##### deserializer(...) -> varies by overload
- **Signature:** `public Builder<K, V> deserializer(final BiFunction<byte[], Type<V>, ? extends V> deserializer) throws IllegalArgumentException`
- **Summary:** Sets the deserializer used to reconstruct values from byte arrays.
- **Parameters:**
  - `deserializer` (`BiFunction<byte[], Type<V>, ? extends V>`) — the deserializer to use
- **Returns:** this builder
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `deserializer` is `null`
- **Signature:** `public BiFunction<byte[], Type<V>, ? extends V> deserializer()`
- **Summary:** Custom deserializer for converting byte arrays back to values.
- **Contract:**
  - The function receives the byte array and type information, and should return the deserialized value instance.
  - When a disk read may be promoted to off-heap memory, the cache preserves a separate pristine copy for that promotion.
  - Default: `null` (uses Kryo if available, otherwise JSON)
- **Parameters:**
  - (none)
- **Returns:** Custom deserializer for converting byte arrays back to values.
##### testerForLoadingItemFromDiskToMemory(...) -> varies by overload
- **Signature:** `public Builder<K, V> testerForLoadingItemFromDiskToMemory(final TriPredicate<ActivityPrint, Integer, Long> testerForLoadingItemFromDiskToMemory) throws IllegalArgumentException`
- **Summary:** Sets the predicate used to decide whether an item should be loaded from disk to memory.
- **Parameters:**
  - `testerForLoadingItemFromDiskToMemory` (`TriPredicate<ActivityPrint, Integer, Long>`) — the predicate to use
- **Returns:** this builder
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `testerForLoadingItemFromDiskToMemory` is `null`
- **Signature:** `public TriPredicate<ActivityPrint, Integer, Long> testerForLoadingItemFromDiskToMemory()`
- **Summary:** Predicate to determine when to load items from disk back to memory based on access patterns.
- **Contract:**
  - Only applies when `#offHeapStore` is configured.
- **Parameters:**
  - (none)
- **Returns:** Predicate to determine when to load items from disk back to memory based on access patterns.
##### storeSelector(...) -> varies by overload
- **Signature:** `public Builder<K, V> storeSelector(final TriFunction<K, V, Integer, Integer> storeSelector) throws IllegalArgumentException`
- **Summary:** Sets the function used to choose the storage location for a put operation.
- **Parameters:**
  - `storeSelector` (`TriFunction<K, V, Integer, Integer>`) — the storage selector to use
- **Returns:** this builder
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `storeSelector` is `null`
- **Signature:** `public TriFunction<K, V, Integer, Integer> storeSelector()`
- **Summary:** Function to determine storage location for each put operation.
- **Contract:**
  - The function receives the key, value, and size (in bytes), and should return: 0 - default routing (try memory first, fallback to disk if memory unavailable) 1 - memory only (never store to disk, operation fails if memory unavailable) 2 - disk only (always store to disk via `#offHeapStore`, skip memory) The selector is evaluated for every put.
- **Parameters:**
  - (none)
- **Returns:** Function to determine storage location for each put operation.
##### build(...) -> OffHeapCache<K, V>
- **Signature:** `public OffHeapCache<K, V> build() throws IllegalArgumentException, OutOfMemoryError, RejectedExecutionException, IllegalStateException`
- **Summary:** Builds and returns a new `OffHeapCache` with the current builder settings.
- **Contract:**
  - If `maxBlockSizeInBytes` is `0`, the default block size (8192) is applied.
- **Parameters:**
  - (none)
- **Returns:** a new `OffHeapCache` instance configured with the builder settings
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `capacityInMB` is not positive, if `maxBlockSizeInBytes` is non-zero and outside \[1024, 1048576\] (a value of 0 is replaced with the default 8192), or if `vacatingFactor` is NaN or outside \[0.0, 1.0\]
  - `java.lang.OutOfMemoryError` — if native memory allocation fails, or if the Java heap cannot hold the cache's per-megabyte segment bookkeeping (possible for a very large `capacityInMB`)
  - `java.util.concurrent.RejectedExecutionException` — if `evictDelay` is positive and the maintenance scheduler rejects its task
  - `java.lang.IllegalStateException` — if the JVM is already shutting down when the cache registers its shutdown hook
- **Examples:**
  ```java
  // Basic cache with required fields
  OffHeapCache<String, Data> cache = OffHeapCache.<String, Data>builder()
      .capacityInMB(200)
      .evictDelay(60000)
      .vacatingFactor(0.3f)
      .build();                              // application-scoped cache; native memory now allocated
  cache.put("key", data);                    // returns true once the value is stored
  Data retrieved = cache.getOrNull("key");   // returns the stored value, or null if absent

  // maxBlockSizeInBytes(0) is replaced with the default 8192 (does NOT throw)
  OffHeapCache<String, Data> defaultBlock = OffHeapCache.<String, Data>builder()
      .capacityInMB(100)
      .maxBlockSizeInBytes(0)        // 0 -> default block size 8192
      .build();                      // returns an application-scoped cache

  // Invalid configuration is rejected by build()
  OffHeapCache.<String, Data>builder().build();                                          // throws IllegalArgumentException (capacityInMB not positive)
  OffHeapCache.<String, Data>builder().capacityInMB(8).maxBlockSizeInBytes(512).build(); // throws IllegalArgumentException (non-zero, < 1024)

  // Advanced cache with disk spillover
  // OffHeapStore is an interface; supply your own implementation (e.g., a file-backed store).
  OffHeapStore<String> store = myFileBackedOffHeapStore(Paths.get("/tmp/cache"));
  OffHeapCache<String, Data> advancedCache = OffHeapCache.<String, Data>builder()
      .capacityInMB(100)
      .maxBlockSizeInBytes(16384)
      .evictDelay(30000)
      .defaultLiveTime(3600000)
      .defaultMaxIdleTime(1800000)
      .vacatingFactor(0.25f)
      .offHeapStore(store)
      .statsTimeOnDisk(true)
      .build();                      // returns an application-scoped cache backed by memory + disk spillover
  ```
##### capacityInMB(...) -> varies by overload
- **Signature:** `public int capacityInMB()`
- **Summary:** The total off-heap memory capacity in megabytes.
- **Contract:**
  - This is a required field and must be set to a positive value before calling `#build()`.
  - Default: 0 (must be set to a positive value)
- **Parameters:**
  - (none)
- **Returns:** The total off-heap memory capacity in megabytes.
- **Signature:** `public Builder capacityInMB(int capacityInMB)`
- **Summary:** The total off-heap memory capacity in megabytes.
- **Contract:**
  - This is a required field and must be set to a positive value before calling `#build()`.
  - Default: 0 (must be set to a positive value)
- **Parameters:**
  - `capacityInMB` (`int`) — The total off-heap memory capacity in megabytes.
- **Returns:** unspecified
##### maxBlockSizeInBytes(...) -> varies by overload
- **Signature:** `public int maxBlockSizeInBytes()`
- **Summary:** Maximum size of a single memory block in bytes.
- **Contract:**
  - Must be `0` (replaced with the default 8192 by `#build()`) or between 1024 and `SEGMENT_SIZE` (1,048,576), and is rounded up to the nearest multiple of `MIN_BLOCK_SIZE` (64 bytes).
  - Larger blocks reduce the number of slots needed for large values; each segment is dedicated to a single slot size and may have an unused tail when that size does not divide it.
- **Parameters:**
  - (none)
- **Returns:** Maximum size of a single memory block in bytes.
- **Signature:** `public Builder maxBlockSizeInBytes(int maxBlockSizeInBytes)`
- **Summary:** Maximum size of a single memory block in bytes.
- **Contract:**
  - Must be `0` (replaced with the default 8192 by `#build()`) or between 1024 and `SEGMENT_SIZE` (1,048,576), and is rounded up to the nearest multiple of `MIN_BLOCK_SIZE` (64 bytes).
  - Larger blocks reduce the number of slots needed for large values; each segment is dedicated to a single slot size and may have an unused tail when that size does not divide it.
- **Parameters:**
  - `maxBlockSizeInBytes` (`int`) — Maximum size of a single memory block in bytes.
- **Returns:** unspecified
##### evictDelay(...) -> varies by overload
- **Signature:** `public long evictDelay()`
- **Summary:** Delay between eviction runs in milliseconds.
- **Parameters:**
  - (none)
- **Returns:** Delay between eviction runs in milliseconds.
- **Signature:** `public Builder evictDelay(long evictDelay)`
- **Summary:** Delay between eviction runs in milliseconds.
- **Parameters:**
  - `evictDelay` (`long`) — Delay between eviction runs in milliseconds.
- **Returns:** unspecified
##### defaultLiveTime(...) -> varies by overload
- **Signature:** `public long defaultLiveTime()`
- **Summary:** Default time-to-live for cache entries in milliseconds.
- **Parameters:**
  - (none)
- **Returns:** Default time-to-live for cache entries in milliseconds.
- **Signature:** `public Builder defaultLiveTime(long defaultLiveTime)`
- **Summary:** Default time-to-live for cache entries in milliseconds.
- **Parameters:**
  - `defaultLiveTime` (`long`) — Default time-to-live for cache entries in milliseconds.
- **Returns:** unspecified
##### defaultMaxIdleTime(...) -> varies by overload
- **Signature:** `public long defaultMaxIdleTime()`
- **Summary:** Default maximum idle time for cache entries in milliseconds.
- **Parameters:**
  - (none)
- **Returns:** Default maximum idle time for cache entries in milliseconds.
- **Signature:** `public Builder defaultMaxIdleTime(long defaultMaxIdleTime)`
- **Summary:** Default maximum idle time for cache entries in milliseconds.
- **Parameters:**
  - `defaultMaxIdleTime` (`long`) — Default maximum idle time for cache entries in milliseconds.
- **Returns:** unspecified
##### vacatingFactor(...) -> varies by overload
- **Signature:** `public float vacatingFactor()`
- **Summary:** Factor (0.0-1.0) controlling how aggressive a vacate is.
- **Contract:**
  - Vacate is triggered when off-heap memory cannot satisfy a put and no disk store absorbs the value; this fraction of the memory-resident entries (at least one) is then evicted (LRU first) to free space.
  - It does NOT control when vacating starts.
- **Parameters:**
  - (none)
- **Returns:** Factor (0.0-1.0) controlling how aggressive a vacate is.
- **Signature:** `public Builder vacatingFactor(float vacatingFactor)`
- **Summary:** Factor (0.0-1.0) controlling how aggressive a vacate is.
- **Contract:**
  - Vacate is triggered when off-heap memory cannot satisfy a put and no disk store absorbs the value; this fraction of the memory-resident entries (at least one) is then evicted (LRU first) to free space.
  - It does NOT control when vacating starts.
- **Parameters:**
  - `vacatingFactor` (`float`) — Factor (0.0-1.0) controlling how aggressive a vacate is.
- **Returns:** unspecified
##### offHeapStore(...) -> varies by overload
- **Signature:** `public OffHeapStore<K> offHeapStore()`
- **Summary:** Optional disk store for spillover when memory is full.
- **Contract:**
  - When configured, values that don't fit in memory can be stored to disk instead of being rejected.
- **Parameters:**
  - (none)
- **Returns:** Optional disk store for spillover when memory is full.
- **Signature:** `public Builder offHeapStore(OffHeapStore<K> offHeapStore)`
- **Summary:** Optional disk store for spillover when memory is full.
- **Contract:**
  - When configured, values that don't fit in memory can be stored to disk instead of being rejected.
- **Parameters:**
  - `offHeapStore` (`OffHeapStore<K>`) — Optional disk store for spillover when memory is full.
- **Returns:** unspecified
##### statsTimeOnDisk(...) -> varies by overload
- **Signature:** `public boolean statsTimeOnDisk()`
- **Summary:** Whether to collect detailed disk I/O timing statistics.
- **Contract:**
  - When `true`, tracks min/max/average read and write times to disk.
  - Only applies when `#offHeapStore` is configured.
- **Parameters:**
  - (none)
- **Returns:** Whether to collect detailed disk I/O timing statistics.
- **Signature:** `public Builder statsTimeOnDisk(boolean statsTimeOnDisk)`
- **Summary:** Whether to collect detailed disk I/O timing statistics.
- **Contract:**
  - When `true`, tracks min/max/average read and write times to disk.
  - Only applies when `#offHeapStore` is configured.
- **Parameters:**
  - `statsTimeOnDisk` (`boolean`) — Whether to collect detailed disk I/O timing statistics.
- **Returns:** unspecified
##### equals(...) -> boolean
- **Signature:** `public boolean equals(Object other)`
- **Parameters:**
  - `other` (`Object`)
- **Returns:** unspecified
##### hashCode(...) -> int
- **Signature:** `public int hashCode()`
- **Parameters:**
  - (none)
- **Returns:** unspecified
##### toString(...) -> String
- **Signature:** `public String toString()`
- **Parameters:**
  - (none)
- **Returns:** unspecified

### Record OffHeapCacheStats (com.landawn.abacus.cache.OffHeapCacheStats)
An immutable snapshot of off-heap cache statistics at a specific point in time.

**Thread-safety:** unspecified
**Nullability:** unspecified

#### Record Components
- `capacity` (`int`) - a theoretical upper bound on the number of memory-resident entries, derived from the off-heap memory budget as `allocatedMemory / MIN_BLOCK_SIZE` (currently `allocatedMemory / 64`), capped at `Integer#MAX_VALUE`: every such entry occupies at least one 64-byte slot. Entries above 64 bytes need larger or additional slots, so off-heap memory typically fills long before the memory-resident count (`size - sizeOnDisk`) approaches this value. It does not bound `size`, which also counts disk-spilled entries.
- `size` (`int`) - the current number of entries in the cache, counting both memory-resident and disk-spilled entries. To compute the memory-only entry count, use `size - sizeOnDisk`.
- `sizeOnDisk` (`long`) - the current number of entries whose value bytes are stored on disk via the configured `OffHeapStore`. These entries are still counted in `size`; only their payloads live on disk.
- `putCount` (`long`) - the total number of successful put operations since cache creation, counting values stored in off-heap memory and values written to the disk store. A put that throws after its value was already mapped is counted. A put that fails (e.g., neither memory nor disk could accept the value) is not counted; internal disk-to-memory promotions are not counted either.
- `putCountToDisk` (`long`) - the number of put operations that resulted in writing data to disk. This occurs when off-heap memory is full, or the value can never fit in off-heap memory, and the value is stored to disk via the configured `OffHeapStore`, or when the `storeSelector` explicitly routes the value to disk. It is a subset of `putCount`.
- `getCount` (`long`) - the total number of lookups classified as a hit or miss since cache creation. Failures before that classification (such as a throwing store read or a corrupt payload length) are not counted. Conceptually, `getCount = hitCount + missCount`; the sum saturates at `Long#MAX_VALUE` if the two counters would overflow.
- `hitCount` (`long`) - the number of get operations that found a live entry and its bytes. A get that finds a disk-backed entry whose backing bytes are confirmed missing counts as a miss, not a hit. To approximate hits served purely from off-heap memory, subtract `hitCountFromDisk` from `hitCount`. A get that found live bytes but then threw during deserialization remains counted here because exceptional reconstruction is not tracked separately.
- `hitCountFromDisk` (`long`) - the number of get operations whose value was successfully reconstructed from bytes read from the configured `OffHeapStore`. It normally forms a subset of `hitCount`, but independently sampled counters can be transiently inconsistent.
- `missCount` (`long`) - the number of failed get operations where the entry was not found in either memory or disk. This can occur when the key never existed, was explicitly removed, has expired, or still had a cache mapping whose backing-store bytes were missing. A lookup that finds an entry just as a concurrent operation removes or replaces it also counts as a miss.
- `evictionCount` (`long`) - the total number of entries removed because they expired (lazily on access or by the periodic maintenance sweep) or were reclaimed by the memory-pressure vacate pass. Explicit `remove()` / `clear()` calls and `put()` replacements are NOT counted here.
- `evictionCountFromDisk` (`long`) - the number of disk-stored entries removed because they expired (lazily on access or by the maintenance sweep). The vacate pass evicts only memory-resident entries, so it never contributes here. Explicit `remove()` / `clear()` and `put()` replacements of a disk-stored key are NOT counted here.
- `allocatedMemory` (`long`) - the total allocated off-heap memory in bytes. This is the fixed amount of native memory reserved for the cache at creation, organized into fixed-size segments.
- `occupiedMemory` (`long`) - the currently occupied off-heap slot space in bytes. This includes serialized data plus slot-rounding/alignment padding, but not the heap-resident entry metadata.
- `dataSize` (`long`) - the total size of actual serialized data tracked by the cache in bytes, across both the off-heap memory pool and the disk store, excluding any slot-allocation padding or internal overhead. To isolate the in-memory portion, subtract `dataSizeOnDisk` from `dataSize`.
- `dataSizeOnDisk` (`long`) - the total size of serialized data currently stored on disk in bytes. This is a subset of `dataSize` and counts only entries that have been persisted to disk storage.
- `writeToDiskTimeStats` (`MinMaxAvg`) - statistics for successful disk-spilled put operations, tracking the minimum, maximum, and average time in milliseconds. The measured window is the store write (`OffHeapStore.put()`) itself, excluding serialization, the preceding failed in-memory slot search, and the entry installation - the write counterpart of `readFromDiskTimeStats`. Timing observations are recorded only when the cache is built with `statsTimeOnDisk(true)` (it defaults to `false`); when disabled, no observations are ever recorded and all three values stay `0.0`.
- `readFromDiskTimeStats` (`MinMaxAvg`) - statistics for disk read operations, tracking the minimum, maximum, and average time in milliseconds for reading entry bytes from the store. The measured window is the store read (`OffHeapStore.get()`) itself; lookups that never reach the store (entry already removed or replaced concurrently), throwing store reads, and corrupt-length results are not recorded. Confirmed store misses are recorded. Timing observations are recorded only when the cache is built with `statsTimeOnDisk(true)` (it defaults to `false`); when disabled, no observations are ever recorded and all three values stay `0.0`. This helps monitor disk read performance and cache hit efficiency.
- `segmentSize` (`int`) - the size of each memory segment in bytes. The off-heap memory is organized into fixed-size segments (the built-in caches always use 1MB = 1048576 bytes) to manage memory allocation and reduce fragmentation.
- `occupiedSlots` (`Map<Integer, Map<Integer, Integer>>`) - a detailed map showing memory slot occupation across segments. The outer map's key is the slot size in bytes - any multiple of the 64-byte minimum block size up to `maxBlockSize` (e.g., 64, 128, 192, 256, 320,...), not only powers of two - and the inner map contains segment index as key and the number of occupied slots in that segment as value. This provides granular visibility into memory fragmentation and utilization patterns.

#### Public Constructors
- `public OffHeapCacheStats(int capacity, int size, long sizeOnDisk, long putCount, long putCountToDisk, long getCount, long hitCount, long hitCountFromDisk, long missCount, long evictionCount, long evictionCountFromDisk, long allocatedMemory, long occupiedMemory, long dataSize, long dataSizeOnDisk, MinMaxAvg writeToDiskTimeStats, MinMaxAvg readFromDiskTimeStats, int segmentSize, Map<Integer, Map<Integer, Integer>> occupiedSlots)` — Canonical constructor that validates every component (numeric ranges, non-`null` references, and the `occupiedSlots` entries) and stores a deeply unmodifiable defensive copy of `occupiedSlots`.
  - **Throws:**
    - `java.lang.IllegalArgumentException` — if any numeric component is negative, or if `occupiedSlots` contains a non-positive slot size, or a negative segment index or occupied-slot count
    - `java.lang.NullPointerException` — if `writeToDiskTimeStats`, `readFromDiskTimeStats`, or `occupiedSlots` is `null`, or if `occupiedSlots` contains a `null` key, nested map, segment index, or occupied-slot count

#### Public Static Methods
- (none)

#### Public Instance Methods
##### hitRate(...) -> double
- **Signature:** `public double hitRate()`
- **Summary:** Returns the cache hit rate as a fraction in `[0.0, 1.0]`.
- **Parameters:**
  - (none)
- **Returns:** the ratio of hits to total get requests, or `0.0` when no get requests have been recorded (i.e. `hitCount + missCount == 0`)
- **See also:** `#missRate()`
##### missRate(...) -> double
- **Signature:** `public double missRate()`
- **Summary:** Returns the cache miss rate as a fraction in `[0.0, 1.0]`.
- **Parameters:**
  - (none)
- **Returns:** the ratio of misses to total get requests, or `0.0` when no get requests have been recorded (i.e. `hitCount + missCount == 0`)
- **See also:** `#hitRate()`
##### occupiedSlots(...) -> Map<Integer, Map<Integer, Integer>>
- **Signature:** `public Map<Integer, Map<Integer, Integer>> occupiedSlots()`
- **Summary:** Returns a map of occupied memory slots organized by slot size.
- **Parameters:**
  - (none)
- **Returns:** an unmodifiable map of slot sizes (in bytes) to per-segment occupation details; never `null`
- **Examples:**
  ```java
  Map<Integer, Map<Integer, Integer>> slots = stats.occupiedSlots();
  // For this snapshot, slots == {1024={0=5, 1=3}, 2048={2=2}}:
  //   5 slots of 1KB in segment 0, 3 slots of 1KB in segment 1, 2 slots of 2KB in segment 2.
  slots.get(1024).get(0);                                          // returns 5 (Integer)
  slots.get(1024).get(1);                                          // returns 3 (Integer)
  slots.get(2048).get(2);                                          // returns 2 (Integer)

  // Sum the occupied 1KB slots across every segment.
  int total1k = slots.get(1024).values().stream()
          .mapToInt(Integer::intValue).sum();                     // total1k == 8

  // The returned map is deeply unmodifiable (outer map and nested maps are defensive copies).
  slots.put(4096, Map.of());                                      // throws UnsupportedOperationException
  slots.get(1024).put(9, 9);                                      // throws UnsupportedOperationException

  // A fresh cache, or a cache just cleared without concurrent writes, has no dedicated segments.
  Map<Integer, Map<Integer, Integer>> empty = emptyStats.occupiedSlots();
  empty.isEmpty();                                                // returns true
  ```
##### capacity(...) -> int
- **Signature:** `public int capacity()`
- **Summary:** a theoretical upper bound on the number of memory-resident entries, derived from the off-heap memory budget as `allocatedMemory / MIN_BLOCK_SIZE` (currently `allocatedMemory / 64`), capped at `Integer#MAX_VALUE`: every such entry occupies at least one 64-byte slot. Entries above 64 bytes need larger or additional slots, so off-heap memory typically fills long before the memory-resident count (`size - sizeOnDisk`) approaches this value. It does not bound `size`, which also counts disk-spilled entries.
- **Parameters:**
  - (none)
- **Returns:** a theoretical upper bound on the number of memory-resident entries, derived from the off-heap memory budget as `allocatedMemory / MIN_BLOCK_SIZE` (currently `allocatedMemory / 64`), capped at `Integer#MAX_VALUE`: every such entry occupies at least one 64-byte slot. Entries above 64 bytes need larger or additional slots, so off-heap memory typically fills long before the memory-resident count (`size - sizeOnDisk`) approaches this value. It does not bound `size`, which also counts disk-spilled entries.
##### size(...) -> int
- **Signature:** `public int size()`
- **Summary:** the current number of entries in the cache, counting both memory-resident and disk-spilled entries. To compute the memory-only entry count, use `size - sizeOnDisk`.
- **Parameters:**
  - (none)
- **Returns:** the current number of entries in the cache, counting both memory-resident and disk-spilled entries. To compute the memory-only entry count, use `size - sizeOnDisk`.
##### sizeOnDisk(...) -> long
- **Signature:** `public long sizeOnDisk()`
- **Summary:** the current number of entries whose value bytes are stored on disk via the configured `OffHeapStore`. These entries are still counted in `size`; only their payloads live on disk.
- **Parameters:**
  - (none)
- **Returns:** the current number of entries whose value bytes are stored on disk via the configured `OffHeapStore`. These entries are still counted in `size`; only their payloads live on disk.
##### putCount(...) -> long
- **Signature:** `public long putCount()`
- **Summary:** the total number of successful put operations since cache creation, counting values stored in off-heap memory and values written to the disk store. A put that throws after its value was already mapped is counted. A put that fails (e.g., neither memory nor disk could accept the value) is not counted; internal disk-to-memory promotions are not counted either.
- **Parameters:**
  - (none)
- **Returns:** the total number of successful put operations since cache creation, counting values stored in off-heap memory and values written to the disk store. A put that throws after its value was already mapped is counted. A put that fails (e.g., neither memory nor disk could accept the value) is not counted; internal disk-to-memory promotions are not counted either.
##### putCountToDisk(...) -> long
- **Signature:** `public long putCountToDisk()`
- **Summary:** the number of put operations that resulted in writing data to disk. This occurs when off-heap memory is full, or the value can never fit in off-heap memory, and the value is stored to disk via the configured `OffHeapStore`, or when the `storeSelector` explicitly routes the value to disk. It is a subset of `putCount`.
- **Parameters:**
  - (none)
- **Returns:** the number of put operations that resulted in writing data to disk. This occurs when off-heap memory is full, or the value can never fit in off-heap memory, and the value is stored to disk via the configured `OffHeapStore`, or when the `storeSelector` explicitly routes the value to disk. It is a subset of `putCount`.
##### getCount(...) -> long
- **Signature:** `public long getCount()`
- **Summary:** the total number of lookups classified as a hit or miss since cache creation. Failures before that classification (such as a throwing store read or a corrupt payload length) are not counted. Conceptually, `getCount = hitCount + missCount`; the sum saturates at `Long#MAX_VALUE` if the two counters would overflow.
- **Parameters:**
  - (none)
- **Returns:** the total number of lookups classified as a hit or miss since cache creation. Failures before that classification (such as a throwing store read or a corrupt payload length) are not counted. Conceptually, `getCount = hitCount + missCount`; the sum saturates at `Long#MAX_VALUE` if the two counters would overflow.
##### hitCount(...) -> long
- **Signature:** `public long hitCount()`
- **Summary:** the number of get operations that found a live entry and its bytes. A get that finds a disk-backed entry whose backing bytes are confirmed missing counts as a miss, not a hit. To approximate hits served purely from off-heap memory, subtract `hitCountFromDisk` from `hitCount`. A get that found live bytes but then threw during deserialization remains counted here because exceptional reconstruction is not tracked separately.
- **Parameters:**
  - (none)
- **Returns:** the number of get operations that found a live entry and its bytes. A get that finds a disk-backed entry whose backing bytes are confirmed missing counts as a miss, not a hit. To approximate hits served purely from off-heap memory, subtract `hitCountFromDisk` from `hitCount`. A get that found live bytes but then threw during deserialization remains counted here because exceptional reconstruction is not tracked separately.
##### hitCountFromDisk(...) -> long
- **Signature:** `public long hitCountFromDisk()`
- **Summary:** the number of get operations whose value was successfully reconstructed from bytes read from the configured `OffHeapStore`. It normally forms a subset of `hitCount`, but independently sampled counters can be transiently inconsistent.
- **Parameters:**
  - (none)
- **Returns:** the number of get operations whose value was successfully reconstructed from bytes read from the configured `OffHeapStore`. It normally forms a subset of `hitCount`, but independently sampled counters can be transiently inconsistent.
##### missCount(...) -> long
- **Signature:** `public long missCount()`
- **Summary:** the number of failed get operations where the entry was not found in either memory or disk. This can occur when the key never existed, was explicitly removed, has expired, or still had a cache mapping whose backing-store bytes were missing. A lookup that finds an entry just as a concurrent operation removes or replaces it also counts as a miss.
- **Parameters:**
  - (none)
- **Returns:** the number of failed get operations where the entry was not found in either memory or disk. This can occur when the key never existed, was explicitly removed, has expired, or still had a cache mapping whose backing-store bytes were missing. A lookup that finds an entry just as a concurrent operation removes or replaces it also counts as a miss.
##### evictionCount(...) -> long
- **Signature:** `public long evictionCount()`
- **Summary:** the total number of entries removed because they expired (lazily on access or by the periodic maintenance sweep) or were reclaimed by the memory-pressure vacate pass. Explicit `remove()` / `clear()` calls and `put()` replacements are NOT counted here.
- **Parameters:**
  - (none)
- **Returns:** the total number of entries removed because they expired (lazily on access or by the periodic maintenance sweep) or were reclaimed by the memory-pressure vacate pass. Explicit `remove()` / `clear()` calls and `put()` replacements are NOT counted here.
##### evictionCountFromDisk(...) -> long
- **Signature:** `public long evictionCountFromDisk()`
- **Summary:** the number of disk-stored entries removed because they expired (lazily on access or by the maintenance sweep). The vacate pass evicts only memory-resident entries, so it never contributes here. Explicit `remove()` / `clear()` and `put()` replacements of a disk-stored key are NOT counted here.
- **Parameters:**
  - (none)
- **Returns:** the number of disk-stored entries removed because they expired (lazily on access or by the maintenance sweep). The vacate pass evicts only memory-resident entries, so it never contributes here. Explicit `remove()` / `clear()` and `put()` replacements of a disk-stored key are NOT counted here.
##### allocatedMemory(...) -> long
- **Signature:** `public long allocatedMemory()`
- **Summary:** the total allocated off-heap memory in bytes. This is the fixed amount of native memory reserved for the cache at creation, organized into fixed-size segments.
- **Parameters:**
  - (none)
- **Returns:** the total allocated off-heap memory in bytes. This is the fixed amount of native memory reserved for the cache at creation, organized into fixed-size segments.
##### occupiedMemory(...) -> long
- **Signature:** `public long occupiedMemory()`
- **Summary:** the currently occupied off-heap slot space in bytes. This includes serialized data plus slot-rounding/alignment padding, but not the heap-resident entry metadata.
- **Parameters:**
  - (none)
- **Returns:** the currently occupied off-heap slot space in bytes. This includes serialized data plus slot-rounding/alignment padding, but not the heap-resident entry metadata.
##### dataSize(...) -> long
- **Signature:** `public long dataSize()`
- **Summary:** the total size of actual serialized data tracked by the cache in bytes, across both the off-heap memory pool and the disk store, excluding any slot-allocation padding or internal overhead. To isolate the in-memory portion, subtract `dataSizeOnDisk` from `dataSize`.
- **Parameters:**
  - (none)
- **Returns:** the total size of actual serialized data tracked by the cache in bytes, across both the off-heap memory pool and the disk store, excluding any slot-allocation padding or internal overhead. To isolate the in-memory portion, subtract `dataSizeOnDisk` from `dataSize`.
##### dataSizeOnDisk(...) -> long
- **Signature:** `public long dataSizeOnDisk()`
- **Summary:** the total size of serialized data currently stored on disk in bytes. This is a subset of `dataSize` and counts only entries that have been persisted to disk storage.
- **Parameters:**
  - (none)
- **Returns:** the total size of serialized data currently stored on disk in bytes. This is a subset of `dataSize` and counts only entries that have been persisted to disk storage.
##### writeToDiskTimeStats(...) -> MinMaxAvg
- **Signature:** `public MinMaxAvg writeToDiskTimeStats()`
- **Summary:** statistics for successful disk-spilled put operations, tracking the minimum, maximum, and average time in milliseconds. The measured window is the store write (`OffHeapStore.put()`) itself, excluding serialization, the preceding failed in-memory slot search, and the entry installation - the write counterpart of `readFromDiskTimeStats`. Timing observations are recorded only when the cache is built with `statsTimeOnDisk(true)` (it defaults to `false`); when disabled, no observations are ever recorded and all three values stay `0.0`.
- **Parameters:**
  - (none)
- **Returns:** statistics for successful disk-spilled put operations, tracking the minimum, maximum, and average time in milliseconds. The measured window is the store write (`OffHeapStore.put()`) itself, excluding serialization, the preceding failed in-memory slot search, and the entry installation - the write counterpart of `readFromDiskTimeStats`. Timing observations are recorded only when the cache is built with `statsTimeOnDisk(true)` (it defaults to `false`); when disabled, no observations are ever recorded and all three values stay `0.0`.
##### readFromDiskTimeStats(...) -> MinMaxAvg
- **Signature:** `public MinMaxAvg readFromDiskTimeStats()`
- **Summary:** statistics for disk read operations, tracking the minimum, maximum, and average time in milliseconds for reading entry bytes from the store. The measured window is the store read (`OffHeapStore.get()`) itself; lookups that never reach the store (entry already removed or replaced concurrently), throwing store reads, and corrupt-length results are not recorded. Confirmed store misses are recorded. Timing observations are recorded only when the cache is built with `statsTimeOnDisk(true)` (it defaults to `false`); when disabled, no observations are ever recorded and all three values stay `0.0`. This helps monitor disk read performance and cache hit efficiency.
- **Parameters:**
  - (none)
- **Returns:** statistics for disk read operations, tracking the minimum, maximum, and average time in milliseconds for reading entry bytes from the store. The measured window is the store read (`OffHeapStore.get()`) itself; lookups that never reach the store (entry already removed or replaced concurrently), throwing store reads, and corrupt-length results are not recorded. Confirmed store misses are recorded. Timing observations are recorded only when the cache is built with `statsTimeOnDisk(true)` (it defaults to `false`); when disabled, no observations are ever recorded and all three values stay `0.0`. This helps monitor disk read performance and cache hit efficiency.
##### segmentSize(...) -> int
- **Signature:** `public int segmentSize()`
- **Summary:** the size of each memory segment in bytes. The off-heap memory is organized into fixed-size segments (the built-in caches always use 1MB = 1048576 bytes) to manage memory allocation and reduce fragmentation.
- **Parameters:**
  - (none)
- **Returns:** the size of each memory segment in bytes. The off-heap memory is organized into fixed-size segments (the built-in caches always use 1MB = 1048576 bytes) to manage memory allocation and reduce fragmentation.
##### equals(...) -> boolean
- **Signature:** `public final boolean equals(Object o)`
- **Parameters:**
  - `o` (`Object`)
- **Returns:** unspecified
##### hashCode(...) -> int
- **Signature:** `public final int hashCode()`
- **Parameters:**
  - (none)
- **Returns:** unspecified
##### toString(...) -> String
- **Signature:** `public final String toString()`
- **Parameters:**
  - (none)
- **Returns:** unspecified

### Record MinMaxAvg (com.landawn.abacus.cache.OffHeapCacheStats.MinMaxAvg)
Statistics for the minimum, maximum, and average values of a metric.

**Thread-safety:** unspecified
**Nullability:** unspecified

#### Record Components
- `min` (`double`) - the minimum observed value (`0.0` if no observations have been recorded)
- `max` (`double`) - the maximum observed value (`0.0` if no observations have been recorded)
- `avg` (`double`) - the average of all observed values (`0.0` if no observations have been recorded)

#### Public Constructors
- `public MinMaxAvg(double min, double max, double avg)` — Canonical constructor that validates the values are non-negative and finite.
  - **Throws:**
    - `java.lang.IllegalArgumentException` — if `min`, `max`, or `avg` is negative, NaN, or infinite, or if `min <= avg <= max` does not hold

#### Public Static Methods
- (none)

#### Public Instance Methods
##### toString(...) -> String
- **Signature:** `public String toString()`
- **Summary:** Returns a string representation of the statistics in a JSON-like format.
- **Parameters:**
  - (none)
- **Returns:** a formatted string showing the min, max, and avg values in a JSON-like format
- **Examples:**
  ```java
  MinMaxAvg stats = new MinMaxAvg(5.2, 150.8, 45.3);
  stats.toString();                                  // returns "{min: 5.2, max: 150.8, avg: 45.3}"

  // When no observations have been recorded every value is 0.0 (a double, so it prints as "0.0").
  new MinMaxAvg(0, 0, 0).toString();                 // returns "{min: 0.0, max: 0.0, avg: 0.0}"
  ```
##### min(...) -> double
- **Signature:** `public double min()`
- **Summary:** the minimum observed value (`0.0` if no observations have been recorded)
- **Parameters:**
  - (none)
- **Returns:** the minimum observed value (`0.0` if no observations have been recorded)
##### max(...) -> double
- **Signature:** `public double max()`
- **Summary:** the maximum observed value (`0.0` if no observations have been recorded)
- **Parameters:**
  - (none)
- **Returns:** the maximum observed value (`0.0` if no observations have been recorded)
##### avg(...) -> double
- **Signature:** `public double avg()`
- **Summary:** the average of all observed values (`0.0` if no observations have been recorded)
- **Parameters:**
  - (none)
- **Returns:** the average of all observed values (`0.0` if no observations have been recorded)
##### equals(...) -> boolean
- **Signature:** `public final boolean equals(Object o)`
- **Parameters:**
  - `o` (`Object`)
- **Returns:** unspecified
##### hashCode(...) -> int
- **Signature:** `public final int hashCode()`
- **Parameters:**
  - (none)
- **Returns:** unspecified

### Interface OffHeapStore (com.landawn.abacus.cache.OffHeapStore)
Interface for persistent storage backing an off-heap cache.

**Thread-safety:** unspecified
**Nullability:** unspecified
**Extends:** `AutoCloseable`

#### Public Constructors
- (none)

#### Public Static Methods
- (none)

#### Public Instance Methods
##### get(...) -> byte\[\]
- **Signature:** `byte[] get(K key) throws RuntimeException`
- **Summary:** Retrieves the byte array associated with the specified key.
- **Contract:**
  - Returns `null` only if the key is confirmed not to exist.
  - An operational retrieval failure must be reported with an unchecked exception; the owning cache interprets `null` as a definitive miss and retires its stale entry metadata.
  - Thread Safety: Implementations of this method must be thread-safe and support concurrent access from multiple threads.
  - Usage Examples: Round-trip contract: the returned array must contain exactly the bytes passed to `put(key, value)` — same content and same length.
  - Any compression or encoding applied by the implementation must be fully transparent (decoded before returning).
- **Parameters:**
  - `key` (`K`) — the key whose associated value is to be retrieved; must not be `null`
- **Returns:** the stored byte array, or `null` if the key is confirmed not to exist
- **Throws:**
  - `java.lang.RuntimeException` — if the implementation cannot read the stored bytes or determine whether `key` exists, for example because its storage is unavailable
- **Examples:**
  ```java
  OffHeapStore<String> store = new FileBasedOffHeapStore<>(Paths.get("/tmp/cache"));
  byte[] data = store.get("user:123");
  if (data != null) {
      User user = deserialize(data);
      System.out.println("User loaded from disk");
  } else {
      System.out.println("User not found");
  }
  store.close();
  ```
##### put(...) -> boolean
- **Signature:** `boolean put(K key, byte[] value) throws RuntimeException`
- **Summary:** Stores a byte array under the specified key.
- **Contract:**
  - If a value already exists for the key, it is replaced.
  - Implementations should consider making a defensive copy of the byte array to prevent external modifications, though this behavior is implementation-specific.
  - The owning cache already supplies a private array and does not modify it after this call, so an implementation may retain that array when used exclusively through the cache.
  - Failure atomicity: Returning `false` or throwing must leave any value that was previously associated with `key` unchanged.
  - Implementations should write to temporary storage and atomically publish/rename it, or otherwise roll back a failed replacement.
  - Thread Safety: Implementations of this method must be thread-safe and support concurrent access from multiple threads.
- **Parameters:**
  - `key` (`K`) — the key with which the specified value is to be associated; must not be `null`
  - `value` (`byte[]`) — the byte array value to store; must not be `null`
- **Returns:** `true` if the value was successfully stored, `false` if it was not stored and any previous mapping remains unchanged
- **Throws:**
  - `java.lang.RuntimeException` — if the implementation reports a failed write by throwing rather than returning `false`; the previous mapping and bytes must remain unchanged
- **Examples:**
  ```java
  OffHeapStore<String> store = new FileBasedOffHeapStore<>(Paths.get("/tmp/cache"));
  User user = new User("John", 30);
  byte[] serializedData = serialize(user);
  boolean success = store.put("user:123", serializedData);
  if (success) {
      System.out.println("Data stored to disk successfully");
  } else {
      System.out.println("Failed to store data");
  }
  store.close();
  ```
##### remove(...) -> boolean
- **Signature:** `boolean remove(K key) throws RuntimeException`
- **Summary:** Removes the value associated with the specified key.
- **Contract:**
  - Returns `true` if a value was removed or `false` if the key is confirmed not to exist.
  - An operational removal failure must be reported with an unchecked exception; it must not be collapsed into `false`.
  - Thread Safety: Implementations of this method must be thread-safe and support concurrent access from multiple threads.
- **Parameters:**
  - `key` (`K`) — the key whose associated value is to be removed; must not be `null`
- **Returns:** `true` if a value was removed, `false` if the key is confirmed not to exist
- **Throws:**
  - `java.lang.RuntimeException` — if the implementation cannot remove the stored bytes or determine whether `key` exists, for example because its storage is unavailable
- **Examples:**
  ```java
  OffHeapStore<String> store = new FileBasedOffHeapStore<>(Paths.get("/tmp/cache"));
  if (store.remove("user:123")) {
      System.out.println("User data removed from disk");
  } else {
      System.out.println("User data not found");
  }
  store.close();
  ```
##### close(...) -> void
- **Signature:** `default void close()`
- **Summary:** Releases any OS resources held by this store (file handles, memory-mapped regions, embedded database connections, etc.).
- **Contract:**
  - Called by the owning off-heap cache when the cache itself is closed; may also be called directly by users (this interface extends `AutoCloseable`, so a store can be used in try-with-resources).
  - Implementations that open files, mmap regions, or connections should override it.
  - During normal cache shutdown an unchecked exception raised here propagates from the cache's `close()` (or is attached as a suppressed exception when an earlier cleanup step already failed); the cache's other cleanup steps still run.
  - If cache construction fails after the cache's native memory was allocated, the cache closes the store during its cleanup and attaches a store-close failure to the construction failure as a suppressed exception; a construction failure before that point (such as an invalid argument) leaves the store open and owned by the caller.
- **Parameters:**
  - (none)

### Class SpyMemcached (com.landawn.abacus.cache.SpyMemcached)
A Memcached distributed cache client implementation backed by the SpyMemcached library.

**Thread-safety:** thread-safe
**Nullability:** unspecified
**Extends:** `AbstractDistributedCacheClient<T>`

#### Public Constructors
- `public SpyMemcached(final String serverUrl) throws IllegalArgumentException, UncheckedIOException` — Creates a new `SpyMemcached` instance using `DistributedCacheClient#DEFAULT_TIMEOUT` as the operation timeout.
  - **Throws:**
    - `java.lang.IllegalArgumentException` — if `serverUrl` is `null`, empty, blank, or contains no valid server addresses, or if a host named in `serverUrl` cannot be resolved (rejected before any client resources are created)
    - `com.landawn.abacus.exception.UncheckedIOException` — if local client/socket setup fails. Note: connections are established asynchronously by the SpyMemcached IO thread — a resolvable but unreachable or down server does not fail construction; operations against it fail later with timeouts.
  ```java
  SpyMemcached<String> cache = new SpyMemcached<>("localhost:11211"); // uses DEFAULT_TIMEOUT
  cache.put("key1", "value1", 3600000);                               // stores with 3600s TTL; returns true on success
  // Retain and share cache; optionally disconnect it during application shutdown.

  // A null/blank serverUrl is rejected before any connection attempt.
  new SpyMemcached<>((String) null); // throws IllegalArgumentException
  new SpyMemcached<>("   ");         // throws IllegalArgumentException (blank)
  ```
- `public SpyMemcached(final String serverUrl, final long timeout) throws IllegalArgumentException, UncheckedIOException` — Creates a new `SpyMemcached` instance with the specified operation timeout.
  - **Throws:**
    - `java.lang.IllegalArgumentException` — if `serverUrl` is `null`, empty, blank, or contains no valid server addresses, if `timeout` is not positive, or if a host named in `serverUrl` cannot be resolved (rejected before any client resources are created)
    - `com.landawn.abacus.exception.UncheckedIOException` — if local client/socket setup fails. Note: connections are established asynchronously by the SpyMemcached IO thread — a resolvable but unreachable or down server does not fail construction; operations against it fail later with timeouts.
  ```java
  // Create cache with 5-second operation timeout
  SpyMemcached<User> cache = new SpyMemcached<>("localhost:11211", 5000); // 5000ms operation timeout
  cache.put("user:123", user, 3600000);                                   // stores with 3600s TTL; returns true on success
  // Retain and share cache; optionally disconnect it during application shutdown.

  // timeout must be strictly positive (checkArgPositive).
  new SpyMemcached<>("localhost:11211", 0);  // throws IllegalArgumentException
  new SpyMemcached<>("localhost:11211", -1); // throws IllegalArgumentException
  ```

#### Public Static Methods
##### checkKey(...) -> void
- **Signature:** `public static void checkKey(final String key) throws IllegalArgumentException`
- **Summary:** Validates a key exactly as this client does before value encoding or backend dispatch, for the ASCII protocol supplied by its `DefaultConnectionFactory`.
- **Parameters:**
  - `key` (`String`) — the key to validate
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, is empty, exceeds 250 UTF-8 bytes, or contains a space, CR, LF, or NUL byte
- **Examples:**
  ```java
  SpyMemcached.checkKey("user:42");                       // passes
  SpyMemcached.checkKey("user 42");                       // throws IllegalArgumentException (contains a space)
  SpyMemcached.checkKey(String.valueOf((char) 0xD800));   // throws IllegalArgumentException (unpaired surrogate)
  ```

#### Public Instance Methods
##### get(...) -> T
- **Signature:** `@SuppressWarnings("unchecked") public T get(final String key) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Retrieves an object from the cache by its key.
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be retrieved; must not be `null`
- **Returns:** the cached object of type `T`, or `null` if not found, expired, or evicted
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, or fails the memcached client's key validation (empty, longer than 250 UTF-8 bytes, or containing a space, CR, LF, or NUL byte)
  - `java.lang.RuntimeException` — if the operation fails or is cancelled, the response wait times out or is interrupted, or a returned value cannot be decoded
- **Examples:**
  ```java
  // Simple get operation
  User user = cache.get("user:123"); // returns the cached User, or null if absent
  if (user != null) {
      System.out.println("Found user: " + user.getName()); // prints the cached user's name
  } else {
      System.out.println("User not found in cache"); // printed when get returned null
  }

  // Get with fallback to database
  User loaded = cache.get("user:123"); // returns null if not cached
  if (loaded == null) {
      loaded = database.findUser(123); // load from the source of truth
      cache.put("user:123", loaded, 3600000); // re-populate the cache; returns true on success
  }

  cache.get((String) null); // throws IllegalArgumentException (key must not be null)
  ```
##### asyncGet(...) -> ContinuableFuture<T>
- **Signature:** `@SuppressWarnings("unchecked") public ContinuableFuture<T> asyncGet(final String key) throws IllegalStateException, IllegalArgumentException`
- **Summary:** Asynchronously retrieves an object from the cache by its key.
- **Contract:**
  - This method returns after validation and enqueueing without waiting for the server response; enqueueing can still block briefly if the operation queue is full.
  - The returned Future can be used to check completion status and retrieve the result when available.
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be retrieved; must not be `null`
- **Returns:** a `ContinuableFuture` that will yield the cached object of type `T`, or `null` if not found, expired, or evicted
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, or fails the memcached client's key validation (empty, longer than 250 UTF-8 bytes, or containing a space, CR, LF, or NUL byte)
- **Examples:**
  ```java
  // Simple async get
  ContinuableFuture<User> getFuture = cache.asyncGet("user:123"); // dispatched; never null
  User user = getFuture.get();                                    // blocks until complete; yields the value or null if absent

  // Async get with timeout (requires: import java.util.concurrent.TimeoutException;)
  ContinuableFuture<User> timedGet = cache.asyncGet("user:123"); // dispatches the get; returns the Future
  try {
      User timedUser = timedGet.get(1000, TimeUnit.MILLISECONDS); // waits up to 1s for the result
  } catch (TimeoutException e) {
      timedGet.cancel(true);                         // abandon the operation if it did not complete in time
      System.out.println("Get operation timed out"); // printed if the result was not ready in time
  }

  cache.asyncGet((String) null); // throws IllegalArgumentException (key must not be null)
  ```
##### getBulk(...) -> Map<String, T>
- **Signature:** `@SuppressWarnings("unchecked") public final Map<String, T> getBulk(final String... keys) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Retrieves multiple objects in one batched request.
- **Contract:**
  - ⚠️ A missing key is not always a cache miss: when an operation sits unwritten longer than the operation timeout (server stalled or reconnecting, write-queue backlog), the underlying client completes it as timed out without cancelling or erroring it, and the returned map silently lacks that server's keys instead of this method throwing.
- **Parameters:**
  - `keys` (`String...`) — the cache keys whose associated values are to be retrieved; must not be `null` and must not contain `null` elements
- **Returns:** a map containing the found key-value pairs; never `null`, but possibly empty if no keys are found
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `keys` is `null`, contains a `null` element or a key with an unpaired UTF-16 surrogate, or contains a key that fails the memcached client's key validation (empty, longer than 250 UTF-8 bytes, or containing a space, CR, LF, or NUL byte)
  - `java.lang.RuntimeException` — if the operation fails or is cancelled, the response wait times out or is interrupted, or a returned value cannot be decoded
- **Examples:**
  ```java
  // Basic bulk get
  Map<String, User> users = cache.getBulk("user:1", "user:2", "user:3");         // never null; missing keys are simply absent
  users.forEach((key, user) -> System.out.println(key + ": " + user.getName())); // iterates only the found entries

  // Bulk get with missing key handling
  Map<String, User> admins = cache.getBulk("admin:1", "admin:2", "admin:3"); // returns only the keys present in cache
  System.out.println("Found " + admins.size() + " out of 3 admins");          // size() <= number of requested keys

  // Identify missing keys
  String[] requestedKeys = {"user:1", "user:2", "user:3"};
  Map<String, User> found = cache.getBulk(requestedKeys); // returns the subset that was cached
  Arrays.stream(requestedKeys)
        .filter(key -> !found.containsKey(key))
        .forEach(key -> System.out.println("Missing: " + key)); // prints each requested key absent from the result

  cache.getBulk((String[]) null);   // throws IllegalArgumentException (keys must not be null)
  cache.getBulk("user:1", null);    // throws IllegalArgumentException (no null elements allowed)
  ```
- **Signature:** `@SuppressWarnings("unchecked") public final Map<String, T> getBulk(final Collection<String> keys) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Retrieves multiple objects from the cache using a batched collection request.
- **Parameters:**
  - `keys` (`Collection<String>`) — the collection of cache keys whose associated values are to be retrieved; must not be `null` and must not contain `null` elements
- **Returns:** a map containing the found key-value pairs; never `null`, but possibly empty if no keys are found
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `keys` is `null`, contains a `null` element or a key with an unpaired UTF-16 surrogate, or contains a key that fails the memcached client's key validation (empty, longer than 250 UTF-8 bytes, or containing a space, CR, LF, or NUL byte)
  - `java.lang.RuntimeException` — if the operation fails or is cancelled, the response wait times out or is interrupted, or a returned value cannot be decoded
- **Examples:**
  ```java
  // Using a List (requires: import java.util.List;)
  List<String> userKeys = Arrays.asList("user:123", "user:456", "user:789");
  Map<String, User> listUsers = cache.getBulk(userKeys); // never null; missing keys are simply absent

  // Using a Set (requires: import java.util.Set; import java.util.HashSet;)
  Set<String> keySet = new HashSet<>(Arrays.asList("user:321", "user:654"));
  Map<String, User> setUsers = cache.getBulk(keySet); // returns only the keys present in cache

  // Dynamically built key collection (requires: import java.util.stream.Collectors;)
  List<Integer> userIds = Arrays.asList(101, 102, 103);
  List<String> keys = userIds.stream()
                             .map(id -> "user:" + id)
                             .collect(Collectors.toList()); // builds ["user:101", "user:102", "user:103"]
  Map<String, User> generatedUsers = cache.getBulk(keys);   // returns the cached subset

  cache.getBulk((Collection<String>) null);      // throws IllegalArgumentException (keys must not be null)
  cache.getBulk(Arrays.asList("user:1", null));  // throws IllegalArgumentException (no null elements allowed)
  ```
##### asyncGetBulk(...) -> ContinuableFuture<Map<String, T>>
- **Signature:** `@SuppressWarnings({ "unchecked", "rawtypes" }) public final ContinuableFuture<Map<String, T>> asyncGetBulk(final String... keys) throws IllegalStateException, IllegalArgumentException`
- **Summary:** Asynchronously retrieves multiple objects from the cache.
- **Contract:**
  - This method returns after validation and enqueueing without waiting for server responses; enqueueing can still block briefly if the operation queue is full.
  - A no-argument `Future#get()` is bounded by the configured operation timeout; if that outer wait expires, the delegate is cancelled and `ExecutionException` is thrown with a `TimeoutException` cause.
- **Parameters:**
  - `keys` (`String...`) — the cache keys whose associated values are to be retrieved; must not be `null` and must not contain `null` elements
- **Returns:** a `ContinuableFuture` that will yield the map of found key-value pairs; the map is never `null` but may be empty
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `keys` is `null`, contains a `null` element or a key with an unpaired UTF-16 surrogate, or contains a key that fails the memcached client's key validation (empty, longer than 250 UTF-8 bytes, or containing a space, CR, LF, or NUL byte)
- **Examples:**
  ```java
  // Simple async bulk get
  ContinuableFuture<Map<String, User>> bulkFuture = cache.asyncGetBulk("user:1", "user:2"); // dispatched; never null
  Map<String, User> users = bulkFuture.get();                                               // bounded by the configured operation timeout

  // Async bulk get with timeout (requires: import java.util.concurrent.TimeoutException;)
  ContinuableFuture<Map<String, User>> timedBulk = cache.asyncGetBulk("user:1", "user:2", "user:3"); // dispatches the bulk get
  try {
      Map<String, User> timedUsers = timedBulk.get(2000, TimeUnit.MILLISECONDS); // waits up to 2s
      System.out.println("Retrieved " + timedUsers.size() + " users");           // size() <= number of requested keys
  } catch (TimeoutException e) {
      timedBulk.cancel(true); // abandon the operation if it did not complete in time
  }

  cache.asyncGetBulk((String[]) null);  // throws IllegalArgumentException (keys must not be null)
  cache.asyncGetBulk("user:1", null);   // throws IllegalArgumentException (no null elements allowed)
  ```
- **Signature:** `@SuppressWarnings({ "rawtypes", "unchecked" }) public final ContinuableFuture<Map<String, T>> asyncGetBulk(final Collection<String> keys) throws IllegalStateException, IllegalArgumentException`
- **Summary:** Asynchronously retrieves multiple objects using a collection of keys.
- **Contract:**
  - This method returns after validation and enqueueing without waiting for server responses; enqueueing can still block briefly if the operation queue is full.
- **Parameters:**
  - `keys` (`Collection<String>`) — the collection of cache keys whose associated values are to be retrieved; must not be `null` and must not contain `null` elements
- **Returns:** a `ContinuableFuture` that will yield the map of found key-value pairs; the map is never `null` but may be empty
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `keys` is `null`, contains a `null` element or a key with an unpaired UTF-16 surrogate, or contains a key that fails the memcached client's key validation (empty, longer than 250 UTF-8 bytes, or containing a space, CR, LF, or NUL byte)
- **Examples:**
  ```java
  // Simple async bulk get with collection (requires: import java.util.Set;)
  Set<String> keys = new HashSet<>(Arrays.asList("user:1", "user:2"));
  ContinuableFuture<Map<String, User>> userFuture = cache.asyncGetBulk(keys); // dispatched; never null
  Map<String, User> users = userFuture.get();                                 // bounded by the configured operation timeout

  // Async bulk get from dynamically generated keys (requires: import java.util.concurrent.TimeoutException;)
  List<String> userKeys = generateUserKeys();
  ContinuableFuture<Map<String, User>> generatedFuture = cache.asyncGetBulk(userKeys); // dispatches the bulk get
  Map<String, User> generatedUsers = generatedFuture.get(3000, TimeUnit.MILLISECONDS);  // waits up to 3s

  cache.asyncGetBulk((Collection<String>) null);     // throws IllegalArgumentException (keys must not be null)
  cache.asyncGetBulk(Arrays.asList("user:1", null)); // throws IllegalArgumentException (no null elements allowed)
  ```
##### put(...) -> boolean
- **Signature:** `public boolean put(final String key, final T value, final long liveTime) throws IllegalStateException, IllegalArgumentException, RuntimeException, StackOverflowError`
- **Summary:** Stores an object in the cache with a specified time-to-live.
- **Contract:**
  - The liveTime is converted from milliseconds to seconds for Memcached (rounded up if not exact).
  - When multiple clients set the same key concurrently, the last write wins.
- **Parameters:**
  - `key` (`String`) — the cache key with which the specified value is to be associated; must not be `null`
  - `value` (`T`) — the value to cache; may be `null` when the Kryo transcoder is active (the default with Kryo on the classpath). With the stock spymemcached `SerializingTranscoder` (Kryo absent), a `null` value throws `IllegalArgumentException`
  - `liveTime` (`long`) — the time-to-live in milliseconds (`0` or negative means no expiration); a positive value is converted to seconds, rounded up if not exact; values over 30 days are stored as an absolute expiration timestamp. A TTL whose absolute expiration would exceed epoch second 2^31-1 (January 2038) is rejected with `IllegalArgumentException` (memcached expirations are 32-bit).
- **Returns:** `true` if the operation succeeded; `false` otherwise
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, is empty, exceeds 250 UTF-8 bytes, or contains a space, CR, LF, or NUL byte; if `value` is `null` while the stock SerializingTranscoder is active; if `liveTime` exceeds the supported relative or absolute expiration range; or if the encoded value exceeds the active transcoder's maximum item size
  - `java.lang.RuntimeException` — if `value` cannot be serialized (for example, it is not serializable by the active transcoder or its encoded form exceeds the maximum item size), or if the operation fails or is cancelled, or the response wait times out or is interrupted
  - `java.lang.StackOverflowError` — if the active Kryo transcoder encounters a cycle made only of collections, maps, or arrays in `value`
- **Examples:**
  ```java
  // Cache with 1 hour TTL
  User user = new User("John", "john@example.com");
  boolean success = cache.put("user:123", user, 3600000); // 3600000ms -> 3600s TTL; returns true on success
  if (success) {
      System.out.println("User cached successfully"); // printed when the store succeeded
  }

  // Cache a guest user with 30 minute TTL
  User guest = new User("Guest", "guest@example.com");
  cache.put("user:guest", guest, 1800000); // 1800000ms -> 1800s TTL; returns true on success

  // Cache with no expiration
  User admin = loadAdminUser();
  cache.put("user:admin", admin, 0);   // liveTime 0 -> 0s ("no expiration"); returns true on success

  // A null value is accepted when the Kryo transcoder is active (Kryo on the classpath, the
  // default for this wrapper); when the stock SerializingTranscoder is active, the wrapper
  // rejects a null value with IllegalArgumentException. A null key is always rejected.
  cache.put("maybe-null", null, 60000); // returns true on success; stores a null value

  // Updating existing value
  User existing = cache.get("user:456"); // returns null if not cached
  if (existing != null) {
      existing.setEmail("new@example.com");      // mutate the retrieved object before re-storing
      cache.put("user:456", existing, 7200000); // 7200000ms -> 7200s (2-hour) TTL; returns true on success
  }

  cache.put((String) null, user, 3600000); // throws IllegalArgumentException (key must not be null)
  ```
##### asyncPut(...) -> ContinuableFuture<Boolean>
- **Signature:** `public ContinuableFuture<Boolean> asyncPut(final String key, final T value, final long liveTime) throws IllegalStateException, IllegalArgumentException, RuntimeException, StackOverflowError`
- **Summary:** Asynchronously stores an object in the cache with a specified time-to-live.
- **Contract:**
  - The returned Future can be used to check if the operation succeeded.
  - This method returns after validation, synchronous value serialization, and enqueueing without waiting for the server response; enqueueing can still block briefly if the operation queue is full.
  - The liveTime is converted from milliseconds to seconds for Memcached (rounded up if not exact).
- **Parameters:**
  - `key` (`String`) — the cache key with which the specified value is to be associated; must not be `null`
  - `value` (`T`) — the value to cache; may be `null` when the Kryo transcoder is active (the default with Kryo on the classpath). With the stock spymemcached `SerializingTranscoder` (Kryo absent), a `null` value throws `IllegalArgumentException`
  - `liveTime` (`long`) — the time-to-live in milliseconds (`0` or negative means no expiration); a positive value is converted to seconds, rounded up if not exact; values over 30 days are stored as an absolute expiration timestamp. A TTL whose absolute expiration would exceed epoch second 2^31-1 (January 2038) is rejected with `IllegalArgumentException` (memcached expirations are 32-bit).
- **Returns:** a `ContinuableFuture` that will yield `true` on success or `false` on failure
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, is empty, exceeds 250 UTF-8 bytes, or contains a space, CR, LF, or NUL byte; if `value` is `null` while the stock SerializingTranscoder is active; if `liveTime` exceeds the supported relative or absolute expiration range; or if the encoded value exceeds the active transcoder's maximum item size
  - `java.lang.RuntimeException` — if `value` cannot be serialized (for example, it is not serializable by the active transcoder or its encoded form exceeds the maximum item size)
  - `java.lang.StackOverflowError` — if the active Kryo transcoder encounters a cycle made only of collections, maps, or arrays in `value`
- **Examples:**
  ```java
  // Simple async set
  ContinuableFuture<Boolean> storeFuture = cache.asyncPut("user:123", user, 3600000); // dispatched; never null
  boolean stored = storeFuture.get();                                                 // blocks until complete; yields true on success
  if (stored) {
      System.out.println("Set operation succeeded"); // printed when the store succeeded
  }

  // Async set with timeout (requires: import java.util.concurrent.TimeoutException;)
  ContinuableFuture<Boolean> timedStore = cache.asyncPut("user:456", anotherUser, 7200000); // dispatches the store
  try {
      boolean storedInTime = timedStore.get(1000, TimeUnit.MILLISECONDS); // waits up to 1s for completion
  } catch (TimeoutException e) {
      timedStore.cancel(true); // abandon the operation if it did not complete in time
      System.err.println("Set operation timed out"); // printed when the wait elapsed
  }

  cache.asyncPut((String) null, user, 3600000); // throws IllegalArgumentException (key must not be null)
  ```
##### asyncSet(...) -> Future<Boolean>
- **Signature:** `@Deprecated(since = "2.8.5") public Future<Boolean> asyncSet(final String key, final T value, final long liveTime) throws IllegalStateException, IllegalArgumentException, RuntimeException, StackOverflowError`
- **Summary:** Compatibility alias for `#asyncPut(String, Object, long)`.
- **Deprecated:** yes (since 2.8.5) — use `#asyncPut(String, Object, long)`; retained with its original `Future` return descriptor for source and binary compatibility
- **Parameters:**
  - `key` (`String`) — the cache key; must not be `null`
  - `value` (`T`) — the value to cache
  - `liveTime` (`long`) — the time-to-live in milliseconds
- **Returns:** a future that completes with the storage result
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, is empty, exceeds 250 UTF-8 bytes, or contains a space, CR, LF, or NUL byte; if `value` is `null` while the stock SerializingTranscoder is active; if `liveTime` exceeds the supported relative or absolute expiration range; or if the encoded value exceeds the active transcoder's maximum item size
  - `java.lang.RuntimeException` — if `value` cannot be serialized (for example, it is not serializable by the active transcoder or its encoded form exceeds the maximum item size)
  - `java.lang.StackOverflowError` — if the active Kryo transcoder encounters a cycle made only of collections, maps, or arrays in `value`
##### add(...) -> boolean
- **Signature:** `public boolean add(final String key, final T value, final long liveTime) throws IllegalStateException, IllegalArgumentException, RuntimeException, StackOverflowError`
- **Summary:** Adds an object to the cache only if the key doesn't already exist.
- **Contract:**
  - If the key already exists, this operation will fail and return `false`.
  - The liveTime is converted from milliseconds to seconds for Memcached (rounded up if not exact).
- **Parameters:**
  - `key` (`String`) — the cache key with which the specified value is to be associated; must not be `null`
  - `value` (`T`) — the value to cache; may be `null` when the Kryo transcoder is active (the default with Kryo on the classpath). With the stock spymemcached `SerializingTranscoder` (Kryo absent), a `null` value throws `IllegalArgumentException`
  - `liveTime` (`long`) — the time-to-live in milliseconds (`0` or negative means no expiration); a positive value is converted to seconds, rounded up if not exact; values over 30 days are stored as an absolute expiration timestamp. A TTL whose absolute expiration would exceed epoch second 2^31-1 (January 2038) is rejected with `IllegalArgumentException` (memcached expirations are 32-bit).
- **Returns:** `true` if the object was added; `false` if the key already exists
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, is empty, exceeds 250 UTF-8 bytes, or contains a space, CR, LF, or NUL byte; if `value` is `null` while the stock SerializingTranscoder is active; if `liveTime` exceeds the supported relative or absolute expiration range; or if the encoded value exceeds the active transcoder's maximum item size
  - `java.lang.RuntimeException` — if `value` cannot be serialized (for example, it is not serializable by the active transcoder or its encoded form exceeds the maximum item size), or if the operation fails or is cancelled, or the response wait times out or is interrupted
  - `java.lang.StackOverflowError` — if the active Kryo transcoder encounters a cycle made only of collections, maps, or arrays in `value`
- **Examples:**
  ```java
  // Simple conditional add
  if (cache.add("user:123", user, 3600000)) {        // returns true only if "user:123" was absent
      System.out.println("User added successfully"); // printed when the key did not previously exist
  } else {
      System.out.println("User already exists in cache"); // printed when add returned false (key present)
  }

  // Install a shared entry only once (no read-then-write race)
  boolean installed = cache.add("user:system", systemUser, 0); // no expiration; false when already present
  System.out.println(installed ? "System user installed" : "System user already exists");

  cache.add((String) null, user, 3600000); // throws IllegalArgumentException (key must not be null)
  ```
##### asyncAdd(...) -> ContinuableFuture<Boolean>
- **Signature:** `public ContinuableFuture<Boolean> asyncAdd(final String key, final T value, final long liveTime) throws IllegalStateException, IllegalArgumentException, RuntimeException, StackOverflowError`
- **Summary:** Asynchronously adds an object to the cache only if the key doesn't already exist.
- **Contract:**
  - The method returns after validation, synchronous value serialization, and enqueueing without waiting for the server response; enqueueing can still block briefly if the operation queue is full.
  - If the key already exists, the Future will contain `false`.
  - The liveTime is converted from milliseconds to seconds for Memcached (rounded up if not exact).
- **Parameters:**
  - `key` (`String`) — the cache key with which the specified value is to be associated; must not be `null`
  - `value` (`T`) — the value to cache; may be `null` when the Kryo transcoder is active (the default with Kryo on the classpath). With the stock spymemcached `SerializingTranscoder` (Kryo absent), a `null` value throws `IllegalArgumentException`
  - `liveTime` (`long`) — the time-to-live in milliseconds (`0` or negative means no expiration); a positive value is converted to seconds, rounded up if not exact; values over 30 days are stored as an absolute expiration timestamp. A TTL whose absolute expiration would exceed epoch second 2^31-1 (January 2038) is rejected with `IllegalArgumentException` (memcached expirations are 32-bit).
- **Returns:** a `ContinuableFuture` that will yield `true` if the add succeeded, or `false` if the key already exists
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, is empty, exceeds 250 UTF-8 bytes, or contains a space, CR, LF, or NUL byte; if `value` is `null` while the stock SerializingTranscoder is active; if `liveTime` exceeds the supported relative or absolute expiration range; or if the encoded value exceeds the active transcoder's maximum item size
  - `java.lang.RuntimeException` — if `value` cannot be serialized (for example, it is not serializable by the active transcoder or its encoded form exceeds the maximum item size)
  - `java.lang.StackOverflowError` — if the active Kryo transcoder encounters a cycle made only of collections, maps, or arrays in `value`
- **Examples:**
  ```java
  // Simple async add
  ContinuableFuture<Boolean> addFuture = cache.asyncAdd("user:123", user, 3600000); // dispatched; never null
  if (addFuture.get()) {                                                            // blocks; yields true only if the key was absent
      System.out.println("Added");                                                  // printed when the add succeeded
  } else {
      System.out.println("Key already exists"); // printed when add yielded false
  }

  // Async add with timeout (requires: import java.util.concurrent.TimeoutException;)
  ContinuableFuture<Boolean> timedAdd = cache.asyncAdd("user:guest", guest, 1800000); // dispatches the add
  try {
      boolean added = timedAdd.get(500, TimeUnit.MILLISECONDS); // waits up to 500ms
      System.out.println("Add successful: " + added); // true if the key was newly added
  } catch (TimeoutException e) {
      timedAdd.cancel(true); // abandon the operation if it did not complete in time
  }

  cache.asyncAdd((String) null, user, 3600000); // throws IllegalArgumentException (key must not be null)
  ```
##### replace(...) -> boolean
- **Signature:** `public boolean replace(final String key, final T value, final long liveTime) throws IllegalStateException, IllegalArgumentException, RuntimeException, StackOverflowError`
- **Summary:** Replaces an object in the cache only if the key already exists.
- **Contract:**
  - If the key doesn't exist, this operation will fail and return `false`.
  - The liveTime is converted from milliseconds to seconds for Memcached (rounded up if not exact).
- **Parameters:**
  - `key` (`String`) — the cache key with which the specified value is to be associated; must not be `null`
  - `value` (`T`) — the value to cache; may be `null` when the Kryo transcoder is active (the default with Kryo on the classpath). With the stock spymemcached `SerializingTranscoder` (Kryo absent), a `null` value throws `IllegalArgumentException`
  - `liveTime` (`long`) — the time-to-live in milliseconds (`0` or negative means no expiration); a positive value is converted to seconds, rounded up if not exact; values over 30 days are stored as an absolute expiration timestamp. A TTL whose absolute expiration would exceed epoch second 2^31-1 (January 2038) is rejected with `IllegalArgumentException` (memcached expirations are 32-bit).
- **Returns:** `true` if the object was replaced; `false` if the key does not exist
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, is empty, exceeds 250 UTF-8 bytes, or contains a space, CR, LF, or NUL byte; if `value` is `null` while the stock SerializingTranscoder is active; if `liveTime` exceeds the supported relative or absolute expiration range; or if the encoded value exceeds the active transcoder's maximum item size
  - `java.lang.RuntimeException` — if `value` cannot be serialized (for example, it is not serializable by the active transcoder or its encoded form exceeds the maximum item size), or if the operation fails or is cancelled, or the response wait times out or is interrupted
  - `java.lang.StackOverflowError` — if the active Kryo transcoder encounters a cycle made only of collections, maps, or arrays in `value`
- **Examples:**
  ```java
  // Simple replace operation
  if (cache.replace("user:123", updatedUser, 3600000)) { // returns true only if "user:123" already exists
      System.out.println("User updated");                // printed when the key existed and was replaced
  } else {
      System.out.println("User not found in cache"); // printed when replace returned false (key absent)
  }

  // Update existing cache entry
  User user = cache.get("user:456"); // returns null if not cached
  if (user != null) {
      user.setLastAccess(System.currentTimeMillis()); // update the field before replacing
      if (cache.replace("user:456", user, 7200000)) { // 7200000ms -> 7200s TTL; true if the key existed
          System.out.println("User access time updated"); // printed on successful replace
      }
  }

  cache.replace((String) null, updatedUser, 3600000); // throws IllegalArgumentException (key must not be null)
  ```
##### asyncReplace(...) -> ContinuableFuture<Boolean>
- **Signature:** `public ContinuableFuture<Boolean> asyncReplace(final String key, final T value, final long liveTime) throws IllegalStateException, IllegalArgumentException, RuntimeException, StackOverflowError`
- **Summary:** Asynchronously replaces an object in the cache only if the key already exists.
- **Contract:**
  - The method returns after validation, synchronous value serialization, and enqueueing without waiting for the server response; enqueueing can still block briefly if the operation queue is full.
  - If the key doesn't exist, the Future will contain `false`.
  - The liveTime is converted from milliseconds to seconds for Memcached (rounded up if not exact).
- **Parameters:**
  - `key` (`String`) — the cache key with which the specified value is to be associated; must not be `null`
  - `value` (`T`) — the value to cache; may be `null` when the Kryo transcoder is active (the default with Kryo on the classpath). With the stock spymemcached `SerializingTranscoder` (Kryo absent), a `null` value throws `IllegalArgumentException`
  - `liveTime` (`long`) — the time-to-live in milliseconds (`0` or negative means no expiration); a positive value is converted to seconds, rounded up if not exact; values over 30 days are stored as an absolute expiration timestamp. A TTL whose absolute expiration would exceed epoch second 2^31-1 (January 2038) is rejected with `IllegalArgumentException` (memcached expirations are 32-bit).
- **Returns:** a `ContinuableFuture` that will yield `true` if the replacement succeeded, or `false` if the key does not exist
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, is empty, exceeds 250 UTF-8 bytes, or contains a space, CR, LF, or NUL byte; if `value` is `null` while the stock SerializingTranscoder is active; if `liveTime` exceeds the supported relative or absolute expiration range; or if the encoded value exceeds the active transcoder's maximum item size
  - `java.lang.RuntimeException` — if `value` cannot be serialized (for example, it is not serializable by the active transcoder or its encoded form exceeds the maximum item size)
  - `java.lang.StackOverflowError` — if the active Kryo transcoder encounters a cycle made only of collections, maps, or arrays in `value`
- **Examples:**
  ```java
  // Simple async replace
  ContinuableFuture<Boolean> replaceFuture = cache.asyncReplace("user:123", updatedUser, 3600000); // dispatched; never null
  boolean replaced = replaceFuture.get();                                                          // true only if the key already existed
  if (replaced) {
      System.out.println("Replaced successfully"); // printed when the replace succeeded
  }

  // Async replace with timeout (requires: import java.util.concurrent.TimeoutException;)
  ContinuableFuture<Boolean> timedReplace = cache.asyncReplace("user:admin", updatedAdmin, 86400000); // dispatches the replace
  try {
      boolean replacedInTime = timedReplace.get(1000, TimeUnit.MILLISECONDS); // waits up to 1s
      System.out.println("Admin replaced: " + replacedInTime); // true if the key existed
  } catch (TimeoutException e) {
      timedReplace.cancel(true); // abandon the operation if it did not complete in time
  }

  cache.asyncReplace((String) null, updatedUser, 3600000); // throws IllegalArgumentException (key must not be null)
  ```
##### remove(...) -> boolean
- **Signature:** `public boolean remove(final String key) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Removes an object from the cache.
- **Contract:**
  - The return value reflects the server's response: `true` when the key existed and was removed (a `DELETED` response), `false` when the key was not found (`NOT_FOUND`).
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be removed; must not be `null`
- **Returns:** `true` if the key existed and was removed; `false` if the key was not found
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, or fails the memcached client's key validation (empty, longer than 250 UTF-8 bytes, or containing a space, CR, LF, or NUL byte)
  - `java.lang.RuntimeException` — if the operation fails or is cancelled, the response wait times out or is interrupted
- **Examples:**
  ```java
  // Simple delete
  boolean success = cache.remove("user:123");    // true if the key existed and was removed, false if absent
  System.out.println("Key removed: " + success); // prints "Key removed: true" or "Key removed: false"

  // Delete after update
  User user = cache.get("user:456"); // returns null if not cached
  if (user != null && user.isInactive()) {
      cache.remove("user:456"); // returns true if the key existed
  }

  // Delete multiple keys
  String[] keysToDelete = {"session:1", "session:2", "session:3"};
  Arrays.stream(keysToDelete).forEach(cache::remove); // deletes each key in turn

  // Invalidate cache on entity update
  void updateUser(User user) {
      database.save(user); // persist to the source of truth first
      cache.remove("user:" + user.getId());   // invalidate cache; returns true if the entry existed
  }

  cache.remove((String) null); // throws IllegalArgumentException (key must not be null)
  ```
##### asyncRemove(...) -> ContinuableFuture<Boolean>
- **Signature:** `public ContinuableFuture<Boolean> asyncRemove(final String key) throws IllegalStateException, IllegalArgumentException`
- **Summary:** Asynchronously removes an object from the cache.
- **Contract:**
  - The method returns after validation and enqueueing without waiting for the server response; enqueueing can still block briefly if the operation queue is full.
  - The returned Future yields `true` when the key existed and was removed (a `DELETED` response), or `false` when the key was not found (`NOT_FOUND`).
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be removed; must not be `null`
- **Returns:** a `ContinuableFuture` that will yield `true` if the key existed and was removed, or `false` if the key was not found
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, or fails the memcached client's key validation (empty, longer than 250 UTF-8 bytes, or containing a space, CR, LF, or NUL byte)
- **Examples:**
  ```java
  // Simple async delete
  ContinuableFuture<Boolean> removeFuture = cache.asyncRemove("user:123"); // dispatched; never null
  boolean deleted = removeFuture.get();                                    // true if the key existed, false if absent
  if (deleted) {
      System.out.println("Delete operation acknowledged"); // printed when the key was removed
  }

  // Async delete with timeout (requires: import java.util.concurrent.TimeoutException;)
  ContinuableFuture<Boolean> timedRemove = cache.asyncRemove("session:abc"); // dispatches the delete
  try {
      boolean deletedInTime = timedRemove.get(500, TimeUnit.MILLISECONDS); // waits up to 500ms
      System.out.println("Session deleted: " + deletedInTime); // true if the key existed
  } catch (TimeoutException e) {
      timedRemove.cancel(true); // abandon the operation if it did not complete in time
  }

  cache.asyncRemove((String) null); // throws IllegalArgumentException (key must not be null)
  ```
##### asyncDelete(...) -> Future<Boolean>
- **Signature:** `@Deprecated(since = "2.8.5") public Future<Boolean> asyncDelete(final String key) throws IllegalStateException, IllegalArgumentException`
- **Summary:** Compatibility alias for `#asyncRemove(String)`.
- **Deprecated:** yes (since 2.8.5) — use `#asyncRemove(String)`; retained with its original `Future` return descriptor for source and binary compatibility
- **Parameters:**
  - `key` (`String`) — the cache key to remove; must not be `null`
- **Returns:** a future that completes with `true` when the key was removed, or `false` when it was absent
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, or fails the memcached client's key validation (empty, longer than 250 UTF-8 bytes, or containing a space, CR, LF, or NUL byte)
##### incr(...) -> long
- **Signature:** `public long incr(final String key) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Atomically increments a numeric value by 1.
- **Contract:**
  - Memcached-Specific Behavior: If the key doesn't exist, returns -1.
  - The value must be stored as a decimal string representation.
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be incremented; must not be `null`
- **Returns:** the value after the increment, or `-1` if the key does not exist (the client maps memcached's `NOT_FOUND` response to `-1`)
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, or fails the memcached client's key validation (empty, longer than 250 UTF-8 bytes, or containing a space, CR, LF, or NUL byte)
  - `java.lang.RuntimeException` — if the operation fails or is cancelled, the response wait times out or is interrupted, or the key holds a value that is not an ASCII decimal counter (e.g. a value previously stored via `put`)
- **Examples:**
  ```java
  // Simple counter
  long pageViews = cache.incr("page:views"); // returns value+1, or -1 if "page:views" does not exist
  if (pageViews != -1) {
      System.out.println("Page views: " + pageViews); // printed when the key existed
  } else {
      // Initialize the counter via the auto-seeding overload. Do NOT seed counters with
      // put(key, "1", ...): put() serializes through the configured transcoder (Kryo when
      // available), producing bytes that memcached's native incr/decr cannot mutate.
      pageViews = cache.incr("page:views", 1, 1); // seeds absent key with 1; returns 1
  }

  // Rate limiting (auto-seeding overload initializes absent keys)
  String key = "rate:limit:" + userId;
  long attempts = cache.incr(key, 1, 1, 60000); // seeds absent key with 1 (60s TTL); else returns value+1
  if (attempts > MAX_ATTEMPTS) {
      throw new RateLimitException("Too many requests");
  }

  cache.incr((String) null); // throws IllegalArgumentException (key must not be null)
  ```
- **Signature:** `public long incr(final String key, final long delta) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Atomically increments a numeric value by a specified amount.
- **Contract:**
  - Memcached-Specific Behavior: If the key doesn't exist, returns -1.
  - The value must be stored as a decimal string representation.
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be incremented; must not be `null`
  - `delta` (`long`) — the amount by which to increment the value; must be non-negative
- **Returns:** the value after the increment, or `-1` if the key does not exist (the client maps memcached's `NOT_FOUND` response to `-1`)
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, is empty, exceeds 250 UTF-8 bytes, or contains a space, CR, LF, or NUL byte; or if `delta` is negative
  - `java.lang.RuntimeException` — if the operation fails or is cancelled, the response wait times out or is interrupted, or the key holds a value that is not an ASCII decimal counter (e.g. a value previously stored via `put`)
- **Examples:**
  ```java
  // Game score increment
  long score = cache.incr("player:score", 10); // returns value+10, or -1 if the key is absent
  if (score != -1) {
      System.out.println("New score: " + score); // printed when the key existed
  } else {
      // Initialize via the auto-seeding overload. Do NOT seed counters with put(): it
      // serializes through the configured transcoder (Kryo when available), producing
      // bytes that memcached's native incr/decr cannot mutate.
      score = cache.incr("player:score", 10, 10); // seeds absent key with 10; returns 10
  }

  // Batch processing counter
  long processed = cache.incr("batch:processed", 100); // returns value+100, or -1 if absent

  // Points system
  int points = calculatePoints(action);
  long totalPoints = cache.incr("user:points:" + userId, points); // returns value+points, or -1 if absent

  // Bandwidth tracking
  long bytesTransferred = cache.incr("bandwidth:today", fileSize); // returns value+fileSize, or -1 if absent
  if (bytesTransferred > QUOTA) {
      logger.warn("Bandwidth quota exceeded"); // emitted when the running total exceeds QUOTA
  }

  cache.incr("counter", -1);        // throws IllegalArgumentException (delta must be non-negative)
  cache.incr((String) null, 1);     // throws IllegalArgumentException (key must not be null)
  ```
- **Signature:** `public long incr(final String key, final long delta, final long defaultValue) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Increments a numeric value, initializing it to `defaultValue` when the key doesn't exist.
- **Contract:**
  - Memcached-Specific Behavior: Unlike `#incr(String)` and `#incr(String, long)`, which return `-1` when the key is absent, this overload first-writes the key with `defaultValue` when missing.
  - Per this wrapper's ASCII-protocol seeding contract, the increment is NOT applied on the initial insert: when the key is absent the stored value is `defaultValue` and that same `defaultValue` is returned (not `defaultValue + delta`).
  - The `delta` only takes effect on subsequent calls when the key already exists.
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be incremented; must not be `null`
  - `delta` (`long`) — the amount by which to increment the value when the key already exists; must be non-negative
  - `defaultValue` (`long`) — the initial value to set if the key does not exist; on first-write this value is stored verbatim (`delta` is NOT added on insert)
- **Returns:** the value after the operation: `defaultValue` if the key did not exist (no delta applied on insert), otherwise the previously stored value plus `delta`; `-1` if the key could neither be found nor seeded (e.g., deleted concurrently between the seeding attempt and the retry)
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, is empty, exceeds 250 UTF-8 bytes, or contains a space, CR, LF, or NUL byte; or if `delta` is negative or `defaultValue` is negative
  - `java.lang.RuntimeException` — if the operation fails or is cancelled, the response wait times out or is interrupted, or the key holds a value that is not an ASCII decimal counter (e.g. a value previously stored via `put`)
- **Examples:**
  ```java
  // First call when key is absent: stores 0, returns 0 (delta is NOT applied on init).
  long count = cache.incr("counter:views", 1, 0); // seeds absent key with 0 (exp 0); returns 0
  // Subsequent call: existing value 0 is incremented by 1, returns 1.
  count = cache.incr("counter:views", 1, 0); // returns 1 (existing 0 + delta 1)

  // Auto-initializing counter
  long requestCount = cache.incr("api:requests:" + endpoint, 1, 0);  // first call returns defaultValue 0
  System.out.println("Request " + requestCount + " to " + endpoint); // prints the post-operation count

  cache.incr("counter", -1, 0L);    // throws IllegalArgumentException (delta must be non-negative)
  cache.incr((String) null, 1, 0L); // throws IllegalArgumentException (key must not be null)
  ```
- **Signature:** `public long incr(final String key, final long delta, final long defaultValue, final long liveTime) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Increments a numeric value, initializing it to `defaultValue` with the given expiration when the key doesn't exist.
- **Contract:**
  - The `liveTime` is converted from milliseconds to seconds for Memcached (rounded up if not exact).
  - Memcached-Specific Behavior: Unlike `#incr(String)` and `#incr(String, long)`, which return `-1` when the key is absent, this overload first-writes the key with `defaultValue` when missing.
  - Per this wrapper's ASCII-protocol seeding contract, the increment is NOT applied on the initial insert: when the key is absent the stored value is `defaultValue` and that same `defaultValue` is returned (not `defaultValue + delta`).
  - The `delta` only takes effect on subsequent calls when the key already exists.
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be incremented; must not be `null`
  - `delta` (`long`) — the amount by which to increment the value when the key already exists; must be non-negative
  - `defaultValue` (`long`) — the initial value to set if the key does not exist; on first-write this value is stored verbatim (`delta` is NOT added on insert)
  - `liveTime` (`long`) — the time-to-live in milliseconds for the key (`0` or negative means no expiration); a positive value is converted to seconds, rounded up if not exact; values over 30 days are stored as an absolute expiration timestamp. A TTL whose absolute expiration would exceed epoch second 2^31-1 (January 2038) is rejected with `IllegalArgumentException` (memcached expirations are 32-bit).
- **Returns:** the value after the operation: `defaultValue` if the key did not exist (no delta applied on insert), otherwise the previously stored value plus `delta`; `-1` if the key could neither be found nor seeded (e.g., deleted concurrently between the seeding attempt and the retry)
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, is empty, exceeds 250 UTF-8 bytes, or contains a space, CR, LF, or NUL byte; or if `delta` is negative or `defaultValue` is negative; or if `liveTime` exceeds the supported relative or absolute expiration range
  - `java.lang.RuntimeException` — if the operation fails or is cancelled, the response wait times out or is interrupted, or the key holds a value that is not an ASCII decimal counter (e.g. a value previously stored via `put`)
- **Examples:**
  ```java
  // Hourly counter that expires after 1 hour. First call when key is absent stores 0 and
  // returns 0; subsequent calls increment by 1.
  long count = cache.incr("counter:hourly", 1, 0, 3600000); // 3600000ms -> 3600s TTL; first call returns 0
  System.out.println("Hourly count: " + count);             // prints the post-operation count

  // Daily quota counter
  long dailyRequests = cache.incr("quota:daily:" + userId, 1, 0, 86400000); // 86400000ms -> 86400s TTL; first call returns 0
  if (dailyRequests > DAILY_LIMIT) {
      throw new QuotaExceededException("Daily limit reached");
  }

  cache.incr("counter", -1, 0L, 1000L);    // throws IllegalArgumentException (delta must be non-negative)
  cache.incr((String) null, 1, 0L, 1000L); // throws IllegalArgumentException (key must not be null)
  ```
##### decr(...) -> long
- **Signature:** `public long decr(final String key) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Atomically decrements a numeric value by 1.
- **Contract:**
  - Memcached-Specific Behavior: If the key doesn't exist, returns -1.
  - The value must be stored as a decimal string representation, and must be below 263 — a stored value at or above that is valid to the server but unparseable by this client, and every subsequent access fails and tears down the connection.
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be decremented; must not be `null`
- **Returns:** the value after the decrement (cannot be negative due to Memcached's underflow clamping), or `-1` if the key does not exist
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, or fails the memcached client's key validation (empty, longer than 250 UTF-8 bytes, or containing a space, CR, LF, or NUL byte)
  - `java.lang.RuntimeException` — if the operation fails or is cancelled, the response wait times out or is interrupted, or the key holds a value that is not an ASCII decimal counter (e.g. a value previously stored via `put`)
- **Examples:**
  ```java
  // Token bucket rate limiting (the auto-seeding overload initializes absent keys; never
  // seed counters with put() - it stores transcoder-encoded bytes that incr/decr cannot mutate)
  long remainingTokens = cache.decr("api:tokens:" + userId, 1, 100, 60000); // seeds absent key with 100 (60s TTL)
  if (remainingTokens == 0) {
      throw new RateLimitException("Rate limit exceeded");
  }

  // Inventory management
  long stock = cache.decr("product:stock:123"); // returns value-1 (never below 0), or -1 if absent
  if (stock == -1) {
      // Handle key not found
      throw new KeyNotFoundException();
  } else if (stock == 0) {
      // Memcached prevents going below 0
      throw new OutOfStockException();
  }

  cache.decr((String) null); // throws IllegalArgumentException (key must not be null)
  ```
- **Signature:** `public long decr(final String key, final long delta) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Atomically decrements a numeric value by a specified amount.
- **Contract:**
  - Memcached-Specific Behavior: If the key doesn't exist, returns -1.
  - Values cannot go below 0 (Memcached prevents underflow - if delta is larger than the current value, the result will be 0, not negative).
  - The value must be stored as a decimal string representation, and must be below 263 — a stored value at or above that is valid to the server but unparseable by this client, and every subsequent access fails and tears down the connection.
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be decremented; must not be `null`
  - `delta` (`long`) — the amount by which to decrement the value; must be non-negative
- **Returns:** the value after the decrement (cannot be negative due to Memcached's underflow clamping), or `-1` if the key does not exist
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, is empty, exceeds 250 UTF-8 bytes, or contains a space, CR, LF, or NUL byte; or if `delta` is negative
  - `java.lang.RuntimeException` — if the operation fails or is cancelled, the response wait times out or is interrupted, or the key holds a value that is not an ASCII decimal counter (e.g. a value previously stored via `put`)
- **Examples:**
  ```java
  // Bulk inventory decrement (the auto-seeding overload initializes absent keys; never seed
  // counters with put() - it stores transcoder-encoded bytes that incr/decr cannot mutate, and
  // counters cannot be read back with get())
  long inventory = cache.decr("product:stock:456", 5); // returns value-5 (clamped at 0), or -1 if absent
  if (inventory == -1) {
      // Key doesn't exist
      throw new KeyNotFoundException();
  } else if (inventory == 0) {
      // Either reached 0 or underflow prevented (delta > original value)
      System.out.println("Inventory depleted or insufficient"); // printed when the result clamped to 0
  } else {
      System.out.println("Remaining inventory: " + inventory); // prints the post-decrement value
  }

  // API quota management
  int requestCost = calculateCost(request);
  long quotaRemaining = cache.decr("quota:" + apiKey, requestCost); // returns value-requestCost (>=0), or -1 if absent
  if (quotaRemaining == -1) {
      throw new KeyNotFoundException();
  } else if (quotaRemaining == 0) {
      // Quota exhausted or exceeded
      throw new QuotaExceededException();
  }

  cache.decr("counter", -1);    // throws IllegalArgumentException (delta must be non-negative)
  cache.decr((String) null, 1); // throws IllegalArgumentException (key must not be null)
  ```
- **Signature:** `public long decr(final String key, final long delta, final long defaultValue) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Decrements a numeric value, initializing it to `defaultValue` when the key doesn't exist.
- **Contract:**
  - Memcached-Specific Behavior: Unlike `#decr(String)` and `#decr(String, long)`, which return `-1` when the key is absent, this overload first-writes the key with `defaultValue` when missing.
  - Per this wrapper's ASCII-protocol seeding contract, the decrement is NOT applied on the initial insert: when the key is absent the stored value is `defaultValue` and that same `defaultValue` is returned (not `defaultValue - delta`).
  - The `delta` only takes effect on subsequent calls when the key already exists.
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be decremented; must not be `null`
  - `delta` (`long`) — the amount by which to decrement the value when the key already exists; must be non-negative
  - `defaultValue` (`long`) — the initial value to set if the key does not exist; on first-write this value is stored verbatim (`delta` is NOT subtracted on insert)
- **Returns:** the value after the operation: `defaultValue` if the key did not exist (no delta applied on insert), otherwise the previously stored value minus `delta`, clamped at `0`; `-1` if the key could neither be found nor seeded (e.g., deleted concurrently between the seeding attempt and the retry)
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, is empty, exceeds 250 UTF-8 bytes, or contains a space, CR, LF, or NUL byte; or if `delta` is negative or `defaultValue` is negative
  - `java.lang.RuntimeException` — if the operation fails or is cancelled, the response wait times out or is interrupted, or the key holds a value that is not an ASCII decimal counter (e.g. a value previously stored via `put`)
- **Examples:**
  ```java
  // First call when key is absent: stores 100, returns 100 (delta NOT applied on init).
  long remaining = cache.decr("inventory:item123", 1, 100); // seeds absent key with 100 (exp 0); returns 100
  // Subsequent call: existing value 100 is decremented by 1, returns 99.
  remaining = cache.decr("inventory:item123", 1, 100); // returns 99 (existing 100 - delta 1)

  // Auto-initializing quota
  long quotaRemaining = cache.decr("quota:user:" + userId, 1, 1000); // first call returns defaultValue 1000
  if (quotaRemaining == 0) {
      System.out.println("Quota exhausted"); // printed when the value clamped to 0
  }

  cache.decr("counter", -1, 0L);    // throws IllegalArgumentException (delta must be non-negative)
  cache.decr((String) null, 1, 0L); // throws IllegalArgumentException (key must not be null)
  ```
- **Signature:** `public long decr(final String key, final long delta, final long defaultValue, final long liveTime) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Decrements a numeric value, initializing it to `defaultValue` with the given expiration when the key doesn't exist.
- **Contract:**
  - The `liveTime` is converted from milliseconds to seconds for Memcached (rounded up if not exact).
  - Memcached-Specific Behavior: Unlike `#decr(String)` and `#decr(String, long)`, which return `-1` when the key is absent, this overload first-writes the key with `defaultValue` when missing.
  - Per this wrapper's ASCII-protocol seeding contract, the decrement is NOT applied on the initial insert: when the key is absent the stored value is `defaultValue` and that same `defaultValue` is returned (not `defaultValue - delta`).
  - The `delta` only takes effect on subsequent calls when the key already exists.
- **Parameters:**
  - `key` (`String`) — the cache key whose associated value is to be decremented; must not be `null`
  - `delta` (`long`) — the amount by which to decrement the value when the key already exists; must be non-negative
  - `defaultValue` (`long`) — the initial value to set if the key does not exist; on first-write this value is stored verbatim (`delta` is NOT subtracted on insert)
  - `liveTime` (`long`) — the time-to-live in milliseconds for the key (`0` or negative means no expiration); a positive value is converted to seconds, rounded up if not exact; values over 30 days are stored as an absolute expiration timestamp. A TTL whose absolute expiration would exceed epoch second 2^31-1 (January 2038) is rejected with `IllegalArgumentException` (memcached expirations are 32-bit).
- **Returns:** the value after the operation: `defaultValue` if the key did not exist (no delta applied on insert), otherwise the previously stored value minus `delta`, clamped at `0`; `-1` if the key could neither be found nor seeded (e.g., deleted concurrently between the seeding attempt and the retry)
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the operation cannot be enqueued because the operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `key` is `null`, contains an unpaired UTF-16 surrogate, is empty, exceeds 250 UTF-8 bytes, or contains a space, CR, LF, or NUL byte; or if `delta` is negative or `defaultValue` is negative; or if `liveTime` exceeds the supported relative or absolute expiration range
  - `java.lang.RuntimeException` — if the operation fails or is cancelled, the response wait times out or is interrupted, or the key holds a value that is not an ASCII decimal counter (e.g. a value previously stored via `put`)
- **Examples:**
  ```java
  // Daily quota that expires after 24 hours. First call when key is absent stores 1000 and
  // returns 1000; subsequent calls decrement by 1.
  long remaining = cache.decr("quota:user:123", 1, 1000, 86400000); // 86400000ms -> 86400s TTL; first call returns 1000
  System.out.println("Remaining quota: " + remaining);              // prints the post-operation value

  // Hourly rate limit
  long hourlyLimit = cache.decr("rate:hourly:" + userId, 1, 100, 3600000); // 3600000ms -> 3600s TTL; first call returns 100
  if (hourlyLimit == 0) {
      throw new RateLimitException("Hourly limit reached");
  }

  cache.decr("counter", -1, 0L, 1000L);    // throws IllegalArgumentException (delta must be non-negative)
  cache.decr((String) null, 1, 0L, 1000L); // throws IllegalArgumentException (key must not be null)
  ```
##### flushAll(...) -> varies by overload
- **Signature:** `public void flushAll() throws IllegalStateException, RuntimeException`
- **Summary:** Requests an immediate flush from every connected Memcached server.
- **Contract:**
  - Partial flushes: the command is enqueued to each server in turn, so if enqueueing fails for one server of a multi-server configuration, servers enqueued before it can still be flushed although this method throws.
  - If the response wait times out or is interrupted, the flush is cancelled, which stops it only on servers it has not been sent to yet.
- **Parameters:**
  - (none)
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, if the flush operations cannot be enqueued because an operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space, or if the flush is not reported successful. Note that with multiple servers, spymemcached aggregates the per-server outcomes into a single last-writer-wins flag, so one server's failure can be masked by a later server's success; the exception is therefore guaranteed only for single-server configurations
  - `java.lang.RuntimeException` — if the operation fails or is cancelled, the response wait times out or is interrupted
- **Examples:**
  ```java
  // WARNING: This can remove ALL data from every configured cache server!
  cache.flushAll();                              // blocks for spymemcached's aggregate result; see multi-server caveat below
  System.out.println("Flush command completed"); // verify servers individually if full-cluster proof is required

  // Safe usage in testing
  @AfterEach
  public void cleanupCache() {
      if (isTestEnvironment()) {
          cache.flushAll(); // requests a flush from every configured server
      }
  }

  // Production usage with confirmation
  public void clearCache(String confirmationToken) {
      if (!"CONFIRM_FLUSH_ALL".equals(confirmationToken)) {
          throw new IllegalArgumentException("Invalid confirmation");
      }
      logger.warn("Flushing all cache data"); // audit the impending destructive flush
      cache.flushAll(); // requests a flush from every configured server
      auditLog.record("CACHE_FLUSH_ALL", user); // record the destructive action
  }
  ```
- **Signature:** `public boolean flushAll(final long delay) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Requests a delayed flush from every connected Memcached server.
- **Contract:**
  - The delay is converted from milliseconds to seconds for Memcached (rounded up if not exact).
- **Parameters:**
  - `delay` (`long`) — the delay in milliseconds before the flush operation is executed; a positive value is converted to seconds, rounded up if not exact. A value of `0` or negative flushes immediately.
- **Returns:** the final server callback's result; in a single-server configuration, `true` means the flush was scheduled successfully and `false` means it was not
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the flush operations cannot be enqueued because an operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `delay` is large enough that its absolute expiration timestamp would exceed memcached's 32-bit expiration limit (roughly beyond the year 2038)
  - `java.lang.RuntimeException` — if the operation fails or is cancelled, the response wait times out or is interrupted
- **Examples:**
  ```java
  // Schedule a flush to happen in 5 seconds
  boolean scheduled = cache.flushAll(5000); // 5000ms -> relative delay of 5s; single-server result
  if (scheduled) {
      System.out.println("Flush scheduled for 5 seconds from now"); // printed when scheduling succeeded
  }

  // Like set/add/replace expirations, delays over 30 days are sent as an absolute
  // now+delay Unix timestamp (memcached's protocol rule for all expiration values).
  cache.flushAll(3_456_000_000L); // 40 days -> absolute timestamp 40 days from now

  // Delayed flush for maintenance window
  long delayUntilMaintenance = calculateDelayToMaintenance();
  boolean maintenanceScheduled = cache.flushAll(delayUntilMaintenance); // converts ms to seconds (rounded up)
  if (maintenanceScheduled) {
      logger.info("Cache flush scheduled for maintenance window"); // emitted once scheduling succeeded
  }
  ```
##### asyncFlushAll(...) -> ContinuableFuture<Boolean>
- **Signature:** `public ContinuableFuture<Boolean> asyncFlushAll() throws IllegalStateException`
- **Summary:** Asynchronously requests a flush from every connected Memcached server.
- **Contract:**
  - The method returns after enqueueing without waiting for server responses; enqueueing can still block briefly if the operation queue is full.
  - The returned Future can be used to check when the operation completes.
- **Parameters:**
  - (none)
- **Returns:** a `ContinuableFuture` that yields the final server callback's result; for one server, `true` means the flush was accepted and `false` means it was not
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the flush operations cannot be enqueued because an operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
- **Examples:**
  ```java
  // Simple async flush
  ContinuableFuture<Boolean> flushFuture = cache.asyncFlushAll(); // dispatched; never null
  boolean flushed = flushFuture.get();                            // see the multi-server result limitation above
  if (flushed) {
      System.out.println("Final server callback reported success");
  }

  // Async flush with timeout (requires: import java.util.concurrent.TimeoutException;)
  ContinuableFuture<Boolean> timedFlush = cache.asyncFlushAll(); // dispatches the flush
  try {
      boolean flushedInTime = timedFlush.get(2000, TimeUnit.MILLISECONDS); // waits up to 2s
      System.out.println("Flush completed: " + flushedInTime); // final callback's result
  } catch (TimeoutException e) {
      timedFlush.cancel(true); // stops the flush on servers it was not sent to yet; others stay flushed
      logger.warn("Flush operation timed out"); // emitted if the result was not ready within 2s
  }
  ```
- **Signature:** `public ContinuableFuture<Boolean> asyncFlushAll(final long delay) throws IllegalStateException, IllegalArgumentException`
- **Summary:** Asynchronously schedules a flush after a delay.
- **Contract:**
  - The method returns after enqueueing without waiting for server responses; enqueueing can still block briefly if the operation queue is full.
  - The delay is converted from milliseconds to seconds for Memcached (rounded up if not exact).
- **Parameters:**
  - `delay` (`long`) — the delay in milliseconds before the flush operation is executed; a positive value is converted to seconds, rounded up if not exact. A value of `0` or negative flushes immediately.
- **Returns:** a `ContinuableFuture` that yields the final server callback's result; for one server, `true` means the flush was scheduled and `false` means it was not
- **Throws:**
  - `java.lang.IllegalStateException` — if this client has been disconnected or is being disconnected, or if the flush operations cannot be enqueued because an operation queue remains full beyond the client's maximum block time or the calling thread is interrupted before or while waiting for queue space
  - `java.lang.IllegalArgumentException` — if `delay` is large enough that its absolute expiration timestamp would exceed memcached's 32-bit expiration limit (roughly beyond the year 2038)
- **Examples:**
  ```java
  // Schedule a flush to happen in 10 seconds
  ContinuableFuture<Boolean> scheduleFuture = cache.asyncFlushAll(10000); // 10000ms -> relative delay of 10s
  boolean scheduled = scheduleFuture.get();                               // final callback's result

  // Like set/add/replace expirations, delays over 30 days are sent as an absolute
  // now+delay Unix timestamp (memcached's protocol rule for all expiration values).
  cache.asyncFlushAll(3_456_000_000L); // 40 days -> absolute timestamp 40 days from now

  // Async delayed flush with timeout (requires: import java.util.concurrent.TimeoutException;)
  ContinuableFuture<Boolean> timedSchedule = cache.asyncFlushAll(30000); // 30000ms -> relative delay of 30s
  try {
      boolean scheduledInTime = timedSchedule.get(1000, TimeUnit.MILLISECONDS); // waits up to 1s
      System.out.println("Flush scheduled: " + scheduledInTime);                // final callback's result
  } catch (TimeoutException e) {
      timedSchedule.cancel(true);              // stops the request only on servers it was not sent to yet
      logger.warn("Failed to schedule flush"); // emitted if confirmation was not received within 1s
  }
  ```
##### disconnect(...) -> void
- **Signature:** `public synchronized void disconnect()`
- **Summary:** Disconnects from all Memcached servers and releases resources.
- **Contract:**
  - After this method returns, the cache client must not be used again; any subsequent operations will fail.
  - Operations still pending when the delegate shuts down are neither completed nor cancelled by it; a thread waiting on one, synchronously or through a returned future, is released only when its wait bound (the operation timeout by default) elapses.
  - Once called, no other operations should be attempted on this client instance.
  - It releases network connections, thread pools, and other client resources; it should not be invoked after an individual cache operation or request.
- **Parameters:**
  - (none)
- **Examples:**
  ```java
  // Optional Spring Bean destruction for the shared application client
  @PreDestroy
  public void cleanup() {
      if (cache != null) {
          cache.disconnect(); // releases connections and threads; no-op if already shut down
      }
  }

  // Application shutdown hook
  Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Shutting down cache client"); // emitted as the JVM shuts down
      cache.disconnect(); // immediate shutdown of the shared client
  }));
  ```
- **Signature:** `public synchronized void disconnect(final long timeout) throws IllegalArgumentException, RuntimeException`
- **Summary:** Disconnects from all Memcached servers, optionally waiting for responses to pending operations.
- **Contract:**
  - After this method returns, the cache client must not be used again; any subsequent operations will fail.
  - Once called, no other operations should be attempted on this client instance.
- **Parameters:**
  - `timeout` (`long`) — the maximum response-wait time, in milliseconds, after the graceful no-op commands have been enqueued; must not be negative. A value of `0` skips graceful enqueueing and waiting for pending operations. Synchronization, queue admission, and resource teardown are outside this bound (unlike the constructor's operation `timeout`, this parameter permits zero).
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `timeout` is negative
  - `java.lang.RuntimeException` — if the calling thread is interrupted during the graceful wait, or if the no-op operations used for that wait cannot be enqueued (`IllegalStateException`); the client is shut down in either case. With a positive timeout, a caller whose interrupt status is already set is refused at that enqueue step, and, as for cache operations, the refusal carries an `InterruptedException` cause. Whenever the cause is `InterruptedException`, the calling thread's interrupt status is set when this method throws.
- **Examples:**
  ```java
  // Optional application shutdown with a graceful response timeout
  public void shutdown() {
      logger.info("Shutting down cache client");     // emitted before the graceful shutdown
      cache.disconnect(10000);                       // response wait: up to 10s; queueing and teardown add time
      logger.info("Cache client shutdown complete"); // emitted after disconnect returns
  }

  cache.disconnect(0);  // skips the graceful wait; synchronization and teardown can still take time
  cache.disconnect(-1); // throws IllegalArgumentException (timeout must not be negative)
  ```

### Class cs (com.landawn.abacus.cache.cs)
Internal constants container providing the standardized parameter-name string literals used by argument-validation calls (e.g., `N.checkArgNotNull(key, cs.key)`) throughout the cache module (including `com.landawn.abacus.util.MemcachedLock`).

**Thread-safety:** unspecified
**Nullability:** unspecified

#### Public Constructors
- (none)

#### Public Fields
- `public static final String action` — Parameter name `"action"`.
  - **Value:** `"action"`
- `public static final String bytes` — Parameter name `"bytes"`.
  - **Value:** `"bytes"`
- `public static final String cache` — Parameter name `"cache"`.
  - **Value:** `"cache"`
- `public static final String capacity` — Parameter name `"capacity"`.
  - **Value:** `"capacity"`
- `public static final String capacityInMB` — Parameter name `"capacityInMB"`.
  - **Value:** `"capacityInMB"`
- `public static final String client` — Parameter name `"client"`.
  - **Value:** `"client"`
- `public static final String cluster` — Parameter name `"cluster"`.
  - **Value:** `"cluster"`
- `public static final String connFactory` — Parameter name `"connFactory"`.
  - **Value:** `"connFactory"`
- `public static final String defaultValue` — Parameter name `"defaultValue"`.
  - **Value:** `"defaultValue"`
- `public static final String delta` — Parameter name `"delta"`.
  - **Value:** `"delta"`
- `public static final String deserializer` — Parameter name `"deserializer"`.
  - **Value:** `"deserializer"`
- `public static final String entries` — Parameter name `"entries"`.
  - **Value:** `"entries"`
- `public static final String evictDelay` — Parameter name `"evictDelay"`.
  - **Value:** `"evictDelay"`
- `public static final String function` — Parameter name `"function"`.
  - **Value:** `"function"`
- `public static final String future` — Parameter name `"future"`.
  - **Value:** `"future"`
- `public static final String key` — Parameter name `"key"`.
  - **Value:** `"key"`
- `public static final String keyBytes` — Parameter name `"keyBytes"`.
  - **Value:** `"keyBytes"`
- `public static final String keys` — Parameter name `"keys"`.
  - **Value:** `"keys"`
- `public static final String kryoParser` — Parameter name `"kryoParser"`.
  - **Value:** `"kryoParser"`
- `public static final String liveTime` — Parameter name `"liveTime"`.
  - **Value:** `"liveTime"`
- `public static final String logger` — Parameter name `"logger"`.
  - **Value:** `"logger"`
- `public static final String mappingFunction` — Parameter name `"mappingFunction"`.
  - **Value:** `"mappingFunction"`
- `public static final String maxFailuresBeforeCircuitOpen` — Parameter name `"maxFailuresBeforeCircuitOpen"`.
  - **Value:** `"maxFailuresBeforeCircuitOpen"`
- `public static final String maxSize` — Parameter name `"maxSize"`.
  - **Value:** `"maxSize"`
- `public static final String pool` — Parameter name `"pool"`.
  - **Value:** `"pool"`
- `public static final String remappingFunction` — Parameter name `"remappingFunction"`.
  - **Value:** `"remappingFunction"`
- `public static final String retryDelay` — Parameter name `"retryDelay"`.
  - **Value:** `"retryDelay"`
- `public static final String serializer` — Parameter name `"serializer"`.
  - **Value:** `"serializer"`
- `public static final String serverUrl` — Parameter name `"serverUrl"`.
  - **Value:** `"serverUrl"`
- `public static final String srcBytes` — Parameter name `"srcBytes"`.
  - **Value:** `"srcBytes"`
- `public static final String storeSelector` — Parameter name `"storeSelector"`.
  - **Value:** `"storeSelector"`
- `public static final String target` — Parameter name `"target"`.
  - **Value:** `"target"`
- `public static final String testerForLoadingItemFromDiskToMemory` — Parameter name `"testerForLoadingItemFromDiskToMemory"`.
  - **Value:** `"testerForLoadingItemFromDiskToMemory"`
- `public static final String timeout` — Parameter name `"timeout"`.
  - **Value:** `"timeout"`
- `public static final String value` — Parameter name `"value"`.
  - **Value:** `"value"`

#### Public Static Methods
- (none)

#### Public Instance Methods
- (none)

## com.landawn.abacus.util
### Class MemcachedLock (com.landawn.abacus.util.MemcachedLock)
A distributed lock implementation using Memcached as the coordination service.

**Thread-safety:** thread-safe
**Nullability:** unspecified
**Implements:** `AutoCloseable`

#### Public Constructors
- `public MemcachedLock(final String serverUrl) throws IllegalArgumentException, UncheckedIOException` — Creates a new MemcachedLock instance backed by the specified Memcached server(s).
  - **Throws:**
    - `java.lang.IllegalArgumentException` — if `serverUrl` is `null`, empty, or blank, contains no valid `host:port` addresses (e.g., a missing or out-of-range port), or names a host that cannot be resolved (rejected before any client connection resources are created)
    - `com.landawn.abacus.exception.UncheckedIOException` — if creating the local client selector or sockets fails. Because connections are established asynchronously by the underlying SpyMemcached IO thread, a resolvable but unreachable or down server does not fail construction; operations against it fail later with timeouts.
  ```java
  // Both clients are closed when the block exits.
  try (MemcachedLock<String, String> single = new MemcachedLock<>("localhost:11211");
          MemcachedLock<String, String> multi = new MemcachedLock<>("server1:11211,server2:11211")) {
      // acquire and release leases through single or multi
  }
  ```

#### Public Static Methods
- (none)

#### Public Instance Methods
##### tryLock(...) -> boolean
- **Signature:** `public boolean tryLock(final K target, final long liveTime) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Attempts to acquire a lock on the specified target for the given duration.
- **Contract:**
  - The lock will be automatically released after the specified live time expires, ensuring that locks don't persist indefinitely if a holder crashes or fails to release them.
  - If the lock is already held by another client, this method returns `false` without retrying.
- **Parameters:**
  - `target` (`K`) — the target resource on which to acquire the lock (must not be null)
  - `liveTime` (`long`) — the time-to-live in milliseconds before the lock automatically expires. Must be positive: a zero or non-expiring lock is disallowed because it would risk a permanent deadlock if the holder died before releasing it. This is a deliberate exception to the lenient TTL handling used by the cache `put` APIs. Converted to whole seconds for Memcached, with fractional seconds rounded up; values whose absolute expiration exceeds Memcached's signed 32-bit field are rejected.
- **Returns:** `true` if the lock was successfully acquired, `false` if it's already held
- **Throws:**
  - `java.lang.IllegalStateException` — if this lock client has been closed or is being closed
  - `java.lang.IllegalArgumentException` — if `target` is `null`; if the key derived from `target` (via `toKey`) is `null`, contains an unpaired UTF-16 surrogate, or is rejected by the memcached client — empty, longer than 250 bytes (UTF-8), or containing a space, CR, LF, or NUL byte (the default `toKey` uses the target's string form verbatim, so composite targets whose string representation is JSON-like (maps, beans) typically need a sanitizing `toKey` override); or if `liveTime` is not positive or cannot be represented by Memcached's expiration field
  - `java.lang.RuntimeException` — if key conversion fails, or if the Memcached operation fails. After command dispatch the lock state can be indeterminate: the `add` command may have reached the server even though its response was lost or timed out, in which case the lock IS held server-side (under this client's marker) until the TTL expires unless something releases it (the failed caller cannot tell that it holds the lock). Prefer short TTLs where this matters for availability.
- **Examples:**
  ```java
  try (MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211")) {
      // Basic lock usage with 30-second TTL (30000 ms -> 30 s on the server)
      if (lock.tryLock("resource1", 30000)) {
          try {
              performOperation();
          } finally {
              lock.unlockQuietly("resource1"); // operational release failures are logged
          }
      } else {
          System.out.println("Failed to acquire lock - already held by another process");
      }
  }
  ```
- **See also:** `#tryLock(Object, Object, long)`, `#tryUnlock(Object)`
- **Signature:** `@SuppressWarnings("unchecked") public boolean tryLock(final K target, final V value, final long liveTime) throws IllegalStateException, IllegalArgumentException, RuntimeException, StackOverflowError`
- **Summary:** Attempts to acquire a lock on the specified target with an associated value.
- **Contract:**
  - The requested live time is converted to whole seconds (rounded up if needed) before being sent to Memcached.
  - The implementation uses Memcached's atomic add operation (via mc.add) which only succeeds if the key doesn't already exist, providing mutual exclusion while the lease key remains present.
  - Common use cases for the value parameter: Storing the hostname or IP address of the lock holder Recording the thread ID or process ID that acquired the lock Storing a timestamp of when the lock was acquired Adding contextual information for debugging distributed systems Memcached TTL upper bound: Memcached treats any TTL greater than 30 days (2,592,000 seconds = 2,592,000,000 ms) as an absolute Unix timestamp.
- **Parameters:**
  - `target` (`K`) — the target resource on which to acquire the lock (must not be null)
  - `value` (`V`) — the value to associate with the lock (may be `null`; a `null` value is stored as the same value-less marker used by `#tryLock(Object, long)`)
  - `liveTime` (`long`) — the time-to-live in milliseconds before the lock automatically expires (must be positive; converted to whole seconds for Memcached, with fractional seconds rounded up; rejected if its absolute expiration exceeds Memcached's signed 32-bit field)
- **Returns:** `true` if the lock was successfully acquired, `false` if it's already held
- **Throws:**
  - `java.lang.IllegalStateException` — if this lock client has been closed or is being closed
  - `java.lang.IllegalArgumentException` — if `target` is `null`; if the key derived from `target` (via `toKey`) is `null`, contains an unpaired UTF-16 surrogate, or is rejected by the memcached client — empty, longer than 250 bytes (UTF-8), or containing a space, CR, LF, or NUL byte (the default `toKey` uses the target's string form verbatim, so composite targets whose string representation is JSON-like (maps, beans) typically need a sanitizing `toKey` override); if `liveTime` is not positive or cannot be represented by Memcached's expiration field; or if the client's transcoder rejects `value` before anything is sent (for example, spymemcached's default transcoder rejects a non-`Serializable` value, and either transcoder rejects an encoded form larger than its maximum size), in which case no lock was acquired
  - `java.lang.RuntimeException` — if key conversion fails; if the client's transcoder fails to serialize `value` for another reason (for example, a `KryoException` for a cycle through object fields), in which case nothing was sent and no lock was acquired; or if the Memcached operation fails. A transcoder or Memcached failure is thrown as a plain `RuntimeException` whose cause is the original exception. After command dispatch the lock state can be indeterminate: the `add` command may have reached the server even though its response was lost or timed out, in which case the lock IS held server-side (under this client's value) until the TTL expires unless something releases it (the failed caller cannot tell that it holds the lock). Prefer short TTLs where this matters for availability.
  - `java.lang.StackOverflowError` — if the bundled Kryo transcoder attempts to serialize a cyclic collection, map, or object array in `value`
- **Examples:**
  ```java
  try (MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211")) {
      // Example 1: Store hostname with lock (60000 ms -> 60 s TTL)
      String lockHolder = "server-1"; // stable diagnostic identifier for this process
      if (lock.tryLock("resource1", lockHolder, 60000)) {
          try {
              System.out.println("Lock acquired by: " + lock.get("resource1"));
          } finally {
              lock.unlockQuietly("resource1");
          }
      } else {
          System.out.println("Lock is held by: " + lock.get("resource1"));
      }
  }

  // Example 2: Store structured metadata (V must be a compatible type,
  // e.g., MemcachedLock<String, Map<String, Object>>)
  try (MemcachedLock<String, Map<String, Object>> metaLock = new MemcachedLock<>("localhost:11211")) {
      Map<String, Object> metadata = new HashMap<>();
      metadata.put("host", "server1");
      metadata.put("thread", Thread.currentThread().getName());
      metadata.put("timestamp", System.currentTimeMillis());
      if (metaLock.tryLock("resource2", metadata, 30000)) {
          try {
              Map<String, Object> current = metaLock.get("resource2"); // diagnostic snapshot
          } finally {
              metaLock.unlockQuietly("resource2");
          }
      }
  }
  ```
- **See also:** `#tryLock(Object, long)`, `#get(Object)`, `#tryUnlock(Object)`
##### isLocked(...) -> boolean
- **Signature:** `public boolean isLocked(final K target) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Checks whether a lock is currently held on the specified target.
- **Contract:**
  - Returns `true` if a value exists for the lock key, `false` otherwise.
  - This is a point-in-time check and should not be relied upon for critical synchronization logic.
  - Always use the return value of `#tryLock(Object, long)` or `#tryLock(Object, Object, long)` to determine if you successfully acquired the lock rather than checking first with this method.
  - This method is primarily useful for: Monitoring and diagnostics Logging and alerting when resources are locked Non-critical decision making where race conditions are acceptable Usage Examples:
- **Parameters:**
  - `target` (`K`) — the target resource whose lock status is to be checked (must not be null)
- **Returns:** `true` if the lock is currently held, `false` otherwise
- **Throws:**
  - `java.lang.IllegalStateException` — if this lock client has been closed or is being closed
  - `java.lang.IllegalArgumentException` — if `target` is `null`, or if the key derived from `target` (via `toKey`) is `null`, contains an unpaired UTF-16 surrogate, or is rejected by the memcached client (empty, longer than 250 bytes, or containing a space, CR, LF, or NUL byte)
  - `java.lang.RuntimeException` — if key conversion fails, or if the Memcached read or value decoding fails
- **Examples:**
  ```java
  try (MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211")) {

  // Example 1: Check lock status for monitoring
  if (lock.isLocked("resource1")) {                         // true if the key exists (even a value-less lock counts as held)
      System.out.println("Resource is currently locked");   // reached when isLocked() returned true
  } else {                                                  // false: no key present
      System.out.println("Resource is available");          // reached when isLocked() returned false
  }

  // Example 2: INCORRECT usage - race condition
  if (!lock.isLocked("resource1")) {       // point-in-time check only; not atomic with the lock below
      // Lock could be acquired by another process here!
      boolean acquired = lock.tryLock("resource1", 30000); // another process may have raced in
      if (acquired) lock.unlockQuietly("resource1");       // cleanup; the pre-check was still incorrect
  }

  // Example 3: CORRECT usage - atomic check
  if (lock.tryLock("resource1", 30000)) {     // true: acquired atomically (single add-if-absent attempt)
      try {
          // Lock successfully acquired
      } finally {
          lock.unlockQuietly("resource1");    // operational release failures are logged
      }
  }
  }
  ```
- **See also:** `#tryLock(Object, long)`, `#tryLock(Object, Object, long)`
##### get(...) -> V
- **Signature:** `@SuppressWarnings("unchecked") public V get(final K target) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Retrieves the value associated with a lock on the specified target.
- **Contract:**
  - If no lock exists (key not found), this method returns `null`.
  - If the lock stores an empty byte array (the default when using `#tryLock(Object, long)`), that value-less marker is normalized to `null`, so a value-less lock reads the same as no lock.
  - Returns `null` if the lock doesn't exist or if it stores an empty byte array Usage Examples:
- **Parameters:**
  - `target` (`K`) — the target resource whose associated lock value is to be retrieved (must not be null)
- **Returns:** the value associated with the lock, or `null` if the target is not locked or if the lock stores an empty byte array
- **Throws:**
  - `java.lang.IllegalStateException` — if this lock client has been closed or is being closed
  - `java.lang.IllegalArgumentException` — if `target` is `null`, or if the key derived from `target` (via `toKey`) is `null`, contains an unpaired UTF-16 surrogate, or is rejected by the memcached client (empty, longer than 250 bytes, or containing a space, CR, LF, or NUL byte)
  - `java.lang.RuntimeException` — if key conversion fails, or if the Memcached read or value decoding fails
- **Examples:**
  ```java
  try (MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211")) {

  // Example 1: Check who holds the lock
  String holder = lock.get("resource1");   // the stored value, or null if absent / value-less lock
  if (holder != null) {
      System.out.println("Lock held by: " + holder);            // reached when get() returned a non-null value
  } else {                                                      // null: no lock, or a value-less tryLock(target, liveTime)
      System.out.println("No lock exists or value is empty");   // reached when get() returned null
  }

  // Example 2: Inspect the claimed holder for diagnostics (not as an ownership proof)
  String myId = "server-1"; // stable diagnostic identifier for this process
  if (lock.tryLock("resource2", myId, 60000)) {       // true: acquired, myId stored as the value
      try {
          String observedHolder = lock.get("resource2");
          System.out.println("Claimed holder: " + observedHolder);
          performOperation();
      } finally {
          // Do not perform a read-before-delete here: it is racy and can itself mask a work failure.
          lock.unlockQuietly("resource2");
      }
  }
  }

  // Example 3: Retrieve structured metadata with a matching value type
  try (MemcachedLock<String, Map<String, Object>> metaLock = new MemcachedLock<>("localhost:11211")) {
      Map<String, Object> lockInfo = metaLock.get("resource3"); // stored map, or null if absent
      if (lockInfo != null) {
          System.out.println("Locked by: " + lockInfo.get("host"));
          System.out.println("Thread: " + lockInfo.get("thread"));
      }
  }
  ```
- **See also:** `#tryLock(Object, Object, long)`, `#isLocked(Object)`
##### tryUnlock(...) -> boolean
- **Signature:** `public boolean tryUnlock(final K target) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Releases the lock on the specified target.
- **Contract:**
  - It's important to always unlock in a finally block to ensure locks are released even if exceptions occur.
  - If your TTL is short relative to how long the critical section may take (due to GC pauses, scheduling, network jitter), the original holder's lock can expire and be reacquired by a second client before the first client's unlock call runs — at which point the first client deletes the second client's lock and a third client may immediately acquire it.
  - Interruption: like `java.util.concurrent.locks.Lock#unlock()`, the release is attempted even if the calling thread's interrupt status is already set (for example, in the `finally` block of a cancelled task).
  - Best practices: Release the lock in a finally block to ensure cleanup; in a finally block prefer `#unlockQuietly(Object)`, which logs and returns `false` on a communication error instead of throwing (a throwing `tryUnlock` in finally can mask the exception from the guarded code) Do not rely on a read-before-delete ownership check for correctness; it remains racy Don't assume tryUnlock() always succeeds - check the return value if needed Be aware that locks can expire automatically, so tryUnlock() may return false Usage Examples:
- **Parameters:**
  - `target` (`K`) — the target resource whose lock is to be released (must not be null)
- **Returns:** `true` if an entry was deleted from Memcached for this target (regardless of which client originally acquired the lock), `false` if no entry existed (e.g., the lock had already expired or was never acquired)
- **Throws:**
  - `java.lang.IllegalStateException` — if this lock client has been closed or is being closed
  - `java.lang.IllegalArgumentException` — if `target` is `null`, or if the key derived from `target` (via `toKey`) is `null`, contains an unpaired UTF-16 surrogate, or is rejected by the memcached client (empty, longer than 250 bytes, or containing a space, CR, LF, or NUL byte)
  - `java.lang.RuntimeException` — if key conversion fails, or if the Memcached deletion fails
- **Examples:**
  ```java
  try (MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211")) {
      // Example 1: Inspect the explicit release result when no guarded exception can be masked.
      if (lock.tryLock("release-probe", 30000)) {
          boolean unlocked = lock.tryUnlock("release-probe"); // true: key deleted; false: already expired/removed
          if (!unlocked) {
              System.out.println("Lock may have already expired or been removed");   // reached when tryUnlock() returned false
          }
      }

      // Example 2: In a failure-sensitive finally block, use the quiet counterpart.
      if (lock.tryLock("resource2", 60000)) {
          try {
              performOperation();
          } finally {
              lock.unlockQuietly("resource2");
          }
      }
  }
  ```
- **See also:** `#unlockQuietly(Object)`, `#tryLock(Object, long)`, `#tryLock(Object, Object, long)`
##### unlockQuietly(...) -> boolean
- **Signature:** `public boolean unlockQuietly(final K target) throws IllegalArgumentException, RuntimeException`
- **Summary:** Releases the lock on the specified target without throwing when the Memcached operation fails.
- **Contract:**
  - This is the "quiet" counterpart of `#tryUnlock(Object)`, intended for use inside a `finally` block: if releasing the lock fails because of a network or protocol error, this method logs the failure at `WARN` level and returns `false` instead of throwing, so a communication failure cannot mask an exception thrown by the guarded critical section.
  - As with `tryUnlock`, the release is attempted even if the calling thread's interrupt status is already set, and that status is preserved.
- **Parameters:**
  - `target` (`K`) — the target resource whose lock is to be released (must not be null)
- **Returns:** `true` if an entry was deleted from Memcached for this target; `false` if no entry existed (e.g., the lock had already expired or was never acquired) or if the release failed during the Memcached operation, including because this lock client has been closed (which is logged at `WARN`)
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `target` is `null`, or if the key derived from `target` (via `toKey`) is `null`, contains an unpaired UTF-16 surrogate, or is rejected by the memcached client (empty, longer than 250 bytes, or containing a space, CR, LF, or NUL byte). The key is validated eagerly, so this is thrown even when the lock client has already been closed - a deterministic programming error is never downgraded to the quiet `false`
  - `java.lang.RuntimeException` — if `#toKey(Object)` or conversion of `target` to a string fails before the release attempt; only operational release failures are suppressed
- **Examples:**
  ```java
  try (MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211")) {
      if (lock.tryLock("resource1", 30000)) {                     // true: acquired
          try {
              performOperation();                                 // may throw; that exception must survive
          } finally {
              // Does not throw for a Memcached hiccup, so it cannot suppress the work exception.
              boolean released = lock.unlockQuietly("resource1");
              if (!released) {
                  // already absent, or the failed release was logged and the lease may expire later
                  System.out.println("Lock was not released by this call");
              }
          }
      }
  }
  ```
- **See also:** `#tryUnlock(Object)`, `#tryLock(Object, long)`
##### lock(...) -> boolean
- **Signature:** `@Deprecated(since = "2.8.5", forRemoval = true) public boolean lock(final K target, final long liveTime) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Attempts to acquire a value-less lock on the specified target for the given duration.
- **Deprecated:** yes (since 2.8.5); marked for removal — renamed to `#tryLock(Object, long)` to reflect its single-attempt semantics: it performs one server round-trip but does not poll, retry, or wait for the current holder to release. This alias delegates to it and will be removed in a future release.
- **Parameters:**
  - `target` (`K`) — the target resource on which to acquire the lock (must not be null)
  - `liveTime` (`long`) — the time-to-live in milliseconds before the lock automatically expires; must be positive
- **Returns:** `true` if the lock was successfully acquired, `false` if it is already held
- **Throws:**
  - `java.lang.IllegalStateException` — if this lock client has been closed or is being closed
  - `java.lang.IllegalArgumentException` — if `target` is `null`, if the key derived from `target` (via `toKey`) is invalid as described by `#tryLock(Object, long)`, or if `liveTime` is not positive or cannot be represented by Memcached's expiration field
  - `java.lang.RuntimeException` — if key conversion fails, or if the Memcached operation fails; after dispatch the lock state may be indeterminate, as described by `#tryLock(Object, long)`
- **Signature:** `@Deprecated(since = "2.8.5", forRemoval = true) public boolean lock(final K target, final V value, final long liveTime) throws IllegalStateException, IllegalArgumentException, RuntimeException, StackOverflowError`
- **Summary:** Attempts to acquire a lock on the specified target with an associated value.
- **Deprecated:** yes (since 2.8.5); marked for removal — renamed to `#tryLock(Object, Object, long)` to reflect its single-attempt semantics: it performs one server round-trip but does not poll, retry, or wait for the current holder to release. This alias delegates to it and will be removed in a future release.
- **Parameters:**
  - `target` (`K`) — the target resource on which to acquire the lock (must not be null)
  - `value` (`V`) — the value to associate with the lock (may be `null`)
  - `liveTime` (`long`) — the time-to-live in milliseconds before the lock automatically expires; must be positive
- **Returns:** `true` if the lock was successfully acquired, `false` if it is already held
- **Throws:**
  - `java.lang.IllegalStateException` — if this lock client has been closed or is being closed
  - `java.lang.IllegalArgumentException` — if `target` is `null`, if the key derived from `target` (via `toKey`) is invalid as described by `#tryLock(Object, Object, long)`, if `liveTime` is not positive or cannot be represented by Memcached's expiration field, or if the client's transcoder rejects `value`
  - `java.lang.RuntimeException` — if key conversion or serialization of `value` fails, or if the Memcached operation fails; after dispatch the lock state may be indeterminate, as described by `#tryLock(Object, Object, long)`
  - `java.lang.StackOverflowError` — if the bundled Kryo transcoder attempts to serialize a cyclic collection, map, or object array in `value`
##### unlock(...) -> boolean
- **Signature:** `@Deprecated(since = "2.8.5", forRemoval = true) public boolean unlock(final K target) throws IllegalStateException, IllegalArgumentException, RuntimeException`
- **Summary:** Releases the lock on the specified target.
- **Deprecated:** yes (since 2.8.5); marked for removal — renamed to `#tryUnlock(Object)` to pair with `#tryLock(Object, long)`; this alias delegates to it and will be removed in a future release.
- **Parameters:**
  - `target` (`K`) — the target resource whose lock is to be released (must not be null)
- **Returns:** `true` if an entry was deleted for this target; `false` if no entry existed
- **Throws:**
  - `java.lang.IllegalStateException` — if this lock client has been closed or is being closed
  - `java.lang.IllegalArgumentException` — if `target` is `null`, or if the key derived from `target` (via `toKey`) is invalid as described by `#tryUnlock(Object)`
  - `java.lang.RuntimeException` — if key conversion fails, or if the Memcached deletion fails
##### tryUnlockQuietly(...) -> boolean
- **Signature:** `@Deprecated(since = "2.8.5", forRemoval = true) public boolean tryUnlockQuietly(final K target) throws IllegalArgumentException, RuntimeException`
- **Summary:** Releases the lock on the specified target without throwing when the Memcached operation fails.
- **Deprecated:** yes (since 2.8.5); marked for removal — renamed to `#unlockQuietly(Object)`. The `try` prefix wrongly implied a non-blocking acquisition attempt, whereas this method's distinguishing behavior is swallowing communication errors on release. This alias delegates to it and will be removed in a future release.
- **Parameters:**
  - `target` (`K`) — the target resource whose lock is to be released (must not be null)
- **Returns:** `true` if an entry was deleted for this target; `false` if no entry existed or the release failed (which is logged at `WARN`)
- **Throws:**
  - `java.lang.IllegalArgumentException` — if `target` is `null`, or if the key derived from `target` (via `toKey`) is invalid as described by `#unlockQuietly(Object)`; thrown even when this lock client has already been closed
  - `java.lang.RuntimeException` — if key conversion fails before the release attempt, as described by `#unlockQuietly(Object)`
##### client(...) -> SpyMemcached<V>
- **Signature:** `@SuppressFBWarnings("EI_EXPOSE_REP") public SpyMemcached<V> client()`
- **Summary:** Returns the underlying SpyMemcached client used by this lock.
- **Parameters:**
  - (none)
- **Returns:** the SpyMemcached client instance (never `null`)
- **Examples:**
  ```java
  try (MemcachedLock<String, String> lock = new MemcachedLock<>("localhost:11211")) {
      // Example 1: Access the underlying client for custom operations
      SpyMemcached<String> client = lock.client();       // never null; the same instance on every call
      client.put("custom:key", "value", 60000);          // returns true on success (60000 ms -> 60 s TTL)

      // Example 2: Store metadata alongside lock
      String metadata = client.get("custom:key");        // returns "value" (or null if absent/expired)
      System.out.println("Metadata: " + metadata);       // prints: value

      // Example 3: Perform bulk operations
      Map<String, String> data = new HashMap<>();
      data.put("data:key1", "value1");                   // seed local data map
      data.put("data:key2", "value2");                   // seed local data map
      // Note: Use different key prefix to avoid conflicts with lock keys
      for (Map.Entry<String, String> entry : data.entrySet()) {
          client.put(entry.getKey(), entry.getValue(), 300000); // 300000 ms -> 300 s TTL
      }
  }
  ```
##### close(...) -> void
- **Signature:** `public synchronized void close()`
- **Summary:** Closes the underlying Memcached client and releases all associated resources.
- **Contract:**
  - This method is synchronized and idempotent: it marks the instance closed before invoking `disconnect()`, and later calls are no-ops even if that first disconnect attempt threw.
- **Parameters:**
  - (none)
- **Examples:**
  ```java
  // Example 1: Recommended pattern with try-with-resources
  try (MemcachedLock<String, String> autoLock = new MemcachedLock<>("localhost:11211")) {
      if (autoLock.tryLock("resource", 30000)) {
          try {
              // Critical section
              performOperation();                // your exclusive work runs here
          } finally {
              autoLock.unlockQuietly("resource"); // logs operational release failures
          }
      }
  } // close() runs automatically: disconnects the client; does NOT release any still-held locks

  // Example 2: Manual close (not recommended)
  MemcachedLock<String, String> manualLock = new MemcachedLock<>("localhost:11211");
  try {
      if (manualLock.tryLock("resource", 30000)) {
          try {
              performOperation();                // your exclusive work runs here
          } finally {
              manualLock.unlockQuietly("resource");
          }
      }
  } finally {
      manualLock.close();   // idempotent: disconnects the client; safe to call more than once
  }

  // Example 3: Multiple locks with single client
  try (MemcachedLock<String, String> multiLock = new MemcachedLock<>("localhost:11211")) {
      boolean lock1 = false;
      boolean lock2 = false;

      try {
          // Acquire inside the try so a failure acquiring the second lock still releases the first.
          lock1 = multiLock.tryLock("resource1", 30000);
          lock2 = lock1 && multiLock.tryLock("resource2", 30000);

          if (lock2) {
              // Both locks acquired
              performOperation();                      // your exclusive work runs here
          }
      } finally {
          if (lock2) multiLock.unlockQuietly("resource2");
          if (lock1) multiLock.unlockQuietly("resource1");
      }
  } // close() runs automatically here
  ```

