/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.landawn.abacus.TestBase;

/**
 * Integration tests for {@link DistributedCache} backed by a real {@link SpyMemcached} client talking
 * to a Memcached server reachable at {@code localhost:11211}
 * (e.g. {@code docker run --name memcached -p 11211:11211 -d memcached:latest}).
 *
 * <p>Storage and most lifecycle/key-handling behavior is exercised end-to-end against the live
 * server. A narrowly scoped mock is used for the close-idempotence regression where a deliberately
 * overridden lifecycle accessor must not influence the wrapper's private state. The circuit-breaker
 * tests still need <em>failures</em> from the
 * backend; rather than fake them, they drive a <b>real</b> client that has been shut down — its
 * {@code get} then throws an {@link IllegalStateException} instantly and deterministically — and/or
 * set the breaker's internal counters by reflection while reading a genuinely-present value back from
 * the server.
 */
@Tag("2025")
public class DistributedCacheTest extends TestBase {

    private static final String SERVER_URL = "localhost:11211";
    private static final String DISTRIBUTED_CACHE_LOGGER = "com.landawn.abacus.cache.DistributedCache";

    /** Long-lived client used only to flush the server between tests and to host constructor-validation. */
    private static SpyMemcached<String> flushClient;

    @BeforeAll
    static void connect() {
        flushClient = new SpyMemcached<>(SERVER_URL);
    }

    @AfterAll
    static void disconnect() {
        if (flushClient != null) {
            flushClient.disconnect();
        }
    }

    @BeforeEach
    void flush() {
        flushClient.flushAll();
    }

    private static SpyMemcached<String> newClient() {
        return new SpyMemcached<>(SERVER_URL);
    }

    // --- construction validation (no live operation) -------------------------------------------

    @Test
    public void testConstructor_EdgeCase_NullClient() {
        assertThrows(IllegalArgumentException.class, () -> new DistributedCache<>(null));
    }

    @Test
    public void testConstructor_EdgeCase_NegativeMaxFailed() {
        // The client is a real one but is never used: the argument check fails first.
        assertThrows(IllegalArgumentException.class, () -> new DistributedCache<>(flushClient, "p:", -1, 1000));
    }

    @Test
    public void testConstructor_EdgeCase_NegativeRetryDelay() {
        assertThrows(IllegalArgumentException.class, () -> new DistributedCache<>(flushClient, "p:", 100, -1));
    }

    // --- basic operations against the real server ----------------------------------------------

    @Test
    public void testPutAndGetOrNull() {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());
        try {
            assertTrue(cache.put("k1", "v1", 60_000, 0));
            assertEquals("v1", cache.getOrNull("k1"));
        } finally {
            cache.close();
        }
    }

    @Test
    public void testPut_WithKeyPrefix() {
        final SpyMemcached<String> client = newClient();
        final DistributedCache<String, String> cache = new DistributedCache<>(client, "myapp:");
        try {
            assertTrue(cache.put("k1", "v1", 60_000, 0));
            // Round-trips through the cache...
            assertEquals("v1", cache.getOrNull("k1"));
            // ...and the prefix is actually applied to the key stored on the server.
            final String storedKey = cache.generateKey("k1");
            assertTrue(storedKey.startsWith("myapp:"));
            assertEquals("v1", client.get(storedKey));
        } finally {
            cache.close();
        }
    }

    @Test
    public void testRemove() {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());
        try {
            cache.put("k", "v", 60_000, 0);
            cache.remove("k");
            assertNull(cache.getOrNull("k"));
        } finally {
            cache.close();
        }
    }

    /** Null keys are rejected consistently across put/remove, mirroring the getOrNull coverage. */
    @Test
    public void testPutAndRemove_EdgeCase_NullKey() {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());
        try {
            assertThrows(IllegalArgumentException.class, () -> cache.put(null, "v", 1000, 0));
            assertThrows(IllegalArgumentException.class, () -> cache.remove(null));
        } finally {
            cache.close();
        }
    }

    /** The documented non-String key path: {@code generateKey(12345)} base64-encodes "12345". */
    @Test
    public void testGenerateKey_NonStringKey() {
        final DistributedCache<Integer, String> cache = new DistributedCache<>(newClient());
        try {
            assertEquals("MTIzNDU=", cache.generateKey(12345));
        } finally {
            cache.close();
        }
    }

    @Test
    public void testContainsKey() {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());
        try {
            cache.put("k", "v", 60_000, 0);
            assertTrue(cache.containsKey("k"));
            assertFalse(cache.containsKey("missing"));
        } finally {
            cache.close();
        }
    }

    @Test
    public void testClear() {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());
        try {
            cache.put("k", "v", 0, 0);
            cache.clear();
            assertNull(cache.getOrNull("k"));
        } finally {
            cache.close();
        }
    }

    @Test
    public void testKeySet_Unsupported() {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());
        try {
            final UnsupportedOperationException e = assertThrows(UnsupportedOperationException.class, cache::keySet);
            assertEquals("keySet() is not supported by DistributedCache", e.getMessage());
        } finally {
            cache.close();
        }
    }

    @Test
    public void testSize_Unsupported() {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());
        try {
            final UnsupportedOperationException e = assertThrows(UnsupportedOperationException.class, cache::size);
            assertEquals("size() is not supported by DistributedCache", e.getMessage());
        } finally {
            cache.close();
        }
    }

    @Test
    public void testClose_IsIdempotent() {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());
        cache.close();
        cache.close(); // idempotent
        assertTrue(cache.isClosed());
    }

    /**
     * Internal lifecycle decisions must read the private field, not dispatch through an overridable
     * accessor. A subclass that decorates {@code isClosed()} must not make {@code close()} disconnect
     * the same client repeatedly.
     */
    @Test
    public void testClose_IdempotenceCannotBeDefeatedByIsClosedOverride() {
        final DistributedCacheClient<String> client = mock(DistributedCacheClient.class);
        final DistributedCache<String, String> cache = new DistributedCache<>(client) {
            @Override
            public boolean isClosed() {
                return false;
            }
        };

        cache.close();
        cache.close();

        verify(client, times(1)).disconnect();
    }

    @Test
    public void testOperations_AfterClose_Throw() {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());
        cache.close();
        assertThrows(IllegalStateException.class, () -> cache.getOrNull("k"));
        assertThrows(IllegalStateException.class, () -> cache.put("k", "v", 0, 0));
        assertThrows(IllegalStateException.class, () -> cache.remove("k"));
        assertThrows(IllegalStateException.class, cache::clear);
    }

    @Test
    public void testGenerateKey_EdgeCase_NullKey() {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());
        try {
            assertThrows(IllegalArgumentException.class, () -> cache.getOrNull(null));
        } finally {
            cache.close();
        }
    }

    /**
     * {@code generateKey} rejects a non-null key whose string representation is null. An empty
     * {@link Optional} is non-null yet {@code N.stringOf(Optional.empty())} returns {@code null}.
     * (Uses {@link #flushClient}; {@code generateKey} is a pure transformation and performs no I/O.)
     */
    @Test
    public void testGenerateKey_EdgeCase_NullStringRepresentation() {
        final DistributedCache<Optional<Object>, String> cache = new DistributedCache<>(flushClient);
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> cache.generateKey(Optional.empty()));
        assertTrue(ex.getMessage() != null && ex.getMessage().contains("Key string representation cannot be null"),
                "expected the null-string-representation message but was: " + ex.getMessage());
    }

    /**
     * Base64 encodes an empty byte sequence as an empty string, but Memcached rejects an empty key.
     * The wrapper must therefore use a non-empty marker that cannot collide with standard Base64.
     */
    @Test
    public void testGenerateKey_EmptyStringUsesMemcachedSafeMarker() {
        final DistributedCache<String, String> cache = new DistributedCache<>(flushClient);

        assertEquals("-", cache.generateKey(""));
    }

    /**
     * Local key conversion is deterministic validation and must not be bypassed by an unrelated open
     * backend circuit. Otherwise the same invalid key alternates between throwing and looking absent.
     */
    @Test
    public void testGenerateKey_NullStringRepresentationThrowsEvenWhenCircuitOpen() throws Exception {
        final DistributedCache<Optional<Object>, String> cache = new DistributedCache<>(flushClient, "", 1, 60_000);
        setBreakerState(cache, 1, System.nanoTime());

        assertThrows(IllegalArgumentException.class, () -> cache.getOrNull(Optional.empty()));
    }

    // --- circuit breaker -----------------------------------------------------------------------

    /**
     * Regression guard for the torn-state race: the failure count and its timestamp must be
     * published through one atomic holder, not through independently writable atomics.
     */
    @Test
    public void testCircuitBreaker_CountAndTimestampUseOneAtomicState() throws Exception {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient());
        try {
            final Field stateField = DistributedCache.class.getDeclaredField("circuitBreaker");
            stateField.setAccessible(true);

            assertTrue(AtomicReference.class.isAssignableFrom(stateField.getType()));

            final Object state = ((AtomicReference<?>) stateField.get(cache)).get();
            final Field failedCountField = state.getClass().getDeclaredField("failedCount");
            final Field lastFailedTimeField = state.getClass().getDeclaredField("lastFailedTime");
            final Field hasFailureField = state.getClass().getDeclaredField("hasFailure");
            failedCountField.setAccessible(true);
            lastFailedTimeField.setAccessible(true);
            hasFailureField.setAccessible(true);
            assertEquals(0, failedCountField.getInt(state));
            assertEquals(0L, lastFailedTimeField.getLong(state));
            assertFalse(hasFailureField.getBoolean(state));
            assertThrows(NoSuchFieldException.class, () -> DistributedCache.class.getDeclaredField("failedCounter"));
            assertThrows(NoSuchFieldException.class, () -> DistributedCache.class.getDeclaredField("lastFailedTime"));
        } finally {
            cache.close();
        }
    }

    /**
     * A read failure from the backend is swallowed and surfaced as a cache miss (null). The failure is
     * real: the underlying client is shut down, so {@code get} throws instantly.
     */
    @Test
    public void testGetOrNull_failingBackendIsSwallowedAsNull() {
        final SpyMemcached<String> client = newClient();
        // High threshold + long retry window so the breaker stays closed and the failing get actually
        // reaches the (shut-down) client.
        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 5, 10_000);
        try {
            client.disconnect(); // every subsequent get() now throws IllegalStateException
            assertNull(cache.getOrNull("k"));
        } finally {
            cache.close();
        }
    }

    /**
     * With a genuinely-present value on the server, forcing the breaker into the open state (via its
     * internal counters) makes {@code getOrNull} short-circuit to {@code null} despite the value being
     * available; once the retry window elapses the read succeeds again and the counter resets.
     */
    @Test
    public void testCircuitBreaker_opensThenRecovers() throws Exception {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient(), "", 1, 60_000);
        try {
            assertTrue(cache.put("k", "v", 60_000, 0));
            assertEquals("v", cache.getOrNull("k"));

            // Open the circuit: counter at threshold, last failure just now. The breaker measures
            // elapsed time with System.nanoTime() (monotonic), not the wall clock.
            setBreakerState(cache, 1, System.nanoTime());
            assertNull(cache.getOrNull("k"), "an open circuit must short-circuit to null even though the value is present");

            // Let the retry window elapse: backdate the last failure past the 60s window so the
            // read reaches the server again and the breaker closes.
            setBreakerState(cache, 1, System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(60_001));
            assertEquals("v", cache.getOrNull("k"));
            assertEquals(0, failedCounter(cache), "a successful read must reset the failure counter");
        } finally {
            cache.close();
        }
    }

    /**
     * Regression test for the monotonic-clock circuit breaker: the fail-fast window must close by
     * genuinely elapsed time. (Previously the window was measured with {@code System.currentTimeMillis()},
     * so a backward wall-clock adjustment extended the read blackout by the size of the jump; the breaker
     * now uses {@code System.nanoTime()}, which cannot move backwards.)
     */
    @Test
    public void testCircuitBreaker_recoversAfterRealElapsedDelay() throws Exception {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient(), "", 1, 100);
        try {
            assertTrue(cache.put("k", "v", 60_000, 0));

            setBreakerState(cache, 1, System.nanoTime()); // open: last failure "just now", 100ms window
            assertNull(cache.getOrNull("k"), "circuit must be open immediately after the failure");

            Thread.sleep(150); // let the 100ms retry window genuinely elapse

            assertEquals("v", cache.getOrNull("k"), "circuit must close once the retry delay has elapsed");
            assertEquals(0, failedCounter(cache));
        } finally {
            cache.close();
        }
    }

    /**
     * Regression test for breaker poisoning by deterministic validation errors.
     *
     * <p>Base64 encoding expands keys by 4/3, so a long natural key produces a generated key over
     * memcached's 250-character limit, which the client rejects with {@link IllegalArgumentException}
     * on every call. Previously that IAE was swallowed as a cache miss AND counted toward the
     * availability breaker — a hot invalid key could open the circuit and blank reads for ALL keys.
     * The IAE is now rethrown and leaves the breaker state untouched.
     */
    @Test
    public void testGetOrNull_overlongKey_throwsIAEWithoutTouchingBreaker() throws Exception {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient(), "", 5, 60_000);
        try {
            final String overlongKey = "k".repeat(300); // Base64-encodes to ~400 chars, over the 250 limit

            assertThrows(IllegalArgumentException.class, () -> cache.getOrNull(overlongKey));
            assertEquals(0, failedCounter(cache), "a client-side validation error must not count as an availability failure");

            // Normal keys keep working and the breaker is still closed.
            assertTrue(cache.put("ok", "v", 60_000, 0));
            assertEquals("v", cache.getOrNull("ok"));
        } finally {
            cache.close();
        }
    }

    /**
     * Regression test for the unvalidated key prefix: a prefix containing a space (or any
     * non-printable-ASCII character) would make every generated key invalid at the server — every
     * write throwing and every read silently feeding the circuit breaker. It is now rejected at
     * construction time.
     */
    @Test
    public void testConstructor_invalidKeyPrefix_throwsIAE() {
        final SpyMemcached<String> client = newClient();
        try {
            assertThrows(IllegalArgumentException.class, () -> new DistributedCache<>(client, "my app: ", 100, 1000));
            assertThrows(IllegalArgumentException.class, () -> new DistributedCache<>(client, "café:", 100, 1000));
            // A printable-ASCII prefix is accepted.
            final DistributedCache<String, String> cache = new DistributedCache<>(client, "myapp:", 100, 1000);
            try {
                assertTrue(cache.put("k", "v", 60_000, 0));
                assertEquals("v", cache.getOrNull("k"));
            } finally {
                cache.close();
            }
        } finally {
            client.disconnect();
        }
    }

    /**
     * The documented contract of {@link DistributedCache#getOrNull(Object)} rejects a null key with
     * {@link IllegalArgumentException} even when the circuit breaker is open (the null check runs before
     * the breaker short-circuit).
     */
    @Test
    public void testGetOrNull_NullKey_ThrowsEvenWhenCircuitOpen() throws Exception {
        final DistributedCache<String, String> cache = new DistributedCache<>(newClient(), "", 1, 60_000);
        try {
            setBreakerState(cache, 1, System.nanoTime()); // force the circuit open (nanoTime-based window)
            assertThrows(IllegalArgumentException.class, () -> cache.getOrNull(null));
        } finally {
            cache.close();
        }
    }

    /**
     * The failure counter must never exceed {@code maxFailuresBeforeCircuitOpen}, even under concurrent failing
     * reads. With {@code retryDelay == 0} the breaker never short-circuits, so every concurrent get
     * flows through the capped increment path. Failures are real (shut-down client) and instant.
     */
    @Test
    public void testCircuitBreaker_FailureCounterNeverExceedsCapUnderConcurrency() throws Exception {
        final int cap = 5;
        final SpyMemcached<String> client = newClient();
        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", cap, 0);
        try {
            client.disconnect(); // all reads now fail instantly

            final int threadCount = 16;
            final ExecutorService pool = Executors.newFixedThreadPool(threadCount);
            final CountDownLatch start = new CountDownLatch(1);
            final List<Future<?>> futures = new ArrayList<>();

            for (int t = 0; t < threadCount; t++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < 2000; i++) {
                        cache.getOrNull("k");
                    }
                    return null;
                }));
            }

            start.countDown();
            for (final Future<?> f : futures) {
                f.get();
            }
            pool.shutdown();

            assertEquals(cap, failedCounter(cache), "failure counter must be capped at maxFailuresBeforeCircuitOpen under concurrency");
        } finally {
            cache.close();
        }
    }

    /**
     * Guards the failure counter against unbounded growth: with {@code retryDelay == 0} the breaker
     * never fast-fails, so each failing get passes through the (clamped) increment branch. Driving many
     * failures past the threshold must leave the counter pinned at the cap rather than overflowing.
     */
    @Test
    public void testCircuitBreaker_FailureCounterDoesNotGrowUnbounded() throws Exception {
        final int cap = 2;
        final SpyMemcached<String> client = newClient();
        final DistributedCache<String, String> cache = new DistributedCache<>(client, "", cap, 0);
        try {
            client.disconnect();
            for (int i = 0; i < 50; i++) {
                assertNull(cache.getOrNull("k"));
            }
            assertEquals(cap, failedCounter(cache), "counter must stay clamped at the cap, never overflow");
        } finally {
            cache.close();
        }
    }

    /**
     * A read failure is swallowed (treated as a cache miss) and logged at debug. Raising the logger
     * level to DEBUG exercises the debug-logging line inside the read-failure catch block.
     */
    @Test
    public void testGetOrNull_readFailureLoggedAtDebug() {
        Configurator.setLevel(DISTRIBUTED_CACHE_LOGGER, Level.DEBUG);
        try {
            final SpyMemcached<String> client = newClient();
            // High threshold + long retry window so the breaker stays closed and the failing get reaches
            // the (shut-down) client and the debug-logging branch.
            final DistributedCache<String, String> cache = new DistributedCache<>(client, "", 5, 10_000);
            try {
                client.disconnect();
                assertNull(cache.getOrNull("k"));
            } finally {
                cache.close();
            }
        } finally {
            Configurator.setLevel(DISTRIBUTED_CACHE_LOGGER, Level.ERROR);
        }
    }

    // --- reflection helpers for the atomically-paired circuit-breaker state --------------------

    @SuppressWarnings("unchecked")
    private static void setBreakerState(final DistributedCache<?, ?> cache, final int failedCount, final long lastFailedTime) throws Exception {
        final Field stateField = DistributedCache.class.getDeclaredField("circuitBreaker");
        stateField.setAccessible(true);
        final AtomicReference<Object> stateRef = (AtomicReference<Object>) stateField.get(cache);
        final Class<?> stateType = Class.forName(DistributedCache.class.getName() + "$CircuitBreakerState");
        final Constructor<?> constructor = stateType.getDeclaredConstructor(int.class, long.class, boolean.class);
        constructor.setAccessible(true);
        stateRef.set(constructor.newInstance(failedCount, lastFailedTime, true));
    }

    private static int failedCounter(final DistributedCache<?, ?> cache) throws Exception {
        final Field stateField = DistributedCache.class.getDeclaredField("circuitBreaker");
        stateField.setAccessible(true);
        final Object state = ((AtomicReference<?>) stateField.get(cache)).get();
        final Field countField = state.getClass().getDeclaredField("failedCount");
        countField.setAccessible(true);
        return countField.getInt(state);
    }
}
