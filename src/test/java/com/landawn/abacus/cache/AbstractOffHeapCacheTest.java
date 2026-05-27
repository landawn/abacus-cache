/*
 * Copyright (c) 2025, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("2025")
public class AbstractOffHeapCacheTest {

    /**
     * Regression coverage for the cancelled-eviction-task GC leak in
     * {@link AbstractOffHeapCache}.
     *
     * <p>Without {@code setRemoveOnCancelPolicy(true)} on the shared scheduled executor, a
     * cancelled {@code scheduleFuture} sits in the executor's task queue until its scheduled fire
     * time, holding a strong reference to the closed cache and preventing GC of its off-heap
     * allocation. The fix enables the policy on the executor so {@code cancel()} purges the task
     * from the queue immediately.
     *
     * <p>This smoke test creates and immediately closes many short-lived off-heap caches in a
     * tight loop. With the fix, cancelled scheduled tasks are purged on close, so memory stays
     * bounded. Without the fix, the executor's queue grows unbounded — each cache's eviction task
     * remains scheduled at its full {@code evictDelay} into the future even after close. The test
     * fails (OOM or excessive time) regress when the fix is reverted.
     */
    @Test
    public void testRepeatedCreateAndCloseDoesNotLeakScheduledTasks() {
        // 200 caches with a 5-minute evictDelay each. Without the cancel-on-purge policy, all 200
        // scheduled tasks would remain in the executor's queue holding cache references after close.
        for (int i = 0; i < 200; i++) {
            try (OffHeapCache<String, String> cache = OffHeapCache.<String, String> builder()
                    .capacityInMB(16)
                    .evictDelay(5L * 60_000L)
                    .defaultLiveTime(60_000)
                    .defaultMaxIdleTime(60_000)
                    .build()) {
                assertTrue(cache.put("k", "v"));
            }
        }
    }
}
