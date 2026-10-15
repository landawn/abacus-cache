package com.landawn.abacus.cache;

import java.util.concurrent.atomic.AtomicBoolean;

/** Factories shared by contract tests; every invocation owns a fresh native allocation. */
enum OffHeapBackend {
    UNSAFE, FOREIGN_MEMORY;

    <V> AbstractOffHeapCache<String, V> create(final int capacityInMB, final long evictDelay) {
        return this == UNSAFE ? OffHeapCache.<String, V> builder().capacityInMB(capacityInMB).evictDelay(evictDelay).build()
                : ForeignMemoryOffHeapCache.<String, V> builder().capacityInMB(capacityInMB).evictDelay(evictDelay).build();
    }

    <V> AbstractOffHeapCache<String, V> createWithDefaults() {
        return this == UNSAFE ? OffHeapCache.<String, V> builder().capacityInMB(1).maxBlockSizeInBytes(0).build()
                : ForeignMemoryOffHeapCache.<String, V> builder().capacityInMB(1).maxBlockSizeInBytes(0).build();
    }

    <V> AbstractOffHeapCache<String, V> createDiskCache(final OffHeapStore<String> store, final AtomicBoolean promote) {
        return this == UNSAFE
                ? OffHeapCache.<String, V> builder()
                        .capacityInMB(1)
                        .evictDelay(0)
                        .offHeapStore(store)
                        .storeSelector((key, value, size) -> 2)
                        .testerForLoadingItemFromDiskToMemory((activity, key, value) -> promote.get())
                        .build()
                : ForeignMemoryOffHeapCache.<String, V> builder()
                        .capacityInMB(1)
                        .evictDelay(0)
                        .offHeapStore(store)
                        .storeSelector((key, value, size) -> 2)
                        .testerForLoadingItemFromDiskToMemory((activity, key, value) -> promote.get())
                        .build();
    }
}
