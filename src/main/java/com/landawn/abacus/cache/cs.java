/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

/** Standardized parameter names used by argument validation in the cache module. */
final class cs { // NOSONAR

    static final String action = "action";
    static final String deserializer = "deserializer";
    static final String function = "function";
    static final String mappingFunction = "mappingFunction";
    static final String remappingFunction = "remappingFunction";
    static final String serializer = "serializer";
    static final String storeSelector = "storeSelector";
    static final String testerForLoadingItemFromDiskToMemory = "testerForLoadingItemFromDiskToMemory";

    private cs() {
        // Utility class for constant string values.
    }
}
