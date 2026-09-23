/*
 * Copyright (c) 2026, Haiyang Li. All rights reserved.
 */

package com.landawn.abacus.cache;

/**
 * Internal constants container providing the standardized parameter-name string literals used by
 * argument-validation calls (e.g., {@code N.checkArgNotNull(key, cs.key)}) throughout the cache module
 * (including {@code com.landawn.abacus.util.MemcachedLock}).
 *
 * <p><b>Contract:</b> each constant's name and its string value are identical, and both must equal the
 * declared name of the method parameter it validates. Renaming a method parameter therefore requires
 * renaming the corresponding constant (and its value) in the same change.</p>
 *
 * <p>This class is public only so that other packages of this module can reference it; it is not part
 * of the supported API.</p>
 */
public final class cs { // NOSONAR

    /** Parameter name {@code "action"}. */
    public static final String action = "action";
    /** Parameter name {@code "cache"}. */
    public static final String cache = "cache";
    /** Parameter name {@code "capacity"}. */
    public static final String capacity = "capacity";
    /** Parameter name {@code "capacityInMB"}. */
    public static final String capacityInMB = "capacityInMB";
    /** Parameter name {@code "client"}. */
    public static final String client = "client";
    /** Parameter name {@code "cluster"}. */
    public static final String cluster = "cluster";
    /** Parameter name {@code "connFactory"}. */
    public static final String connFactory = "connFactory";
    /** Parameter name {@code "defaultValue"}. */
    public static final String defaultValue = "defaultValue";
    /** Parameter name {@code "delta"}. */
    public static final String delta = "delta";
    /** Parameter name {@code "deserializer"}. */
    public static final String deserializer = "deserializer";
    /** Parameter name {@code "entries"}. */
    public static final String entries = "entries";
    /** Parameter name {@code "evictDelay"}. */
    public static final String evictDelay = "evictDelay";
    /** Parameter name {@code "function"}. */
    public static final String function = "function";
    /** Parameter name {@code "future"}. */
    public static final String future = "future";
    /** Parameter name {@code "key"}. */
    public static final String key = "key";
    /** Parameter name {@code "keys"}. */
    public static final String keys = "keys";
    /** Parameter name {@code "kryoParser"}. */
    public static final String kryoParser = "kryoParser";
    /** Parameter name {@code "liveTime"}. */
    public static final String liveTime = "liveTime";
    /** Parameter name {@code "logger"}. */
    public static final String logger = "logger";
    /** Parameter name {@code "mappingFunction"}. */
    public static final String mappingFunction = "mappingFunction";
    /** Parameter name {@code "maxFailuresBeforeCircuitOpen"}. */
    public static final String maxFailuresBeforeCircuitOpen = "maxFailuresBeforeCircuitOpen";
    /** Parameter name {@code "maxSize"}. */
    public static final String maxSize = "maxSize";
    /** Parameter name {@code "pool"}. */
    public static final String pool = "pool";
    /** Parameter name {@code "remappingFunction"}. */
    public static final String remappingFunction = "remappingFunction";
    /** Parameter name {@code "retryDelay"}. */
    public static final String retryDelay = "retryDelay";
    /** Parameter name {@code "serializer"}. */
    public static final String serializer = "serializer";
    /** Parameter name {@code "serverUrl"}. */
    public static final String serverUrl = "serverUrl";
    /** Parameter name {@code "storeSelector"}. */
    public static final String storeSelector = "storeSelector";
    /** Parameter name {@code "target"}. */
    public static final String target = "target";
    /** Parameter name {@code "testerForLoadingItemFromDiskToMemory"}. */
    public static final String testerForLoadingItemFromDiskToMemory = "testerForLoadingItemFromDiskToMemory";
    /** Parameter name {@code "timeout"}. */
    public static final String timeout = "timeout";
    /** Parameter name {@code "value"}. */
    public static final String value = "value";

    /**
     * Prevents instantiation of this constants holder.
     */
    private cs() {
        // Utility class for constant string values.
    }
}
