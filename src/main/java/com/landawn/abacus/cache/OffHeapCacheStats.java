package com.landawn.abacus.cache;

import java.util.List;
import java.util.Map;

public record OffHeapCacheStats(int capacity, int cachedCount, long cachedCountOnDisk, long putCount, long putCountToDisk, long getCount, long hitCount,
        long hitCountByDisk, long missCount, long evictionCount, long allocatedMemory, long occupiedMemory, long dataSize, long dataSizeOnDisk, int segmentSize,
        Map<Integer, List<Integer>> usedSlots, double averageWriteTimeToDisk, double averageReadTimeFromDisk) {

    /**
     * Returns a map of slot size to a list of used slot count in each segment
     *
     * @return
     */
    public Map<Integer, List<Integer>> usedSlots() {
        return usedSlots;
    }

}
