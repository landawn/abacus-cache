package com.landawn.abacus.cache;

import java.util.Map;

public record OffHeapCacheStats(int capacity, int size, long sizeOnDisk, long putCount, long putCountToDisk, long getCount, long hitCount, long hitCountByDisk,
        long missCount, long evictionCount, long evictionCountFromDisk, long allocatedMemory, long occupiedMemory, long dataSize, long dataSizeOnDisk,
        MinMaxAvg writeToDiskTimeStats, MinMaxAvg readFromDiskTimeStats, int segmentSize, Map<Integer, Map<Integer, Integer>> occupiedSlots) {

    public final Map<Integer, Map<Integer, Integer>> occupiedSlots() {
        return occupiedSlots;
    }

    public record MinMaxAvg(double min, double max, double avg) {
        @Override
        public String toString() {
            return "{min: " + min + ", max: " + max + ", avg: " + avg + "}";
        }
    }

    public record OccupiedSlot(int sizeOfSlot, Map<Integer, Integer> occupiedSlots) {
    }
}
