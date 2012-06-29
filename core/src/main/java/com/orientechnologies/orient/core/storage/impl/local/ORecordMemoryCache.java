package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OByteSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;
import com.orientechnologies.orient.core.type.tree.OMemory;
import com.orientechnologies.orient.core.type.tree.OMemoryIntHashMap;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 24.06.12
 */
public class ORecordMemoryCache {
  private static final int        CLUSTER_ID_OFFSET      = 0;
  private static final int        NEXT_POINTER_OFFSET    = OIntegerSerializer.INT_SIZE;
  private static final int        PREV_POINTER_OFFSET    = 2 * OIntegerSerializer.INT_SIZE;
  private static final int        DATA_POINTER_OFFSET    = 3 * OIntegerSerializer.INT_SIZE;
  private static final int        RECORD_STATE_OFFSET    = 4 * OIntegerSerializer.INT_SIZE;
  private static final int        DATA_SEGMENT_ID_OFFSET = 4 * OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;

  private static final int        ENTRY_SIZE             = 5 * OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE;

  private final OMemory           memory;
  private final OMemoryIntHashMap memoryIntHashMap;

  private int                     lruHeader              = OMemory.NULL_POINTER;
  private int                     lruTail                = OMemory.NULL_POINTER;

  private int                     defaultEvictionPercent = 20;

  private long                    size                   = 0;

  private boolean                 scheduleEviction       = true;

  public enum RecordState {
    SHARED, MODIFIED, NEW
  }

  public ORecordMemoryCache(OMemory memory) {
    this.memory = memory;
    this.memoryIntHashMap = new OMemoryIntHashMap(memory);
  }

  public synchronized boolean put(int dataSegmentId, int clusterId, byte[] content, RecordState recordState) {
    final int existingEntryPointer = memoryIntHashMap.get(clusterId);
    if (existingEntryPointer != OMemory.NULL_POINTER) {
      final RecordState existingRecordState = getRecordState(existingEntryPointer);

      checkRecordStateChange(existingRecordState, recordState);
    }

    final int dataPointer = storeData(content);

    if (dataPointer == OMemory.NULL_POINTER)
      return false;

    if (existingEntryPointer != OMemory.NULL_POINTER) {
      final int existingDataSegmentId = getDataSegmentId(existingEntryPointer);

      if (existingDataSegmentId != dataSegmentId)
        throw new OException("Cached and updated data segment IDs should be the same.");

      memory.free(getDataPointer(existingEntryPointer));

      setRecordState(existingEntryPointer, recordState);
      setDataPointer(existingEntryPointer, dataPointer);

      updateItemInLRU(existingEntryPointer);
    } else {
      final int entryPointer = storeEntry(clusterId, dataPointer, recordState, dataSegmentId);
      if (entryPointer == OMemory.NULL_POINTER) {
        memory.free(dataPointer);

        return false;
      }

      final int result = memoryIntHashMap.put(clusterId, dataPointer);
      if (result == -1) {
        memory.free(dataPointer);
        memory.free(entryPointer);

        return false;
      }

      addEntryToLRU(entryPointer);

      size++;
    }

    return true;
  }

  public synchronized byte[] get(int clusterId) {
    final int existingEntryPointer = memoryIntHashMap.get(clusterId);
    if (existingEntryPointer == OMemory.NULL_POINTER)
      return null;

    updateItemInLRU(existingEntryPointer);

    return getData(getDataPointer(existingEntryPointer));
  }

  public synchronized boolean remove(int clusterId) {
    final int removedDataPointer = memoryIntHashMap.remove(clusterId);
    if (removedDataPointer == OMemory.NULL_POINTER)
      return false;

    final int dataPointer = getDataPointer(removedDataPointer);
    memory.free(dataPointer);

    removeItemFromLRU(removedDataPointer);

    memory.free(removedDataPointer);

    size--;

    return true;
  }

  public long size() {
    return size;
  }

  private void checkRecordStateChange(final RecordState oldRecordState, final RecordState newRecordState) {
    switch (oldRecordState) {
    case NEW:
      if (newRecordState == RecordState.NEW)
        throw new OException("Existing record can not be new once again");
      if (newRecordState == RecordState.SHARED)
        throw new OException("New records should be flushed before read replacement");
      break;

    case SHARED:
      if (newRecordState == RecordState.NEW)
        throw new OException("Existing record can not be new once again");
      break;

    case MODIFIED:
      if (newRecordState == RecordState.NEW)
        throw new OException("Existing record can not be new once again");

      if (newRecordState == RecordState.SHARED)
        throw new OException("Modified records should be flushed before read replacement");
    }
  }

  private void updateItemInLRU(final int pointer) {
    if (pointer == lruHeader)
      return;

    final int prevPointer = getPrevLRUPointer(pointer);
    final int nextPointer = getNextLRUPointer(pointer);

    int prevHeader = lruHeader;

    if (prevPointer != OMemory.NULL_POINTER)
      setNextLRUPointer(prevPointer, nextPointer);

    if (nextPointer != OMemory.NULL_POINTER)
      setPrevLRUPointer(nextPointer, prevPointer);

    if (prevPointer != OMemory.NULL_POINTER)
      setPrevLRUPointer(pointer, OMemory.NULL_POINTER);

    setNextLRUPointer(pointer, lruHeader);

    setPrevLRUPointer(prevHeader, pointer);

    lruHeader = pointer;

    if (lruTail == pointer)
      lruTail = prevPointer;
  }

  private void removeItemFromLRU(int pointer) {
    if (lruHeader == pointer && lruTail == pointer) {
      lruHeader = OMemory.NULL_POINTER;
      lruTail = OMemory.NULL_POINTER;
      return;
    }

    int prevPointer = getPrevLRUPointer(pointer);
    int nextPointer = getNextLRUPointer(pointer);

    if (lruHeader == pointer)
      lruHeader = nextPointer;

    if (lruTail == pointer)
      lruTail = prevPointer;

    if (prevPointer != OMemory.NULL_POINTER)
      setNextLRUPointer(prevPointer, nextPointer);

    if (nextPointer != OMemory.NULL_POINTER)
      setPrevLRUPointer(nextPointer, prevPointer);
  }

  private int storeData(byte[] data) {
    final int pointer = memory.allocate(data.length + OIntegerSerializer.INT_SIZE);
    if (pointer == OMemory.NULL_POINTER)
      return OMemory.NULL_POINTER;

    memory.setInt(pointer, 0, data.length);
    memory.set(pointer, OIntegerSerializer.INT_SIZE, data.length, data);

    return pointer;
  }

  private byte[] getData(int pointer) {
    int dataLength = memory.getInt(pointer, 0);
    return memory.get(pointer, OIntegerSerializer.INT_SIZE, dataLength);
  }

  private int storeEntry(int clusterId, int dataPointer, RecordState recordState, int dataSegmentId) {
    final int entryPointer = memory.allocate(ENTRY_SIZE);

    if (entryPointer == OMemory.NULL_POINTER)
      return OMemory.NULL_POINTER;

    memory.setInt(entryPointer, CLUSTER_ID_OFFSET, clusterId);
    memory.setInt(entryPointer, NEXT_POINTER_OFFSET, OMemory.NULL_POINTER);
    memory.setInt(entryPointer, PREV_POINTER_OFFSET, OMemory.NULL_POINTER);
    memory.setInt(entryPointer, DATA_POINTER_OFFSET, dataPointer);
    memory.setByte(entryPointer, RECORD_STATE_OFFSET, (byte) recordState.ordinal());
    memory.setInt(entryPointer, DATA_SEGMENT_ID_OFFSET, dataSegmentId);

    return entryPointer;
  }

  private void addEntryToLRU(int pointer) {
    if (lruHeader == OMemory.NULL_POINTER) {
      lruHeader = pointer;
      lruTail = pointer;
      return;
    }

    int nextPointer = lruHeader;
    setNextLRUPointer(pointer, nextPointer);
    lruHeader = pointer;
    setPrevLRUPointer(nextPointer, pointer);
  }

  private int getClusterId(int pointer) {
    return memory.getInt(pointer, CLUSTER_ID_OFFSET);
  }

  private void setPrevLRUPointer(int pointer, int prevPointer) {
    memory.setInt(pointer, PREV_POINTER_OFFSET, prevPointer);
  }

  private void setNextLRUPointer(int pointer, int nextPointer) {
    memory.setInt(pointer, NEXT_POINTER_OFFSET, nextPointer);
  }

  private int getPrevLRUPointer(int pointer) {
    return memory.getInt(pointer, PREV_POINTER_OFFSET);
  }

  private int getNextLRUPointer(int pointer) {
    return memory.getInt(pointer, NEXT_POINTER_OFFSET);
  }

  private RecordState getRecordState(int pointer) {
    return RecordState.values()[memory.getByte(pointer, RECORD_STATE_OFFSET)];
  }

  private void setRecordState(int pointer, RecordState value) {
    memory.setByte(pointer, RECORD_STATE_OFFSET, (byte) value.ordinal());
  }

  private int getDataSegmentId(int pointer) {
    return memory.getInt(pointer, DATA_SEGMENT_ID_OFFSET);
  }

  private void setDataPointer(int pointer, int value) {
    memory.setInt(pointer, DATA_POINTER_OFFSET, value);
  }

  private int getDataPointer(int pointer) {
    return memory.getInt(pointer, DATA_POINTER_OFFSET);
  }

  public synchronized boolean evict(final ORecordMemoryCacheFlusher flusher) {
    long evictionSize = (size * defaultEvictionPercent) / 100;
    if (evictionSize == 0)
      return false;

    int evicted = 0;
    int currentVictim = lruTail;
    scheduleEviction = false;

    while (currentVictim != OMemory.NULL_POINTER && evicted < evictionSize) {
      int evictedItem = currentVictim;
      currentVictim = getPrevLRUPointer(evictedItem);

      final RecordState recordState = getRecordState(evictedItem);

      if (recordState != RecordState.SHARED)
        flusher.flushRecord(getClusterId(evictedItem), getDataSegmentId(evictedItem), getData(getDataPointer(evictedItem)),
            recordState);

      memoryIntHashMap.remove(getClusterId(evictedItem));
      evicted++;
      size--;

      memory.free(getDataPointer(evictedItem));
      memory.free(evictedItem);

    }

    lruTail = currentVictim;
    if (lruTail != OMemory.NULL_POINTER)
      setNextLRUPointer(lruTail, OMemory.NULL_POINTER);

    return true;
  }

  public synchronized boolean evictSharedRecordsOnly() {
    long evictionSize = (size * defaultEvictionPercent) / 100;
    if (evictionSize == 0)
      return false;

    int evicted = 0;
    int currentVictim = lruTail;
    scheduleEviction = false;

    while (currentVictim != OMemory.NULL_POINTER && evicted < evictionSize) {
      int evictedItem = currentVictim;
      currentVictim = getPrevLRUPointer(evictedItem);

      final RecordState recordState = getRecordState(evictedItem);

      if (recordState != RecordState.SHARED) {
        scheduleEviction = true;
        continue;
      }

      memoryIntHashMap.remove(getClusterId(evictedItem));
      evicted++;
      size--;

      memory.free(getDataPointer(evictedItem));
      memory.free(evictedItem);

    }

    lruTail = currentVictim;
    if (lruTail != OMemory.NULL_POINTER)
      setNextLRUPointer(lruTail, OMemory.NULL_POINTER);

    return true;
  }

  public boolean isScheduleEviction() {
    return scheduleEviction;
  }
}
