package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OByteSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;
import com.orientechnologies.orient.core.type.tree.OMemory;
import com.orientechnologies.orient.core.type.tree.OMemoryHashMap;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 24.06.12
 */
public class OClusterLocalMemoryCache {
  private final OMemory        memory;
  private final OMemoryHashMap memoryHashMap;

  private int                  lruHeader              = OMemory.NULL_POINTER;
  private int                  lruTail                = OMemory.NULL_POINTER;

  private int                  defaultEvictionPercent = 20;

  private long                 size                   = 0;

  public enum RecordState {
    SHARED, MODIFIED, NEW
  }

  public OClusterLocalMemoryCache(OMemory memory) {
    this.memory = memory;
    this.memoryHashMap = new OMemoryHashMap(memory);
  }

  public synchronized boolean put(int dataSegmentId, int clusterId, byte[] content, RecordState recordState) {
    final int dataPointer = memory.allocate(content.length);
    if (dataPointer == OMemory.NULL_POINTER)
      return false;

    final int entryPointer = storeEntry(clusterId, dataPointer, recordState, dataSegmentId);
    if (entryPointer == OMemory.NULL_POINTER) {
      memory.free(dataPointer);

      return false;
    }

    final int result = memoryHashMap.put(clusterId, dataPointer);
    if (result == -1) {
      memory.free(dataPointer);
      memory.free(entryPointer);

      return false;
    }

    addEntryToLRU(entryPointer);

    size++;

    return true;
  }

  public synchronized byte[] get(int clusterId) {
    return null;
  }

  public synchronized boolean remove(int clusterId) {
    return true;
  }

  private int storeEntry(int clusterId, int dataPointer, RecordState recordState, int dataSegmentId) {
    final int entryPointer = memory.allocate(5 * OIntegerSerializer.INT_SIZE + OByteSerializer.BYTE_SIZE);

    if (entryPointer == OMemory.NULL_POINTER)
      return OMemory.NULL_POINTER;

    int offset = 0;
    memory.setInt(entryPointer, offset, clusterId);
    offset += OIntegerSerializer.INT_SIZE;

    memory.setInt(entryPointer, offset, OMemory.NULL_POINTER);
    offset += OIntegerSerializer.INT_SIZE;

    memory.setInt(entryPointer, offset, OMemory.NULL_POINTER);
    offset += OIntegerSerializer.INT_SIZE;

    memory.setInt(entryPointer, offset, dataPointer);
    offset += OIntegerSerializer.INT_SIZE;

    memory.setByte(entryPointer, offset, (byte) recordState.ordinal());
    offset += OByteSerializer.BYTE_SIZE;

    memory.setInt(entryPointer, offset, dataSegmentId);

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

  private void setPrevLRUPointer(int pointer, int prevPointer) {
    int offset = 2 * OIntegerSerializer.INT_SIZE;
    memory.setInt(pointer, offset, prevPointer);
  }

  private void setNextLRUPointer(int pointer, int nextPointer) {
    int offset = OIntegerSerializer.INT_SIZE;
    memory.setInt(pointer, offset, nextPointer);
  }

  public synchronized boolean evict(final OStorageLocal storageLocal) {
    return false;
  }

  public synchronized boolean evictSharedRecordsOnly(final OStorageLocal storageLocal) {
    return false;
  }
}
