package com.orientechnologies.orient.core.type.tree;

import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLongSerializer;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 23.06.12
 */
public class OMemoryLongHashMap {
  private static final int   DEFAULT_INITIAL_CAPACITY = 128;
  private static final float DEFAULT_LOAD_FACTOR      = 0.8f;

  private static final int   NEXT_OFFSET              = 0;
  private static final int   CLUSTER_POSITION_OFFSET  = OIntegerSerializer.INT_SIZE;
  private static final int   DATA_POINTER_OFFSET      = OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE;

  private static final int   ENTRY_SIZE               = 2 * OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE;

  private float              loadFactor;

  private int                entriesPointer;
  private int                nextThreshold;

  private int                size;

  private int                entriesLength;

  private final OMemory      memory;

  public OMemoryLongHashMap(OMemory memory) {
    this(memory, DEFAULT_LOAD_FACTOR, DEFAULT_INITIAL_CAPACITY);
  }

  public OMemoryLongHashMap(OMemory memory, float loadFactor, int initialCapacity) {
    this.memory = memory;
    this.loadFactor = loadFactor;

    int cp = 1;
    while (cp < initialCapacity)
      cp <<= 1;

    init(cp);
  }

  public int put(long clusterPosition, int dataPointer) {
    int[] res = doGet(clusterPosition);

    if (res[0] == 0) {
      final int entryPointer = storeEntry(clusterPosition, dataPointer);

      if (entryPointer == OMemory.NULL_POINTER)
        return -1;

      if (res[1] == OMemory.NULL_POINTER) {
        final int index = index(clusterPosition);
        setEntryPointer(index, entryPointer);
      } else
        setNext(res[1], entryPointer);

      size++;

      if (size >= nextThreshold)
        rehash();
    } else
      setDataPointer(res[1], dataPointer);

    return 1;
  }

  public int get(long clusterPosition) {
    int[] res = doGet(clusterPosition);
    if (res[0] == 1)
      return getDataPointer(res[1]);

    return OMemory.NULL_POINTER;
  }

  public int remove(long clusterPosition) {
    final int index = index(clusterPosition);

    int current = getEntryPointer(index);
    int prevCurrent = OMemory.NULL_POINTER;

    while (current != OMemory.NULL_POINTER) {
      if (getClusterPosition(current) == clusterPosition) {
        final int dataPointer = getDataPointer(current);

        doRemove(index, prevCurrent, current);

        size--;

        return dataPointer;
      }

      prevCurrent = current;
      current = getNext(current);
    }

    return OMemory.NULL_POINTER;
  }

  public int size() {
    return size;
  }

  private void doRemove(int index, int prevCurrent, int current) {
    final int next = getNext(current);
    if (prevCurrent == OMemory.NULL_POINTER) {
      setEntryPointer(index, next);

      memory.free(current);
      return;
    }

    setNext(prevCurrent, next);
    memory.free(current);
  }

  private int[] doGet(long clusterPosition) {
    final int index = index(clusterPosition);

    int current = getEntryPointer(index);
    int prevCurrent = OMemory.NULL_POINTER;

    while (current != OMemory.NULL_POINTER) {
      if (getClusterPosition(current) == clusterPosition)
        return new int[] { 1, current };

      prevCurrent = current;
      current = getNext(current);
    }

    return new int[] { 0, prevCurrent };
  }

  private boolean init(int capacity) {
    final int allocationSize = capacity * OIntegerSerializer.INT_SIZE;

    final int pointer = memory.allocate(allocationSize);
    if (pointer == OMemory.NULL_POINTER)
      return false;

    entriesPointer = pointer;
    entriesLength = capacity;

    int offset = 0;
    for (int i = 0; i < entriesLength; i++) {
      memory.setInt(entriesPointer, offset, OMemory.NULL_POINTER);
      offset += OIntegerSerializer.INT_SIZE;
    }

    nextThreshold = (int) (loadFactor * capacity);

    return true;
  }

  private int index(long value) {
    return ((int) (value ^ (value >>> 32))) & (entriesLength - 1);
  }

  private void rehash() {
    final int oldLength = entriesLength;
    final int oldEntriesPointer = entriesPointer;

    if (!init(oldLength << 1))
      return;

    move(oldEntriesPointer, oldLength);
    memory.free(oldEntriesPointer);
  }

  private void move(int oldEntriesPointer, int oldEntriesLength) {
    for (int oldIndex = 0; oldIndex < oldEntriesLength; oldIndex++) {
      int oldCurrent = getOldEntryPointer(oldEntriesPointer, oldIndex);

      while (oldCurrent != OMemory.NULL_POINTER) {
        final long oldClusterPosition = getClusterPosition(oldCurrent);
        final int newIndex = index(oldClusterPosition);

        int current = getEntryPointer(newIndex);

        if (current == OMemory.NULL_POINTER)
          setEntryPointer(newIndex, oldCurrent);
        else {
          int next;
          while ((next = getNext(current)) != OMemory.NULL_POINTER)
            current = next;

          setNext(current, oldCurrent);
        }

        final int addedEntry = oldCurrent;
        oldCurrent = getNext(oldCurrent);
        setNext(addedEntry, OMemory.NULL_POINTER);
      }
    }
  }

  private long getClusterPosition(int pointer) {
    return memory.getLong(pointer, CLUSTER_POSITION_OFFSET);
  }

  private int getNext(int pointer) {
    return memory.getInt(pointer, NEXT_OFFSET);
  }

  private void setNext(int pointer, int next) {
    memory.setInt(pointer, NEXT_OFFSET, next);
  }

  private int getDataPointer(int pointer) {
    return memory.getInt(pointer, DATA_POINTER_OFFSET);
  }

  private void setDataPointer(int pointer, int dataPointer) {
    memory.setInt(pointer, DATA_POINTER_OFFSET, dataPointer);
  }

  private int storeEntry(long clusterPosition, int dataPointer) {
    final int pointer = memory.allocate(ENTRY_SIZE);

    if (pointer == OMemory.NULL_POINTER)
      return OMemory.NULL_POINTER;

    memory.setInt(pointer, NEXT_OFFSET, OMemory.NULL_POINTER);
    memory.setLong(pointer, CLUSTER_POSITION_OFFSET, clusterPosition);
    memory.setInt(pointer, DATA_POINTER_OFFSET, dataPointer);

    return pointer;
  }

  private int getEntryPointer(int index) {
    return memory.getInt(entriesPointer, index * OIntegerSerializer.INT_SIZE);
  }

  private int getOldEntryPointer(int oldEntriesPointer, int index) {
    return memory.getInt(oldEntriesPointer, index * OIntegerSerializer.INT_SIZE);
  }

  private void setEntryPointer(int index, int pointer) {
    memory.setInt(entriesPointer, index * OIntegerSerializer.INT_SIZE, pointer);
  }
}