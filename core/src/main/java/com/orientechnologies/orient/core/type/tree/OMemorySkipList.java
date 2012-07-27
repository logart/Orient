package com.orientechnologies.orient.core.type.tree;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 17.06.12
 */
public class OMemorySkipList<K> {
  private static final int            MAX_LEVEL = 31;

  private final OBinarySerializer<K>  keySerializer;
  private final OMemory               memory;

  private static final Random         random    = new Random();

  private int                         seed;

  private int[]                       header;

  private long                        size      = 0;

  private final Comparator<? super K> comparator;

  public OMemorySkipList(final OMemory memory, final OBinarySerializer<K> keySerializer, Comparator<? super K> comparator) {
    this.memory = memory;

    this.keySerializer = keySerializer;
    if (comparator == null) {
      this.comparator = new NativeComparator<K>();
    } else {
      this.comparator = comparator;
    }

    header = new int[MAX_LEVEL + 1];
    Arrays.fill(header, OMemory.NULL_POINTER);

    seed = random.nextInt() | 0x0100;
  }

  public int add(K key, int dataPointer) {
    final int[] update = new int[MAX_LEVEL];
    Arrays.fill(update, OMemory.NULL_POINTER);

    if (size == 0) {
      int[] pointers = createPointers(update);

      final int itemPointer = storeItem(pointers, key, dataPointer);
      if (itemPointer == OMemory.NULL_POINTER) {
        return -1;
      }

      Arrays.fill(header, 0, pointers.length, itemPointer);

      size++;

      return 1;
    }

    int level = getStartLevel();

    int forwardPointer;
    int pointer = OMemory.NULL_POINTER;
    int oldForwardPointer = OMemory.NULL_POINTER;
    int oldCompareResult = -1;

    while (level >= 0) {
      if (pointer == OMemory.NULL_POINTER)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(pointer, level);

      if (forwardPointer == OMemory.NULL_POINTER) {
        update[level] = pointer;
        level--;
        oldForwardPointer = forwardPointer;
        continue;
      }

      K currentKey = getKey(forwardPointer);
      int compareResult;
      if (oldForwardPointer == forwardPointer)
        compareResult = oldCompareResult;
      else
        compareResult = comparator.compare(key, currentKey);

      if (compareResult == 0)
        return 0;

      if (compareResult < 0) {
        update[level] = pointer;
        level--;
        oldForwardPointer = forwardPointer;
        oldCompareResult = compareResult;
        continue;
      }

      pointer = forwardPointer;
      oldForwardPointer = OMemory.NULL_POINTER;
    }

    final int[] pointers = createPointers(update);
    final int newItemPointer = storeItem(pointers, key, dataPointer);

    if (newItemPointer == OMemory.NULL_POINTER) {
      return -1;
    }

    int updatePointer;

    for (int i = 0; i < pointers.length; i++) {
      updatePointer = update[i];
      if (updatePointer != OMemory.NULL_POINTER) {
        setNPointer(updatePointer, i, newItemPointer);
      } else
        header[i] = newItemPointer;
    }

    size++;

    return 1;
  }

  public int get(K key) {
    if (size == 0)
      return OMemory.NULL_POINTER;

    int level = getStartLevel();

    int pointer = OMemory.NULL_POINTER;
    int oldForwardPointer = OMemory.NULL_POINTER;
    int oldCompareResult = -1;
    int forwardPointer;

    while (level >= 0) {
      if (pointer == OMemory.NULL_POINTER)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(pointer, level);

      if (forwardPointer == OMemory.NULL_POINTER) {
        level--;
        oldForwardPointer = forwardPointer;
        continue;
      }

      K currentKey = getKey(forwardPointer);
      int compareResult;
      if (oldForwardPointer == forwardPointer)
        compareResult = oldCompareResult;
      else
        compareResult = comparator.compare(key, currentKey);

      if (compareResult == 0)
        return getDataPointer(forwardPointer);

      if (compareResult < 0) {
        level--;
        oldForwardPointer = forwardPointer;
        oldCompareResult = compareResult;
        continue;
      }

      pointer = forwardPointer;
      oldForwardPointer = OMemory.NULL_POINTER;
    }

    return OMemory.NULL_POINTER;
  }

  public long size() {
    return size;
  }

  public int getCeiling(K key) {
    if (size == 0)
      return OMemory.NULL_POINTER;

    int level = getStartLevel();
    int pointer = OMemory.NULL_POINTER;
    int oldForwardPointer = OMemory.NULL_POINTER;
    int oldCompareResult = -1;
    int forwardPointer;
    while (level >= 0) {
      if (pointer == OMemory.NULL_POINTER)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(pointer, level);

      if (forwardPointer == OMemory.NULL_POINTER) {
        level--;
        oldForwardPointer = forwardPointer;
        continue;
      }

      K currentKey = getKey(forwardPointer);
      int compareResult;
      if (oldForwardPointer == forwardPointer)
        compareResult = oldCompareResult;
      else
        compareResult = comparator.compare(key, currentKey);

      if (compareResult == 0)
        return getDataPointer(forwardPointer);

      if (compareResult < 0) {
        level--;
        oldForwardPointer = forwardPointer;
        oldCompareResult = compareResult;
        continue;
      }

      pointer = forwardPointer;
      oldForwardPointer = OMemory.NULL_POINTER;
    }

    int nextPointer;
    if (pointer == OMemory.NULL_POINTER)
      nextPointer = header[0];
    else
      nextPointer = getNPointer(pointer, 0);

    if (nextPointer == OMemory.NULL_POINTER)
      return OMemory.NULL_POINTER;

    return getDataPointer(nextPointer);
  }

  public int getFloor(K key) {
    int level = getStartLevel();

    int forwardPointer;
    int pointer = OMemory.NULL_POINTER;
    int oldForwardPointer = OMemory.NULL_POINTER;
    int oldCompareResult = -1;
    while (level >= 0) {
      if (pointer == OMemory.NULL_POINTER)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(pointer, level);

      if (forwardPointer == OMemory.NULL_POINTER) {
        oldForwardPointer = forwardPointer;
        level--;
        continue;
      }

      K currentKey = getKey(forwardPointer);
      int compareResult;
      if (oldForwardPointer == forwardPointer)
        compareResult = oldCompareResult;
      else
        compareResult = comparator.compare(key, currentKey);

      if (compareResult == 0)
        return getDataPointer(forwardPointer);

      if (compareResult < 0) {
        oldForwardPointer = forwardPointer;
        oldCompareResult = compareResult;
        level--;
        continue;
      }

      pointer = forwardPointer;
    }

    if (pointer == OMemory.NULL_POINTER)
      return OMemory.NULL_POINTER;

    return getDataPointer(pointer);
  }

  public int getFirst() {
    return getDataPointer(header[0]);
  }

  public int getLast() {
    int level = getStartLevel();

    int forwardPointer;
    int pointer = OMemory.NULL_POINTER;
    while (level >= 0) {
      if (pointer == OMemory.NULL_POINTER)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(pointer, level);

      if (forwardPointer == OMemory.NULL_POINTER)
        level--;
      else
        pointer = forwardPointer;
    }

    if (pointer == OMemory.NULL_POINTER)
      return OMemory.NULL_POINTER;

    return getDataPointer(pointer);
  }

  public int remove(K key) {
    final int[] update = new int[MAX_LEVEL];
    Arrays.fill(update, OMemory.NULL_POINTER);

    int level = getStartLevel();
    int forwardPointer = header[level];
    int pointer = OMemory.NULL_POINTER;
    int oldForwardPointer = OMemory.NULL_POINTER;

    int oldCompareResult = -1;
    int compareResult = -1;
    while (level >= 0) {
      if (pointer == OMemory.NULL_POINTER)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(pointer, level);

      if (forwardPointer == OMemory.NULL_POINTER) {
        update[level] = pointer;
        level--;
        oldForwardPointer = forwardPointer;
        continue;
      }

      K currentKey = getKey(forwardPointer);
      if (oldForwardPointer == forwardPointer)
        compareResult = oldCompareResult;
      else
        compareResult = comparator.compare(key, currentKey);

      if (compareResult <= 0) {
        update[level] = pointer;
        level--;
        oldForwardPointer = forwardPointer;
        oldCompareResult = compareResult;
        continue;
      }

      pointer = forwardPointer;
      oldForwardPointer = OMemory.NULL_POINTER;
    }

    if (compareResult != 0)
      return OMemory.NULL_POINTER;

    final int itemLevel = getPointersSize(forwardPointer);

    int updatePointer;
    for (int i = itemLevel - 1; i >= 0; i--) {
      updatePointer = update[i];
      if (updatePointer != OMemory.NULL_POINTER) {
        setNPointer(updatePointer, i, getNPointer(forwardPointer, i));
      } else
        header[i] = getNPointer(forwardPointer, i);
    }

    final int dataPointer = getDataPointer(forwardPointer);
    freeItem(forwardPointer);

    size--;
    return dataPointer;
  }

  private int getStartLevel() {
    int level = MAX_LEVEL;
    int forwardPointer = header[level];

    while (forwardPointer == OMemory.NULL_POINTER && level > 0) {
      level--;
      forwardPointer = header[level];
    }

    return level;
  }

  private int[] createPointers(int[] update) {
    int[] pointers = new int[randomLevel() + 1];
    for (int i = 0; i < pointers.length; i++)
      if (update[i] == OMemory.NULL_POINTER)
        pointers[i] = header[i];
      else {
        pointers[i] = getNPointer(update[i], i);
      }

    return pointers;
  }

  private int randomLevel() {
    int x = seed;
    x ^= x << 13;
    x ^= x >>> 17;
    seed = x ^= x << 5;
    if ((x & 0x8001) != 0) // test highest and lowest bits
      return 0;

    int level = 1;
    while (((x >>>= 1) & 1) != 0)
      ++level;
    return level;
  }

  private int storeItem(int[] pointers, K key, int dataPointer) {
    final int pointersSize = OIntegerSerializer.INT_SIZE * (pointers.length + 1);
    final int keySize = keySerializer.getObjectSize(key);
    final int size = 5 * OIntegerSerializer.INT_SIZE + keySize;

    final int pointer = memory.allocate(size);

    if (pointer == OMemory.NULL_POINTER)
      return OMemory.NULL_POINTER;

    final int pointersPointer = memory.allocate(pointersSize);
    if (pointersPointer == OMemory.NULL_POINTER) {
      memory.free(pointer);
      return OMemory.NULL_POINTER;
    }

    int offset = 0;
    memory.setInt(pointer, 0, pointersPointer);
    offset += OIntegerSerializer.INT_SIZE;

    memory.setInt(pointer, offset, dataPointer);
    offset += OIntegerSerializer.INT_SIZE;

    memory.set(pointer, offset, key, keySerializer);

    offset = 0;
    memory.setInt(pointersPointer, offset, pointers.length);
    offset += OIntegerSerializer.INT_SIZE;

    for (int pointersItem : pointers) {
      memory.setInt(pointersPointer, offset, pointersItem);
      offset += OIntegerSerializer.INT_SIZE;
    }

    return pointer;
  }

  private void freeItem(int pointer) {
    final int pointersPointer = memory.getInt(pointer, 0);

    memory.free(pointersPointer);
    memory.free(pointer);
  }

  private K getKey(int pointer) {
    final int offset = 2 * OIntegerSerializer.INT_SIZE;

    return memory.get(pointer, offset, keySerializer);
  }

  private int getNPointer(int pointer, int level) {
    final int pointersPointer = memory.getInt(pointer, 0);
    final int offset = (level + 1) * OIntegerSerializer.INT_SIZE;

    return memory.getInt(pointersPointer, offset);
  }

  private int getPointersSize(int pointer) {
    final int pointersPointer = memory.getInt(pointer, 0);

    return memory.getInt(pointersPointer, 0);
  }

  private int getDataPointer(int pointer) {
    final int offset = OIntegerSerializer.INT_SIZE;

    return memory.getInt(pointer, offset);
  }

  private void setDataPointer(int pointer, int dataPointer) {
    final int offset = OIntegerSerializer.INT_SIZE;

    memory.setInt(pointer, offset, dataPointer);
  }

  private void setNPointer(int pointer, int level, int dataPointer) {
    final int pointersPointer = memory.getInt(pointer, 0);
    final int offset = (level + 1) * OIntegerSerializer.INT_SIZE;

    memory.setInt(pointersPointer, offset, dataPointer);
  }

  /**
   * Comparator which supposes that K is Comparable.
   * 
   * @param <K>
   *          the type of objects that may be compared by this comparator
   */
  private static class NativeComparator<K> implements Comparator<K> {
    @SuppressWarnings("unchecked")
    public int compare(K o1, K o2) {
      return ((Comparable<K>) o1).compareTo(o2);
    }
  }
}
