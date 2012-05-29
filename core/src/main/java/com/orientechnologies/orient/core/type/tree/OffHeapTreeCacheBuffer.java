/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.type.tree;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;

import java.util.Arrays;
import java.util.Random;

/**
 * @author Andrey Lomakin
 * @since 07.04.12
 */
public class OffHeapTreeCacheBuffer<K extends Comparable<K>> {
  private static final int           MAX_LEVEL = 25;

  private final OBinarySerializer<K> keySerializer;
  private final OffHeapMemory        memory;

  private final Random               random    = new Random();

  private int[]                      header;

  private long size = 0;

  private boolean debug = false;

  private int printStructureForNItems = 100;

  public OffHeapTreeCacheBuffer(final OffHeapMemory memory, final OBinarySerializer<K> keySerializer) {
    this.memory = memory;

    this.keySerializer = keySerializer;

    header = new int[MAX_LEVEL + 1];
    Arrays.fill(header, OffHeapMemory.NULL_POINTER);
  }

  public void setDebug(boolean debug) {
    this.debug = debug;
  }

  public void setPrintStructureForNItems(int printStructureForNItems) {
    this.printStructureForNItems = printStructureForNItems;
  }

  public boolean add(CacheEntry<K> entry) {
    final int[] update = new int[MAX_LEVEL];
    Arrays.fill(update, OffHeapMemory.NULL_POINTER);

    int level = MAX_LEVEL;
    int forwardPointer = header[level];

    while (forwardPointer == OffHeapMemory.NULL_POINTER && level > 0) {
      level--;
      forwardPointer = header[level];
    }

    if (forwardPointer == OffHeapMemory.NULL_POINTER) {
      int[] pointers = createPointers(update);
      byte[] dataStream = fromEntryToStream(entry);
      int dataPointer = memory.add(dataStream);

      byte[] stream  = fromItemToStream(pointers, entry.firstKey, dataPointer);

      final int itemPointer = memory.add(stream);

      if (itemPointer == OffHeapMemory.NULL_POINTER)
        return false;

      Arrays.fill(header, 0, pointers.length, itemPointer);

      size++;

      if(debug && size % printStructureForNItems == 0)
        printStructure();

      return true;
    }

    byte[] stream = null;
    int pointer = OffHeapMemory.NULL_POINTER;

    while (level >= 0) {
      byte[] forwardStream;

      if (stream == null)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(stream, level);

      if (forwardPointer == OffHeapMemory.NULL_POINTER) {
        update[level] = pointer;
        level--;

        continue;
      }

      forwardStream = memory.get(forwardPointer);

      K key = getKey(forwardStream);
      int compareResult = entry.firstKey.compareTo(key);

      if (compareResult == 0)
        return false;

      if (compareResult < 0) {
        update[level] = pointer;
        level--;
        continue;
      }

      stream = forwardStream;
      pointer = forwardPointer;
    }


    final int[] pointers = createPointers(update);
    byte[] dataStream = fromEntryToStream(entry);
    int dataPointer = memory.add(dataStream);

    stream = fromItemToStream(pointers, entry.firstKey, dataPointer);

    final int newItemPointer = memory.add(stream);

    if (newItemPointer == OffHeapMemory.NULL_POINTER)
      return false;

    byte[] updateStream = null;
    int updatePointer = OffHeapMemory.NULL_POINTER;

    boolean streamIsDirty = false;
    for (int i = 0; i < pointers.length; i++) {
      if (update[i] != updatePointer) {
        if (streamIsDirty) {
          memory.update(updatePointer, updateStream);
          streamIsDirty = false;
        }

        updatePointer = update[i];

        if (updatePointer != OffHeapMemory.NULL_POINTER)
          updateStream = memory.get(updatePointer);
      }

      if (updatePointer != OffHeapMemory.NULL_POINTER) {
        setNPointer(updateStream, i, newItemPointer);
        streamIsDirty = true;
      } else
        header[i] = newItemPointer;
    }

    if (streamIsDirty)
      memory.update(updatePointer, updateStream);

    size++;

    if(debug && size % printStructureForNItems == 0)
      printStructure();

    return true;
  }

  public CacheEntry<K> get(K firstKey) {
    int level = MAX_LEVEL;
    int forwardPointer = header[level];

    while (forwardPointer == OffHeapMemory.NULL_POINTER && level > 0) {
      level--;
      forwardPointer = header[level];
    }

    if (forwardPointer == OffHeapMemory.NULL_POINTER) {
      return null;
    }

    byte[] stream = null;
    while (level >= 0) {
      byte[] forwardStream;

      if (stream == null)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(stream, level);

      if (forwardPointer == OffHeapMemory.NULL_POINTER) {
        level--;
        continue;
      }

      forwardStream = memory.get(forwardPointer);

      K key = getKey(forwardStream);
      int compareResult = firstKey.compareTo(key);

      if (compareResult == 0) {
        byte[] dataStream = memory.get(getDataPointer(forwardStream));
        return fromStreamToEntry(key, dataStream);
      }

      if (compareResult < 0) {
        level--;
        continue;
      }

      stream = forwardStream;
    }

    return null;
  }

  public long size() {
    return size;
  }

  public CacheEntry<?> getCeiling(K firstKey) {
    return null;
  }

  public CacheEntry<K> getFloor(K firstKey) {
    int level = MAX_LEVEL;
    int forwardPointer = header[level];

    while (forwardPointer == OffHeapMemory.NULL_POINTER && level > 0) {
      level--;
      forwardPointer = header[level];
    }

    if (forwardPointer == OffHeapMemory.NULL_POINTER) {
      return null;
    }

    byte[] stream = null;
    while (level >= 0) {
      byte[] forwardStream;

      if (stream == null)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(stream, level);

      if (forwardPointer == OffHeapMemory.NULL_POINTER) {
        level--;
        continue;
      }

      forwardStream = memory.get(forwardPointer);

      K key = getKey(forwardStream);
      int compareResult = firstKey.compareTo(key);

      if (compareResult == 0) {
        byte[] dataStream = memory.get(getDataPointer(forwardStream));
        return fromStreamToEntry(key, dataStream);
      }

      if (compareResult < 0) {
        level--;
        continue;
      }

      stream = forwardStream;
    }

    if(stream == null)
      return null;

    byte[] dataStream = memory.get(getDataPointer(stream));
    return fromStreamToEntry(getKey(stream), dataStream);
  }

  public CacheEntry<K> remove(K firstKey) {
    final int[] update = new int[MAX_LEVEL];
    Arrays.fill(update, OffHeapMemory.NULL_POINTER);

    int level = MAX_LEVEL;
    int forwardPointer = header[level];

    while (forwardPointer == OffHeapMemory.NULL_POINTER && level > 0) {
      level--;
      forwardPointer = header[level];
    }

    if (forwardPointer == OffHeapMemory.NULL_POINTER) {
       return null;
    }

    byte[] stream = null;
    int pointer = OffHeapMemory.NULL_POINTER;

    int compareResult = -1;
    byte[] forwardStream = null;
    while (level >= 0) {
      if (stream == null)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(stream, level);

      if (forwardPointer == OffHeapMemory.NULL_POINTER) {
        update[level] = pointer;
        level--;

        continue;
      }

      forwardStream = memory.get(forwardPointer);

      K key = getKey(forwardStream);
      compareResult = firstKey.compareTo(key);

      if (compareResult <= 0) {
        update[level] = pointer;
        level--;
        continue;
      }

      stream = forwardStream;
      pointer = forwardPointer;
    }

    if(compareResult != 0)
      return null;

    memory.remove(forwardPointer);

    final int itemLevel = getPointersSize(forwardStream);
    boolean streamIsDirty = false;
    int updatePointer = OffHeapMemory.NULL_POINTER;
    byte[] updateStream = null;

    for(int i = itemLevel - 1; i >= 0; i--) {
      if (update[i] != updatePointer) {
        if (streamIsDirty) {
          memory.update(updatePointer, updateStream);
          streamIsDirty = false;
        }

        updatePointer = update[i];

        if (updatePointer != OffHeapMemory.NULL_POINTER)
          updateStream = memory.get(updatePointer);
      }

      if (updatePointer != OffHeapMemory.NULL_POINTER) {
        setNPointer(updateStream, i, getNPointer(forwardStream, i));
        streamIsDirty = true;
      } else
        header[i] = getNPointer(forwardStream, i);
    }

    if (streamIsDirty)
      memory.update(updatePointer, updateStream);

    size--;

    if(debug && size % printStructureForNItems == 0)
      printStructure();

    int dataPointer = getDataPointer(forwardStream);
    byte[] dataStream = memory.get(dataPointer);
    memory.remove(dataPointer);

    return fromStreamToEntry(firstKey, dataStream);
  }

  public boolean update(CacheEntry<K> entry) {
    int level = MAX_LEVEL;
    int forwardPointer = header[level];

    while (forwardPointer == OffHeapMemory.NULL_POINTER && level > 0) {
      level--;
      forwardPointer = header[level];
    }

    if (forwardPointer == OffHeapMemory.NULL_POINTER) {
      return false;
    }

    byte[] stream = null;
    while (level >= 0) {
      byte[] forwardStream;

      if (stream == null)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(stream, level);

      if (forwardPointer == OffHeapMemory.NULL_POINTER) {
        level--;
        continue;
      }

      forwardStream = memory.get(forwardPointer);

      K key = getKey(forwardStream);
      int compareResult = entry.firstKey.compareTo(key);

      if (compareResult == 0) {
        int dataPointer = getDataPointer(forwardStream);
        memory.remove(dataPointer);

        byte[] dataStream = fromEntryToStream(entry);

        dataPointer = memory.add(dataStream);

        setDataPointer(forwardStream, dataPointer);
        memory.update(forwardPointer, forwardStream);

        return true;
      }

      if (compareResult < 0) {
        level--;
        continue;
      }

      stream = forwardStream;
    }

    return false;
  }

  private int[] createPointers(int[] update) {
    int[] pointers = new int[randomLevel()];
    for (int i = 0; i < pointers.length; i++)
      if (update[i] == OffHeapMemory.NULL_POINTER)
        pointers[i] = header[i];
      else {
        byte[] content = memory.get(update[i]);
        pointers[i] = getNPointer(content, i);
      }

    return pointers;
  }

  private int randomLevel() {
    int newLevel = 1;
    // TODO: should be converted from double to int generation.
    while (random.nextDouble() < 0.5 && newLevel <= MAX_LEVEL)
      newLevel++;

    return newLevel;
  }

  private CacheEntry<K> fromStreamToEntry(K firstKey, byte[] content) {
    int offset = 0;

    final K lastKey = keySerializer.deserialize(content, offset);
    offset += keySerializer.getObjectSize(lastKey);

    final ORID rid = OLinkSerializer.INSTANCE.deserialize(content, offset);
		offset += OLinkSerializer.INSTANCE.getObjectSize(rid);

		final ORID leftRid = OLinkSerializer.INSTANCE.deserialize(content, offset);
		offset += OLinkSerializer.INSTANCE.getObjectSize(leftRid);

		final ORID rightRid = OLinkSerializer.INSTANCE.deserialize(content, offset);
		offset += OLinkSerializer.INSTANCE.getObjectSize(rightRid);

		final ORID parentRid = OLinkSerializer.INSTANCE.deserialize(content, offset);
		offset += OLinkSerializer.INSTANCE.getObjectSize(parentRid);

		return new CacheEntry<K>(firstKey, lastKey, rid, parentRid, leftRid, rightRid);
  }

  private byte[] fromEntryToStream(CacheEntry<K> entry) {
    int size = keySerializer.getObjectSize(entry.lastKey);
    size += OLinkSerializer.INSTANCE.getObjectSize(entry.rid);
    size += OLinkSerializer.INSTANCE.getObjectSize(entry.leftRid);
    size += OLinkSerializer.INSTANCE.getObjectSize(entry.rightRid);
    size += OLinkSerializer.INSTANCE.getObjectSize(entry.parentRid);

    final byte[] content = new byte[size];

    int offset = 0;

    keySerializer.serialize(entry.lastKey, content, offset);
    offset += keySerializer.getObjectSize(entry.lastKey);

    OLinkSerializer.INSTANCE.serialize(entry.rid, content, offset);
		offset += OLinkSerializer.INSTANCE.getObjectSize(entry.rid);

		OLinkSerializer.INSTANCE.serialize(entry.leftRid, content, offset);
		offset += OLinkSerializer.INSTANCE.getObjectSize(entry.leftRid);

		OLinkSerializer.INSTANCE.serialize(entry.rightRid, content, offset);
		offset += OLinkSerializer.INSTANCE.getObjectSize(entry.rightRid);

		OLinkSerializer.INSTANCE.serialize(entry.parentRid, content, offset);
		offset += OLinkSerializer.INSTANCE.getObjectSize(entry.parentRid);

		return content;
  }

  private byte[] fromItemToStream(int[] pointers, K firstKey, int dataPointer) {
    int size = 2* OIntegerSerializer.INT_SIZE + OIntegerSerializer.INT_SIZE * pointers.length;
    size += keySerializer.getObjectSize(firstKey);

    byte[] stream = new byte[size];

    int offset = 0;

    OIntegerSerializer.INSTANCE.serialize(pointers.length, stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (int pointer : pointers) {
      OIntegerSerializer.INSTANCE.serialize(pointer, stream, offset);
      offset += OIntegerSerializer.INT_SIZE;
    }

    keySerializer.serialize(firstKey, stream, offset);
    offset += keySerializer.getObjectSize(firstKey);

    OIntegerSerializer.INSTANCE.serialize(dataPointer, stream, offset);

    return stream;
  }

  private K getKey(byte[] stream) {
    int pointersLen = OIntegerSerializer.INSTANCE.deserialize(stream, 0);

    return keySerializer.deserialize(stream, OIntegerSerializer.INT_SIZE + pointersLen * OIntegerSerializer.INT_SIZE);
  }

  private int getNPointer(byte[] stream, int level) {
    final int offset = OIntegerSerializer.INT_SIZE + level * OIntegerSerializer.INT_SIZE;

    return OIntegerSerializer.INSTANCE.deserialize(stream, offset);
  }

  private int getPointersSize(byte[] stream) {
    return OIntegerSerializer.INSTANCE.deserialize(stream, 0);
  }

  private int getDataPointer(byte[] stream) {
    int offset =  OIntegerSerializer.INT_SIZE + OIntegerSerializer.INSTANCE.deserialize(stream, 0) * OIntegerSerializer.INT_SIZE;

    offset += keySerializer.getObjectSize(stream, offset);

    return OIntegerSerializer.INSTANCE.deserialize(stream, offset);
  }

  private void setDataPointer(byte[] stream, int dataPointer) {
    int offset =  OIntegerSerializer.INT_SIZE + OIntegerSerializer.INSTANCE.deserialize(stream, 0) * OIntegerSerializer.INT_SIZE;

    offset += keySerializer.getObjectSize(stream, offset);

    OIntegerSerializer.INSTANCE.serialize(dataPointer, stream, offset);
  }

  private void setNPointer(byte[] content, int level, int pointer) {
    final int offset = OIntegerSerializer.INT_SIZE + level * OIntegerSerializer.INT_SIZE;
    OIntegerSerializer.INSTANCE.serialize(pointer, content, offset);
  }

  private void printStructure() {
    System.out.println("---------------------------------------------------------------------------------------------");
    System.out.println("Size : " + size);

    int level = MAX_LEVEL;
    int forwardPointer = header[level];

    while (forwardPointer == OffHeapMemory.NULL_POINTER && level > 0) {
      level--;
      forwardPointer = header[level];
    }

    System.out.println("Max level : " + level);

    for(int n = level; n>=0; n--) {
      int itemsCount = 0;

      byte[] content = null;

      while (true) {
        if (content == null)
          forwardPointer = header[n];
        else
          forwardPointer = getNPointer(content, n);

        if(forwardPointer == OffHeapMemory.NULL_POINTER)
          break;

        content = memory.get(forwardPointer);
        itemsCount++;
      }
      System.out.println("Current level :" + n + ", items :" + itemsCount);
    }

    System.out.println("---------------------------------------------------------------------------------------------");
  }

  public static final class CacheEntry<K> {
    final K    firstKey;
    final K    lastKey;
    final ORID rid;
		final ORID parentRid;
		final ORID leftRid;
		final ORID rightRid;

    public CacheEntry(K firstKey, K lastKey, ORID rid, ORID parentRid, ORID leftRid, ORID rightRid) {
      this.firstKey = firstKey;
      this.lastKey = lastKey;
      this.rid = rid;
			this.parentRid = parentRid;
			this.leftRid = leftRid;
			this.rightRid = rightRid;
    }

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			CacheEntry<?> that = (CacheEntry<?>) o;

			if (!firstKey.equals(that.firstKey)) return false;
			if (!lastKey.equals(that.lastKey)) return false;
			if (!leftRid.equals(that.leftRid)) return false;
			if (!parentRid.equals(that.parentRid)) return false;
			if (!rid.equals(that.rid)) return false;
			if (!rightRid.equals(that.rightRid)) return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result = firstKey.hashCode();
			result = 31 * result + lastKey.hashCode();
			result = 31 * result + rid.hashCode();
			result = 31 * result + parentRid.hashCode();
			result = 31 * result + leftRid.hashCode();
			result = 31 * result + rightRid.hashCode();
			return result;
		}
	}
}
