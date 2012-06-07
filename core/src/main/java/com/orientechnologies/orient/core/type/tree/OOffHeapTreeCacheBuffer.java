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
public class OOffHeapTreeCacheBuffer<K extends Comparable<K>> {
  private static final int           MAX_LEVEL = 25;

  private final OBinarySerializer<K> keySerializer;
  private final OOffHeapMemory memory;

  private final Random               random    = new Random();

  private int[]                      header;

  private int lruHeader = OOffHeapMemory.NULL_POINTER;
  private int lruTail = OOffHeapMemory.NULL_POINTER;

  private int evictionSize = -1;
  private int defaultEvictionPercent = 20;

  private long size = 0;

  public OOffHeapTreeCacheBuffer(final OOffHeapMemory memory, final OBinarySerializer<K> keySerializer) {
    this.memory = memory;

    this.keySerializer = keySerializer;

    header = new int[MAX_LEVEL + 1];
    Arrays.fill(header, OOffHeapMemory.NULL_POINTER);
  }

  public void setEvictionSize(int evictionSize) {
    this.evictionSize = evictionSize;
  }

  public void setDefaultEvictionPercent(int defaultEvictionPercent) {
    this.defaultEvictionPercent = defaultEvictionPercent;
  }

  public boolean add(CacheEntry<K> entry) {
    final int[] update = new int[MAX_LEVEL];
    Arrays.fill(update, OOffHeapMemory.NULL_POINTER);

    int level = MAX_LEVEL;
    int forwardPointer = header[level];

    while (forwardPointer == OOffHeapMemory.NULL_POINTER && level > 0) {
      level--;
      forwardPointer = header[level];
    }

    if (forwardPointer == OOffHeapMemory.NULL_POINTER) {
      int[] pointers = createPointers(update);
      byte[] dataStream = fromEntryToStream(entry);
      int dataPointer = memory.allocate(dataStream);

			if (dataPointer == OOffHeapMemory.NULL_POINTER)
				return evict() && add(entry);

			byte[] stream  = fromItemToStream(pointers, entry.firstKey, dataPointer, OOffHeapMemory.NULL_POINTER,
              OOffHeapMemory.NULL_POINTER);

      final int itemPointer = memory.allocate(stream);

      if (itemPointer == OOffHeapMemory.NULL_POINTER)
        return evict() && add(entry);

      Arrays.fill(header, 0, pointers.length, itemPointer);

      size++;

      addItemToLRU(itemPointer);

      return true;
    }

    int pointer = OOffHeapMemory.NULL_POINTER;

    while (level >= 0) {
      if (pointer == OOffHeapMemory.NULL_POINTER)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(pointer, level);

      if (forwardPointer == OOffHeapMemory.NULL_POINTER) {
        update[level] = pointer;
        level--;

        continue;
      }

      K key = getKey(forwardPointer);
      int compareResult = entry.firstKey.compareTo(key);

      if (compareResult == 0)
        return false;

      if (compareResult < 0) {
        update[level] = pointer;
        level--;
        continue;
      }

      pointer = forwardPointer;
    }


    final int[] pointers = createPointers(update);
    byte[] dataStream = fromEntryToStream(entry);
    int dataPointer = memory.allocate(dataStream);

		if (dataPointer == OOffHeapMemory.NULL_POINTER)
			return evict() && add(entry);

		final byte[] stream = fromItemToStream(pointers, entry.firstKey, dataPointer, OOffHeapMemory.NULL_POINTER,
            OOffHeapMemory.NULL_POINTER);

    final int newItemPointer = memory.allocate(stream);

		if(newItemPointer == OOffHeapMemory.NULL_POINTER)
			return evict() && add(entry);

    addItemToLRU(newItemPointer);

    int updatePointer;

    for (int i = 0; i < pointers.length; i++) {
      updatePointer = update[i];
      if (updatePointer != OOffHeapMemory.NULL_POINTER) {
        setNPointer(updatePointer, i, newItemPointer);
      } else
        header[i] = newItemPointer;
    }

    size++;

    if(evictionSize > 0 && size >= evictionSize)
      evict();

    return true;
  }

  public CacheEntry<K> get(K firstKey) {
    int level = MAX_LEVEL;
    int forwardPointer = header[level];

    while (forwardPointer == OOffHeapMemory.NULL_POINTER && level > 0) {
      level--;
      forwardPointer = header[level];
    }

    if (forwardPointer == OOffHeapMemory.NULL_POINTER) {
      return null;
    }

		int pointer = OOffHeapMemory.NULL_POINTER;
    while (level >= 0) {
      if (pointer == OOffHeapMemory.NULL_POINTER)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(pointer, level);

      if (forwardPointer == OOffHeapMemory.NULL_POINTER) {
        level--;
        continue;
      }

      K key = getKey(forwardPointer);
      int compareResult = firstKey.compareTo(key);

      if (compareResult == 0) {
        byte[] dataStream = memory.get(getDataPointer(forwardPointer), 0, -1);
        updateItemInLRU(forwardPointer);
        return fromStreamToEntry(key, dataStream);
      }

      if (compareResult < 0) {
        level--;
        continue;
      }

      pointer = forwardPointer;
    }

    return null;
  }

  public long size() {
    return size;
  }

  public CacheEntry<K> getCeiling(K firstKey) {
    int level = MAX_LEVEL;
    int forwardPointer = header[level];

    while (forwardPointer == OOffHeapMemory.NULL_POINTER && level > 0) {
      level--;
      forwardPointer = header[level];
    }

    if (forwardPointer == OOffHeapMemory.NULL_POINTER) {
      return null;
    }

    int pointer = OOffHeapMemory.NULL_POINTER;
    while (level >= 0) {
      if (pointer == OOffHeapMemory.NULL_POINTER)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(pointer, level);

      if (forwardPointer == OOffHeapMemory.NULL_POINTER) {
        level--;
        continue;
      }

      K key = getKey(forwardPointer);
      int compareResult = firstKey.compareTo(key);

      if (compareResult == 0) {
        byte[] dataStream = memory.get(getDataPointer(forwardPointer), 0, -1);
        updateItemInLRU(forwardPointer);
        return fromStreamToEntry(key, dataStream);
      }

      if (compareResult < 0) {
        level--;
        continue;
      }

			pointer = forwardPointer;
    }

    int nextPointer;
    if(pointer == OOffHeapMemory.NULL_POINTER)
      nextPointer = header[0];
    else
      nextPointer = getNPointer(pointer, 0);

    if(nextPointer == OOffHeapMemory.NULL_POINTER)
      return null;

    updateItemInLRU(nextPointer);
    byte[] dataStream = memory.get(getDataPointer(nextPointer), 0, -1);
    return fromStreamToEntry(getKey(nextPointer), dataStream);
  }

  public CacheEntry<K> getFloor(K firstKey) {
    int level = MAX_LEVEL;
    int forwardPointer = header[level];

    while (forwardPointer == OOffHeapMemory.NULL_POINTER && level > 0) {
      level--;
      forwardPointer = header[level];
    }

    if (forwardPointer == OOffHeapMemory.NULL_POINTER) {
      return null;
    }


    int pointer = OOffHeapMemory.NULL_POINTER;
    while (level >= 0) {
      if (pointer == OOffHeapMemory.NULL_POINTER)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(pointer, level);

      if (forwardPointer == OOffHeapMemory.NULL_POINTER) {
        level--;
        continue;
      }

      K key = getKey(forwardPointer);
      int compareResult = firstKey.compareTo(key);

      if (compareResult == 0) {
        byte[] dataStream = memory.get(getDataPointer(forwardPointer), 0, -1);
        updateItemInLRU(forwardPointer);
        return fromStreamToEntry(key, dataStream);
      }

      if (compareResult < 0) {
        level--;
        continue;
      }

      pointer = forwardPointer;
    }

    if(pointer == OOffHeapMemory.NULL_POINTER)
      return null;

    updateItemInLRU(pointer);
    byte[] dataStream = memory.get(getDataPointer(pointer), 0, -1);
    return fromStreamToEntry(getKey(pointer), dataStream);
  }

  public CacheEntry<K> remove(K firstKey) {
    final int[] update = new int[MAX_LEVEL];
    Arrays.fill(update, OOffHeapMemory.NULL_POINTER);

    int level = MAX_LEVEL;
    int forwardPointer = header[level];

    while (forwardPointer == OOffHeapMemory.NULL_POINTER && level > 0) {
      level--;
      forwardPointer = header[level];
    }

    if (forwardPointer == OOffHeapMemory.NULL_POINTER) {
       return null;
    }

    int pointer = OOffHeapMemory.NULL_POINTER;

    int compareResult = -1;
    while (level >= 0) {
      if (pointer == OOffHeapMemory.NULL_POINTER)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(pointer, level);

      if (forwardPointer == OOffHeapMemory.NULL_POINTER) {
        update[level] = pointer;
        level--;

        continue;
      }

      K key = getKey(forwardPointer);
      compareResult = firstKey.compareTo(key);

      if (compareResult <= 0) {
        update[level] = pointer;
        level--;
        continue;
      }

      pointer = forwardPointer;
    }

    if(compareResult != 0)
      return null;


		final int itemLevel = getPointersSize(forwardPointer);


    int updatePointer;
    for(int i = itemLevel - 1; i >= 0; i--) {
       updatePointer = update[i];
      if (updatePointer != OOffHeapMemory.NULL_POINTER) {
        setNPointer(updatePointer, i, getNPointer(forwardPointer, i));
      } else
        header[i] = getNPointer(forwardPointer, i);
    }

		removeItemFromLRU(forwardPointer);
		memory.free(forwardPointer);

		size--;


    int dataPointer = getDataPointer(forwardPointer);
    byte[] dataStream = memory.get(dataPointer, 0, -1);
    memory.free(dataPointer);

    return fromStreamToEntry(firstKey, dataStream);
  }

  public boolean update(CacheEntry<K> entry) {
    int level = MAX_LEVEL;
    int forwardPointer = header[level];

    while (forwardPointer == OOffHeapMemory.NULL_POINTER && level > 0) {
      level--;
      forwardPointer = header[level];
    }

    if (forwardPointer == OOffHeapMemory.NULL_POINTER) {
      return false;
    }

    int pointer = OOffHeapMemory.NULL_POINTER;
    while (level >= 0) {
      if (pointer == OOffHeapMemory.NULL_POINTER)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(pointer, level);

      if (forwardPointer == OOffHeapMemory.NULL_POINTER) {
        level--;
        continue;
      }

      K key = getKey(forwardPointer);
      int compareResult = entry.firstKey.compareTo(key);

      if (compareResult == 0) {
        int dataPointer = getDataPointer(forwardPointer);
        memory.free(dataPointer);

        byte[] dataStream = fromEntryToStream(entry);

        dataPointer = memory.allocate(dataStream);

				if(dataPointer == OOffHeapMemory.NULL_POINTER)
					return evict() && update(entry);

        setDataPointer(forwardPointer, dataPointer);

        updateItemInLRU(forwardPointer);
        return true;
      }

      if (compareResult < 0) {
        level--;
        continue;
      }

      pointer = forwardPointer;
    }

    return false;
  }

  public boolean evict() {
    return evict(defaultEvictionPercent);
  }

  public boolean evict(int percent) {
    if(percent <= 0 || percent > 100)
      return false;

    long evictionSize = (size * percent) / 100;
    if(evictionSize == 0)
      return false;

    int evicted = 0;
    int currentVictim = lruTail;
    while (currentVictim != OOffHeapMemory.NULL_POINTER && evicted < evictionSize) {
      int evictedItem = currentVictim;
      currentVictim = getPrevLRUPointer(evictedItem);

      evictItem(evictedItem);
      evicted++;
    }

    lruTail = currentVictim;
    if(lruTail != OOffHeapMemory.NULL_POINTER)
      setNextLRUPointer(lruTail, OOffHeapMemory.NULL_POINTER);

    return true;
  }

	private void printLRU() {
		System.out.println("from head----------------------------------------------------------------------------------------------");
		int pointer = lruHeader;
		while (pointer != OOffHeapMemory.NULL_POINTER) {
			System.out.print(getKey(pointer) + "-");
			pointer = getNextLRUPointer(pointer);
		}
		System.out.println("\nfrom tail----------------------------------------------------------------------------------------------");

		pointer = lruTail;
		while (pointer != OOffHeapMemory.NULL_POINTER) {
			System.out.print(getKey(pointer) + "-");
			pointer = getPrevLRUPointer(pointer);
		}
		System.out.println("\n----------------------------------------------------------------------------------------------");

	}

  private void evictItem(int pointer) {
    int pointersLen = getPointersSize(pointer);

    int update[] = new int[pointersLen];
    Arrays.fill(update, OOffHeapMemory.NULL_POINTER);

    int forwardPointer = header[0];

    while (forwardPointer != pointer) {
      int currentPointer = forwardPointer;

      forwardPointer = getNPointer(currentPointer, 0);

      pointersLen = getPointersSize(currentPointer);

      int updateSize = Math.min(update.length, pointersLen);
      for(int i = 0; i < updateSize; i++) {
        update[i] = currentPointer;
      }
    }

    int updatePointer;
    for(int i = update.length - 1; i >= 0; i--) {
      updatePointer = update[i];

      if (updatePointer != OOffHeapMemory.NULL_POINTER) {
        setNPointer(updatePointer, i, getNPointer(pointer, i));
      } else
        header[i] = getNPointer(pointer, i);
    }

    int dataPointer = getDataPointer(pointer);
    memory.free(pointer);
    memory.free(dataPointer);

    size--;
  }

  private void addItemToLRU(int pointer) {
    if(lruHeader == OOffHeapMemory.NULL_POINTER) {
      lruHeader = pointer;
      lruTail = pointer;
      return;
    }

    int nextPointer = lruHeader;
    setNextLRUPointer(pointer, nextPointer);
    lruHeader = pointer;
    setPrevLRUPointer(nextPointer, pointer);
  }

  private void updateItemInLRU(final int pointer) {
    if(pointer == lruHeader)
      return;

    final int prevPointer = getPrevLRUPointer(pointer);
    final int nextPointer = getNextLRUPointer(pointer);

		int prevHeader = lruHeader;

		if(prevPointer != OOffHeapMemory.NULL_POINTER)
			setNextLRUPointer(prevPointer, nextPointer);

		if(nextPointer != OOffHeapMemory.NULL_POINTER)
			setPrevLRUPointer(nextPointer, prevPointer);

    if(prevPointer != OOffHeapMemory.NULL_POINTER)
      setPrevLRUPointer(pointer, OOffHeapMemory.NULL_POINTER);

    setNextLRUPointer(pointer, lruHeader);

		setPrevLRUPointer(prevHeader, pointer);

    lruHeader = pointer;

    if(lruTail == pointer)
      lruTail = prevPointer;

  }

  private void removeItemFromLRU(int pointer) {
    if(lruHeader == pointer && lruTail == pointer) {
      lruHeader = OOffHeapMemory.NULL_POINTER;
      lruTail = OOffHeapMemory.NULL_POINTER;
      return;
    }

    int prevPointer = getPrevLRUPointer(pointer);
    int nextPointer = getNextLRUPointer(pointer);

    if(lruHeader == pointer)
      lruHeader = nextPointer;

    if(lruTail == pointer)
      lruTail = prevPointer;

    if(prevPointer != OOffHeapMemory.NULL_POINTER)
      setNextLRUPointer(prevPointer, nextPointer);

    if(nextPointer != OOffHeapMemory.NULL_POINTER)
      setPrevLRUPointer(nextPointer, prevPointer);
  }

  private int[] createPointers(int[] update) {
    int[] pointers = new int[randomLevel()];
    for (int i = 0; i < pointers.length; i++)
      if (update[i] == OOffHeapMemory.NULL_POINTER)
        pointers[i] = header[i];
      else {
        pointers[i] = getNPointer(update[i], i);
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

  private byte[] fromItemToStream(int[] pointers, K firstKey, int dataPointer, int nextLRUPointer, int prevLRUPointer) {
    int size = 4 * OIntegerSerializer.INT_SIZE + OIntegerSerializer.INT_SIZE * pointers.length;
    size += keySerializer.getObjectSize(firstKey);

    byte[] stream = new byte[size];

    int offset = 0;

    OIntegerSerializer.INSTANCE.serialize(pointers.length, stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (int pointer : pointers) {
      OIntegerSerializer.INSTANCE.serialize(pointer, stream, offset);
      offset += OIntegerSerializer.INT_SIZE;
    }

		OIntegerSerializer.INSTANCE.serialize(dataPointer, stream, offset);
		offset += OIntegerSerializer.INT_SIZE;

		OIntegerSerializer.INSTANCE.serialize(nextLRUPointer, stream, offset);
		offset += OIntegerSerializer.INT_SIZE;

		OIntegerSerializer.INSTANCE.serialize(prevLRUPointer, stream, offset);
		offset += OIntegerSerializer.INT_SIZE;

		keySerializer.serialize(firstKey, stream, offset);
    keySerializer.getObjectSize(firstKey);

    return stream;
  }

  private K getKey(int pointer) {
    int pointersLen = memory.getInt(pointer, 0);

    return keySerializer.deserialize(memory.get(pointer, 4 * OIntegerSerializer.INT_SIZE +
						pointersLen * OIntegerSerializer.INT_SIZE, -1), 0);
  }

  private int getNPointer(int pointer, int level) {
    final int offset = OIntegerSerializer.INT_SIZE + level * OIntegerSerializer.INT_SIZE;

    return memory.getInt(pointer, offset);
  }

  private int getPointersSize(int pointer) {
    return memory.getInt(pointer, 0);
  }

  private int getDataPointer(int pointer) {
    final int pointersLen = memory.getInt(pointer, 0);
    final int offset =  OIntegerSerializer.INT_SIZE + pointersLen * OIntegerSerializer.INT_SIZE;

    return memory.getInt(pointer, offset);
  }

  private void setDataPointer(int pointer, int dataPointer) {
    final int offset =  OIntegerSerializer.INT_SIZE +  memory.getInt(pointer, 0) * OIntegerSerializer.INT_SIZE;

		memory.setInt(pointer, offset, dataPointer);
  }

  private void setNPointer(int pointer, int level, int dataPointer) {
    final int offset = OIntegerSerializer.INT_SIZE + level * OIntegerSerializer.INT_SIZE;
		memory.setInt(pointer, offset, dataPointer);
  }

  private int getNextLRUPointer(int pointer) {
    final int pointersLen = memory.getInt(pointer, 0);

    final int offset = 2 * OIntegerSerializer.INT_SIZE + pointersLen * OIntegerSerializer.INT_SIZE;

    return memory.getInt(pointer, offset);
  }

  private void setNextLRUPointer(int pointer, int lruPointer) {
		final int pointersLen = memory.getInt(pointer, 0);

		final int offset = 2 * OIntegerSerializer.INT_SIZE + pointersLen * OIntegerSerializer.INT_SIZE;

		memory.setInt(pointer, offset, lruPointer);
  }

  private int getPrevLRUPointer(int pointer) {
		final int pointersLen = memory.getInt(pointer, 0);
    final int offset = 3 * OIntegerSerializer.INT_SIZE + pointersLen * OIntegerSerializer.INT_SIZE;

    return memory.getInt(pointer, offset);
  }

  private void setPrevLRUPointer(int pointer, int lruPointer) {
		final int pointersLen = memory.getInt(pointer, 0);
		final int offset = 3 * OIntegerSerializer.INT_SIZE + pointersLen * OIntegerSerializer.INT_SIZE;

		memory.setInt(pointer, offset, lruPointer);
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
