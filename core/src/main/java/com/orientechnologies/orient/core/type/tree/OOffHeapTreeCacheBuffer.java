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

  private static final Random               random    = new Random();

	private int seed;

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

		seed = random.nextInt() | 0x0100;
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
      int dataPointer = storeEntry(entry);

			if (dataPointer == OOffHeapMemory.NULL_POINTER)
				return evict() && add(entry);

      final int itemPointer = storeItem(pointers, entry.firstKey, dataPointer);

      if (itemPointer == OOffHeapMemory.NULL_POINTER) {
				freeEntry(dataPointer);
				return evict() && add(entry);
			}


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
    int dataPointer = storeEntry(entry);

		if (dataPointer == OOffHeapMemory.NULL_POINTER)
			return evict() && add(entry);

    final int newItemPointer = storeItem(pointers, entry.firstKey, dataPointer);

		if(newItemPointer == OOffHeapMemory.NULL_POINTER) {
			freeEntry(dataPointer);
			return evict() && add(entry);
		}


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
        final int dataPointer = getDataPointer(forwardPointer);
        updateItemInLRU(forwardPointer);
        return loadEntry(key, dataPointer);
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
        final int dataPointer = getDataPointer(forwardPointer);
        updateItemInLRU(forwardPointer);
        return loadEntry(key, dataPointer);
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
    final int dataPointer = getDataPointer(nextPointer);
    return loadEntry(getKey(nextPointer), dataPointer);
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
        final int dataPointer = getDataPointer(forwardPointer);
        updateItemInLRU(forwardPointer);
        return loadEntry(key, dataPointer);
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
    final int dataPointer = getDataPointer(pointer);
    return loadEntry(getKey(pointer), dataPointer);
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
		final int dataPointer = getDataPointer(forwardPointer);
		final CacheEntry<K> cacheEntry = loadEntry(firstKey, dataPointer);
		freeItem(forwardPointer);

		size--;
    return cacheEntry;
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
        freeEntry(dataPointer);
				setDataPointer(forwardPointer, OOffHeapMemory.NULL_POINTER);

        dataPointer = storeEntry(entry);

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

      remove(getKey(evictedItem));
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
    int[] pointers = new int[randomLevel() + 1];
    for (int i = 0; i < pointers.length; i++)
      if (update[i] == OOffHeapMemory.NULL_POINTER)
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
		while (((x >>>= 1) & 1) != 0) ++level;
		return level;
  }

  private int storeEntry(CacheEntry<K> entry) {
		final int lastKeySize = keySerializer.getObjectSize(entry.lastKey);
		final int ridSize = OLinkSerializer.RID_SIZE;
		final int size = OIntegerSerializer.INT_SIZE + 4 * ridSize;

		final int lastKeyPointer = memory.allocate(lastKeySize);
		if(lastKeyPointer == OOffHeapMemory.NULL_POINTER)
			return OOffHeapMemory.NULL_POINTER;

		final int pointer = memory.allocate(size);
		if(pointer == OOffHeapMemory.NULL_POINTER) {
			memory.free(lastKeyPointer);
			return OOffHeapMemory.NULL_POINTER;
		}

    int offset = 0;
		memory.setInt(pointer, offset, lastKeyPointer);
		offset += OIntegerSerializer.INT_SIZE;

		final byte[] serializedRid = new byte[ridSize];
		OLinkSerializer.INSTANCE.serialize(entry.rid, serializedRid, 0);
		memory.set(pointer, offset,ridSize, serializedRid);
		offset += ridSize;

		OLinkSerializer.INSTANCE.serialize(entry.leftRid, serializedRid, 0);
		memory.set(pointer, offset,ridSize, serializedRid);
		offset += ridSize;

		OLinkSerializer.INSTANCE.serialize(entry.rightRid, serializedRid, 0);
		memory.set(pointer, offset,ridSize, serializedRid);
		offset += ridSize;

		OLinkSerializer.INSTANCE.serialize(entry.parentRid, serializedRid, 0);
		memory.set(pointer, offset,ridSize, serializedRid);
		offset += ridSize;

		byte[] serializedKey = new byte[lastKeySize];
		keySerializer.serialize(entry.lastKey, serializedKey, 0);
		memory.set(lastKeyPointer, 0, lastKeySize, serializedKey);

    return pointer;
  }

	private void freeEntry(int pointer) {
		final int lastKeyPointer = memory.getInt(pointer, 0);
		memory.free(lastKeyPointer);
		memory.free(pointer);
	}

	private CacheEntry<K> loadEntry(K firstKey, int pointer) {
		final int lastKeyPointer = memory.getInt(pointer, 0);
		final byte[] serializedKey = memory.get(lastKeyPointer, 0, -1);
		final K lastKey = keySerializer.deserialize(serializedKey, 0);

		int offset = OIntegerSerializer.INT_SIZE;

		byte[] serializedRid;

	  serializedRid = memory.get(pointer, offset, OLinkSerializer.RID_SIZE);
		offset += OLinkSerializer.RID_SIZE;

		final  ORID rid = OLinkSerializer.INSTANCE.deserialize(serializedRid, 0);

		serializedRid = memory.get(pointer, offset, OLinkSerializer.RID_SIZE);
		offset += OLinkSerializer.RID_SIZE;

		final  ORID leftRid = OLinkSerializer.INSTANCE.deserialize(serializedRid, 0);

		serializedRid = memory.get(pointer, offset, OLinkSerializer.RID_SIZE);
		offset += OLinkSerializer.RID_SIZE;

		final  ORID rightRid = OLinkSerializer.INSTANCE.deserialize(serializedRid, 0);

		serializedRid = memory.get(pointer, offset, OLinkSerializer.RID_SIZE);

		final  ORID parentRid = OLinkSerializer.INSTANCE.deserialize(serializedRid, 0);

		return new CacheEntry<K>(firstKey, lastKey, rid, parentRid, leftRid, rightRid);
	}

	private int storeItem(int[] pointers, K firstKey, int dataPointer) {
		final int size = 5 * OIntegerSerializer.INT_SIZE;
		final int pointersSize = OIntegerSerializer.INT_SIZE * (pointers.length + 1);
		final int firstKeySize = keySerializer.getObjectSize(firstKey);

		final int pointer = memory.allocate(size);

		if(pointer == OOffHeapMemory.NULL_POINTER)
			return OOffHeapMemory.NULL_POINTER;

		final int pointersPointer = memory.allocate(pointersSize);
		if(pointersPointer == OOffHeapMemory.NULL_POINTER) {
			memory.free(pointer);
			return OOffHeapMemory.NULL_POINTER;
		}

		final int firstKeyPointer = memory.allocate(firstKeySize);
		if(firstKeyPointer == OOffHeapMemory.NULL_POINTER) {
			memory.free(pointer);
			memory.free(pointersPointer);
			return OOffHeapMemory.NULL_POINTER;
		}

		int offset = 0;
		memory.setInt(pointer, 0, pointersPointer);
		offset += OIntegerSerializer.INT_SIZE;

		memory.setInt(pointer, offset, dataPointer);
		offset += OIntegerSerializer.INT_SIZE;

		memory.setInt(pointer, offset, firstKeyPointer);
		offset += OIntegerSerializer.INT_SIZE;

		memory.setInt(pointer, offset, OOffHeapMemory.NULL_POINTER);
		offset += OIntegerSerializer.INT_SIZE;

		memory.setInt(pointer, offset, OOffHeapMemory.NULL_POINTER);

		offset = 0;
		memory.setInt(pointersPointer, offset, pointers.length);
		offset += OIntegerSerializer.INT_SIZE;

		for (int pointersItem : pointers) {
			memory.setInt(pointersPointer, offset, pointersItem);
			offset += OIntegerSerializer.INT_SIZE;
		}

		byte[] firstKeySerialized = new byte[firstKeySize];
		keySerializer.serialize(firstKey, firstKeySerialized, 0);

		memory.set(firstKeyPointer, 0, firstKeySize, firstKeySerialized);

		return pointer;
	}

	private void freeItem(int pointer) {
		int offset = 0;

		final int pointersPointer = memory.getInt(pointer, 0);
		offset += OIntegerSerializer.INT_SIZE;

		final int dataPointer = memory.getInt(pointer, offset);
		offset += OIntegerSerializer.INT_SIZE;

		final int  firstKeyPointer = memory.getInt(pointer, offset);

		freeEntry(dataPointer);

		memory.free(pointersPointer);
		memory.free(firstKeyPointer);
		memory.free(pointer);
	}

  private K getKey(int pointer) {
    final int offset = 2 * OIntegerSerializer.INT_SIZE;
		final int keyPointer = memory.getInt(pointer, offset);
		final byte[] serializedKey = memory.get(keyPointer, 0, -1);

    return keySerializer.deserialize(serializedKey, 0);
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
    final int offset =  OIntegerSerializer.INT_SIZE;

    return memory.getInt(pointer, offset);
  }

  private void setDataPointer(int pointer, int dataPointer) {
		final int offset =  OIntegerSerializer.INT_SIZE;

		memory.setInt(pointer, offset, dataPointer);
  }

  private void setNPointer(int pointer, int level, int dataPointer) {
		final int pointersPointer = memory.getInt(pointer, 0);
		final int offset = (level + 1) * OIntegerSerializer.INT_SIZE;

		memory.setInt(pointersPointer, offset, dataPointer);
  }

  private int getNextLRUPointer(int pointer) {
    final int offset = 3 * OIntegerSerializer.INT_SIZE;

    return memory.getInt(pointer, offset);
  }

  private void setNextLRUPointer(int pointer, int lruPointer) {
		final int offset = 3 * OIntegerSerializer.INT_SIZE;

		memory.setInt(pointer, offset, lruPointer);
  }

  private int getPrevLRUPointer(int pointer) {
		final int offset = 4 * OIntegerSerializer.INT_SIZE;

		return memory.getInt(pointer, offset);
  }

  private void setPrevLRUPointer(int pointer, int lruPointer) {
		final int offset = 4 * OIntegerSerializer.INT_SIZE;

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
