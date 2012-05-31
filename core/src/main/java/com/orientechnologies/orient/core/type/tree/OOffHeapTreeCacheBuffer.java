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
      int dataPointer = memory.add(dataStream);

      byte[] stream  = fromItemToStream(pointers, entry.firstKey, dataPointer, OOffHeapMemory.NULL_POINTER,
              OOffHeapMemory.NULL_POINTER);

      final int itemPointer = memory.add(stream);

      if (itemPointer == OOffHeapMemory.NULL_POINTER)
        return evict() && add(entry);

      Arrays.fill(header, 0, pointers.length, itemPointer);

      size++;

      addItemToLRU(itemPointer, stream);

      return true;
    }

    byte[] stream = null;
    int pointer = OOffHeapMemory.NULL_POINTER;

    while (level >= 0) {
      byte[] forwardStream;

      if (stream == null)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(stream, level);

      if (forwardPointer == OOffHeapMemory.NULL_POINTER) {
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

    stream = fromItemToStream(pointers, entry.firstKey, dataPointer, OOffHeapMemory.NULL_POINTER,
            OOffHeapMemory.NULL_POINTER);
    final int newItemPointer = memory.add(stream);
    addItemToLRU(newItemPointer, stream);

    if(newItemPointer == OOffHeapMemory.NULL_POINTER)
       return evict() && add(entry);

    byte[] updateStream = null;
    int updatePointer = OOffHeapMemory.NULL_POINTER;

    boolean streamIsDirty = false;
    for (int i = 0; i < pointers.length; i++) {
      if (update[i] != updatePointer) {
        if (streamIsDirty) {
          memory.update(updatePointer, updateStream);
          streamIsDirty = false;
        }

        updatePointer = update[i];

        if (updatePointer != OOffHeapMemory.NULL_POINTER)
          updateStream = memory.get(updatePointer);
      }

      if (updatePointer != OOffHeapMemory.NULL_POINTER) {
        setNPointer(updateStream, i, newItemPointer);
        streamIsDirty = true;
      } else
        header[i] = newItemPointer;
    }

    if (streamIsDirty)
      memory.update(updatePointer, updateStream);

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

    byte[] stream = null;
    while (level >= 0) {
      byte[] forwardStream;

      if (stream == null)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(stream, level);

      if (forwardPointer == OOffHeapMemory.NULL_POINTER) {
        level--;
        continue;
      }

      forwardStream = memory.get(forwardPointer);

      K key = getKey(forwardStream);
      int compareResult = firstKey.compareTo(key);

      if (compareResult == 0) {
        byte[] dataStream = memory.get(getDataPointer(forwardStream));
        updateItemInLRU(forwardPointer, forwardStream);
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

    byte[] stream = null;
    while (level >= 0) {
      byte[] forwardStream;

      if (stream == null)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(stream, level);

      if (forwardPointer == OOffHeapMemory.NULL_POINTER) {
        level--;
        continue;
      }

      forwardStream = memory.get(forwardPointer);

      K key = getKey(forwardStream);
      int compareResult = firstKey.compareTo(key);

      if (compareResult == 0) {
        byte[] dataStream = memory.get(getDataPointer(forwardStream));
        updateItemInLRU(forwardPointer, forwardStream);
        return fromStreamToEntry(key, dataStream);
      }

      if (compareResult < 0) {
        level--;
        continue;
      }

      stream = forwardStream;
    }

    int nextPointer;
    if(stream == null)
      nextPointer = header[0];
    else
      nextPointer = getNPointer(stream, 0);

    if(nextPointer == OOffHeapMemory.NULL_POINTER)
      return null;

    stream = memory.get(nextPointer);

    updateItemInLRU(nextPointer, stream);
    byte[] dataStream = memory.get(getDataPointer(stream));
    return fromStreamToEntry(getKey(stream), dataStream);
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

    byte[] stream = null;
    int pointer = OOffHeapMemory.NULL_POINTER;
    while (level >= 0) {
      byte[] forwardStream;

      if (stream == null)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(stream, level);

      if (forwardPointer == OOffHeapMemory.NULL_POINTER) {
        level--;
        continue;
      }

      forwardStream = memory.get(forwardPointer);

      K key = getKey(forwardStream);
      int compareResult = firstKey.compareTo(key);

      if (compareResult == 0) {
        byte[] dataStream = memory.get(getDataPointer(forwardStream));
        updateItemInLRU(forwardPointer, forwardStream);
        return fromStreamToEntry(key, dataStream);
      }

      if (compareResult < 0) {
        level--;
        continue;
      }

      stream = forwardStream;
      pointer = forwardPointer;
    }

    if(stream == null)
      return null;

    updateItemInLRU(pointer, stream);
    byte[] dataStream = memory.get(getDataPointer(stream));
    return fromStreamToEntry(getKey(stream), dataStream);
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

    byte[] stream = null;
    int pointer = OOffHeapMemory.NULL_POINTER;

    int compareResult = -1;
    byte[] forwardStream = null;
    while (level >= 0) {
      if (stream == null)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(stream, level);

      if (forwardPointer == OOffHeapMemory.NULL_POINTER) {
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
    removeItemFromLRU(forwardPointer, forwardStream);

    final int itemLevel = getPointersSize(forwardStream);
    boolean streamIsDirty = false;
    int updatePointer = OOffHeapMemory.NULL_POINTER;
    byte[] updateStream = null;

    for(int i = itemLevel - 1; i >= 0; i--) {
      if (update[i] != updatePointer) {
        if (streamIsDirty) {
          memory.update(updatePointer, updateStream);
          streamIsDirty = false;
        }

        updatePointer = update[i];

        if (updatePointer != OOffHeapMemory.NULL_POINTER)
          updateStream = memory.get(updatePointer);
      }

      if (updatePointer != OOffHeapMemory.NULL_POINTER) {
        setNPointer(updateStream, i, getNPointer(forwardStream, i));
        streamIsDirty = true;
      } else
        header[i] = getNPointer(forwardStream, i);
    }

    if (streamIsDirty)
      memory.update(updatePointer, updateStream);

    size--;


    int dataPointer = getDataPointer(forwardStream);
    byte[] dataStream = memory.get(dataPointer);
    memory.remove(dataPointer);

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

    byte[] stream = null;
    while (level >= 0) {
      byte[] forwardStream;

      if (stream == null)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(stream, level);

      if (forwardPointer == OOffHeapMemory.NULL_POINTER) {
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

        updateItemInLRU(forwardPointer, forwardStream);
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
    byte[] stream = null;
    while (currentVictim != OOffHeapMemory.NULL_POINTER && evicted < evictionSize) {
      stream = memory.get(currentVictim);

      int evictedItem = currentVictim;
      currentVictim = getPrevLRUPointer(stream);

      evictItem(evictedItem, stream);
      evicted++;
    }

    lruTail = currentVictim;
    if(lruTail != OOffHeapMemory.NULL_POINTER) {
      stream = memory.get(lruTail);
      setNextLRUPointer(stream, OOffHeapMemory.NULL_POINTER);
      memory.update(lruTail, stream);
    }

    return true;
  }

  private void evictItem(int pointer, byte[] stream) {
    int pointersLen = getPointersSize(stream);

    int update[] = new int[pointersLen];
    Arrays.fill(update, OOffHeapMemory.NULL_POINTER);

    int forwardPointer = header[0];

    while (forwardPointer != pointer) {
      int currentPointer = forwardPointer;

      byte[] currentStream = memory.get(currentPointer);
      forwardPointer = getNPointer(currentStream, 0);

      pointersLen = getPointersSize(currentStream);

      int updateSize = Math.min(update.length, pointersLen);
      for(int i = 0; i < updateSize; i++) {
        update[i] = currentPointer;
      }
    }

    boolean streamIsDirty = false;
    int updatePointer = OOffHeapMemory.NULL_POINTER;
    byte[] updateStream = null;

    for(int i = update.length - 1; i >= 0; i--) {
      if (update[i] != updatePointer) {
        if (streamIsDirty) {
          memory.update(updatePointer, updateStream);
          streamIsDirty = false;
        }

        updatePointer = update[i];

        if (updatePointer != OOffHeapMemory.NULL_POINTER)
          updateStream = memory.get(updatePointer);
      }

      if (updatePointer != OOffHeapMemory.NULL_POINTER) {
        setNPointer(updateStream, i, getNPointer(stream, i));
        streamIsDirty = true;
      } else
        header[i] = getNPointer(stream, i);
    }

    if (streamIsDirty)
      memory.update(updatePointer, updateStream);

    int dataPointer = getDataPointer(stream);
    memory.remove(pointer);
    memory.remove(dataPointer);

    size--;
  }

  private void addItemToLRU(int pointer, byte[] stream) {
    if(lruHeader == OOffHeapMemory.NULL_POINTER) {
      lruHeader = pointer;
      lruTail = pointer;

      return;
    }

    int nextPointer = lruHeader;
    byte[] nextStream = memory.get(nextPointer);

    setNextLRUPointer(stream, nextPointer);
    lruHeader = pointer;

    setPrevLRUPointer(nextStream, pointer);

    memory.update(pointer, stream);
    memory.update(nextPointer, nextStream);
  }

  private void updateItemInLRU(int pointer, byte[] stream) {
    if(pointer == lruHeader)
      return;

    int prevPointer = getPrevLRUPointer(stream);
    int nextPointer = getNextLRUPointer(stream);

    byte[] prevStream = null;
    if(prevPointer != OOffHeapMemory.NULL_POINTER)
      prevStream = memory.get(prevPointer);

    byte[] nextStream = null;

    if(nextPointer != OOffHeapMemory.NULL_POINTER)
      nextStream = memory.get(nextPointer);

    if(prevPointer != OOffHeapMemory.NULL_POINTER)
      setPrevLRUPointer(stream, OOffHeapMemory.NULL_POINTER);

    setNextLRUPointer(stream, lruHeader);

    lruHeader = pointer;

    if(lruTail == pointer)
      lruTail = prevPointer;

    if(prevStream != null)
      setNextLRUPointer(prevStream, nextPointer);

    if(nextStream != null)
      setPrevLRUPointer(nextStream, prevPointer);

    memory.update(pointer, stream);
    if(prevStream != null)
      memory.update(prevPointer, prevStream);
    if(nextStream != null)
      memory.update(nextPointer, nextStream);
  }

  private void removeItemFromLRU(int pointer, byte[] stream) {
    if(lruHeader == pointer && lruTail == pointer) {
      lruHeader = OOffHeapMemory.NULL_POINTER;
      lruTail = OOffHeapMemory.NULL_POINTER;
      return;
    }

    int prevPointer = getPrevLRUPointer(stream);
    int nextPointer = getNextLRUPointer(stream);

    byte[] prevStream = null;
    if(prevPointer != OOffHeapMemory.NULL_POINTER)
      prevStream = memory.get(prevPointer);

    byte[] nextStream = null;

    if(nextPointer != OOffHeapMemory.NULL_POINTER)
      nextStream = memory.get(nextPointer);

    if(lruHeader == pointer)
      lruHeader = nextPointer;

    if(lruTail == pointer)
      lruTail = prevPointer;

    if(prevStream != null)
      setNextLRUPointer(prevStream, nextPointer);

    if(nextStream != null)
      setPrevLRUPointer(nextStream, prevPointer);

    if(prevStream != null)
      memory.update(prevPointer, prevStream);
    if(nextStream != null)
      memory.update(nextPointer, nextStream);
  }

  private int[] createPointers(int[] update) {
    int[] pointers = new int[randomLevel()];
    for (int i = 0; i < pointers.length; i++)
      if (update[i] == OOffHeapMemory.NULL_POINTER)
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

    keySerializer.serialize(firstKey, stream, offset);
    offset += keySerializer.getObjectSize(firstKey);

    OIntegerSerializer.INSTANCE.serialize(dataPointer, stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serialize(nextLRUPointer, stream, offset);
    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serialize(prevLRUPointer, stream, offset);
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
    int pointersLen = OIntegerSerializer.INSTANCE.deserialize(stream, 0);
    int offset =  OIntegerSerializer.INT_SIZE + pointersLen * OIntegerSerializer.INT_SIZE;

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

  private int getNextLRUPointer(byte[] stream) {
    int pointersLen = OIntegerSerializer.INSTANCE.deserialize(stream, 0);
    int offset = OIntegerSerializer.INT_SIZE + pointersLen * OIntegerSerializer.INT_SIZE;

    offset += keySerializer.getObjectSize(stream, offset);

    offset += OIntegerSerializer.INT_SIZE;

    return OIntegerSerializer.INSTANCE.deserialize(stream, offset);
  }

  private void setNextLRUPointer(byte[] stream, int pointer) {
    int pointersLen = OIntegerSerializer.INSTANCE.deserialize(stream, 0);
    int offset = OIntegerSerializer.INT_SIZE + pointersLen * OIntegerSerializer.INT_SIZE;

    offset += keySerializer.getObjectSize(stream, offset);

    offset += OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serialize(pointer, stream, offset);
  }

  private int getPrevLRUPointer(byte[] stream) {
    int pointersLen = OIntegerSerializer.INSTANCE.deserialize(stream, 0);
    int offset = OIntegerSerializer.INT_SIZE + pointersLen * OIntegerSerializer.INT_SIZE;

    offset += keySerializer.getObjectSize(stream, offset);

    offset += 2 * OIntegerSerializer.INT_SIZE;

    return OIntegerSerializer.INSTANCE.deserialize(stream, offset);
  }

  private void setPrevLRUPointer(byte[] stream, int pointer) {
    int pointersLen = OIntegerSerializer.INSTANCE.deserialize(stream, 0);
    int offset = OIntegerSerializer.INT_SIZE + pointersLen * OIntegerSerializer.INT_SIZE;

    offset += keySerializer.getObjectSize(stream, offset);

    offset += 2 * OIntegerSerializer.INT_SIZE;

    OIntegerSerializer.INSTANCE.serialize(pointer, stream, offset);
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
