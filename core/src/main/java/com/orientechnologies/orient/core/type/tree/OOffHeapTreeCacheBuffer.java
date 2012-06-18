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

/**
 * @author Andrey Lomakin
 * @since 07.04.12
 */
public class OOffHeapTreeCacheBuffer<K extends Comparable<K>> {
  private final OMemory                   memory;
  private final OManagedMemorySkipList<K> skipList;
  private final OBinarySerializer<K>      keySerializer;

  private int                             lruHeader              = OMemory.NULL_POINTER;
  private int                             lruTail                = OMemory.NULL_POINTER;

  private int                             evictionSize           = -1;
  private int                             defaultEvictionPercent = 20;

  private long                            size                   = 0;

  public OOffHeapTreeCacheBuffer(final OMemory memory, final OBinarySerializer<K> keySerializer) {
    this.memory = memory;
    this.skipList = new OManagedMemorySkipList<K>(memory, keySerializer);
    this.keySerializer = keySerializer;
  }

  public void setEvictionSize(int evictionSize) {
    this.evictionSize = evictionSize;
  }

  public void setDefaultEvictionPercent(int defaultEvictionPercent) {
    this.defaultEvictionPercent = defaultEvictionPercent;
  }

  public boolean add(CacheEntry<K> entry) {
    final int dataPointer = storeEntry(entry);
    if (dataPointer == OMemory.NULL_POINTER)
      return evict() && add(entry);

    final int itemPointer = storeItem(dataPointer);
    if (itemPointer == OMemory.NULL_POINTER) {
      freeEntry(dataPointer);
      return evict() && add(entry);
    }

    final int result = skipList.add(entry.firstKey, itemPointer);
    if (result == 0) {
      freeItem(itemPointer);
      return false;
    }

    if (result == -1) {
      freeItem(itemPointer);
      return evict() && add(entry);
    }

    addItemToLRU(itemPointer);

    size++;

    if (evictionSize > 0 && size >= evictionSize)
      evict();

    return true;
  }

  public CacheEntry<K> get(K firstKey) {
    if (size == 0)
      return null;

    final int itemPointer = skipList.get(firstKey);
    if (itemPointer == OMemory.NULL_POINTER)
      return null;

    updateItemInLRU(itemPointer);
    return loadEntry(getDataPointer(itemPointer));
  }

  public long size() {
    return size;
  }

  public CacheEntry<K> getCeiling(K firstKey) {
    if (size == 0)
      return null;

    final int itemPointer = skipList.getCeiling(firstKey);
    if (itemPointer == OMemory.NULL_POINTER)
      return null;

    updateItemInLRU(itemPointer);
    return loadEntry(getDataPointer(itemPointer));
  }

  public CacheEntry<K> getFloor(K firstKey) {
    if (size == 0)
      return null;

    final int itemPointer = skipList.getFloor(firstKey);
    if (itemPointer == OMemory.NULL_POINTER)
      return null;

    updateItemInLRU(itemPointer);
    return loadEntry(getDataPointer(itemPointer));
  }

  public CacheEntry<K> remove(K firstKey) {
    final int itemPointer = skipList.remove(firstKey);
    if (itemPointer == OMemory.NULL_POINTER)
      return null;

    removeItemFromLRU(itemPointer);
    final CacheEntry<K> cacheEntry = loadEntry(getDataPointer(itemPointer));
    freeItem(itemPointer);

    size--;
    return cacheEntry;
  }

  public boolean update(CacheEntry<K> entry) {
    final int itemPointer = skipList.get(entry.firstKey);
    if (itemPointer == OMemory.NULL_POINTER)
      return false;

    final int dataPointer = getDataPointer(itemPointer);
    final int newDataPointer = storeEntry(entry);
    if (newDataPointer == OMemory.NULL_POINTER)
      return evict() && update(entry);

    setDataPointer(itemPointer, newDataPointer);
    updateItemInLRU(itemPointer);

    freeEntry(dataPointer);
    return true;
  }

  public boolean evict() {
    return evict(defaultEvictionPercent);
  }

  public boolean evict(int percent) {
    if (percent <= 0 || percent > 100)
      return false;

    long evictionSize = (size * percent) / 100;
    if (evictionSize == 0)
      return false;

    int evicted = 0;
    int currentVictim = lruTail;
    while (currentVictim != OMemory.NULL_POINTER && evicted < evictionSize) {
      int evictedItem = currentVictim;
      currentVictim = getPrevLRUPointer(evictedItem);

      if (skipList.remove(getFirstKey(getDataPointer(evictedItem))) != OMemory.NULL_POINTER) {
        evicted++;
        size--;
        freeItem(evictedItem);
      }

    }

    lruTail = currentVictim;
    if (lruTail != OMemory.NULL_POINTER)
      setNextLRUPointer(lruTail, OMemory.NULL_POINTER);

    return true;
  }

  private void addItemToLRU(int pointer) {
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

  private int storeEntry(CacheEntry<K> entry) {
    final int firstKeySize = keySerializer.getObjectSize(entry.firstKey);
    final int lastKeySize = keySerializer.getObjectSize(entry.lastKey);
    final int ridSize = OLinkSerializer.RID_SIZE;
    final int size = 2 * OIntegerSerializer.INT_SIZE + 4 * ridSize;

    final int firstKeyPointer = memory.allocate(firstKeySize);
    if (firstKeyPointer == OMemory.NULL_POINTER)
      return OMemory.NULL_POINTER;

    final int lastKeyPointer = memory.allocate(lastKeySize);
    if (lastKeyPointer == OMemory.NULL_POINTER) {
      memory.free(firstKeyPointer);
      return OMemory.NULL_POINTER;
    }

    final int pointer = memory.allocate(size);
    if (pointer == OMemory.NULL_POINTER) {
      memory.free(firstKeyPointer);
      memory.free(lastKeyPointer);
      return OMemory.NULL_POINTER;
    }

    int offset = 0;
    memory.setInt(pointer, offset, firstKeyPointer);
    offset += OIntegerSerializer.INT_SIZE;

    memory.setInt(pointer, offset, lastKeyPointer);
    offset += OIntegerSerializer.INT_SIZE;

    memory.set(pointer, offset, entry.rid, OLinkSerializer.INSTANCE);
    offset += ridSize;

		memory.set(pointer, offset, entry.leftRid, OLinkSerializer.INSTANCE);
		offset += ridSize;

		memory.set(pointer, offset, entry.rightRid, OLinkSerializer.INSTANCE);
		offset += ridSize;

 		memory.set(pointer, offset, entry.parentRid, OLinkSerializer.INSTANCE);
		offset += ridSize;

    memory.set(firstKeyPointer, 0, entry.firstKey, keySerializer);
    memory.set(lastKeyPointer, 0, entry.lastKey, keySerializer);

    return pointer;
  }

  private K getFirstKey(int pointer) {
    final int firstKeyPointer = memory.getInt(pointer, 0);
    return memory.get(firstKeyPointer, 0, keySerializer);
  }

  private void freeEntry(int pointer) {
    final int firstKeyPointer = memory.getInt(pointer, 0);
    final int lastKeyPointer = memory.getInt(pointer, OIntegerSerializer.INT_SIZE);
    memory.free(firstKeyPointer);
    memory.free(lastKeyPointer);
    memory.free(pointer);
  }

  private CacheEntry<K> loadEntry(int pointer) {
    final int firstKeyPointer = memory.getInt(pointer, 0);
    final int lastKeyPointer = memory.getInt(pointer, OIntegerSerializer.INT_SIZE);

    final K firstKey = memory.get(firstKeyPointer, 0, keySerializer);
    final K lastKey = memory.get(lastKeyPointer, 0, keySerializer);

    int offset = 2 * OIntegerSerializer.INT_SIZE;

    final ORID rid = memory.get(pointer, offset, OLinkSerializer.INSTANCE).getIdentity();
		offset += OLinkSerializer.RID_SIZE;


    final ORID leftRid = memory.get(pointer, offset, OLinkSerializer.INSTANCE).getIdentity();
		offset += OLinkSerializer.RID_SIZE;

    final ORID rightRid = memory.get(pointer, offset, OLinkSerializer.INSTANCE).getIdentity();
		offset += OLinkSerializer.RID_SIZE;

    final ORID parentRid = memory.get(pointer, offset, OLinkSerializer.INSTANCE).getIdentity();

    return new CacheEntry<K>(firstKey, lastKey, rid, parentRid, leftRid, rightRid);
  }

  private int storeItem(int dataPointer) {
    final int pointer = memory.allocate(3 * OIntegerSerializer.INT_SIZE);

    int offset = 0;
    memory.setInt(pointer, offset, dataPointer);
    offset += OIntegerSerializer.INT_SIZE;

    memory.setInt(pointer, offset, OMemory.NULL_POINTER);
    offset += OIntegerSerializer.INT_SIZE;

    memory.setInt(pointer, offset, OMemory.NULL_POINTER);

    return pointer;
  }

  private void freeItem(int pointer) {
    final int dataPointer = memory.getInt(pointer, 0);

    freeEntry(dataPointer);
    memory.free(pointer);
  }

  private int getDataPointer(int pointer) {
    return memory.getInt(pointer, 0);
  }

  private void setDataPointer(int pointer, int dataPointer) {
    memory.setInt(pointer, 0, dataPointer);
  }

  private int getNextLRUPointer(int pointer) {
    final int offset = OIntegerSerializer.INT_SIZE;

    return memory.getInt(pointer, offset);
  }

  private void setNextLRUPointer(int pointer, int lruPointer) {
    final int offset = OIntegerSerializer.INT_SIZE;

    memory.setInt(pointer, offset, lruPointer);
  }

  private int getPrevLRUPointer(int pointer) {
    final int offset = 2 * OIntegerSerializer.INT_SIZE;

    return memory.getInt(pointer, offset);
  }

  private void setPrevLRUPointer(int pointer, int lruPointer) {
    final int offset = 2 * OIntegerSerializer.INT_SIZE;

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
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      CacheEntry<?> that = (CacheEntry<?>) o;

      if (!firstKey.equals(that.firstKey))
        return false;
      if (!lastKey.equals(that.lastKey))
        return false;
      if (!leftRid.equals(that.leftRid))
        return false;
      if (!parentRid.equals(that.parentRid))
        return false;
      if (!rid.equals(that.rid))
        return false;
      if (!rightRid.equals(that.rightRid))
        return false;

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
