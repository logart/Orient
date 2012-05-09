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

  public OffHeapTreeCacheBuffer(final OffHeapMemory memory, final OBinarySerializer<K> keySerializer) {
    this.memory = memory;

    this.keySerializer = keySerializer;

    header = new int[MAX_LEVEL + 1];
    Arrays.fill(header, OffHeapMemory.NULL_POINTER);
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
      byte[] content = toContent(pointers, entry);

      final int itemPointer = memory.add(content);

      if (itemPointer == OffHeapMemory.NULL_POINTER)
        return false;

      Arrays.fill(header, 0, pointers.length, itemPointer);
      return true;
    }

    byte[] content = null;
    int pointer = OffHeapMemory.NULL_POINTER;

    while (level >= 0) {
      byte[] forwardContent;

      if (content == null)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(content, level);

      if (forwardPointer == OffHeapMemory.NULL_POINTER) {
        update[level] = pointer;
        level--;

        continue;
      }

      forwardContent = memory.get(forwardPointer);

      K key = getKey(forwardContent);
      int compareResult = entry.firstKey.compareTo(key);

      if (compareResult == 0)
        return false;

      if (compareResult < 0) {
        update[level] = pointer;
        level--;
        continue;
      }

      content = forwardContent;
      pointer = forwardPointer;
    }

    final int[] pointers = createPointers(update);
    content = toContent(pointers, entry);

    final int newItemPointer = memory.add(content);

    if (newItemPointer == OffHeapMemory.NULL_POINTER)
      return false;

    byte[] updateContent = null;
    int updatePointer = OffHeapMemory.NULL_POINTER;

    boolean contentIsDirty = false;
    for (int i = 0; i < pointers.length; i++) {
      if (update[i] != updatePointer) {
        if (contentIsDirty) {
          memory.update(updatePointer, updateContent);
          contentIsDirty = false;
        }

        updatePointer = update[i];

        if (updatePointer != OffHeapMemory.NULL_POINTER)
          updateContent = memory.get(updatePointer);
      }

      if (updatePointer != OffHeapMemory.NULL_POINTER) {
        setNPointer(updateContent, i, newItemPointer);
        contentIsDirty = true;
      } else
        header[i] = newItemPointer;
    }

    if (contentIsDirty)
      memory.update(updatePointer, updateContent);

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

    byte[] content = null;
    while (level >= 0) {
      byte[] forwardContent;

      if (content == null)
        forwardPointer = header[level];
      else
        forwardPointer = getNPointer(content, level);

      if (forwardPointer == OffHeapMemory.NULL_POINTER) {
        level--;
        continue;
      }

      forwardContent = memory.get(forwardPointer);

      K key = getKey(forwardContent);
      int compareResult = firstKey.compareTo(key);

      if (compareResult == 0)
        return fromContent(forwardContent);

      if (compareResult < 0) {
        level--;
        continue;
      }

      content = forwardContent;
    }

    return null;
  }

  public CacheEntry<?> getCeiling(K firstKey) {
    return null;
  }

  public CacheEntry<?> getFloor(K firstKey) {
    return null;
  }

  public CacheEntry<?> remove(K firstKey) {
    return null;
  }

  public void update(CacheEntry<?> entry) {
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

  private CacheEntry<K> fromContent(byte[] content) {
    final int pointersLen = OIntegerSerializer.INSTANCE.deserialize(content, 0);
    int offset = OIntegerSerializer.INT_SIZE + pointersLen * 4;

    final K firstKey = keySerializer.deserialize(content, offset);
    offset += keySerializer.getObjectSize(firstKey);

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

  private byte[] toContent(int[] pointers, CacheEntry<K> entry) {
    int size = OIntegerSerializer.INT_SIZE + OIntegerSerializer.INT_SIZE * pointers.length;
    size += keySerializer.getObjectSize(entry.firstKey);
    size += keySerializer.getObjectSize(entry.lastKey);
    size += OLinkSerializer.INSTANCE.getObjectSize(entry.rid);
    size += OLinkSerializer.INSTANCE.getObjectSize(entry.leftRid);
    size += OLinkSerializer.INSTANCE.getObjectSize(entry.rightRid);
    size += OLinkSerializer.INSTANCE.getObjectSize(entry.parentRid);

    final byte[] content = new byte[size];

    int offset = 0;

    OIntegerSerializer.INSTANCE.serialize(pointers.length, content, offset);
    offset += OIntegerSerializer.INT_SIZE;

    for (int pointer : pointers) {
      OIntegerSerializer.INSTANCE.serialize(pointer, content, offset);
      offset += OIntegerSerializer.INT_SIZE;
    }

    keySerializer.serialize(entry.firstKey, content, offset);
    offset += keySerializer.getObjectSize(entry.firstKey);

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

  private K getKey(byte[] content) {
    int pointersLen = OIntegerSerializer.INSTANCE.deserialize(content, 0);

    return keySerializer.deserialize(content, OIntegerSerializer.INT_SIZE + pointersLen * OIntegerSerializer.INT_SIZE);
  }

  private int getNPointer(byte[] content, int level) {
    final int offset = OIntegerSerializer.INT_SIZE + level * OIntegerSerializer.INT_SIZE;

    return OIntegerSerializer.INSTANCE.deserialize(content, offset);
  }

  private int getPointersSize(byte[] content) {
    return OIntegerSerializer.INSTANCE.deserialize(content, 0);
  }

  private void setNPointer(byte[] content, int level, int pointer) {
    final int offset = OIntegerSerializer.INT_SIZE + level * OIntegerSerializer.INT_SIZE;
    OIntegerSerializer.INSTANCE.serialize(pointer, content, offset);
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
