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
	private static final int MAX_LEVEL = 25;
	private static final int SEED_INTERVAL = 2 << (MAX_LEVEL + 2);


	private final OBinarySerializer<K> keySerializer;
	private final OffHeapFreeList freeList;

	private final Random random = new Random();

	private int[] header;


	public OffHeapTreeCacheBuffer(final int capacity, final int dataChunkSize, OBinarySerializer<K> keySerializer) {
		freeList = new OffHeapFreeList(capacity, dataChunkSize);

		this.keySerializer = keySerializer;

	  header = new int[MAX_LEVEL + 1];
		Arrays.fill(header, OffHeapFreeList.NULL_POINTER);
	}

	public void add(CacheEntry<K> entry) {
		int[] update = new int[MAX_LEVEL];
		Arrays.fill(update, OffHeapFreeList.NULL_POINTER);

		int level = MAX_LEVEL;
		int pointer = header[level];

		while (pointer == OffHeapFreeList.NULL_POINTER && level > 0) {
			level--;
			pointer = header[level];
		}

		if(pointer == OffHeapFreeList.NULL_POINTER) {
			int[] pointers = createPointers(update);
			byte[] content = toContent(pointers, entry);

			int itemPointer = freeList.add(content);

			Arrays.fill(header, 0, pointers.length, itemPointer);
		}

		byte[] content = freeList.get(pointer);
		K key = getKey(content);

		int comparatorResult = entry.firstKey.compareTo(key);

		while (comparatorResult < 0 && level > 0) {
			update[level] = pointer;

			level--;
			pointer = header[level];

			content = freeList.get(pointer);
			key = getKey(content);

			comparatorResult = entry.firstKey.compareTo(key);
		}

		if(comparatorResult < 0) {

		}



	}

	public CacheEntry get(K firstKey) {
		int level = MAX_LEVEL;
		int pointer = header[level];

		while (pointer == OffHeapFreeList.NULL_POINTER && level > 0) {
			level--;
			pointer = header[level];
		}

		if(pointer == OffHeapFreeList.NULL_POINTER)
			return null;

		byte[] content = freeList.get(pointer);
		K key = getKey(content);

		int comparatorResult = firstKey.compareTo(key);

		while (comparatorResult < 0 && level > 0) {
			level--;
			pointer = header[level];

			content = freeList.get(pointer);
			key = getKey(content);

			comparatorResult = firstKey.compareTo(key);
		}

		if(comparatorResult < 0)
			return null;

		if(comparatorResult == 0)
			return fromContent(content);

		int prevPointer = pointer;

		while (level >= 0 && pointer != OffHeapFreeList.NULL_POINTER) {
			while (comparatorResult > 0 && pointer != OffHeapFreeList.NULL_POINTER) {
				prevPointer = pointer;
				pointer = getNPointer(content, level);

				if (pointer != OffHeapFreeList.NULL_POINTER) {
					content = freeList.get(pointer);
					key = getKey(content);

					comparatorResult = firstKey.compareTo(key);

					if(comparatorResult == 0)
						return fromContent(content);
				}
			}

			level--;
			pointer = prevPointer;
		}

		return null;
	}

	public CacheEntry getCeiling(K firstKey) {
		return null;
	}

	public CacheEntry getFloor(K firstKey) {
		return null;
	}

	public CacheEntry remove(K firstKey) {
		return null;
	}

	public void update(CacheEntry entry) {
	}

	private int[] createPointers(int[] update) {
		int seedInterval = SEED_INTERVAL >>> 2;
		int len = 0;
		while (seedInterval < random.nextInt(SEED_INTERVAL)) {
			len++;
			seedInterval = seedInterval >>> 2;
		}

		int[] pointers = new int[len];
		for(int i  = 0; i < pointers.length; i++)
			if(update[i] == OffHeapFreeList.NULL_POINTER)
				pointers[i] = header[i];
		  else {
				byte[] content = freeList.get(update[i]);
				pointers[i] = getNPointer(content, i);
			}


		return pointers;
	}

	private CacheEntry<K> fromContent(byte[] content) {
		final int pointersLen = OIntegerSerializer.INSTANCE.deserialize(content, 0);
		int offset = OIntegerSerializer.INT_SIZE +pointersLen * 4;

		final K firstKey = keySerializer.deserialize(content, offset);
		offset += keySerializer.getObjectSize(firstKey);

		final K lastKey = keySerializer.deserialize(content, offset);
		offset += keySerializer.getObjectSize(lastKey);

		final ORID rid = OLinkSerializer.INSTANCE.deserialize(content, offset);

		return new CacheEntry<K>(firstKey, lastKey, rid);
	}

	private byte[] toContent(int[] pointers, CacheEntry<K> entry) {
		int size = OIntegerSerializer.INT_SIZE + OIntegerSerializer.INT_SIZE * pointers.length;
		size += keySerializer.getObjectSize(entry.firstKey);
		size += keySerializer.getObjectSize(entry.lastKey);
		size += OLinkSerializer.INSTANCE.getObjectSize(entry.rid);

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

		return content;
	}

	private K getKey(byte[] content) {
		int pointersLen = OIntegerSerializer.INSTANCE.deserialize(content, 0);

		return keySerializer.deserialize(content, OIntegerSerializer.INT_SIZE + pointersLen * OIntegerSerializer.INT_SIZE);
	}


	private int getNPointer(byte[] content, int level) {
		int offset = OIntegerSerializer.INT_SIZE + level * OIntegerSerializer.INT_SIZE;

		return OIntegerSerializer.INSTANCE.deserialize(content, offset);
	}

	public static final class CacheEntry<K> {
		final K firstKey;
		final K lastKey;
		final ORID rid;

		public CacheEntry(K firstKey, K lastKey, ORID rid) {
			this.firstKey = firstKey;
			this.lastKey = lastKey;
			this.rid = rid;
		}
	}
}
