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

import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OShortSerializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Andrey Lomakin
 * @since 07.04.12
 */
public class OffHeapFreeList {
	public static final int SYSTEM_INFO_SIZE = OIntegerSerializer.INT_SIZE + 2 * OShortSerializer.SHORT_SIZE;

	public static final int NULL_POINTER = -1;

	private int freeListHead;
	private int freeListHeadSize;

	private int freeListTail;
	private int freeListTailSize;

	private final ByteBuffer byteBuffer;
	private final int dataChunkSize;


	public OffHeapFreeList(int capacity, int dataChunkSize) {
		capacity = (int) (Math.floor(1.0 * capacity / dataChunkSize) * dataChunkSize);
  	byteBuffer = ByteBuffer.allocateDirect(capacity);

		this.dataChunkSize = dataChunkSize;

		freeListHead = 0;
		freeListTail = 0;

		freeListHeadSize = byteBuffer.capacity();
		freeListTailSize = freeListHeadSize;

		byteBuffer.putInt(freeListHeadSize);
	}

	public int add(byte[] bytes) {
		final AllocationInfo allocationInfo =
						new AllocationInfo(freeListHead, freeListHeadSize, freeListTail, freeListTailSize,
										new ArrayList<DataChunkInfo>());

		if(!canAdd(allocationInfo, 0, bytes.length))
			return NULL_POINTER;

		return addInBuffer(allocationInfo, bytes);
	}

	public void remove(int pointer) {
		removeFromBuffer(pointer);
	}

	public byte[] get(int pointer) {
		return fromBuffer(pointer);
	}

	public int freeChunkCount() {
		if (freeListHead == -1)
			return 0;

		if(freeListHead == freeListTail)
			return 1;

		int pos = byteBuffer.getInt(freeListHead + OIntegerSerializer.INT_SIZE);
		int count = 1;

		while (pos != freeListTail) {
			count++;
			pos = byteBuffer.getInt(pos + OIntegerSerializer.INT_SIZE);
		}

		count++;

		return count;
	}

	public int freeSpace() {
		if (freeListHead == -1)
			return 0;

		if(freeListHead == freeListTail)
			return freeListHeadSize;

		int pos = byteBuffer.getInt(freeListHead + OIntegerSerializer.INT_SIZE);
		int freeSize = freeListHeadSize;

		while (pos != freeListTail) {
			freeSize += byteBuffer.getInt(pos);
			pos = byteBuffer.getInt(pos + OIntegerSerializer.INT_SIZE);
		}

		freeSize += freeListTailSize;

		return freeSize;
	}

	private boolean canAdd(AllocationInfo allocationInfo, int offset, int size) {
		if(allocationInfo.freeListHead == -1)
			return false;

		int sizeToAdd = size - offset + SYSTEM_INFO_SIZE;

		int allocationSize = (int) (Math.ceil(1.0 * sizeToAdd / dataChunkSize) * dataChunkSize);

		final int pos;
		if(allocationInfo.freeListHeadSize > allocationSize) {
			pos = allocationInfo.freeListHead + allocationInfo.freeListHeadSize - allocationSize;
			allocationInfo.freeListHeadSize -= allocationSize;
		}  else  {
			pos = allocationInfo.freeListHead;
			allocationSize = allocationInfo.freeListHeadSize;

			if(allocationInfo.freeListHead != allocationInfo.freeListTail) {
				allocationInfo.freeListHead = byteBuffer.getInt(allocationInfo.freeListHead + 4);
				allocationInfo.freeListHeadSize = byteBuffer.getInt(allocationInfo.freeListHead);
			} else {
				allocationInfo.freeListHead = -1;
				allocationInfo.freeListTail = -1;
			}
		}

		if(allocationInfo.freeListHead == allocationInfo.freeListTail)
			allocationInfo.freeListTailSize = allocationInfo.freeListHeadSize;

	  final int recordSize;
		if(allocationSize >= sizeToAdd)
			recordSize = sizeToAdd;
		else
			recordSize = allocationSize;

		allocationInfo.dataChunks.add(new DataChunkInfo(pos, allocationSize, recordSize));
		final int chunkSize = recordSize - SYSTEM_INFO_SIZE;
		if(size - offset > chunkSize )
			return canAdd(allocationInfo, offset + chunkSize, size);
		else
			return true;
	}

	private int addInBuffer(AllocationInfo allocationInfo, byte[] bytes) {
		freeListHead = allocationInfo.freeListHead;
		freeListTail = allocationInfo.freeListTail;
		freeListHeadSize = allocationInfo.freeListHeadSize;
		freeListTailSize = allocationInfo.freeListTailSize;

		if(freeListHead != -1)
			byteBuffer.putInt(freeListHead, freeListHeadSize);

		if(freeListTail != -1)
			byteBuffer.putInt(freeListTail, freeListTailSize);

		final List<DataChunkInfo> dataChunks = allocationInfo.dataChunks;
		final int dataChunksSize = dataChunks.size();

		int offset = 0;
		for(int i = 0; i < dataChunksSize; i++) {
			final DataChunkInfo dataChunkInfo = dataChunks.get(i);

			byteBuffer.position(dataChunkInfo.pointer);
			byteBuffer.putShort((short) dataChunkInfo.allocationSize);
			byteBuffer.putShort((short)dataChunkInfo.recordSize);
			if(i < dataChunksSize - 1)
				byteBuffer.putInt(dataChunks.get(i + 1).pointer);
			else
				byteBuffer.putInt(-1);

			final int chunkSize = dataChunkInfo.recordSize - SYSTEM_INFO_SIZE;
			byteBuffer.put(bytes, offset, chunkSize);
			offset += chunkSize;
		}

		return allocationInfo.dataChunks.get(0).pointer;
	}

	private void removeFromBuffer(int pos) {
		byteBuffer.position(pos);
		final int allocationSize = byteBuffer.getShort();
		byteBuffer.position(byteBuffer.position() + OShortSerializer.SHORT_SIZE);
		final int next = byteBuffer.getInt();

		if(freeListTail == -1) {
			freeListTail = pos;
			freeListHead = pos;

			freeListHeadSize = allocationSize;
			freeListTailSize = allocationSize;

			byteBuffer.putInt(freeListTail, freeListTailSize);
		} else 	if(pos == freeListTail + freeListTailSize) {
			freeListTailSize += allocationSize;

			byteBuffer.putInt(freeListTail, freeListTailSize);
		} else {
			byteBuffer.putInt(freeListTail + OIntegerSerializer.INT_SIZE, pos);

			freeListTail = pos;
			freeListTailSize = allocationSize;

			byteBuffer.putInt(freeListTail, freeListTailSize);
		}

		if(freeListHead == freeListTail)
			freeListHeadSize = freeListTailSize;

		if(next != NULL_POINTER)
			removeFromBuffer(next);
	}

	private byte[] fromBuffer(int pos) {
		int length = dataLength(pos);
		byte[] content = new byte[length];

		int offset = 0;
		do {
			byteBuffer.position(pos + OShortSerializer.SHORT_SIZE);

			final int chunkSize = byteBuffer.getShort() - SYSTEM_INFO_SIZE;

			pos = byteBuffer.getInt();

			byteBuffer.get(content, offset, chunkSize);
			offset += chunkSize;
		} while (pos != NULL_POINTER);

		return content;
	}

	private int dataLength(int pos) {
		byteBuffer.position(pos + OShortSerializer.SHORT_SIZE);
		final int recordSize = byteBuffer.getShort();
		final int next = byteBuffer.getInt();

		int length = recordSize - SYSTEM_INFO_SIZE;
		if(next != NULL_POINTER)
			length += dataLength(next);

		return length;
	}

	private static final class AllocationInfo {
		private int freeListHead;
		private int freeListHeadSize;

		private int freeListTail;
		private int freeListTailSize;

		private List<DataChunkInfo> dataChunks;

		private AllocationInfo(int freeListHead, int freeListHeadSize, int freeListTail,
													 int freeListTailSize, List<DataChunkInfo> dataChunks) {
			this.freeListHead = freeListHead;
			this.freeListHeadSize = freeListHeadSize;
			this.freeListTail = freeListTail;
			this.freeListTailSize = freeListTailSize;
			this.dataChunks = dataChunks;
		}
	}

	private static final class DataChunkInfo {
		private final int pointer;
		private final int allocationSize;
		private final int recordSize;

		private DataChunkInfo(int pointer, int allocationSize, int recordSize) {
			this.pointer = pointer;
			this.allocationSize = allocationSize;
			this.recordSize = recordSize;
		}
	}
}
