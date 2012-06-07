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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OShortSerializer;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Andrey Lomakin
 * @since 07.04.12
 */
public class OOffHeapMemory {
  private static Class<?>	sunClass = null;

  static {
    // GET SUN JDK METHOD TO CLEAN MMAP BUFFERS
    try {
      sunClass = Class.forName("sun.nio.ch.DirectBuffer");
    } catch (Exception e) {
    }
  }

  public static final int  SYSTEM_INFO_SIZE = OIntegerSerializer.INT_SIZE + 2 * OShortSerializer.SHORT_SIZE;

  public static final int  NULL_POINTER     = -1;

  private int              freeListHead;
  private int              freeListHeadSize;

  private int              freeListTail;
  private int              freeListTailSize;

  private final ByteBuffer byteBuffer;
  private final int        dataChunkSize;

  public OOffHeapMemory(int capacity, int dataChunkSize) {
    capacity = (int) (Math.floor(1.0 * capacity / dataChunkSize) * dataChunkSize);
    byteBuffer = ByteBuffer.allocateDirect(capacity);

    this.dataChunkSize = dataChunkSize;

    freeListHead = 0;
    freeListTail = 0;

    freeListHeadSize = byteBuffer.capacity();
    freeListTailSize = freeListHeadSize;

    byteBuffer.putInt(freeListHeadSize);
  }

  public int allocate(byte[] bytes) {
    final AllocationInfo allocationInfo = new AllocationInfo(freeListHead, freeListHeadSize, freeListTail, freeListTailSize,
        new ArrayList<DataChunkInfo>());

    if (!canAllocate(allocationInfo, 0, bytes.length))
      return NULL_POINTER;

    return doAllocation(allocationInfo, bytes);
  }

	public int allocate(int size) {
		final AllocationInfo allocationInfo = new AllocationInfo(freeListHead, freeListHeadSize, freeListTail, freeListTailSize,
						new ArrayList<DataChunkInfo>());

		if (!canAllocate(allocationInfo, 0, size))
			return NULL_POINTER;

		return doAllocation(allocationInfo, null);
	}


	public void free(int pointer) {
    doFree(pointer);
  }

  public byte[] get(int pointer, final int offset, final int length) {
		int dataLength = dataLength(pointer);

		final  byte[] content;
		if(length > 0 )
		 content = new byte[Math.min(dataLength, length)];
		else
		 content = new byte[dataLength - offset];

		int dataOffset = 0;
		int contentOffset = 0;
		boolean startReading = false;
		do {
			byteBuffer.position(pointer + OShortSerializer.SHORT_SIZE);

			final int chunkSize = byteBuffer.getShort() - SYSTEM_INFO_SIZE;
			final int nextPointer = byteBuffer.getInt();

			if(startReading) {
				final int readSize = Math.min(chunkSize, content.length - contentOffset);
				byteBuffer.get(content, contentOffset, readSize);
				contentOffset += readSize;
			}	else {
				startReading = dataOffset + chunkSize > offset;
				if(startReading) {
					byteBuffer.position(pointer + SYSTEM_INFO_SIZE + offset - dataOffset);
					final int readSize = Math.min(chunkSize - (offset - dataOffset), content.length);
					byteBuffer.get(content, 0, readSize);
					contentOffset += readSize;
				}
				dataOffset += chunkSize;
			}

			pointer =  nextPointer;
		} while (contentOffset < content.length && pointer != NULL_POINTER);

		if(!startReading)
			throw new IllegalArgumentException("Passed in offset out of the allocated memory bounds");

		return content;
  }

	public void set(int pointer, final int offset, final int length, final byte[] content) {
		boolean startWriting = false;
		int dataOffset = 0;
		int contentOffset = 0;

		do {
			byteBuffer.position(pointer + OShortSerializer.SHORT_SIZE);

			final int chunkSize = byteBuffer.getShort() - SYSTEM_INFO_SIZE;

			final int nextPointer = byteBuffer.getInt();

			if(startWriting) {
				final int writeSize = Math.min(chunkSize, content.length - contentOffset);
				byteBuffer.put(content, contentOffset, writeSize);
				contentOffset += writeSize;
			}	else {
				startWriting = dataOffset + chunkSize > offset;
				if(startWriting) {
					byteBuffer.position(pointer + SYSTEM_INFO_SIZE + offset - dataOffset);
					final int writeSize = Math.min(chunkSize - (offset - dataOffset), content.length);
					byteBuffer.put(content, 0, writeSize);
					contentOffset += writeSize;
				}
				dataOffset += chunkSize;
			}

			pointer =  nextPointer;
		} while (contentOffset < length && pointer != NULL_POINTER);

		if(!startWriting)
			throw new IllegalArgumentException("Passed in offset out of the allocated memory bounds");
	}

	public int getInt(final int pointer, final int offset) {
		int dataOffset = 0;
		int currentPointer = pointer;

		do {
			byteBuffer.position(pointer + OShortSerializer.SHORT_SIZE);

			final int chunkSize = byteBuffer.getShort() - SYSTEM_INFO_SIZE;

			final int nextPointer = byteBuffer.getInt();

			if(dataOffset + chunkSize > offset) {
				if(chunkSize - (offset - dataOffset) >= OIntegerSerializer.INT_SIZE)
          return byteBuffer.getInt(currentPointer + SYSTEM_INFO_SIZE + offset - dataOffset);
				 else
					return OIntegerSerializer.INSTANCE.deserialize(get(currentPointer, offset - dataOffset, OIntegerSerializer.INT_SIZE), 0);
			}

			dataOffset += chunkSize;
			currentPointer =  nextPointer;
		} while (currentPointer != NULL_POINTER);

		throw new IllegalArgumentException("Passed in offset out of the allocated memory bounds");
	}

	public void setInt(int pointer, final int offset, final int value) {
		int dataOffset = 0;
		int currentPointer = pointer;
		do {
			byteBuffer.position(pointer + OShortSerializer.SHORT_SIZE);

			final int chunkSize = byteBuffer.getShort() - SYSTEM_INFO_SIZE;

      final int nextPointer = byteBuffer.getInt();

			if(dataOffset + chunkSize > offset) {
				if(chunkSize - (offset - dataOffset) >= OIntegerSerializer.INT_SIZE) {
					byteBuffer.putInt(currentPointer + SYSTEM_INFO_SIZE + offset - dataOffset, value);
					return;
				} else {
					byte[] content = new byte[OIntegerSerializer.INT_SIZE];
					OIntegerSerializer.INSTANCE.serialize(value, content, 0);
					set(currentPointer, offset - dataOffset, OIntegerSerializer.INT_SIZE, content);
				}
			}

			dataOffset += chunkSize;
			currentPointer =  nextPointer;
		} while (currentPointer != NULL_POINTER);

		throw new IllegalArgumentException("Passed in offset out of the allocated memory bounds");
	}

  public int freeChunkCount() {
    if (freeListHead == -1)
      return 0;

    if (freeListHead == freeListTail)
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

  public int capacity() {
    return byteBuffer.capacity();
  }

  public int freeSpace() {
    if (freeListHead == -1)
      return 0;

    if (freeListHead == freeListTail)
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

  public void clear() {
    freeListHead = 0;
    freeListTail = 0;

    freeListHeadSize = byteBuffer.capacity();
    freeListTailSize = freeListHeadSize;

    byteBuffer.clear();
    byteBuffer.putInt(freeListHeadSize);
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    if (sunClass != null) {
      // USE SUN JVM SPECIAL METHOD TO FREE RESOURCES
      try {
        final Method m = sunClass.getMethod("cleaner");
        final Object cleaner = m.invoke(byteBuffer);
        cleaner.getClass().getMethod("clean").invoke(cleaner);
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error on calling Sun's MMap buffer clean", e);
      }
    }
  }

  private boolean canAllocate(AllocationInfo allocationInfo, int offset, int size) {
    if (allocationInfo.freeListHead == -1)
      return false;

    int sizeToAdd = size - offset + SYSTEM_INFO_SIZE;

    int allocationSize = (int) (Math.ceil(1.0 * sizeToAdd / dataChunkSize) * dataChunkSize);

    final int pos;
    if (allocationInfo.freeListHeadSize > allocationSize) {
      pos = allocationInfo.freeListHead + allocationInfo.freeListHeadSize - allocationSize;
      allocationInfo.freeListHeadSize -= allocationSize;
    } else {
      pos = allocationInfo.freeListHead;
      allocationSize = allocationInfo.freeListHeadSize;

      if (allocationInfo.freeListHead != allocationInfo.freeListTail) {
        allocationInfo.freeListHead = byteBuffer.getInt(allocationInfo.freeListHead + 4);
				allocationInfo.freeListHeadSize = byteBuffer.getInt(allocationInfo.freeListHead);
      } else {
        allocationInfo.freeListHead = -1;
        allocationInfo.freeListTail = -1;
      }
    }

    if (allocationInfo.freeListHead == allocationInfo.freeListTail)
      allocationInfo.freeListTailSize = allocationInfo.freeListHeadSize;

    final int recordSize;
    if (allocationSize >= sizeToAdd)
      recordSize = sizeToAdd;
    else
      recordSize = allocationSize;

    allocationInfo.dataChunks.add(new DataChunkInfo(pos, allocationSize, recordSize));
    final int chunkSize = recordSize - SYSTEM_INFO_SIZE;
    if (size - offset > chunkSize)
      return canAllocate(allocationInfo, offset + chunkSize, size);
    else
      return true;
  }

  private int doAllocation(AllocationInfo allocationInfo, byte[] bytes) {
    freeListHead = allocationInfo.freeListHead;
    freeListTail = allocationInfo.freeListTail;
    freeListHeadSize = allocationInfo.freeListHeadSize;
    freeListTailSize = allocationInfo.freeListTailSize;

    if (freeListHead != -1)
      byteBuffer.putInt(freeListHead, freeListHeadSize);

    if (freeListTail != -1)
      byteBuffer.putInt(freeListTail, freeListTailSize);

    final List<DataChunkInfo> dataChunks = allocationInfo.dataChunks;
    final int dataChunksSize = dataChunks.size();

    int offset = 0;
    for (int i = 0; i < dataChunksSize; i++) {
      final DataChunkInfo dataChunkInfo = dataChunks.get(i);

      byteBuffer.position(dataChunkInfo.pointer);

      byteBuffer.putShort((short) dataChunkInfo.allocationSize);

      byteBuffer.putShort((short) dataChunkInfo.recordSize);
			if (i < dataChunksSize - 1)
				byteBuffer.putInt(dataChunks.get(i + 1).pointer);
			else
				byteBuffer.putInt(OOffHeapMemory.NULL_POINTER);



      final int chunkSize = dataChunkInfo.recordSize - SYSTEM_INFO_SIZE;
			if(bytes != null)
				byteBuffer.put(bytes, offset, chunkSize);

      offset += chunkSize;
    }

    return allocationInfo.dataChunks.get(0).pointer;
  }

  private void doFree(int pos) {
    byteBuffer.position(pos);

    final int allocationSize = byteBuffer.getShort();

		byteBuffer.position(byteBuffer.position() + OShortSerializer.SHORT_SIZE);
    final int next = byteBuffer.getInt();

    if (freeListTail == -1) {
      freeListTail = pos;
      freeListHead = pos;

      freeListHeadSize = allocationSize;
      freeListTailSize = allocationSize;

      byteBuffer.putInt(freeListTail, freeListTailSize);
    } else if (pos == freeListTail + freeListTailSize) {
      freeListTailSize += allocationSize;

      byteBuffer.putInt(freeListTail, freeListTailSize);
    } else {
      byteBuffer.putInt(freeListTail + OIntegerSerializer.INT_SIZE, pos);

      freeListTail = pos;
      freeListTailSize = allocationSize;

      byteBuffer.putInt(freeListTail, freeListTailSize);
    }

    if (freeListHead == freeListTail)
      freeListHeadSize = freeListTailSize;

    if (next != NULL_POINTER)
      doFree(next);
  }

  private int dataLength(int pos) {
    byteBuffer.position(pos + OShortSerializer.SHORT_SIZE);

    final int recordSize = byteBuffer.getShort();

    final int next = byteBuffer.getInt();

		int length = recordSize - SYSTEM_INFO_SIZE;
    if (next != NULL_POINTER)
      length += dataLength(next);

    return length;
  }

  private static final class AllocationInfo {
    private int                 freeListHead;
    private int                 freeListHeadSize;

    private int                 freeListTail;
    private int                 freeListTailSize;

    private List<DataChunkInfo> dataChunks;

    private AllocationInfo(int freeListHead, int freeListHeadSize, int freeListTail, int freeListTailSize,
        List<DataChunkInfo> dataChunks) {
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
