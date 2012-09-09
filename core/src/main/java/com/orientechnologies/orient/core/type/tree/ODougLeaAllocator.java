package com.orientechnologies.orient.core.type.tree;

import com.orientechnologies.common.types.OBinaryConverter;
import com.orientechnologies.common.types.OBinaryConverterFactory;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializer;

/**
 * @author <a href='mailto:enisher@gmail.com'>Artem Orobets</a>
 */
public class ODougLeaAllocator implements OMemory {

  private static final OBinaryConverter CONVERTER           = OBinaryConverterFactory.getConverter();

  private static final int              BUCKET_OVERHEAD     = 8;
  private static final int              MAX_SIZE            = 64 * 8;

  private static final int              IN_USE            = 0x80000000;

  public static final int               PREV_POINTER_OFFSET = 8;
  public static final int               NEXT_POINTER_OFFSET = 4;

  private byte[]                        buffer;

  private int[]                         smallBin;

  public ODougLeaAllocator(int bufferSize) {
    int alignedBufferSize = bufferSize & 0xFFFFFFF0;
    buffer = new byte[alignedBufferSize];

    int bucketCount = 64;
    smallBin = new int[bucketCount];
    for (int i = 0; i < bucketCount; i++) {
      smallBin[i] = OMemory.NULL_POINTER;
    }

    // init available buckets
    int spaceAvailable = alignedBufferSize;
    int bucketSize = MAX_SIZE;
    int freePointer = 0;
    while (spaceAvailable > 0) {
      if (spaceAvailable >= bucketSize) {
        writeInt(freePointer, 0, ~IN_USE & bucketSize);
        writeInt(freePointer, bucketSize - 4, bucketSize);

        insertSmallChunk(freePointer, bucketSize);

        freePointer += bucketSize;

        spaceAvailable -= bucketSize;
      } else {
        bucketSize -= 8;
      }
    }
  }

  public int allocate(byte[] bytes) {
    return 0;
  }

  public int allocate(int size) {
    int nb = padRequest(size);

    return 0;
  }

  public void free(int pointer) {

  }

  public int getActualSpace(int pointer) {
    return 0;
  }

  public byte[] get(int pointer, int offset, int length) {
    return new byte[0];
  }

  public void set(int pointer, int offset, int length, byte[] content) {

  }

  public <T> T get(int pointer, int offset, OBinarySerializer<T> serializer) {
    return null;
  }

  public <T> void set(int pointer, int offset, T data, OBinarySerializer<T> serializer) {

  }

  public int getInt(int pointer, int offset) {
    return 0;
  }

  public void setInt(int pointer, int offset, int value) {

  }

  public long getLong(int pointer, int offset) {
    return 0;
  }

  public void setLong(int pointer, int offset, long value) {

  }

  public byte getByte(int pointer, int offset) {
    return 0;
  }

  public void setByte(int pointer, int offset, byte value) {

  }

  public int capacity() {
    return buffer.length;
  }

  public int freeSpace() {
    return 0;
  }

  public void clear() {

  }

  /**
   * Checks correctness of allocator representation.
   * 
   * @return free space
   */
  public int checkStructure() {
    int space = 0;
    for (int i = smallBin.length - 1; i >= 0; i--) {
      space += checkFreeList(smallBin[i]);
    }
    return space;
  }

  private void insertSmallChunk(int freePointer, int size) {
    int binNumber = getSmallBinNumber(size);

    writeInt(freePointer, PREV_POINTER_OFFSET, OMemory.NULL_POINTER);

    final int next = smallBin[binNumber];
    writeInt(freePointer, NEXT_POINTER_OFFSET, next);

    if (next != OMemory.NULL_POINTER) {
      assert OMemory.NULL_POINTER == readInt(next, PREV_POINTER_OFFSET);
      writeInt(next, PREV_POINTER_OFFSET, freePointer);
    }

    smallBin[binNumber] = freePointer;
  }

  private int getSmallBinNumber(int size) {
    return (size >>> 3) - 1;
  }

  private int getSize(int smallBinNumber) {
    return (smallBinNumber + 1) << 3;
  }

  private int checkFreeList(int pointer) {
    int space = 0;
    while (pointer != OMemory.NULL_POINTER) {
      final int s = readInt(pointer, 0);
      assert (s & IN_USE) == 0;
      assert s == readInt(pointer, s - 4);

      space += s;

      pointer = readInt(pointer, NEXT_POINTER_OFFSET);
    }
    return space;
  }

  private int padRequest(int size) {
    return size + BUCKET_OVERHEAD;
  }

  private void writeInt(int pointer, int offset, int value) {
    CONVERTER.putInt(buffer, pointer, offset, value);
  }

  private int readInt(int pointer, int offset) {
    return CONVERTER.getInt(buffer, pointer, offset);
  }
}
