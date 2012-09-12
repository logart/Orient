package com.orientechnologies.orient.core.type.tree;

import com.orientechnologies.common.types.OBinaryConverter;
import com.orientechnologies.common.types.OBinaryConverterFactory;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializer;

/**
 * @author <a href='mailto:enisher@gmail.com'>Artem Orobets</a>
 */
public class ODougLeaAllocator implements OMemory {

  private static final OBinaryConverter CONVERTER       = OBinaryConverterFactory.getConverter();

  private static final int              BUCKET_OVERHEAD = 8;
  private static final int              MIN_CHUNK_SIZE  = 16;

  private static final int              SMALL_BIN_COUNT = 64;
  private static final int              BIG_BIN_COUNT   = 64;

  private static final int              SMALL_BIN_SHIFT = 3;
  private static final int              BIG_BIN_SHIFT   = 8;

  private byte[]                        buffer;

  private int[]                         smallBin        = new int[SMALL_BIN_COUNT];
  private BitMap                        smallMap        = new BitMap(0L);

  private int[]                         bigBin          = new int[BIG_BIN_COUNT];
  private BitMap                        bigMap          = new BitMap(0L);

  public ODougLeaAllocator(int bufferSize) {
    int alignedBufferSize = bufferSize & 0xFFFFFFF0;
    buffer = new byte[alignedBufferSize + 1];

    for (int i = 0; i < smallBin.length; i++) {
      smallBin[i] = OMemory.NULL_POINTER;
    }
    for (int i = 0; i < bigBin.length; i++) {
      bigBin[i] = OMemory.NULL_POINTER;
    }

    final DataChunk chunk = new DataChunk(0);
    chunk.updateBoundaryTags(alignedBufferSize, false, true);

    final DataChunk topChunk = new DataChunk(alignedBufferSize);
    topChunk.updateBoundaryTags(1, true, false);
  }

  public int allocate(byte[] bytes) {
    return 0;
  }

  public int allocate(int size) {
    int nb = padRequest(size);

    return OMemory.NULL_POINTER;
  }

  public void free(int pointer) {

  }

  public int getActualSpace(int pointer) {
    return readInt(pointer, 0) & ~0x3;
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
    return buffer.length - 1;
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

  private void insertSmallChunk(int chunkPtr, int size) {
    assert size >= MIN_CHUNK_SIZE;

    int binIndex = getSmallBinIndex(size);

    final DataChunk chunk = new DataChunk(chunkPtr);

    chunk.updateBoundaryTags(size, false, true);
    if (smallMap.isMarked(binIndex)) {
      final int fdPtr = smallBin[chunkPtr];
      final DataChunk fd = new DataChunk(fdPtr);

      final int bkPtr = fd.getBk();
      final DataChunk bk = new DataChunk(bkPtr);

      chunk.setBk(bkPtr);
      bk.setFd(chunkPtr);

      chunk.setFd(fdPtr);
      fd.setBk(chunkPtr);
    } else {
      chunk.setFd(chunkPtr);
      chunk.setBk(chunkPtr);
      smallBin[binIndex] = chunkPtr;
      smallMap.mark(binIndex);
    }
  }

  private void unlinkSmallChunk(int chunkPtr, int size) {
    final DataChunk chunk = new DataChunk(chunkPtr);

    final int bkPtr = chunk.getBk();
    if (chunkPtr == bkPtr) {
      final int binIndex = getSmallBinIndex(size);

      smallMap.clear(binIndex);
      smallBin[binIndex] = OMemory.NULL_POINTER;
    } else {
      final DataChunk bk = new DataChunk(bkPtr);

      final int fdPtr = chunk.getFd();
      final DataChunk fd = new DataChunk(fdPtr);

      bk.setFd(fdPtr);
      fd.setBk(bkPtr);
    }
  }

  private int getSmallBinIndex(int size) {
    return size >> SMALL_BIN_SHIFT;
  }

  private void insertBigChunk(int chunkPtr, int size) {
    throw new RuntimeException("Not implemented");
  }

  private void unlinkBigChunk(int chunkPtr, int size) {
    throw new RuntimeException("Not implemented");
  }

  private void insertChunk(int chunkPtr, int size) {
    if (isSmall(size)) {
      insertSmallChunk(chunkPtr, size);
    } else {
      insertBigChunk(chunkPtr, size);
    }
  }

  private void unlinkChunk(int chunkPtr, int size) {
    if (isSmall(size)) {
      insertSmallChunk(chunkPtr, size);
    } else {
      insertBigChunk(chunkPtr, size);
    }
  }

  private boolean isSmall(int chunkSize) {
    return chunkSize >> SMALL_BIN_SHIFT < SMALL_BIN_COUNT;
  }

  private int getSize(int smallBinNumber) {
    return (smallBinNumber + 1) << 3;
  }

  private int checkFreeList(int pointer) {
    int space = 0;
    // while (pointer != OMemory.NULL_POINTER) {
    // final int s = readInt(pointer, 0);
    // assert (s & IN_USE) == 0;
    // assert s == readInt(pointer, s - 4);
    //
    // space += s;
    //
    // pointer = readInt(pointer, NEXT_POINTER_OFFSET);
    // }
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

  private class DataChunk {
    public static final int  FORWARD_POINTER_OFFSET = 4;
    public static final int  BACK_POINTER_OFFSET    = 8;
    public static final int  LEFT_POINTER_OFFSET    = 12;
    public static final int  RIGHT_POINTER_OFFSET   = 16;
    public static final int  PARENT_POINTER_OFFSET  = 20;
    public static final int  BIG_BIN_INDEX_OFFSET   = 24;

    // HEADER FLAGS
    private static final int IN_USE                 = 0x1;
    private static final int P_IN_USE               = 0x2;

    private final int        headPointer;

    public DataChunk(int headPointer) {
      this.headPointer = headPointer;
    }

    public void updateBoundaryTags(int size, boolean inUse, boolean pInUse) {
      assert (size & 0xF) == 0;
      int head = size;
      if (inUse)
        head |= IN_USE;
      if (pInUse)
        head |= P_IN_USE;

      writeInt(headPointer, 0, head);

      if (!inUse)
        writeInt(headPointer, size - 4, size);
    }

    public int getFd() {
      return readInt(headPointer, FORWARD_POINTER_OFFSET);
    }

    public void setFd(int pointer) {
      writeInt(headPointer, FORWARD_POINTER_OFFSET, pointer);
    }

    public int getBk() {
      return readInt(headPointer, BACK_POINTER_OFFSET);
    }

    public void setBk(int pointer) {
      writeInt(headPointer, BACK_POINTER_OFFSET, pointer);
    }

    public int getLeft() {
      return readInt(headPointer, LEFT_POINTER_OFFSET);
    }

    public void setLeft(int ptr) {
      writeInt(headPointer, LEFT_POINTER_OFFSET, ptr);
    }

    public int getRight() {
      return readInt(headPointer, RIGHT_POINTER_OFFSET);
    }

    public void setRight(int ptr) {
      writeInt(headPointer, RIGHT_POINTER_OFFSET, ptr);
    }

    public int getParent() {
      return readInt(headPointer, PARENT_POINTER_OFFSET);
    }

    public void setParent(int ptr) {
      writeInt(headPointer, PARENT_POINTER_OFFSET, ptr);
    }

    public int getBigBinIndex() {
      return buffer[headPointer + BIG_BIN_INDEX_OFFSET];
    }

    public void setBigBinIndex(int index) {
      buffer[headPointer + BIG_BIN_INDEX_OFFSET] = (byte) (index & 0xFF);
    }
  }

  public static class BitMap {
    private long set;

    public BitMap(long initialValue) {
      set = initialValue;
    }

    public void mark(int index) {
      set |= 1l << index;
    }

    public void clear(int index) {
      set &= ~(1l << index);
    }

    public boolean isMarked(int index) {
      return (set & (1l << index)) != 0;
    }

    public long getSet() {
      return set;
    }
  }
}
