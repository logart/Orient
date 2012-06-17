package com.orientechnologies.orient.core.type.tree;

/**
 * @author Artem Orobets
 * @since 10.06.12
 */
public interface OMemory {
  int NULL_POINTER = -1;

  int allocate(byte[] bytes);

  int allocate(int size);

  void free(int pointer);

  byte[] get(int pointer, int offset, int length);

  void set(int pointer, int offset, int length, byte[] content);

  int getInt(int pointer, int offset);

  void setInt(int pointer, int offset, int value);

  int capacity();

  int freeSpace();

  void clear();
}
