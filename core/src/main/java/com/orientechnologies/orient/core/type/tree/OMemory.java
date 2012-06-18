package com.orientechnologies.orient.core.type.tree;

import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializer;

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

	<T> T get(int pointer, int offset, OBinarySerializer<T> serializer);

	<T> void set(int pointer, int offset, T data, OBinarySerializer<T> serializer);

	int getInt(int pointer, int offset);

  void setInt(int pointer, int offset, int value);

  int capacity();

  int freeSpace();

  void clear();
}
