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

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author Andrey Lomakin
 * @since 08.04.12
 */
@Test
public class OffHeapMemoryTest {
  @Test
  public void testAdd5Remove3rdGetSameChunk() {
    final OOffHeapMemory memory = new OOffHeapMemory(40 * 15, 20);

    final List<Integer> pointers = new ArrayList<Integer>();
    final List<byte[]> data = new ArrayList<byte[]>();

    int size = 600;

    Assert.assertEquals(memory.freeSpace(), size);

    for (int i = 0; i < 15; i++) {
      final byte[] bytes = new byte[32];
      for (int j = 0; j < 32; j++) {
        bytes[j] = (byte) (i + j);
      }

      int pointer = memory.allocate(bytes);
      Assert.assertFalse(pointer == OOffHeapMemory.NULL_POINTER);
      byte[] loadedData = memory.get(pointer, 0, -1);
      Assert.assertEquals(loadedData, bytes);

      if (i % 3 == 0)
        memory.free(pointer);
      else {
        pointers.add(pointer);
        data.add(bytes);

        size -= (bytes.length + OOffHeapMemory.SYSTEM_INFO_SIZE);
      }

      Assert.assertEquals(memory.freeSpace(), size);
    }

    for (int i = 0; i < pointers.size(); i++) {
      final byte[] bytes = data.get(i);
      int pointer = pointers.get(i);

      byte[] loadedData = memory.get(pointer, 0, -1);
      Assert.assertEquals(loadedData, bytes);
    }

    byte[] stub = new byte[32];
    for (int i = 0; i < 5; i++) {
      int pointer = memory.allocate(stub);
      Assert.assertFalse(pointer == OOffHeapMemory.NULL_POINTER);
    }

    int nullPointer = memory.allocate(stub);
    Assert.assertEquals(nullPointer, OOffHeapMemory.NULL_POINTER);
  }

  @Test
  public void testAdd5Remove1stGetSameChunk() {
    final OOffHeapMemory memory = new OOffHeapMemory(40 * 15, 20);

    final List<Integer> pointers = new ArrayList<Integer>();
    final List<byte[]> data = new ArrayList<byte[]>();

    for (int i = 0; i < 15; i++) {
      final byte[] bytes = new byte[32];
      for (int j = 0; j < 32; j++) {
        bytes[j] = (byte) (i + j);
      }

      int pointer = memory.allocate(bytes);
      Assert.assertFalse(pointer == OOffHeapMemory.NULL_POINTER);
      byte[] loadedData = memory.get(pointer, 0, -1);
      Assert.assertEquals(loadedData, bytes);

      pointers.add(pointer);
      data.add(bytes);

      if ((i + 1) % 5 == 0) {
        int index = pointers.size() - 5;
        memory.free(pointers.get(index));
        pointers.remove(index);
        data.remove(index);
      }
    }

    for (int i = 0; i < pointers.size(); i++) {
      final byte[] bytes = data.get(i);
      int pointer = pointers.get(i);

      byte[] loadedData = memory.get(pointer, 0, -1);
      Assert.assertEquals(loadedData, bytes);
    }

    byte[] stub = new byte[12];
    int pointer;
    do {
      pointer = memory.allocate(stub);

    } while (pointer != OOffHeapMemory.NULL_POINTER);

    Assert.assertTrue(stub.length + OOffHeapMemory.SYSTEM_INFO_SIZE > memory.freeSpace());
  }

  @Test
  public void testAddRemoveSameDataSize() {
    final Random random = new Random();
    final OOffHeapMemory memory = new OOffHeapMemory(2000, 20);

    final List<Integer> pointers = new ArrayList<Integer>();
    final List<byte[]> data = new ArrayList<byte[]>();

    fillData(random, memory, pointers, data, 60);

    checkData(memory, pointers, data);

    removeData(memory, 2, pointers, data);

    checkData(memory, pointers, data);

    fillData(random, memory, pointers, data, 60);

    checkData(memory, pointers, data);

    removeData(memory, 3, pointers, data);

    checkData(memory, pointers, data);

    fillData(random, memory, pointers, data, 60);

    checkData(memory, pointers, data);
  }

  @Test
  public void testAddRemoveDiffDataSize() {
    final Random random = new Random();
    final OOffHeapMemory memory = new OOffHeapMemory(2000, 20);

    final List<Integer> pointers = new ArrayList<Integer>();
    final List<byte[]> data = new ArrayList<byte[]>();

    fillData(random, memory, pointers, data, 60);
    Assert.assertTrue(60 + OOffHeapMemory.SYSTEM_INFO_SIZE > memory.freeSpace());

    checkData(memory, pointers, data);

    removeData(memory, 2, pointers, data);

    checkData(memory, pointers, data);

    fillData(random, memory, pointers, data, 90);

    Assert.assertTrue(90 + OOffHeapMemory.SYSTEM_INFO_SIZE > memory.freeSpace());

    checkData(memory, pointers, data);

    removeData(memory, 2, pointers, data);

    checkData(memory, pointers, data);

    fillData(random, memory, pointers, data, 10);

    Assert.assertTrue(10 + OOffHeapMemory.SYSTEM_INFO_SIZE > memory.freeSpace());

    checkData(memory, pointers, data);
  }

  @Test
  public void testAddRemoveDiffDataLineSize() {
    final Random random = new Random();
    final OOffHeapMemory memory = new OOffHeapMemory(2000, 20);

    final List<Integer> pointers = new ArrayList<Integer>();
    final List<byte[]> data = new ArrayList<byte[]>();

    fillData(random, memory, pointers, data, 10, 20, 30, 40, 50, 60, 70, 80, 90);

    checkData(memory, pointers, data);

    removeData(memory, 2, pointers, data);

    checkData(memory, pointers, data);

    fillData(random, memory, pointers, data, 20, 30, 40);

    checkData(memory, pointers, data);

    removeData(memory, 2, pointers, data);

    checkData(memory, pointers, data);

    fillData(random, memory, pointers, data, 10, 90, 50);

    checkData(memory, pointers, data);
  }

  @Test
  public void testAddRemoveRandom() {
    final Random random = new Random();
    final OOffHeapMemory memory = new OOffHeapMemory(2000, 20);

    final List<Integer> pointers = new ArrayList<Integer>();
    final List<byte[]> data = new ArrayList<byte[]>();

    for (int k = 0; k < 1000; k++) {
      int sizesLength;
      do {
        sizesLength = random.nextInt(10);
      } while (sizesLength == 0);

      int[] sizes = new int[sizesLength];
      for (int i = 0; i < sizes.length; i++) {
        int size;
        do {
          size = random.nextInt(100);
        } while (size == 0);

        sizes[i] = size;
      }

      fillData(random, memory, pointers, data, sizes);

      checkData(memory, pointers, data);

      int removeCount;
      do {
        removeCount = random.nextInt(10);
      } while (removeCount == 0);

      removeData(memory, removeCount, pointers, data);

      checkData(memory, pointers, data);
    }
  }

  private void removeData(OOffHeapMemory memory, int interval, List<Integer> pointers, List<byte[]> data) {
    int removed = 0;
    for (int i = 0; i < pointers.size(); i++) {
      if (i % interval == 0) {
        memory.free(pointers.get(i - removed));
        pointers.remove(i - removed);
        data.remove(i - removed);
        removed++;
      }
    }
  }

  private void checkData(OOffHeapMemory memory, List<Integer> pointers, List<byte[]> data) {
    for (int i = 0; i < pointers.size(); i++) {
      final byte[] loadedData = memory.get(pointers.get(i), 0, -1);
      Assert.assertEquals(loadedData, data.get(i), i + "-th dat element is broken");
    }
  }

  private void fillData(Random random, OOffHeapMemory memory, List<Integer> pointers, List<byte[]> data, int... sizes) {
    int pointer;
    int sizeIndex = 0;
    int lastSize;
    do {
      final byte[] dataToStore = new byte[sizes[sizeIndex]];
      random.nextBytes(dataToStore);

      pointer = memory.allocate(dataToStore);
      lastSize = dataToStore.length;

      if (pointer != OOffHeapMemory.NULL_POINTER) {
        final byte[] loadedData = memory.get(pointer, 0, -1);
        Assert.assertEquals(loadedData, dataToStore);

        pointers.add(pointer);
        data.add(dataToStore);
      }

      sizeIndex++;
      if (sizeIndex >= sizes.length)
        sizeIndex = 0;
    } while (pointer != OOffHeapMemory.NULL_POINTER);

    int neededSize = lastSize + memory.freeChunkCount() * OOffHeapMemory.SYSTEM_INFO_SIZE;
    int freeSpace = memory.freeSpace();

    Assert.assertTrue(neededSize > freeSpace, "Needed size : " + neededSize + " less than free space : " + freeSpace);
  }
}
