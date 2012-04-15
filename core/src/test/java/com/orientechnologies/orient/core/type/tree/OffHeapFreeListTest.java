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
public class OffHeapFreeListTest {
	@Test
	public void testAdd5Remove3rdGetSameChunk() {
		final OffHeapFreeList freeList = new OffHeapFreeList(40 * 15, 20);

		final List<Integer> pointers = new ArrayList<Integer>();
		final List<byte[]> data = new ArrayList<byte[]>();

		int size = 600;

		Assert.assertEquals(freeList.freeSpace(), size);

		for (int i = 0; i < 15; i++) {
			final byte[] bytes = new byte[32];
			for (int j = 0; j < 32; j++) {
				bytes[j] = (byte) (i + j);
			}

			int pointer = freeList.add(bytes);
			Assert.assertFalse(pointer == OffHeapFreeList.NULL_POINTER);
			byte[] loadedData = freeList.get(pointer);
			Assert.assertEquals(loadedData, bytes);


			if (i % 3 == 0)
				freeList.remove(pointer);
			else {
				pointers.add(pointer);
				data.add(bytes);

				size -= (bytes.length + OffHeapFreeList.SYSTEM_INFO_SIZE);
			}

			Assert.assertEquals(freeList.freeSpace(), size);
		}

		for (int i = 0; i < pointers.size(); i++) {
			final byte[] bytes = data.get(i);
			int pointer = pointers.get(i);

			byte[] loadedData = freeList.get(pointer);
			Assert.assertEquals(loadedData, bytes);
		}

		byte[] stub = new byte[32];
		for (int i = 0; i < 5; i++) {
			int pointer = freeList.add(stub);
			Assert.assertFalse(pointer == OffHeapFreeList.NULL_POINTER);
		}

		int nullPointer = freeList.add(stub);
		Assert.assertEquals(nullPointer, OffHeapFreeList.NULL_POINTER);
	}

	@Test
	public void testAdd5Remove1stGetSameChunk() {
		final OffHeapFreeList freeList = new OffHeapFreeList(40 * 15, 20);

		final List<Integer> pointers = new ArrayList<Integer>();
		final List<byte[]> data = new ArrayList<byte[]>();

		for (int i = 0; i < 15; i++) {
			final byte[] bytes = new byte[32];
			for (int j = 0; j < 32; j++) {
				bytes[j] = (byte) (i + j);
			}

			int pointer = freeList.add(bytes);
			Assert.assertFalse(pointer == OffHeapFreeList.NULL_POINTER);
			byte[] loadedData = freeList.get(pointer);
			Assert.assertEquals(loadedData, bytes);

			pointers.add(pointer);
			data.add(bytes);


			if ((i + 1) % 5 == 0) {
				int index = pointers.size() - 5;
				freeList.remove(pointers.get(index));
				pointers.remove(index);
				data.remove(index);
			}
		}

		for (int i = 0; i < pointers.size(); i++) {
			final byte[] bytes = data.get(i);
			int pointer = pointers.get(i);

			byte[] loadedData = freeList.get(pointer);
			Assert.assertEquals(loadedData, bytes);
		}

		byte[] stub = new byte[12];
		int pointer;
		do {
			pointer = freeList.add(stub);

		} while (pointer != OffHeapFreeList.NULL_POINTER);

		Assert.assertTrue(stub.length + OffHeapFreeList.SYSTEM_INFO_SIZE > freeList.freeSpace());
	}

	@Test
	public void testAddRemoveSameDataSize() {
		final Random random = new Random();
		final OffHeapFreeList freeList = new OffHeapFreeList(2000, 20);

		final List<Integer> pointers = new ArrayList<Integer>();
		final List<byte[]> data = new ArrayList<byte[]>();

		fillData(random, freeList, pointers, data, 60);

		checkData(freeList, pointers, data);

		removeData(freeList, 2, pointers, data);

		checkData(freeList, pointers, data);

		fillData(random, freeList, pointers, data, 60);

		checkData(freeList, pointers, data);

		removeData(freeList, 3, pointers, data);

		checkData(freeList, pointers, data);

		fillData(random, freeList, pointers, data, 60);

		checkData(freeList, pointers, data);
	}

	@Test
	public void testAddRemoveDiffDataSize() {
		final Random random = new Random();
		final OffHeapFreeList freeList = new OffHeapFreeList(2000, 20);

		final List<Integer> pointers = new ArrayList<Integer>();
		final List<byte[]> data = new ArrayList<byte[]>();

		fillData(random, freeList, pointers, data, 60);
		Assert.assertTrue(60 + OffHeapFreeList.SYSTEM_INFO_SIZE > freeList.freeSpace());

		checkData(freeList, pointers, data);

		removeData(freeList, 2, pointers, data);

		checkData(freeList, pointers, data);

		fillData(random, freeList, pointers, data, 90);

		Assert.assertTrue(90 + OffHeapFreeList.SYSTEM_INFO_SIZE > freeList.freeSpace());

		checkData(freeList, pointers, data);

		removeData(freeList, 2, pointers, data);

		checkData(freeList, pointers, data);

		fillData(random, freeList, pointers, data, 10);

		Assert.assertTrue(10 + OffHeapFreeList.SYSTEM_INFO_SIZE > freeList.freeSpace());

		checkData(freeList, pointers, data);
	}

	@Test
	public void testAddRemoveDiffDataLineSize() {
		final Random random = new Random();
		final OffHeapFreeList freeList = new OffHeapFreeList(2000, 20);

		final List<Integer> pointers = new ArrayList<Integer>();
		final List<byte[]> data = new ArrayList<byte[]>();

		fillData(random, freeList, pointers, data, 10, 20, 30, 40, 50, 60, 70, 80, 90);

		checkData(freeList, pointers, data);

		removeData(freeList, 2, pointers, data);

		checkData(freeList, pointers, data);

		fillData(random, freeList, pointers, data, 20, 30, 40);

		checkData(freeList, pointers, data);

		removeData(freeList, 2, pointers, data);

		checkData(freeList, pointers, data);

		fillData(random, freeList, pointers, data, 10, 90, 50);

		checkData(freeList, pointers, data);
	}

	@Test
	public void testAddRemoveRandom() {
		final Random random = new Random();
		final OffHeapFreeList freeList = new OffHeapFreeList(2000, 20);

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

			fillData(random, freeList, pointers, data, sizes);

			checkData(freeList, pointers, data);

			int removeCount;
			do {
				removeCount = random.nextInt(10);
			} while (removeCount == 0);

			removeData(freeList, removeCount, pointers, data);

			checkData(freeList, pointers, data);
		}
	}


	private void removeData(OffHeapFreeList freeList, int interval, List<Integer> pointers, List<byte[]> data) {
		int removed = 0;
		for (int i = 0; i < pointers.size(); i++) {
			if (i % interval == 0) {
				freeList.remove(pointers.get(i - removed));
				pointers.remove(i - removed);
				data.remove(i - removed);
				removed++;
			}
		}
	}

	private void checkData(OffHeapFreeList freeList,
												 List<Integer> pointers, List<byte[]> data) {
		for (int i = 0; i < pointers.size(); i++) {
			final byte[] loadedData = freeList.get(pointers.get(i));
			Assert.assertEquals(loadedData, data.get(i), i + "-th dat element is broken");
		}
	}

	private void fillData(Random random, OffHeapFreeList freeList,
												List<Integer> pointers, List<byte[]> data, int... sizes) {
		int pointer;
		int sizeIndex = 0;
		int lastSize;
		do {
			final byte[] dataToStore = new byte[sizes[sizeIndex]];
			random.nextBytes(dataToStore);

			pointer = freeList.add(dataToStore);
			lastSize = dataToStore.length;

			if (pointer != OffHeapFreeList.NULL_POINTER) {
				final byte[] loadedData = freeList.get(pointer);
				Assert.assertEquals(loadedData, dataToStore);

				pointers.add(pointer);
				data.add(dataToStore);
			}

			sizeIndex++;
			if (sizeIndex >= sizes.length)
				sizeIndex = 0;
		} while (pointer != OffHeapFreeList.NULL_POINTER);

		int neededSize = lastSize + freeList.freeChunkCount() * OffHeapFreeList.SYSTEM_INFO_SIZE;
		int freeSpace = freeList.freeSpace();

		Assert.assertTrue(neededSize > freeSpace, "Needed size : " + neededSize + " less than free space : " + freeSpace);
	}
}
