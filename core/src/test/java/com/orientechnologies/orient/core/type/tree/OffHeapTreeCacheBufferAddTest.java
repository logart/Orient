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

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 09.05.12
 */
@Test
public class OffHeapTreeCacheBufferAddTest {
  private OffHeapMemory memory = new OffHeapMemory(2000000, 20);
	private OffHeapTreeCacheBuffer<Integer> treeCacheBuffer;

	@BeforeMethod
	public void setUp() {
		treeCacheBuffer =
						new OffHeapTreeCacheBuffer<Integer>(memory, OIntegerSerializer.INSTANCE);
	}

  @AfterMethod
  public void tearDown() {
    memory.clear();
  }

  @Test
	public void testAddOneItem() {
		boolean result = treeCacheBuffer.add(createCacheEntry(1));
		Assert.assertTrue(result);

		final OffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntry = treeCacheBuffer.get(1);
		Assert.assertNotNull(cacheEntry);
		Assert.assertEquals(cacheEntry, createCacheEntry(1));
	}

  @Test
	public void testAddTwoOrderedItems() {
		boolean resultOne = treeCacheBuffer.add(createCacheEntry(1));
		Assert.assertTrue(resultOne);

		boolean resultTwo = treeCacheBuffer.add(createCacheEntry(2));
		Assert.assertTrue(resultTwo);

		final OffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryOne = treeCacheBuffer.get(1);
		Assert.assertNotNull(cacheEntryOne);
		Assert.assertEquals(cacheEntryOne, createCacheEntry(1));

		final OffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryTwo = treeCacheBuffer.get(2);
		Assert.assertNotNull(cacheEntryTwo);
		Assert.assertEquals(cacheEntryTwo, createCacheEntry(2));
	}

  @Test
	public void testAddTwoReverseOrderedItems() {
		boolean resultOne = treeCacheBuffer.add(createCacheEntry(2));
		Assert.assertTrue(resultOne);

		boolean  resultTwo = treeCacheBuffer.add(createCacheEntry(1));
		Assert.assertTrue(resultTwo);

		final OffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryOne = treeCacheBuffer.get(1);
		Assert.assertNotNull(cacheEntryOne);
		Assert.assertEquals(cacheEntryOne, createCacheEntry(1));

		final OffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryTwo = treeCacheBuffer.get(2);
		Assert.assertNotNull(cacheEntryTwo);
		Assert.assertEquals(cacheEntryTwo, createCacheEntry(2));
	}

  @Test
	public void testAddThreeOrderedItems() {
		boolean resultOne = treeCacheBuffer.add(createCacheEntry(1));
		Assert.assertTrue(resultOne);

		boolean resultTwo = treeCacheBuffer.add(createCacheEntry(2));
		Assert.assertTrue(resultTwo);

		boolean resultThree = treeCacheBuffer.add(createCacheEntry(3));
		Assert.assertTrue(resultThree);


		final OffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryOne = treeCacheBuffer.get(1);
		Assert.assertNotNull(cacheEntryOne);
		Assert.assertEquals(cacheEntryOne, createCacheEntry(1));

		final OffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryTwo = treeCacheBuffer.get(2);
		Assert.assertNotNull(cacheEntryTwo);
		Assert.assertEquals(cacheEntryTwo, createCacheEntry(2));

		final OffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryThree = treeCacheBuffer.get(3);
		Assert.assertNotNull(cacheEntryThree);
		Assert.assertEquals(cacheEntryThree, createCacheEntry(3));
	}

  @Test
	public void testAddThreeReversOrderedItems() {
		boolean resultOne = treeCacheBuffer.add(createCacheEntry(3));
		Assert.assertTrue(resultOne);

		boolean resultTwo = treeCacheBuffer.add(createCacheEntry(2));
		Assert.assertTrue(resultTwo);

		boolean resultThree = treeCacheBuffer.add(createCacheEntry(1));
		Assert.assertTrue(resultThree);


		final OffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryOne = treeCacheBuffer.get(1);
		Assert.assertNotNull(cacheEntryOne);
		Assert.assertEquals(cacheEntryOne, createCacheEntry(1));

		final OffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryTwo = treeCacheBuffer.get(2);
		Assert.assertNotNull(cacheEntryTwo);
		Assert.assertEquals(cacheEntryTwo, createCacheEntry(2));

		final OffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryThree = treeCacheBuffer.get(3);
		Assert.assertNotNull(cacheEntryThree);
		Assert.assertEquals(cacheEntryThree, createCacheEntry(3));
	}

  @Test
	public void testAddThreeNonOrderedItems() {
		boolean resultOne = treeCacheBuffer.add(createCacheEntry(2));
		Assert.assertTrue(resultOne);

		boolean resultTwo = treeCacheBuffer.add(createCacheEntry(3));
		Assert.assertTrue(resultTwo);

		boolean resultThree = treeCacheBuffer.add(createCacheEntry(1));
		Assert.assertTrue(resultThree);


		final OffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryOne = treeCacheBuffer.get(1);
		Assert.assertNotNull(cacheEntryOne);
		Assert.assertEquals(cacheEntryOne, createCacheEntry(1));

		final OffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryTwo = treeCacheBuffer.get(2);
		Assert.assertNotNull(cacheEntryTwo);
		Assert.assertEquals(cacheEntryTwo, createCacheEntry(2));

		final OffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryThree = treeCacheBuffer.get(3);
		Assert.assertNotNull(cacheEntryThree);
		Assert.assertEquals(cacheEntryThree, createCacheEntry(3));
	}

  @Test
	public void testAdd10000NonOrderedItems() {
		Set<Integer> addedKeys = new HashSet<Integer>();
		Random random = new Random();
		for(int i  = 0; i < 10000; i++) {
			int key = random.nextInt();
			while (addedKeys.contains(key))
				key = random.nextInt();

			boolean result = treeCacheBuffer.add(createCacheEntry(key));
			Assert.assertTrue(result);

			addedKeys.add(key);
			OffHeapTreeCacheBuffer.CacheEntry<Integer> cacheEntry = treeCacheBuffer.get(key);
			Assert.assertEquals(cacheEntry, createCacheEntry(key));
		}

		for(int key : addedKeys) {
			OffHeapTreeCacheBuffer.CacheEntry<Integer> cacheEntry = treeCacheBuffer.get(key);
			Assert.assertEquals(cacheEntry, createCacheEntry(key));
		}
	}

	private OffHeapTreeCacheBuffer.CacheEntry<Integer> createCacheEntry(int key) {
		return new OffHeapTreeCacheBuffer.CacheEntry<Integer>(key,1, new ORecordId(1, 1),
						new ORecordId(1, 2), new ORecordId(1, 3), new ORecordId(1, 4) );
	}
}