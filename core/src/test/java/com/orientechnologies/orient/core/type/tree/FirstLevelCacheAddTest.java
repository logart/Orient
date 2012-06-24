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

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 09.05.12
 */
@Test
public class FirstLevelCacheAddTest {
  private OMemory                         memory = new OBuddyMemory(4000000, 20);
  private OMemoryFirstLevelCache<Integer> firstLevelCache;

  @BeforeMethod
  public void setUp() {
    firstLevelCache = new OMemoryFirstLevelCache<Integer>(memory, OIntegerSerializer.INSTANCE);
  }

  @AfterMethod
  public void tearDown() {
    memory.clear();
  }

  @Test
  public void testAddOneItem() {
    boolean result = firstLevelCache.add(createCacheEntry(1));
    Assert.assertTrue(result);

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntry = firstLevelCache.get(1);
    Assert.assertNotNull(cacheEntry);
    Assert.assertEquals(cacheEntry, createCacheEntry(1));
  }

  @Test
  public void testAddTwoOrderedItems() {
    boolean resultOne = firstLevelCache.add(createCacheEntry(1));
    Assert.assertTrue(resultOne);

    boolean resultTwo = firstLevelCache.add(createCacheEntry(2));
    Assert.assertTrue(resultTwo);

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryOne = firstLevelCache.get(1);
    Assert.assertNotNull(cacheEntryOne);
    Assert.assertEquals(cacheEntryOne, createCacheEntry(1));

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryTwo = firstLevelCache.get(2);
    Assert.assertNotNull(cacheEntryTwo);
    Assert.assertEquals(cacheEntryTwo, createCacheEntry(2));
  }

  @Test
  public void testAddTwoReverseOrderedItems() {
    boolean resultOne = firstLevelCache.add(createCacheEntry(2));
    Assert.assertTrue(resultOne);

    boolean resultTwo = firstLevelCache.add(createCacheEntry(1));
    Assert.assertTrue(resultTwo);

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryOne = firstLevelCache.get(1);
    Assert.assertNotNull(cacheEntryOne);
    Assert.assertEquals(cacheEntryOne, createCacheEntry(1));

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryTwo = firstLevelCache.get(2);
    Assert.assertNotNull(cacheEntryTwo);
    Assert.assertEquals(cacheEntryTwo, createCacheEntry(2));
  }

  @Test
  public void testAddThreeOrderedItems() {
    boolean resultOne = firstLevelCache.add(createCacheEntry(1));
    Assert.assertTrue(resultOne);

    boolean resultTwo = firstLevelCache.add(createCacheEntry(2));
    Assert.assertTrue(resultTwo);

    boolean resultThree = firstLevelCache.add(createCacheEntry(3));
    Assert.assertTrue(resultThree);

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryOne = firstLevelCache.get(1);
    Assert.assertNotNull(cacheEntryOne);
    Assert.assertEquals(cacheEntryOne, createCacheEntry(1));

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryTwo = firstLevelCache.get(2);
    Assert.assertNotNull(cacheEntryTwo);
    Assert.assertEquals(cacheEntryTwo, createCacheEntry(2));

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryThree = firstLevelCache.get(3);
    Assert.assertNotNull(cacheEntryThree);
    Assert.assertEquals(cacheEntryThree, createCacheEntry(3));
  }

  @Test
  public void testAddThreeReversOrderedItems() {
    boolean resultOne = firstLevelCache.add(createCacheEntry(3));
    Assert.assertTrue(resultOne);

    boolean resultTwo = firstLevelCache.add(createCacheEntry(2));
    Assert.assertTrue(resultTwo);

    boolean resultThree = firstLevelCache.add(createCacheEntry(1));
    Assert.assertTrue(resultThree);

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryOne = firstLevelCache.get(1);
    Assert.assertNotNull(cacheEntryOne);
    Assert.assertEquals(cacheEntryOne, createCacheEntry(1));

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryTwo = firstLevelCache.get(2);
    Assert.assertNotNull(cacheEntryTwo);
    Assert.assertEquals(cacheEntryTwo, createCacheEntry(2));

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryThree = firstLevelCache.get(3);
    Assert.assertNotNull(cacheEntryThree);
    Assert.assertEquals(cacheEntryThree, createCacheEntry(3));
  }

  @Test
  public void testAddThreeNonOrderedItems() {
    boolean resultOne = firstLevelCache.add(createCacheEntry(2));
    Assert.assertTrue(resultOne);

    boolean resultTwo = firstLevelCache.add(createCacheEntry(3));
    Assert.assertTrue(resultTwo);

    boolean resultThree = firstLevelCache.add(createCacheEntry(1));
    Assert.assertTrue(resultThree);

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryOne = firstLevelCache.get(1);
    Assert.assertNotNull(cacheEntryOne);
    Assert.assertEquals(cacheEntryOne, createCacheEntry(1));

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryTwo = firstLevelCache.get(2);
    Assert.assertNotNull(cacheEntryTwo);
    Assert.assertEquals(cacheEntryTwo, createCacheEntry(2));

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryThree = firstLevelCache.get(3);
    Assert.assertNotNull(cacheEntryThree);
    Assert.assertEquals(cacheEntryThree, createCacheEntry(3));
  }

  @Test
  public void testAdd10000NonOrderedItems() {
    Set<Integer> addedKeys = new HashSet<Integer>();
    Random random = new Random();
    for (int i = 0; i < 10000; i++) {
      int key = random.nextInt();
      while (addedKeys.contains(key))
        key = random.nextInt();

      boolean result = firstLevelCache.add(createCacheEntry(key));
      Assert.assertTrue(result);

      addedKeys.add(key);
      OMemoryFirstLevelCache.CacheEntry<Integer> cacheEntry = firstLevelCache.get(key);
      Assert.assertEquals(cacheEntry, createCacheEntry(key));
    }

    for (int key : addedKeys) {
      OMemoryFirstLevelCache.CacheEntry<Integer> cacheEntry = firstLevelCache.get(key);
      Assert.assertEquals(cacheEntry, createCacheEntry(key));
    }
  }

  private OMemoryFirstLevelCache.CacheEntry<Integer> createCacheEntry(int key) {
    return new OMemoryFirstLevelCache.CacheEntry<Integer>(key, 1, new ORecordId(1, 1), new ORecordId(1, 2), new ORecordId(1, 3),
        new ORecordId(1, 4));
  }
}
