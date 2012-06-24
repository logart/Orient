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

@Test
public class FirstLevelCacheUpdateTest {
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
  public void testUpdateOneItem() {
    firstLevelCache.add(createCacheEntry(1));
    boolean result = firstLevelCache.update(createUpdateCacheEntry(1));

    Assert.assertTrue(result);
    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntry = firstLevelCache.get(1);
    Assert.assertNotNull(cacheEntry);
    Assert.assertEquals(cacheEntry, createUpdateCacheEntry(1));

    firstLevelCache.remove(1);

    Assert.assertEquals(memory.freeSpace(), memory.capacity());
  }

  @Test
  public void testUpdateTwoOrderedItems() {
    firstLevelCache.add(createCacheEntry(1));
    firstLevelCache.add(createCacheEntry(2));

    boolean resultOne = firstLevelCache.update(createUpdateCacheEntry(1));
    Assert.assertTrue(resultOne);

    boolean resultTwo = firstLevelCache.update(createUpdateCacheEntry(2));
    Assert.assertTrue(resultTwo);

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryOne = firstLevelCache.get(1);
    Assert.assertNotNull(cacheEntryOne);
    Assert.assertEquals(cacheEntryOne, createUpdateCacheEntry(1));

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryTwo = firstLevelCache.get(2);
    Assert.assertNotNull(cacheEntryTwo);
    Assert.assertEquals(cacheEntryTwo, createUpdateCacheEntry(2));

    firstLevelCache.remove(1);
    firstLevelCache.remove(2);

    Assert.assertEquals(memory.freeSpace(), memory.capacity());
  }

  @Test
  public void testUpdateTwoReverseOrderedItems() {
    firstLevelCache.add(createCacheEntry(1));
    firstLevelCache.add(createCacheEntry(2));

    boolean resultOne = firstLevelCache.update(createUpdateCacheEntry(2));
    Assert.assertTrue(resultOne);

    boolean resultTwo = firstLevelCache.update(createUpdateCacheEntry(1));
    Assert.assertTrue(resultTwo);

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryOne = firstLevelCache.get(1);
    Assert.assertNotNull(cacheEntryOne);
    Assert.assertEquals(cacheEntryOne, createUpdateCacheEntry(1));

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryTwo = firstLevelCache.get(2);
    Assert.assertNotNull(cacheEntryTwo);
    Assert.assertEquals(cacheEntryTwo, createUpdateCacheEntry(2));

    firstLevelCache.remove(1);
    firstLevelCache.remove(2);

    Assert.assertEquals(memory.freeSpace(), memory.capacity());
  }

  @Test
  public void testUpdateThreeOrderedItems() {
    firstLevelCache.add(createCacheEntry(1));
    firstLevelCache.add(createCacheEntry(2));
    firstLevelCache.add(createCacheEntry(3));

    boolean resultOne = firstLevelCache.update(createUpdateCacheEntry(1));
    Assert.assertTrue(resultOne);

    boolean resultTwo = firstLevelCache.update(createUpdateCacheEntry(2));
    Assert.assertTrue(resultTwo);

    boolean resultThree = firstLevelCache.update(createUpdateCacheEntry(3));
    Assert.assertTrue(resultThree);

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryOne = firstLevelCache.get(1);
    Assert.assertNotNull(cacheEntryOne);
    Assert.assertEquals(cacheEntryOne, createUpdateCacheEntry(1));

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryTwo = firstLevelCache.get(2);
    Assert.assertNotNull(cacheEntryTwo);
    Assert.assertEquals(cacheEntryTwo, createUpdateCacheEntry(2));

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryThree = firstLevelCache.get(3);
    Assert.assertNotNull(cacheEntryThree);
    Assert.assertEquals(cacheEntryThree, createUpdateCacheEntry(3));

    firstLevelCache.remove(1);
    firstLevelCache.remove(2);
    firstLevelCache.remove(3);

    Assert.assertEquals(memory.freeSpace(), memory.capacity());
  }

  @Test
  public void testUpdateThreeReverseOrderedItems() {
    firstLevelCache.add(createCacheEntry(1));
    firstLevelCache.add(createCacheEntry(2));
    firstLevelCache.add(createCacheEntry(3));

    boolean resultOne = firstLevelCache.update(createUpdateCacheEntry(3));
    Assert.assertTrue(resultOne);

    boolean resultTwo = firstLevelCache.update(createUpdateCacheEntry(2));
    Assert.assertTrue(resultTwo);

    boolean resultThree = firstLevelCache.update(createUpdateCacheEntry(1));
    Assert.assertTrue(resultThree);

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryOne = firstLevelCache.get(1);
    Assert.assertNotNull(cacheEntryOne);
    Assert.assertEquals(cacheEntryOne, createUpdateCacheEntry(1));

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryTwo = firstLevelCache.get(2);
    Assert.assertNotNull(cacheEntryTwo);
    Assert.assertEquals(cacheEntryTwo, createUpdateCacheEntry(2));

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryThree = firstLevelCache.get(3);
    Assert.assertNotNull(cacheEntryThree);
    Assert.assertEquals(cacheEntryThree, createUpdateCacheEntry(3));

    firstLevelCache.remove(1);
    firstLevelCache.remove(2);
    firstLevelCache.remove(3);

    Assert.assertEquals(memory.freeSpace(), memory.capacity());
  }

  @Test
  public void testUpdateThreeNonOrderedItems() {
    firstLevelCache.add(createCacheEntry(1));
    firstLevelCache.add(createCacheEntry(2));
    firstLevelCache.add(createCacheEntry(3));

    boolean resultOne = firstLevelCache.update(createUpdateCacheEntry(3));
    Assert.assertTrue(resultOne);

    boolean resultTwo = firstLevelCache.update(createUpdateCacheEntry(1));
    Assert.assertTrue(resultTwo);

    boolean resultThree = firstLevelCache.update(createUpdateCacheEntry(2));
    Assert.assertTrue(resultThree);

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryOne = firstLevelCache.get(1);
    Assert.assertNotNull(cacheEntryOne);
    Assert.assertEquals(cacheEntryOne, createUpdateCacheEntry(1));

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryTwo = firstLevelCache.get(2);
    Assert.assertNotNull(cacheEntryTwo);
    Assert.assertEquals(cacheEntryTwo, createUpdateCacheEntry(2));

    final OMemoryFirstLevelCache.CacheEntry<java.lang.Integer> cacheEntryThree = firstLevelCache.get(3);
    Assert.assertNotNull(cacheEntryThree);
    Assert.assertEquals(cacheEntryThree, createUpdateCacheEntry(3));

    firstLevelCache.remove(1);
    firstLevelCache.remove(2);
    firstLevelCache.remove(3);

    Assert.assertEquals(memory.freeSpace(), memory.capacity());
  }

  @Test
  public void testUpdate10000NonOrderedItems() {
    Set<Integer> addedKeys = new HashSet<Integer>();
    Random random = new Random();
    for (int i = 0; i < 10000; i++) {
      int key = random.nextInt();
      while (addedKeys.contains(key))
        key = random.nextInt();

      firstLevelCache.add(createCacheEntry(key));

      addedKeys.add(key);
    }

    for (int key : addedKeys) {
      boolean result = firstLevelCache.update(createUpdateCacheEntry(key));
      Assert.assertTrue(result);
    }

    for (int key : addedKeys) {
      OMemoryFirstLevelCache.CacheEntry<Integer> cacheEntry = firstLevelCache.get(key);
      Assert.assertEquals(cacheEntry, createUpdateCacheEntry(key));
    }

    for (int key : addedKeys) {
      firstLevelCache.remove(key);
    }
    Assert.assertEquals(memory.freeSpace(), memory.capacity());
  }

  private OMemoryFirstLevelCache.CacheEntry<Integer> createCacheEntry(int key) {
    return new OMemoryFirstLevelCache.CacheEntry<Integer>(key, 1, new ORecordId(1, 1), new ORecordId(1, 2), new ORecordId(1, 3),
        new ORecordId(1, 4));
  }

  private OMemoryFirstLevelCache.CacheEntry<Integer> createUpdateCacheEntry(int key) {
    return new OMemoryFirstLevelCache.CacheEntry<Integer>(key, 2, new ORecordId(2, 1), new ORecordId(2, 2), new ORecordId(2, 3),
        new ORecordId(2, 4));
  }
}
