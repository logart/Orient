package com.orientechnologies.orient.core.type.tree;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;

@Test
public class FirstLevelCacheRemoveTest {
  private OMemory                         memory = new OBuddyMemory(4000000, 20);
  private OMemoryFirstLevelCache<Integer> firstLevelCache;

  @BeforeMethod
  public void setUp() {
    firstLevelCache = new OMemoryFirstLevelCache<Integer>(memory, OIntegerSerializer.INSTANCE);
  }

  @Test
  public void testRemoveOneItem() {
    firstLevelCache.add(createCacheEntry(1));
    Assert.assertEquals(firstLevelCache.size(), 1);

    Assert.assertEquals(firstLevelCache.remove(1), createCacheEntry(1));
    Assert.assertEquals(firstLevelCache.size(), 0);
    Assert.assertNull(firstLevelCache.get(1));

    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

  @Test
  public void testRemoveTwoOrderedItems() {
    firstLevelCache.add(createCacheEntry(1));
    firstLevelCache.add(createCacheEntry(2));

    Assert.assertEquals(firstLevelCache.remove(1), createCacheEntry(1));
    Assert.assertEquals(firstLevelCache.size(), 1);
    Assert.assertEquals(firstLevelCache.remove(2), createCacheEntry(2));
    Assert.assertEquals(firstLevelCache.size(), 0);

    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

  @Test
  public void testRemoveTwoReverseOrderedItems() {
    firstLevelCache.add(createCacheEntry(1));
    firstLevelCache.add(createCacheEntry(2));

    Assert.assertEquals(firstLevelCache.remove(2), createCacheEntry(2));
    Assert.assertEquals(firstLevelCache.size(), 1);
    Assert.assertEquals(firstLevelCache.remove(1), createCacheEntry(1));
    Assert.assertEquals(firstLevelCache.size(), 0);

    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

  @Test
  public void testRemoveThreeOrderedItems() {
    firstLevelCache.add(createCacheEntry(1));
    firstLevelCache.add(createCacheEntry(2));
    firstLevelCache.add(createCacheEntry(3));

    Assert.assertEquals(firstLevelCache.remove(1), createCacheEntry(1));
    Assert.assertEquals(firstLevelCache.size(), 2);
    Assert.assertEquals(firstLevelCache.remove(2), createCacheEntry(2));
    Assert.assertEquals(firstLevelCache.size(), 1);
    Assert.assertEquals(firstLevelCache.remove(3), createCacheEntry(3));
    Assert.assertEquals(firstLevelCache.size(), 0);

    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

  @Test
  public void testRemoveThreeReverseOrderedItems() {
    firstLevelCache.add(createCacheEntry(1));
    firstLevelCache.add(createCacheEntry(2));
    firstLevelCache.add(createCacheEntry(3));

    Assert.assertEquals(firstLevelCache.remove(3), createCacheEntry(3));
    Assert.assertEquals(firstLevelCache.size(), 2);
    Assert.assertEquals(firstLevelCache.remove(2), createCacheEntry(2));
    Assert.assertEquals(firstLevelCache.size(), 1);
    Assert.assertEquals(firstLevelCache.remove(1), createCacheEntry(1));
    Assert.assertEquals(firstLevelCache.size(), 0);

    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

  @Test
  public void testRemoveThreeNonOrderedItems() {
    firstLevelCache.add(createCacheEntry(1));
    firstLevelCache.add(createCacheEntry(2));
    firstLevelCache.add(createCacheEntry(3));

    Assert.assertEquals(firstLevelCache.remove(2), createCacheEntry(2));
    Assert.assertEquals(firstLevelCache.size(), 2);
    Assert.assertEquals(firstLevelCache.remove(3), createCacheEntry(3));
    Assert.assertEquals(firstLevelCache.size(), 1);
    Assert.assertEquals(firstLevelCache.remove(1), createCacheEntry(1));
    Assert.assertEquals(firstLevelCache.size(), 0);

    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

  @Test
  public void testRemove10000NonOrderedItems() {
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
      OMemoryFirstLevelCache.CacheEntry<Integer> cacheEntry = firstLevelCache.remove(key);
      Assert.assertEquals(cacheEntry, createCacheEntry(key));
    }

    Assert.assertEquals(firstLevelCache.size(), 0);
    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

  private OMemoryFirstLevelCache.CacheEntry<Integer> createCacheEntry(int key) {
    return new OMemoryFirstLevelCache.CacheEntry<Integer>(key, 1, new ORecordId(1, 1), new ORecordId(1, 2), new ORecordId(1, 3),
        new ORecordId(1, 4));
  }
}
