package com.orientechnologies.orient.core.type.tree;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;

@Test
public class FirstLevelCacheEvictTest {
  private OMemory                         memory = new OBuddyMemory(2000000, 20);
  private OMemoryFirstLevelCache<Integer> firstLevelCache;

  @BeforeMethod
  public void setUp() {
    firstLevelCache = new OMemoryFirstLevelCache<Integer>(memory, OIntegerSerializer.INSTANCE);
  }

  @AfterMethod
  public void tearDown() {
    memory.clear();
  }

  public void testAdd100RemainLast7() {
    firstLevelCache.setEvictionSize(10);
    firstLevelCache.setDefaultEvictionPercent(30);

    for (int i = 0; i < 100; i++) {
      firstLevelCache.add(createCacheEntry(i));
    }

    Assert.assertEquals(7, firstLevelCache.size());
    for (int i = 99; i >= 93; i--)
      Assert.assertEquals(createCacheEntry(i), firstLevelCache.remove(i));

    Assert.assertEquals(0, firstLevelCache.size());
    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

  public void testAdd10RemainFirst6Get() {
    firstLevelCache.setEvictionSize(10);
    firstLevelCache.setDefaultEvictionPercent(30);

    for (int i = 0; i < 9; i++)
      firstLevelCache.add(createCacheEntry(i));

    for (int i = 0; i < 6; i++)
      firstLevelCache.get(i);

    firstLevelCache.add(createCacheEntry(9));

    Assert.assertEquals(7, firstLevelCache.size());
    for (int i = 5; i >= 0; i--)
      Assert.assertEquals(createCacheEntry(i), firstLevelCache.remove(i));

    Assert.assertEquals(createCacheEntry(9), firstLevelCache.remove(9));

    Assert.assertEquals(0, firstLevelCache.size());
    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

  public void testAdd100RemainFirst3Get() {
    firstLevelCache.setEvictionSize(10);
    firstLevelCache.setDefaultEvictionPercent(30);

    for (int n = 0; n < 10; n++) {
      for (int i = 0; i < 3; i++)
        firstLevelCache.add(createCacheEntry(n * 10 + i));

      for (int j = 0; j < 3; j++)
        firstLevelCache.get(j);

      for (int i = 3; i < 6; i++)
        firstLevelCache.add(createCacheEntry(n * 10 + i));

      for (int j = 0; j < 3; j++)
        firstLevelCache.get(j);

      for (int i = 6; i < 9; i++)
        firstLevelCache.add(createCacheEntry(n * 10 + i));

      for (int j = 0; j < 3; j++)
        firstLevelCache.get(j);

      firstLevelCache.add(createCacheEntry(n * 10 + 9));
    }

    Assert.assertEquals(7, firstLevelCache.size());

    for (int i = 0; i < 3; i++)
      Assert.assertEquals(createCacheEntry(i), firstLevelCache.remove(i));

    for (int i = 99; i >= 96; i--)
      Assert.assertEquals(createCacheEntry(i), firstLevelCache.remove(i));

    Assert.assertEquals(0, firstLevelCache.size());
    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

  public void testAdd10RemainFirst6GetCeiling() {
    firstLevelCache.setEvictionSize(10);
    firstLevelCache.setDefaultEvictionPercent(30);

    for (int i = 0; i < 9; i++)
      firstLevelCache.add(createCacheEntry(i));

    for (int i = 0; i < 6; i++)
      firstLevelCache.getCeiling(i);

    firstLevelCache.add(createCacheEntry(9));

    Assert.assertEquals(7, firstLevelCache.size());
    for (int i = 5; i >= 0; i--)
      Assert.assertEquals(createCacheEntry(i), firstLevelCache.remove(i));

    Assert.assertEquals(createCacheEntry(9), firstLevelCache.remove(9));

    Assert.assertEquals(0, firstLevelCache.size());
    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

  public void testAdd10RemainFirst6GetFloor() {
    firstLevelCache.setEvictionSize(10);
    firstLevelCache.setDefaultEvictionPercent(30);

    for (int i = 0; i < 9; i++)
      firstLevelCache.add(createCacheEntry(i));

    for (int i = 0; i < 6; i++)
      firstLevelCache.getFloor(i);

    firstLevelCache.add(createCacheEntry(9));

    Assert.assertEquals(7, firstLevelCache.size());
    for (int i = 5; i >= 0; i--)
      Assert.assertEquals(createCacheEntry(i), firstLevelCache.remove(i));

    Assert.assertEquals(createCacheEntry(9), firstLevelCache.remove(9));

    Assert.assertEquals(0, firstLevelCache.size());
    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

  public void testAdd10RemainFirst6Update() {
    firstLevelCache.setEvictionSize(10);
    firstLevelCache.setDefaultEvictionPercent(30);

    for (int i = 0; i < 9; i++)
      firstLevelCache.add(createCacheEntry(i));

    for (int i = 0; i < 6; i++)
      firstLevelCache.update(createUpdateCacheEntry(i));

    firstLevelCache.add(createCacheEntry(9));

    Assert.assertEquals(7, firstLevelCache.size());
    for (int i = 5; i >= 0; i--)
      Assert.assertEquals(createUpdateCacheEntry(i), firstLevelCache.remove(i));

    Assert.assertEquals(createCacheEntry(9), firstLevelCache.remove(9));

    Assert.assertEquals(0, firstLevelCache.size());
    Assert.assertEquals(memory.capacity(), memory.freeSpace());
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
