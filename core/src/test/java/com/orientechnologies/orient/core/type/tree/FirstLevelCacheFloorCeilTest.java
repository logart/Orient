package com.orientechnologies.orient.core.type.tree;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.ODoubleSerializer;

@Test
public class FirstLevelCacheFloorCeilTest {
  private OMemory                        memory = new OBuddyMemory(4000000, 20);
  private OMemoryFirstLevelCache<Double> firstLevelCache;

  @BeforeMethod
  public void setUp() {
    firstLevelCache = new OMemoryFirstLevelCache<Double>(memory, ODoubleSerializer.INSTANCE);
    for (double i = 0; i < 10; i++)
      firstLevelCache.add(createCacheEntry(i));
  }

  @AfterMethod
  public void tearDown() {
    memory.clear();
  }

  @Test
  public void testFloorItem() {
    OMemoryFirstLevelCache.CacheEntry<Double> cacheEntry = firstLevelCache.getFloor(4.0);
    Assert.assertNotNull(cacheEntry);
    Assert.assertEquals(cacheEntry, createCacheEntry(4.0));

    cacheEntry = firstLevelCache.getFloor(4.5);
    Assert.assertNotNull(cacheEntry);
    Assert.assertEquals(cacheEntry, createCacheEntry(4.0));

    cacheEntry = firstLevelCache.getFloor(11.0);
    Assert.assertNotNull(cacheEntry);
    Assert.assertEquals(cacheEntry, createCacheEntry(9.0));

    cacheEntry = firstLevelCache.getFloor(-1.0);
    Assert.assertNull(cacheEntry);
  }

  @Test
  public void testCeilingItem() {
    OMemoryFirstLevelCache.CacheEntry<Double> cacheEntry = firstLevelCache.getCeiling(4.0);
    Assert.assertNotNull(cacheEntry);
    Assert.assertEquals(cacheEntry, createCacheEntry(4.0));

    cacheEntry = firstLevelCache.getCeiling(4.5);
    Assert.assertNotNull(cacheEntry);
    Assert.assertEquals(cacheEntry, createCacheEntry(5.0));

    cacheEntry = firstLevelCache.getCeiling(-1.0);
    Assert.assertNotNull(cacheEntry);
    Assert.assertEquals(cacheEntry, createCacheEntry(0.0));

    cacheEntry = firstLevelCache.getCeiling(10.0);
    Assert.assertNull(cacheEntry);
  }

  private OMemoryFirstLevelCache.CacheEntry<Double> createCacheEntry(double key) {
    return new OMemoryFirstLevelCache.CacheEntry<Double>(key, 1.0, new ORecordId(1, 1), new ORecordId(1, 2), new ORecordId(1, 3),
        new ORecordId(1, 4));
  }
}
