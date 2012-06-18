package com.orientechnologies.orient.core.type.tree;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.ODoubleSerializer;

@Test
public class OffHeapTreeCacheBufferFloorCeilTest {
  private OMemory                         memory = new OBuddyMemory(4000000, 20);
  private OOffHeapTreeCacheBuffer<Double> treeCacheBuffer;

  @BeforeMethod
  public void setUp() {
    treeCacheBuffer = new OOffHeapTreeCacheBuffer<Double>(memory, ODoubleSerializer.INSTANCE);
    for (double i = 0; i < 10; i++)
      treeCacheBuffer.add(createCacheEntry(i));
  }

  @AfterMethod
  public void tearDown() {
    memory.clear();
  }

  @Test
  public void testFloorItem() {
    OOffHeapTreeCacheBuffer.CacheEntry<Double> cacheEntry = treeCacheBuffer.getFloor(4.0);
    Assert.assertNotNull(cacheEntry);
    Assert.assertEquals(cacheEntry, createCacheEntry(4.0));

    cacheEntry = treeCacheBuffer.getFloor(4.5);
    Assert.assertNotNull(cacheEntry);
    Assert.assertEquals(cacheEntry, createCacheEntry(4.0));

    cacheEntry = treeCacheBuffer.getFloor(11.0);
    Assert.assertNotNull(cacheEntry);
    Assert.assertEquals(cacheEntry, createCacheEntry(9.0));

    cacheEntry = treeCacheBuffer.getFloor(-1.0);
    Assert.assertNull(cacheEntry);
  }

  @Test
  public void testCeilingItem() {
    OOffHeapTreeCacheBuffer.CacheEntry<Double> cacheEntry = treeCacheBuffer.getCeiling(4.0);
    Assert.assertNotNull(cacheEntry);
    Assert.assertEquals(cacheEntry, createCacheEntry(4.0));

    cacheEntry = treeCacheBuffer.getCeiling(4.5);
    Assert.assertNotNull(cacheEntry);
    Assert.assertEquals(cacheEntry, createCacheEntry(5.0));

    cacheEntry = treeCacheBuffer.getCeiling(-1.0);
    Assert.assertNotNull(cacheEntry);
    Assert.assertEquals(cacheEntry, createCacheEntry(0.0));

    cacheEntry = treeCacheBuffer.getCeiling(10.0);
    Assert.assertNull(cacheEntry);
  }

  private OOffHeapTreeCacheBuffer.CacheEntry<Double> createCacheEntry(double key) {
    return new OOffHeapTreeCacheBuffer.CacheEntry<Double>(key, 1.0, new ORecordId(1, 1), new ORecordId(1, 2), new ORecordId(1, 3),
        new ORecordId(1, 4));
  }
}
