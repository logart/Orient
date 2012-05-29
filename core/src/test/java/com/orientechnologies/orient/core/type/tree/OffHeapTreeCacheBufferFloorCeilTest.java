package com.orientechnologies.orient.core.type.tree;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.ODoubleSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class OffHeapTreeCacheBufferFloorCeilTest {
  private OffHeapMemory memory = new OffHeapMemory(2000000, 20);
  private OffHeapTreeCacheBuffer<Double> treeCacheBuffer;

  @BeforeMethod
  public void setUp() {
    treeCacheBuffer =
            new OffHeapTreeCacheBuffer<Double>(memory, ODoubleSerializer.INSTANCE);
    for(double i = 0; i < 10; i++)
      treeCacheBuffer.add(createCacheEntry(i));
  }

  @AfterMethod
  public void tearDown() {
    memory.clear();
  }

  @Test
  public void testFloorItem() {
    OffHeapTreeCacheBuffer.CacheEntry<Double> cacheEntry = treeCacheBuffer.getFloor(4.0);
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

  private OffHeapTreeCacheBuffer.CacheEntry<Double> createCacheEntry(double key) {
    return new OffHeapTreeCacheBuffer.CacheEntry<Double>(key, 1.0, new ORecordId(1, 1),
            new ORecordId(1, 2), new ORecordId(1, 3), new ORecordId(1, 4));
  }
}
