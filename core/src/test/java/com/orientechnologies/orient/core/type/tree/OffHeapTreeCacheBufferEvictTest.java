package com.orientechnologies.orient.core.type.tree;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;
import junit.framework.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class OffHeapTreeCacheBufferEvictTest {
  private OOffHeapMemory memory = new OOffHeapMemory(2000000, 20);
  private OOffHeapTreeCacheBuffer<Integer> treeCacheBuffer;

  @BeforeMethod
  public void setUp() {
    treeCacheBuffer =
            new OOffHeapTreeCacheBuffer<Integer>(memory, OIntegerSerializer.INSTANCE);
  }

  @AfterMethod
  public void tearDown() {
    memory.clear();
  }

  public void testAdd100RemainLast7() {
    treeCacheBuffer.setEvictionSize(10);
    treeCacheBuffer.setDefaultEvictionPercent(30);

    for(int i = 0; i < 100; i++) {
      treeCacheBuffer.add(createCacheEntry(i));
    }

    Assert.assertEquals(7, treeCacheBuffer.size());
    for(int i = 99; i >= 93; i--)
     Assert.assertEquals( i + " -key is absent", createCacheEntry(i), treeCacheBuffer.remove(i));

    Assert.assertEquals(0, treeCacheBuffer.size());
    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

  public void testAdd100RemainLast7ReverseOrder() {
    treeCacheBuffer.setEvictionSize(10);
    treeCacheBuffer.setDefaultEvictionPercent(30);

    for(int j = 0; j < 2; j++) {
      for(int i = 0; i < 9; i++)
        treeCacheBuffer.add(createCacheEntry(j * 10 + i));

      for(int i = 0; i >=0; i--)
        treeCacheBuffer.get(j * 10 + i);

      treeCacheBuffer.add(createCacheEntry(j * 10 + 9));
    }

    Assert.assertEquals(7, treeCacheBuffer.size());
    for(int i = 90; i < 97; i++)
      Assert.assertEquals( i + " -key is absent", createCacheEntry(i), treeCacheBuffer.remove(i));

    Assert.assertEquals(0, treeCacheBuffer.size());
    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

  private OOffHeapTreeCacheBuffer.CacheEntry<Integer> createCacheEntry(int key) {
    return new OOffHeapTreeCacheBuffer.CacheEntry<Integer>(key,1, new ORecordId(1, 1),
            new ORecordId(1, 2), new ORecordId(1, 3), new ORecordId(1, 4) );
  }
}
