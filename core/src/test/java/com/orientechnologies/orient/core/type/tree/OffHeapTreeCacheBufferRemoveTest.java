package com.orientechnologies.orient.core.type.tree;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

@Test
public class OffHeapTreeCacheBufferRemoveTest {
  private OOffHeapMemory memory = new OOffHeapMemory(2000000, 20);
  private OOffHeapTreeCacheBuffer<Integer> treeCacheBuffer;

  @BeforeMethod
  public void setUp() {
    treeCacheBuffer =
            new OOffHeapTreeCacheBuffer<Integer>(memory, OIntegerSerializer.INSTANCE);
  }

  @Test
  public void testRemoveOneItem() {
    treeCacheBuffer.add(createCacheEntry(1));
    Assert.assertEquals(treeCacheBuffer.size(), 1);

    Assert.assertEquals(treeCacheBuffer.remove(1), createCacheEntry(1));
    Assert.assertEquals(treeCacheBuffer.size(), 0);
    Assert.assertNull(treeCacheBuffer.get(1));

    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

  @Test
  public void testRemoveTwoOrderedItems() {
    treeCacheBuffer.add(createCacheEntry(1));
    treeCacheBuffer.add(createCacheEntry(2));

    Assert.assertEquals(treeCacheBuffer.remove(1), createCacheEntry(1));
    Assert.assertEquals(treeCacheBuffer.size(), 1);
    Assert.assertEquals(treeCacheBuffer.remove(2), createCacheEntry(2));
    Assert.assertEquals(treeCacheBuffer.size(), 0);

    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

  @Test
  public void testRemoveTwoReverseOrderedItems() {
    treeCacheBuffer.add(createCacheEntry(1));
    treeCacheBuffer.add(createCacheEntry(2));

    Assert.assertEquals(treeCacheBuffer.remove(2), createCacheEntry(2));
    Assert.assertEquals(treeCacheBuffer.size(), 1);
    Assert.assertEquals(treeCacheBuffer.remove(1), createCacheEntry(1));
    Assert.assertEquals(treeCacheBuffer.size(), 0);

    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

  @Test
  public void testRemoveThreeOrderedItems() {
    treeCacheBuffer.add(createCacheEntry(1));
    treeCacheBuffer.add(createCacheEntry(2));
    treeCacheBuffer.add(createCacheEntry(3));

    Assert.assertEquals(treeCacheBuffer.remove(1), createCacheEntry(1));
    Assert.assertEquals(treeCacheBuffer.size(), 2);
    Assert.assertEquals(treeCacheBuffer.remove(2), createCacheEntry(2));
    Assert.assertEquals(treeCacheBuffer.size(), 1);
    Assert.assertEquals(treeCacheBuffer.remove(3), createCacheEntry(3));
    Assert.assertEquals(treeCacheBuffer.size(), 0);

    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

  @Test
  public void testRemoveThreeReverseOrderedItems() {
    treeCacheBuffer.add(createCacheEntry(1));
    treeCacheBuffer.add(createCacheEntry(2));
    treeCacheBuffer.add(createCacheEntry(3));

    Assert.assertEquals(treeCacheBuffer.remove(3), createCacheEntry(3));
    Assert.assertEquals(treeCacheBuffer.size(), 2);
    Assert.assertEquals(treeCacheBuffer.remove(2), createCacheEntry(2));
    Assert.assertEquals(treeCacheBuffer.size(), 1);
    Assert.assertEquals(treeCacheBuffer.remove(1), createCacheEntry(1));
    Assert.assertEquals(treeCacheBuffer.size(), 0);

    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

  @Test
  public void testRemoveThreeNonOrderedItems() {
    treeCacheBuffer.add(createCacheEntry(1));
    treeCacheBuffer.add(createCacheEntry(2));
    treeCacheBuffer.add(createCacheEntry(3));

    Assert.assertEquals(treeCacheBuffer.remove(2), createCacheEntry(2));
    Assert.assertEquals(treeCacheBuffer.size(), 2);
    Assert.assertEquals(treeCacheBuffer.remove(3), createCacheEntry(3));
    Assert.assertEquals(treeCacheBuffer.size(), 1);
    Assert.assertEquals(treeCacheBuffer.remove(1), createCacheEntry(1));
    Assert.assertEquals(treeCacheBuffer.size(), 0);

    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

  @Test
  public void testRemove10000NonOrderedItems() {
    Set<Integer> addedKeys = new HashSet<Integer>();
    Random random = new Random();
    for(int i  = 0; i < 10000; i++) {
      int key = random.nextInt();
      while (addedKeys.contains(key))
        key = random.nextInt();

      treeCacheBuffer.add(createCacheEntry(key));
      addedKeys.add(key);
    }

    for(int key : addedKeys) {
      OOffHeapTreeCacheBuffer.CacheEntry<Integer> cacheEntry = treeCacheBuffer.remove(key);
      Assert.assertEquals(cacheEntry, createCacheEntry(key));
    }

    Assert.assertEquals(treeCacheBuffer.size(), 0);
    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

  private OOffHeapTreeCacheBuffer.CacheEntry<Integer> createCacheEntry(int key) {
    return new OOffHeapTreeCacheBuffer.CacheEntry<Integer>(key,1, new ORecordId(1, 1),
            new ORecordId(1, 2), new ORecordId(1, 3), new ORecordId(1, 4) );
  }
}
