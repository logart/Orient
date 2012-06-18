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
public class OffHeapTreeCacheBufferUpdateTest {
  private OMemory                          memory = new OBuddyMemory(4000000, 20);
  private OOffHeapTreeCacheBuffer<Integer> treeCacheBuffer;

  @BeforeMethod
  public void setUp() {
    treeCacheBuffer = new OOffHeapTreeCacheBuffer<Integer>(memory, OIntegerSerializer.INSTANCE);
  }

  @AfterMethod
  public void tearDown() {
    memory.clear();
  }

  @Test
  public void testUpdateOneItem() {
    treeCacheBuffer.add(createCacheEntry(1));
    boolean result = treeCacheBuffer.update(createUpdateCacheEntry(1));

    Assert.assertTrue(result);
    final OOffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntry = treeCacheBuffer.get(1);
    Assert.assertNotNull(cacheEntry);
    Assert.assertEquals(cacheEntry, createUpdateCacheEntry(1));

    treeCacheBuffer.remove(1);

    Assert.assertEquals(memory.freeSpace(), memory.capacity());
  }

  @Test
  public void testUpdateTwoOrderedItems() {
    treeCacheBuffer.add(createCacheEntry(1));
    treeCacheBuffer.add(createCacheEntry(2));

    boolean resultOne = treeCacheBuffer.update(createUpdateCacheEntry(1));
    Assert.assertTrue(resultOne);

    boolean resultTwo = treeCacheBuffer.update(createUpdateCacheEntry(2));
    Assert.assertTrue(resultTwo);

    final OOffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryOne = treeCacheBuffer.get(1);
    Assert.assertNotNull(cacheEntryOne);
    Assert.assertEquals(cacheEntryOne, createUpdateCacheEntry(1));

    final OOffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryTwo = treeCacheBuffer.get(2);
    Assert.assertNotNull(cacheEntryTwo);
    Assert.assertEquals(cacheEntryTwo, createUpdateCacheEntry(2));

    treeCacheBuffer.remove(1);
    treeCacheBuffer.remove(2);

    Assert.assertEquals(memory.freeSpace(), memory.capacity());
  }

  @Test
  public void testUpdateTwoReverseOrderedItems() {
    treeCacheBuffer.add(createCacheEntry(1));
    treeCacheBuffer.add(createCacheEntry(2));

    boolean resultOne = treeCacheBuffer.update(createUpdateCacheEntry(2));
    Assert.assertTrue(resultOne);

    boolean resultTwo = treeCacheBuffer.update(createUpdateCacheEntry(1));
    Assert.assertTrue(resultTwo);

    final OOffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryOne = treeCacheBuffer.get(1);
    Assert.assertNotNull(cacheEntryOne);
    Assert.assertEquals(cacheEntryOne, createUpdateCacheEntry(1));

    final OOffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryTwo = treeCacheBuffer.get(2);
    Assert.assertNotNull(cacheEntryTwo);
    Assert.assertEquals(cacheEntryTwo, createUpdateCacheEntry(2));

    treeCacheBuffer.remove(1);
    treeCacheBuffer.remove(2);

    Assert.assertEquals(memory.freeSpace(), memory.capacity());
  }

  @Test
  public void testUpdateThreeOrderedItems() {
    treeCacheBuffer.add(createCacheEntry(1));
    treeCacheBuffer.add(createCacheEntry(2));
    treeCacheBuffer.add(createCacheEntry(3));

    boolean resultOne = treeCacheBuffer.update(createUpdateCacheEntry(1));
    Assert.assertTrue(resultOne);

    boolean resultTwo = treeCacheBuffer.update(createUpdateCacheEntry(2));
    Assert.assertTrue(resultTwo);

    boolean resultThree = treeCacheBuffer.update(createUpdateCacheEntry(3));
    Assert.assertTrue(resultThree);

    final OOffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryOne = treeCacheBuffer.get(1);
    Assert.assertNotNull(cacheEntryOne);
    Assert.assertEquals(cacheEntryOne, createUpdateCacheEntry(1));

    final OOffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryTwo = treeCacheBuffer.get(2);
    Assert.assertNotNull(cacheEntryTwo);
    Assert.assertEquals(cacheEntryTwo, createUpdateCacheEntry(2));

    final OOffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryThree = treeCacheBuffer.get(3);
    Assert.assertNotNull(cacheEntryThree);
    Assert.assertEquals(cacheEntryThree, createUpdateCacheEntry(3));

    treeCacheBuffer.remove(1);
    treeCacheBuffer.remove(2);
    treeCacheBuffer.remove(3);

    Assert.assertEquals(memory.freeSpace(), memory.capacity());
  }

  @Test
  public void testUpdateThreeReverseOrderedItems() {
    treeCacheBuffer.add(createCacheEntry(1));
    treeCacheBuffer.add(createCacheEntry(2));
    treeCacheBuffer.add(createCacheEntry(3));

    boolean resultOne = treeCacheBuffer.update(createUpdateCacheEntry(3));
    Assert.assertTrue(resultOne);

    boolean resultTwo = treeCacheBuffer.update(createUpdateCacheEntry(2));
    Assert.assertTrue(resultTwo);

    boolean resultThree = treeCacheBuffer.update(createUpdateCacheEntry(1));
    Assert.assertTrue(resultThree);

    final OOffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryOne = treeCacheBuffer.get(1);
    Assert.assertNotNull(cacheEntryOne);
    Assert.assertEquals(cacheEntryOne, createUpdateCacheEntry(1));

    final OOffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryTwo = treeCacheBuffer.get(2);
    Assert.assertNotNull(cacheEntryTwo);
    Assert.assertEquals(cacheEntryTwo, createUpdateCacheEntry(2));

    final OOffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryThree = treeCacheBuffer.get(3);
    Assert.assertNotNull(cacheEntryThree);
    Assert.assertEquals(cacheEntryThree, createUpdateCacheEntry(3));

    treeCacheBuffer.remove(1);
    treeCacheBuffer.remove(2);
    treeCacheBuffer.remove(3);

    Assert.assertEquals(memory.freeSpace(), memory.capacity());
  }

  @Test
  public void testUpdateThreeNonOrderedItems() {
    treeCacheBuffer.add(createCacheEntry(1));
    treeCacheBuffer.add(createCacheEntry(2));
    treeCacheBuffer.add(createCacheEntry(3));

    boolean resultOne = treeCacheBuffer.update(createUpdateCacheEntry(3));
    Assert.assertTrue(resultOne);

    boolean resultTwo = treeCacheBuffer.update(createUpdateCacheEntry(1));
    Assert.assertTrue(resultTwo);

    boolean resultThree = treeCacheBuffer.update(createUpdateCacheEntry(2));
    Assert.assertTrue(resultThree);

    final OOffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryOne = treeCacheBuffer.get(1);
    Assert.assertNotNull(cacheEntryOne);
    Assert.assertEquals(cacheEntryOne, createUpdateCacheEntry(1));

    final OOffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryTwo = treeCacheBuffer.get(2);
    Assert.assertNotNull(cacheEntryTwo);
    Assert.assertEquals(cacheEntryTwo, createUpdateCacheEntry(2));

    final OOffHeapTreeCacheBuffer.CacheEntry<java.lang.Integer> cacheEntryThree = treeCacheBuffer.get(3);
    Assert.assertNotNull(cacheEntryThree);
    Assert.assertEquals(cacheEntryThree, createUpdateCacheEntry(3));

    treeCacheBuffer.remove(1);
    treeCacheBuffer.remove(2);
    treeCacheBuffer.remove(3);

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

      treeCacheBuffer.add(createCacheEntry(key));

      addedKeys.add(key);
    }

    for (int key : addedKeys) {
      boolean result = treeCacheBuffer.update(createUpdateCacheEntry(key));
      Assert.assertTrue(result);
    }

    for (int key : addedKeys) {
      OOffHeapTreeCacheBuffer.CacheEntry<Integer> cacheEntry = treeCacheBuffer.get(key);
      Assert.assertEquals(cacheEntry, createUpdateCacheEntry(key));
    }

    for (int key : addedKeys) {
      treeCacheBuffer.remove(key);
    }
    Assert.assertEquals(memory.freeSpace(), memory.capacity());
  }

  private OOffHeapTreeCacheBuffer.CacheEntry<Integer> createCacheEntry(int key) {
    return new OOffHeapTreeCacheBuffer.CacheEntry<Integer>(key, 1, new ORecordId(1, 1), new ORecordId(1, 2), new ORecordId(1, 3),
        new ORecordId(1, 4));
  }

  private OOffHeapTreeCacheBuffer.CacheEntry<Integer> createUpdateCacheEntry(int key) {
    return new OOffHeapTreeCacheBuffer.CacheEntry<Integer>(key, 2, new ORecordId(2, 1), new ORecordId(2, 2), new ORecordId(2, 3),
        new ORecordId(2, 4));
  }
}
