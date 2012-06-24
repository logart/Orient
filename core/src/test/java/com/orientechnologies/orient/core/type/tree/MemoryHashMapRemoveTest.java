package com.orientechnologies.orient.core.type.tree;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import junit.framework.Assert;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 24.06.12
 */
@Test
public class MemoryHashMapRemoveTest {
  private OMemory        memory;
  private OMemoryHashMap memoryHashMap;

  @BeforeMethod
  public void setUp() {
    memory = new OBuddyMemory(4000000, 32);
    memoryHashMap = new OMemoryHashMap(memory, 0.8f, 4);
  }

  public void testRemoveOneItem() {
    final ORID rid = new ORecordId(1, 1);

    final int freeSpaceBefore = memory.freeSpace();

    memoryHashMap.put(rid, 10);
    Assert.assertEquals(10, memoryHashMap.remove(rid));

    Assert.assertEquals(freeSpaceBefore, memory.freeSpace());
    Assert.assertEquals(0, memoryHashMap.size());
  }

  public void testRemoveTwoItems() {
    final ORID ridOne = new ORecordId(1, 0);
    final ORID ridTwo = new ORecordId(1, 4);

    final int freeSpaceBefore = memory.freeSpace();

    memoryHashMap.put(ridOne, 10);
    memoryHashMap.put(ridTwo, 20);

    Assert.assertEquals(10, memoryHashMap.remove(ridOne));
    Assert.assertEquals(20, memoryHashMap.remove(ridTwo));

    Assert.assertEquals(freeSpaceBefore, memory.freeSpace());
    Assert.assertEquals(0, memoryHashMap.size());
  }

  public void testRemoveThreeItems() {
    final ORID ridOne = new ORecordId(1, 0);
    final ORID ridTwo = new ORecordId(1, 1);
    final ORID ridThree = new ORecordId(1, 4);

    memoryHashMap.put(ridOne, 10);
    memoryHashMap.put(ridTwo, 20);
    memoryHashMap.put(ridThree, 30);

    Assert.assertEquals(10, memoryHashMap.remove(ridOne));
    Assert.assertEquals(20, memoryHashMap.remove(ridTwo));
    Assert.assertEquals(30, memoryHashMap.remove(ridThree));

    Assert.assertEquals(0, memoryHashMap.size());
  }

  public void testRemoveFourItems() {
    final ORID ridOne = new ORecordId(1, 0);
    final ORID ridTwo = new ORecordId(1, 1);
    final ORID ridThree = new ORecordId(1, 4);
    final ORID ridFour = new ORecordId(1, 2);

    memoryHashMap.put(ridOne, 10);
    memoryHashMap.put(ridTwo, 20);
    memoryHashMap.put(ridThree, 30);
    memoryHashMap.put(ridFour, 40);

    Assert.assertEquals(10, memoryHashMap.remove(ridOne));
    Assert.assertEquals(20, memoryHashMap.remove(ridTwo));
    Assert.assertEquals(30, memoryHashMap.remove(ridThree));
    Assert.assertEquals(40, memoryHashMap.remove(ridFour));

    Assert.assertEquals(0, memoryHashMap.size());
  }

  public void testAddThreeItemsRemoveOne() {
    final ORID ridOne = new ORecordId(1, 0);
    final ORID ridTwo = new ORecordId(1, 1);
    final ORID ridThree = new ORecordId(1, 4);

    memoryHashMap.put(ridOne, 10);
    memoryHashMap.put(ridTwo, 20);
    memoryHashMap.put(ridThree, 30);

    Assert.assertEquals(10, memoryHashMap.remove(ridOne));

    Assert.assertEquals(20, memoryHashMap.get(ridTwo));
    Assert.assertEquals(30, memoryHashMap.get(ridThree));

    Assert.assertEquals(2, memoryHashMap.size());
  }

  public void testAddFourItemsRemoveTwo() {
    final ORID ridOne = new ORecordId(1, 0);
    final ORID ridTwo = new ORecordId(1, 1);
    final ORID ridThree = new ORecordId(1, 4);
    final ORID ridFour = new ORecordId(1, 2);

    memoryHashMap.put(ridOne, 10);
    memoryHashMap.put(ridTwo, 20);
    memoryHashMap.put(ridThree, 30);
    memoryHashMap.put(ridFour, 40);

    Assert.assertEquals(20, memoryHashMap.remove(ridTwo));
    Assert.assertEquals(10, memoryHashMap.remove(ridOne));

    Assert.assertEquals(30, memoryHashMap.get(ridThree));
    Assert.assertEquals(40, memoryHashMap.get(ridFour));

    Assert.assertEquals(2, memoryHashMap.size());
  }

  public void testRemove10000RandomItems() {
    final Map<ORID, Integer> addedItems = new HashMap<ORID, Integer>();
    final Random random = new Random();

    for (int i = 0; i < 10000; i++) {
      final ORID rid = new ORecordId(random.nextInt(32767), random.nextLong());
      final int pointer = random.nextInt();

      memoryHashMap.put(rid, pointer);
      addedItems.put(rid, pointer);
    }

    for (Map.Entry<ORID, Integer> addedItem : addedItems.entrySet())
      Assert.assertEquals(addedItem.getValue().intValue(), memoryHashMap.remove(addedItem.getKey()));

    Assert.assertEquals(0, memoryHashMap.size());
  }

  public void testAdd10000RandomItemsRemoveHalf() {
    final Map<ORID, Integer> addedItems = new HashMap<ORID, Integer>();
    final Random random = new Random();

    for (int i = 0; i < 10000; i++) {
      final ORID rid = new ORecordId(random.nextInt(32767), random.nextLong());
      final int pointer = random.nextInt();

      memoryHashMap.put(rid, pointer);

      if (random.nextInt(2) > 0)
        Assert.assertEquals(pointer, memoryHashMap.remove(rid));
      else
        addedItems.put(rid, pointer);
    }

    for (Map.Entry<ORID, Integer> addedItem : addedItems.entrySet())
      Assert.assertEquals(addedItem.getValue().intValue(), memoryHashMap.get(addedItem.getKey()));

    Assert.assertEquals(addedItems.size(), memoryHashMap.size());
  }
}
