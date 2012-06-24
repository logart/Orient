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
public class MemoryHashMapPutTest {
  private OMemory        memory;
  private OMemoryHashMap memoryHashMap;

  @BeforeMethod
  public void setUp() {
    memory = new OBuddyMemory(4000000, 32);
    memoryHashMap = new OMemoryHashMap(memory, 0.8f, 4);
  }

  public void testAddOneItem() {
    final ORID rid = new ORecordId(1, 1);

    memoryHashMap.put(rid, 10);
    Assert.assertEquals(10, memoryHashMap.get(rid));
    Assert.assertEquals(1, memoryHashMap.size());
  }

  public void testAddTwoItems() {
    final ORID ridOne = new ORecordId(1, 0);
    final ORID ridTwo = new ORecordId(1, 4);

    memoryHashMap.put(ridOne, 10);
    memoryHashMap.put(ridTwo, 20);

    Assert.assertEquals(10, memoryHashMap.get(ridOne));
    Assert.assertEquals(20, memoryHashMap.get(ridTwo));
    Assert.assertEquals(2, memoryHashMap.size());
  }

  public void testAddThreeItems() {
    final ORID ridOne = new ORecordId(1, 0);
    final ORID ridTwo = new ORecordId(1, 1);
    final ORID ridThree = new ORecordId(1, 4);

    memoryHashMap.put(ridOne, 10);
    memoryHashMap.put(ridTwo, 20);
    memoryHashMap.put(ridThree, 30);

    Assert.assertEquals(10, memoryHashMap.get(ridOne));
    Assert.assertEquals(20, memoryHashMap.get(ridTwo));
    Assert.assertEquals(30, memoryHashMap.get(ridThree));
    Assert.assertEquals(3, memoryHashMap.size());
  }

  public void testAddFourItems() {
    final ORID ridOne = new ORecordId(1, 0);
    final ORID ridTwo = new ORecordId(1, 1);
    final ORID ridThree = new ORecordId(1, 4);
    final ORID ridFour = new ORecordId(1, 2);

    memoryHashMap.put(ridOne, 10);
    memoryHashMap.put(ridTwo, 20);
    memoryHashMap.put(ridThree, 30);
    memoryHashMap.put(ridFour, 40);

    Assert.assertEquals(10, memoryHashMap.get(ridOne));
    Assert.assertEquals(20, memoryHashMap.get(ridTwo));
    Assert.assertEquals(30, memoryHashMap.get(ridThree));
    Assert.assertEquals(40, memoryHashMap.get(ridFour));
    Assert.assertEquals(4, memoryHashMap.size());
  }

  public void testAddThreeItemsUpdateOne() {
    final ORID ridOne = new ORecordId(1, 0);
    final ORID ridTwo = new ORecordId(1, 1);
    final ORID ridThree = new ORecordId(1, 4);

    memoryHashMap.put(ridOne, 10);
    memoryHashMap.put(ridTwo, 20);
    memoryHashMap.put(ridThree, 30);

    memoryHashMap.put(ridOne, 40);

    Assert.assertEquals(40, memoryHashMap.get(ridOne));
    Assert.assertEquals(20, memoryHashMap.get(ridTwo));
    Assert.assertEquals(30, memoryHashMap.get(ridThree));
    Assert.assertEquals(3, memoryHashMap.size());
  }

  public void testAddFourItemsUpdateTwo() {
    final ORID ridOne = new ORecordId(1, 0);
    final ORID ridTwo = new ORecordId(1, 1);
    final ORID ridThree = new ORecordId(1, 4);
    final ORID ridFour = new ORecordId(1, 2);

    memoryHashMap.put(ridOne, 10);
    memoryHashMap.put(ridTwo, 20);
    memoryHashMap.put(ridThree, 30);
    memoryHashMap.put(ridFour, 40);

    memoryHashMap.put(ridTwo, 50);
    memoryHashMap.put(ridOne, 60);

    Assert.assertEquals(60, memoryHashMap.get(ridOne));
    Assert.assertEquals(50, memoryHashMap.get(ridTwo));
    Assert.assertEquals(30, memoryHashMap.get(ridThree));
    Assert.assertEquals(40, memoryHashMap.get(ridFour));
    Assert.assertEquals(4, memoryHashMap.size());
  }

  public void testAdd10000RandomItems() {
    final Map<ORID, Integer> addedItems = new HashMap<ORID, Integer>();
    final Random random = new Random();

    for (int i = 0; i < 10000; i++) {
      final ORID rid = new ORecordId(random.nextInt(32767), random.nextLong());
      final int pointer = random.nextInt();

      memoryHashMap.put(rid, pointer);
      addedItems.put(rid, pointer);
    }

    for (Map.Entry<ORID, Integer> addedItem : addedItems.entrySet())
      Assert.assertEquals(addedItem.getValue().intValue(), memoryHashMap.get(addedItem.getKey()));

    Assert.assertEquals(10000, memoryHashMap.size());
  }

  public void testAdd10000RandomItemsUpdateHalf() {
    final Map<ORID, Integer> addedItems = new HashMap<ORID, Integer>();
    final Random random = new Random();

    for (int i = 0; i < 10000; i++) {
      final ORID rid = new ORecordId(random.nextInt(32767), random.nextLong());
      final int pointer = random.nextInt();

      memoryHashMap.put(rid, pointer);

      if (random.nextInt(2) > 0) {
        memoryHashMap.put(rid, pointer / 2);
        addedItems.put(rid, pointer / 2);
      } else
        addedItems.put(rid, pointer);
    }

    for (Map.Entry<ORID, Integer> addedItem : addedItems.entrySet())
      Assert.assertEquals(addedItem.getValue().intValue(), memoryHashMap.get(addedItem.getKey()));

    Assert.assertEquals(10000, memoryHashMap.size());
  }
}
