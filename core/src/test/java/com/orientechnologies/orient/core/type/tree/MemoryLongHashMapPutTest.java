package com.orientechnologies.orient.core.type.tree;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import junit.framework.Assert;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 24.06.12
 */
@Test
public class MemoryLongHashMapPutTest {
  private OMemory            memory;
  private OMemoryLongHashMap memoryLongHashMap;

  @BeforeMethod
  public void setUp() {
    memory = new OBuddyMemory(4000000, 32);
    memoryLongHashMap = new OMemoryLongHashMap(memory, 0.8f, 4);
  }

  public void testAddOneItem() {
    final int clusterId = 1;

    memoryLongHashMap.put(clusterId, 10);
    Assert.assertEquals(10, memoryLongHashMap.get(clusterId));
    Assert.assertEquals(1, memoryLongHashMap.size());
  }

  public void testAddTwoItems() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 4;

    memoryLongHashMap.put(clusterIdOne, 10);
    memoryLongHashMap.put(clusterIdTwo, 20);

    Assert.assertEquals(10, memoryLongHashMap.get(clusterIdOne));
    Assert.assertEquals(20, memoryLongHashMap.get(clusterIdTwo));
    Assert.assertEquals(2, memoryLongHashMap.size());
  }

  public void testAddThreeItems() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 1;
    final int clusterIdThree = 4;

    memoryLongHashMap.put(clusterIdOne, 10);
    memoryLongHashMap.put(clusterIdTwo, 20);
    memoryLongHashMap.put(clusterIdThree, 30);

    Assert.assertEquals(10, memoryLongHashMap.get(clusterIdOne));
    Assert.assertEquals(20, memoryLongHashMap.get(clusterIdTwo));
    Assert.assertEquals(30, memoryLongHashMap.get(clusterIdThree));
    Assert.assertEquals(3, memoryLongHashMap.size());
  }

  public void testAddFourItems() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 1;
    final int clusterIdThree = 4;
    final int clusterIdFour = 2;

    memoryLongHashMap.put(clusterIdOne, 10);
    memoryLongHashMap.put(clusterIdTwo, 20);
    memoryLongHashMap.put(clusterIdThree, 30);
    memoryLongHashMap.put(clusterIdFour, 40);

    Assert.assertEquals(10, memoryLongHashMap.get(clusterIdOne));
    Assert.assertEquals(20, memoryLongHashMap.get(clusterIdTwo));
    Assert.assertEquals(30, memoryLongHashMap.get(clusterIdThree));
    Assert.assertEquals(40, memoryLongHashMap.get(clusterIdFour));
    Assert.assertEquals(4, memoryLongHashMap.size());
  }

  public void testAddThreeItemsUpdateOne() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 1;
    final int clusterIdThree = 4;

    memoryLongHashMap.put(clusterIdOne, 10);
    memoryLongHashMap.put(clusterIdTwo, 20);
    memoryLongHashMap.put(clusterIdThree, 30);

    memoryLongHashMap.put(clusterIdOne, 40);

    Assert.assertEquals(40, memoryLongHashMap.get(clusterIdOne));
    Assert.assertEquals(20, memoryLongHashMap.get(clusterIdTwo));
    Assert.assertEquals(30, memoryLongHashMap.get(clusterIdThree));
    Assert.assertEquals(3, memoryLongHashMap.size());
  }

  public void testAddFourItemsUpdateTwo() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 1;
    final int clusterIdThree = 4;
    final int clusterIdFour = 2;

    memoryLongHashMap.put(clusterIdOne, 10);
    memoryLongHashMap.put(clusterIdTwo, 20);
    memoryLongHashMap.put(clusterIdThree, 30);
    memoryLongHashMap.put(clusterIdFour, 40);

    memoryLongHashMap.put(clusterIdTwo, 50);
    memoryLongHashMap.put(clusterIdOne, 60);

    Assert.assertEquals(60, memoryLongHashMap.get(clusterIdOne));
    Assert.assertEquals(50, memoryLongHashMap.get(clusterIdTwo));
    Assert.assertEquals(30, memoryLongHashMap.get(clusterIdThree));
    Assert.assertEquals(40, memoryLongHashMap.get(clusterIdFour));
    Assert.assertEquals(4, memoryLongHashMap.size());
  }

  public void testAdd10000RandomItems() {
    final Map<Integer, Integer> addedItems = new HashMap<Integer, Integer>();
    final Random random = new Random();

    for (int i = 0; i < 10000; i++) {
      int clusterId = random.nextInt(32767);
      while (addedItems.containsKey(clusterId))
        clusterId = random.nextInt(32767);

      final int pointer = random.nextInt();

      memoryLongHashMap.put(clusterId, pointer);
      addedItems.put(clusterId, pointer);
    }

    for (Map.Entry<Integer, Integer> addedItem : addedItems.entrySet())
      Assert.assertEquals(addedItem.getValue().intValue(), memoryLongHashMap.get(addedItem.getKey()));

    Assert.assertEquals(10000, memoryLongHashMap.size());
  }

  public void testAdd10000RandomItemsUpdateHalf() {
    final Map<Integer, Integer> addedItems = new HashMap<Integer, Integer>();
    final Random random = new Random();

    for (int i = 0; i < 10000; i++) {
      int clusterId = random.nextInt(32767);
      while (addedItems.containsKey(clusterId))
        clusterId = random.nextInt(32767);

      final int pointer = random.nextInt();

      memoryLongHashMap.put(clusterId, pointer);

      if (random.nextInt(2) > 0) {
        memoryLongHashMap.put(clusterId, pointer / 2);
        addedItems.put(clusterId, pointer / 2);
      } else
        addedItems.put(clusterId, pointer);
    }

    for (Map.Entry<Integer, Integer> addedItem : addedItems.entrySet())
      Assert.assertEquals(addedItem.getValue().intValue(), memoryLongHashMap.get(addedItem.getKey()));

    Assert.assertEquals(10000, memoryLongHashMap.size());
  }
}
