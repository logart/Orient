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
public class MemoryIntHashMapPutTest {
  private OMemory           memory;
  private OMemoryIntHashMap memoryIntHashMap;

  @BeforeMethod
  public void setUp() {
    memory = new OBuddyMemory(4000000, 32);
    memoryIntHashMap = new OMemoryIntHashMap(memory, 0.8f, 4);
  }

  public void testAddOneItem() {
    final int clusterId = 1;

    memoryIntHashMap.put(clusterId, 10);
    Assert.assertEquals(10, memoryIntHashMap.get(clusterId));
    Assert.assertEquals(1, memoryIntHashMap.size());
  }

  public void testAddTwoItems() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 4;

    memoryIntHashMap.put(clusterIdOne, 10);
    memoryIntHashMap.put(clusterIdTwo, 20);

    Assert.assertEquals(10, memoryIntHashMap.get(clusterIdOne));
    Assert.assertEquals(20, memoryIntHashMap.get(clusterIdTwo));
    Assert.assertEquals(2, memoryIntHashMap.size());
  }

  public void testAddThreeItems() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 1;
    final int clusterIdThree = 4;

    memoryIntHashMap.put(clusterIdOne, 10);
    memoryIntHashMap.put(clusterIdTwo, 20);
    memoryIntHashMap.put(clusterIdThree, 30);

    Assert.assertEquals(10, memoryIntHashMap.get(clusterIdOne));
    Assert.assertEquals(20, memoryIntHashMap.get(clusterIdTwo));
    Assert.assertEquals(30, memoryIntHashMap.get(clusterIdThree));
    Assert.assertEquals(3, memoryIntHashMap.size());
  }

  public void testAddFourItems() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 1;
    final int clusterIdThree = 4;
    final int clusterIdFour = 2;

    memoryIntHashMap.put(clusterIdOne, 10);
    memoryIntHashMap.put(clusterIdTwo, 20);
    memoryIntHashMap.put(clusterIdThree, 30);
    memoryIntHashMap.put(clusterIdFour, 40);

    Assert.assertEquals(10, memoryIntHashMap.get(clusterIdOne));
    Assert.assertEquals(20, memoryIntHashMap.get(clusterIdTwo));
    Assert.assertEquals(30, memoryIntHashMap.get(clusterIdThree));
    Assert.assertEquals(40, memoryIntHashMap.get(clusterIdFour));
    Assert.assertEquals(4, memoryIntHashMap.size());
  }

  public void testAddThreeItemsUpdateOne() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 1;
    final int clusterIdThree = 4;

    memoryIntHashMap.put(clusterIdOne, 10);
    memoryIntHashMap.put(clusterIdTwo, 20);
    memoryIntHashMap.put(clusterIdThree, 30);

    memoryIntHashMap.put(clusterIdOne, 40);

    Assert.assertEquals(40, memoryIntHashMap.get(clusterIdOne));
    Assert.assertEquals(20, memoryIntHashMap.get(clusterIdTwo));
    Assert.assertEquals(30, memoryIntHashMap.get(clusterIdThree));
    Assert.assertEquals(3, memoryIntHashMap.size());
  }

  public void testAddFourItemsUpdateTwo() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 1;
    final int clusterIdThree = 4;
    final int clusterIdFour = 2;

    memoryIntHashMap.put(clusterIdOne, 10);
    memoryIntHashMap.put(clusterIdTwo, 20);
    memoryIntHashMap.put(clusterIdThree, 30);
    memoryIntHashMap.put(clusterIdFour, 40);

    memoryIntHashMap.put(clusterIdTwo, 50);
    memoryIntHashMap.put(clusterIdOne, 60);

    Assert.assertEquals(60, memoryIntHashMap.get(clusterIdOne));
    Assert.assertEquals(50, memoryIntHashMap.get(clusterIdTwo));
    Assert.assertEquals(30, memoryIntHashMap.get(clusterIdThree));
    Assert.assertEquals(40, memoryIntHashMap.get(clusterIdFour));
    Assert.assertEquals(4, memoryIntHashMap.size());
  }

  public void testAdd10000RandomItems() {
    final Map<Integer, Integer> addedItems = new HashMap<Integer, Integer>();
    final Random random = new Random();

    for (int i = 0; i < 10000; i++) {
      int clusterId = random.nextInt(32767);
      while (addedItems.containsKey(clusterId))
        clusterId = random.nextInt(32767);

      final int pointer = random.nextInt();

      memoryIntHashMap.put(clusterId, pointer);
      addedItems.put(clusterId, pointer);
    }

    for (Map.Entry<Integer, Integer> addedItem : addedItems.entrySet())
      Assert.assertEquals(addedItem.getValue().intValue(), memoryIntHashMap.get(addedItem.getKey()));

    Assert.assertEquals(10000, memoryIntHashMap.size());
  }

  public void testAdd10000RandomItemsUpdateHalf() {
    final Map<Integer, Integer> addedItems = new HashMap<Integer, Integer>();
    final Random random = new Random();

    for (int i = 0; i < 10000; i++) {
      int clusterId = random.nextInt(32767);
      while (addedItems.containsKey(clusterId))
        clusterId = random.nextInt(32767);

      final int pointer = random.nextInt();

      memoryIntHashMap.put(clusterId, pointer);

      if (random.nextInt(2) > 0) {
        memoryIntHashMap.put(clusterId, pointer / 2);
        addedItems.put(clusterId, pointer / 2);
      } else
        addedItems.put(clusterId, pointer);
    }

    for (Map.Entry<Integer, Integer> addedItem : addedItems.entrySet())
      Assert.assertEquals(addedItem.getValue().intValue(), memoryIntHashMap.get(addedItem.getKey()));

    Assert.assertEquals(10000, memoryIntHashMap.size());
  }
}
