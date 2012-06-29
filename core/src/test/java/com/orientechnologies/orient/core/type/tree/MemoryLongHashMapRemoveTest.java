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
public class MemoryLongHashMapRemoveTest {
  private OMemory            memory;
  private OMemoryLongHashMap memoryLongHashMap;

  @BeforeMethod
  public void setUp() {
    memory = new OBuddyMemory(4000000, 32);
    memoryLongHashMap = new OMemoryLongHashMap(memory, 0.8f, 4);
  }

  public void testRemoveOneItem() {
    final int clusterId = 1;

    final int freeSpaceBefore = memory.freeSpace();

    memoryLongHashMap.put(clusterId, 10);
    Assert.assertEquals(10, memoryLongHashMap.remove(clusterId));

    Assert.assertEquals(freeSpaceBefore, memory.freeSpace());
    Assert.assertEquals(0, memoryLongHashMap.size());
  }

  public void testRemoveTwoItems() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 4;

    final int freeSpaceBefore = memory.freeSpace();

    memoryLongHashMap.put(clusterIdOne, 10);
    memoryLongHashMap.put(clusterIdTwo, 20);

    Assert.assertEquals(10, memoryLongHashMap.remove(clusterIdOne));
    Assert.assertEquals(20, memoryLongHashMap.remove(clusterIdTwo));

    Assert.assertEquals(freeSpaceBefore, memory.freeSpace());
    Assert.assertEquals(0, memoryLongHashMap.size());
  }

  public void testRemoveThreeItems() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 1;
    final int clusterIdThree = 4;

    memoryLongHashMap.put(clusterIdOne, 10);
    memoryLongHashMap.put(clusterIdTwo, 20);
    memoryLongHashMap.put(clusterIdThree, 30);

    Assert.assertEquals(10, memoryLongHashMap.remove(clusterIdOne));
    Assert.assertEquals(20, memoryLongHashMap.remove(clusterIdTwo));
    Assert.assertEquals(30, memoryLongHashMap.remove(clusterIdThree));

    Assert.assertEquals(0, memoryLongHashMap.size());
  }

  public void testRemoveFourItems() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 1;
    final int clusterIdThree = 4;
    final int clusterIdFour = 2;

    memoryLongHashMap.put(clusterIdOne, 10);
    memoryLongHashMap.put(clusterIdTwo, 20);
    memoryLongHashMap.put(clusterIdThree, 30);
    memoryLongHashMap.put(clusterIdFour, 40);

    Assert.assertEquals(10, memoryLongHashMap.remove(clusterIdOne));
    Assert.assertEquals(20, memoryLongHashMap.remove(clusterIdTwo));
    Assert.assertEquals(30, memoryLongHashMap.remove(clusterIdThree));
    Assert.assertEquals(40, memoryLongHashMap.remove(clusterIdFour));

    Assert.assertEquals(0, memoryLongHashMap.size());
  }

  public void testAddThreeItemsRemoveOne() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 1;
    final int clusterIdThree = 4;

    memoryLongHashMap.put(clusterIdOne, 10);
    memoryLongHashMap.put(clusterIdTwo, 20);
    memoryLongHashMap.put(clusterIdThree, 30);

    Assert.assertEquals(10, memoryLongHashMap.remove(clusterIdOne));

    Assert.assertEquals(20, memoryLongHashMap.get(clusterIdTwo));
    Assert.assertEquals(30, memoryLongHashMap.get(clusterIdThree));

    Assert.assertEquals(2, memoryLongHashMap.size());
  }

  public void testAddFourItemsRemoveTwo() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 1;
    final int clusterIdThree = 4;
    final int clusterIdFour = 2;

    memoryLongHashMap.put(clusterIdOne, 10);
    memoryLongHashMap.put(clusterIdTwo, 20);
    memoryLongHashMap.put(clusterIdThree, 30);
    memoryLongHashMap.put(clusterIdFour, 40);

    Assert.assertEquals(20, memoryLongHashMap.remove(clusterIdTwo));
    Assert.assertEquals(10, memoryLongHashMap.remove(clusterIdOne));

    Assert.assertEquals(30, memoryLongHashMap.get(clusterIdThree));
    Assert.assertEquals(40, memoryLongHashMap.get(clusterIdFour));

    Assert.assertEquals(2, memoryLongHashMap.size());
  }

  public void testRemove10000RandomItems() {
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
      Assert.assertEquals(addedItem.getValue().intValue(), memoryLongHashMap.remove(addedItem.getKey()));

    Assert.assertEquals(0, memoryLongHashMap.size());
  }

  public void testAdd10000RandomItemsRemoveHalf() {
    final Map<Integer, Integer> addedItems = new HashMap<Integer, Integer>();
    final Random random = new Random();

    for (int i = 0; i < 10000; i++) {
      int clusterId = random.nextInt(32767);
      while (addedItems.containsKey(clusterId))
        clusterId = random.nextInt(32767);

      final int pointer = random.nextInt();

      memoryLongHashMap.put(clusterId, pointer);

      if (random.nextInt(2) > 0)
        Assert.assertEquals(pointer, memoryLongHashMap.remove(clusterId));
      else
        addedItems.put(clusterId, pointer);
    }

    for (Map.Entry<Integer, Integer> addedItem : addedItems.entrySet())
      Assert.assertEquals(addedItem.getValue().intValue(), memoryLongHashMap.get(addedItem.getKey()));

    Assert.assertEquals(addedItems.size(), memoryLongHashMap.size());
  }
}
