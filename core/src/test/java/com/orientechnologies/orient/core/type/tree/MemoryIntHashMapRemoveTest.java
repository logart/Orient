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
public class MemoryIntHashMapRemoveTest {
  private OMemory           memory;
  private OMemoryIntHashMap memoryIntHashMap;

  @BeforeMethod
  public void setUp() {
    memory = new OBuddyMemory(4000000, 32);
    memoryIntHashMap = new OMemoryIntHashMap(memory, 0.8f, 4);
  }

  public void testRemoveOneItem() {
    final int clusterId = 1;

    final int freeSpaceBefore = memory.freeSpace();

    memoryIntHashMap.put(clusterId, 10);
    Assert.assertEquals(10, memoryIntHashMap.remove(clusterId));

    Assert.assertEquals(freeSpaceBefore, memory.freeSpace());
    Assert.assertEquals(0, memoryIntHashMap.size());
  }

  public void testRemoveTwoItems() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 4;

    final int freeSpaceBefore = memory.freeSpace();

    memoryIntHashMap.put(clusterIdOne, 10);
    memoryIntHashMap.put(clusterIdTwo, 20);

    Assert.assertEquals(10, memoryIntHashMap.remove(clusterIdOne));
    Assert.assertEquals(20, memoryIntHashMap.remove(clusterIdTwo));

    Assert.assertEquals(freeSpaceBefore, memory.freeSpace());
    Assert.assertEquals(0, memoryIntHashMap.size());
  }

  public void testRemoveThreeItems() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 1;
    final int clusterIdThree = 4;

    memoryIntHashMap.put(clusterIdOne, 10);
    memoryIntHashMap.put(clusterIdTwo, 20);
    memoryIntHashMap.put(clusterIdThree, 30);

    Assert.assertEquals(10, memoryIntHashMap.remove(clusterIdOne));
    Assert.assertEquals(20, memoryIntHashMap.remove(clusterIdTwo));
    Assert.assertEquals(30, memoryIntHashMap.remove(clusterIdThree));

    Assert.assertEquals(0, memoryIntHashMap.size());
  }

  public void testRemoveFourItems() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 1;
    final int clusterIdThree = 4;
    final int clusterIdFour = 2;

    memoryIntHashMap.put(clusterIdOne, 10);
    memoryIntHashMap.put(clusterIdTwo, 20);
    memoryIntHashMap.put(clusterIdThree, 30);
    memoryIntHashMap.put(clusterIdFour, 40);

    Assert.assertEquals(10, memoryIntHashMap.remove(clusterIdOne));
    Assert.assertEquals(20, memoryIntHashMap.remove(clusterIdTwo));
    Assert.assertEquals(30, memoryIntHashMap.remove(clusterIdThree));
    Assert.assertEquals(40, memoryIntHashMap.remove(clusterIdFour));

    Assert.assertEquals(0, memoryIntHashMap.size());
  }

  public void testAddThreeItemsRemoveOne() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 1;
    final int clusterIdThree = 4;

    memoryIntHashMap.put(clusterIdOne, 10);
    memoryIntHashMap.put(clusterIdTwo, 20);
    memoryIntHashMap.put(clusterIdThree, 30);

    Assert.assertEquals(10, memoryIntHashMap.remove(clusterIdOne));

    Assert.assertEquals(20, memoryIntHashMap.get(clusterIdTwo));
    Assert.assertEquals(30, memoryIntHashMap.get(clusterIdThree));

    Assert.assertEquals(2, memoryIntHashMap.size());
  }

  public void testAddFourItemsRemoveTwo() {
    final int clusterIdOne = 0;
    final int clusterIdTwo = 1;
    final int clusterIdThree = 4;
    final int clusterIdFour = 2;

    memoryIntHashMap.put(clusterIdOne, 10);
    memoryIntHashMap.put(clusterIdTwo, 20);
    memoryIntHashMap.put(clusterIdThree, 30);
    memoryIntHashMap.put(clusterIdFour, 40);

    Assert.assertEquals(20, memoryIntHashMap.remove(clusterIdTwo));
    Assert.assertEquals(10, memoryIntHashMap.remove(clusterIdOne));

    Assert.assertEquals(30, memoryIntHashMap.get(clusterIdThree));
    Assert.assertEquals(40, memoryIntHashMap.get(clusterIdFour));

    Assert.assertEquals(2, memoryIntHashMap.size());
  }

  public void testRemove10000RandomItems() {
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
      Assert.assertEquals(addedItem.getValue().intValue(), memoryIntHashMap.remove(addedItem.getKey()));

    Assert.assertEquals(0, memoryIntHashMap.size());
  }

  public void testAdd10000RandomItemsRemoveHalf() {
    final Map<Integer, Integer> addedItems = new HashMap<Integer, Integer>();
    final Random random = new Random();

    for (int i = 0; i < 10000; i++) {
      int clusterId = random.nextInt(32767);
      while (addedItems.containsKey(clusterId))
        clusterId = random.nextInt(32767);

      final int pointer = random.nextInt();

      memoryIntHashMap.put(clusterId, pointer);

      if (random.nextInt(2) > 0)
        Assert.assertEquals(pointer, memoryIntHashMap.remove(clusterId));
      else
        addedItems.put(clusterId, pointer);
    }

    for (Map.Entry<Integer, Integer> addedItem : addedItems.entrySet())
      Assert.assertEquals(addedItem.getValue().intValue(), memoryIntHashMap.get(addedItem.getKey()));

    Assert.assertEquals(addedItems.size(), memoryIntHashMap.size());
  }
}
