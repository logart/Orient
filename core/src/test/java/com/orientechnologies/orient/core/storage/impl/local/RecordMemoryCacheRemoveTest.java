package com.orientechnologies.orient.core.storage.impl.local;

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.type.tree.OBuddyMemory;
import com.orientechnologies.orient.core.type.tree.OMemory;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 29.06.12
 */
@Test
public class RecordMemoryCacheRemoveTest {
  private OMemory            memory;
  private ORecordMemoryCache recordMemoryCache;

  @BeforeMethod
  public void setUp() {
    memory = new OBuddyMemory(160000000, 32);
    recordMemoryCache = new ORecordMemoryCache(memory, 12);
  }

  @AfterMethod
  public void tearDown() {
    memory = null;
    recordMemoryCache = null;
  }

  public void testRemoveOneItem() {
    final byte[] content = new byte[] { 1, 2, 3, 4 };

    final int beforeMemory = memory.freeSpace();

    recordMemoryCache.put(1, 2, content, ORecordMemoryCache.RecordState.NEW);
    Assert.assertTrue(recordMemoryCache.remove(2));

    Assert.assertEquals(memory.freeSpace(), beforeMemory);
    Assert.assertEquals(recordMemoryCache.size(), 0);
  }

  public void testRemoveTwoItems() {
    final byte[] contentOne = new byte[] { 1, 2, 3, 4 };
    final byte[] contentTwo = new byte[] { 5, 6, 7, 8 };

    final long posOne = 2;
    final long posTwo = 3;

    final int beforeMemory = memory.freeSpace();

    recordMemoryCache.put(1, posOne, contentOne, ORecordMemoryCache.RecordState.NEW);
    recordMemoryCache.put(1, posTwo, contentTwo, ORecordMemoryCache.RecordState.NEW);

    Assert.assertTrue(recordMemoryCache.remove(posOne));
    Assert.assertEquals(recordMemoryCache.size(), 1);

    Assert.assertTrue(recordMemoryCache.remove(posTwo));
    Assert.assertEquals(recordMemoryCache.size(), 0);

    Assert.assertEquals(memory.freeSpace(), beforeMemory);

  }

  public void testRemoveThreeItems() {
    final byte[] contentOne = new byte[] { 1, 2, 3, 4 };
    final byte[] contentTwo = new byte[] { 5, 6, 7, 8 };
    final byte[] contentThree = new byte[] { 5, 6, 7, 8, 9 };

    final long posOne = 2;
    final long posTwo = 3;
    final long posThree = 4;

    final int beforeMemory = memory.freeSpace();

    recordMemoryCache.put(1, posOne, contentOne, ORecordMemoryCache.RecordState.NEW);
    recordMemoryCache.put(1, posTwo, contentTwo, ORecordMemoryCache.RecordState.NEW);
    recordMemoryCache.put(1, posThree, contentThree, ORecordMemoryCache.RecordState.NEW);

    Assert.assertTrue(recordMemoryCache.remove(posOne));
    Assert.assertEquals(recordMemoryCache.size(), 2);

    Assert.assertTrue(recordMemoryCache.remove(posTwo));
    Assert.assertEquals(recordMemoryCache.size(), 1);

    Assert.assertTrue(recordMemoryCache.remove(posThree));
    Assert.assertEquals(recordMemoryCache.size(), 0);

    Assert.assertEquals(memory.freeSpace(), beforeMemory);
  }

  public void testRemove10000Items() {
    final Map<Long, byte[]> positionContentMap = new HashMap<Long, byte[]>();

    for (long i = 0; i < 1; i++) {
      final byte[] content = new byte[(int) i + 1];
      for (int n = 0; n < content.length; n++) {
        content[n] = (byte) (n % 125);
      }

      positionContentMap.put(i, content);
      Assert.assertTrue(recordMemoryCache.put(1, i, content, ORecordMemoryCache.RecordState.NEW), "Put of " + i
          + " item was failed");
    }

    final int beforeMemory = memory.freeSpace();

    for (Long entryKey : positionContentMap.keySet())
      Assert.assertTrue(recordMemoryCache.remove(entryKey));

    Assert.assertTrue(memory.freeSpace() > beforeMemory);
    Assert.assertEquals(recordMemoryCache.size(), 0);
  }

  public void testAdd10000ItemsRemoveHalf() {
    final Map<Long, byte[]> positionContentMap = new HashMap<Long, byte[]>();

    for (long i = 0; i < 10000; i++) {
      byte[] content = new byte[(int) i + 1];

      for (int n = 0; n < content.length; n++) {
        content[n] = (byte) (n % 125);
      }

      Assert.assertTrue(recordMemoryCache.put(1, i, content, ORecordMemoryCache.RecordState.NEW), "Put of " + i
          + " item was failed");

      if (i % 2 == 0)
        Assert.assertTrue(recordMemoryCache.remove(i));
      else
        positionContentMap.put(i, content);
    }

    for (Map.Entry<Long, byte[]> entry : positionContentMap.entrySet())
      Assert.assertEquals(recordMemoryCache.get(entry.getKey()), entry.getValue());

    Assert.assertEquals(positionContentMap.size(), recordMemoryCache.size());
  }

}
