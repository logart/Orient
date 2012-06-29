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
public class RecordMemoryCachePutTest {
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

  public void testAddOneItem() {
    final byte[] content = new byte[] { 1, 2, 3, 4 };

    recordMemoryCache.put(1, 2, content, ORecordMemoryCache.RecordState.NEW);

    final byte[] returnedContent = recordMemoryCache.get(2);

    Assert.assertEquals(returnedContent, content);
  }

  public void testAddTwoItems() {
    final byte[] contentOne = new byte[] { 1, 2, 3, 4 };
    final byte[] contentTwo = new byte[] { 5, 6, 7, 8 };

    final long posOne = 2;
    final long posTwo = 3;

    recordMemoryCache.put(1, posOne, contentOne, ORecordMemoryCache.RecordState.NEW);
    recordMemoryCache.put(1, posTwo, contentTwo, ORecordMemoryCache.RecordState.NEW);

    final byte[] returnedContentOne = recordMemoryCache.get(posOne);
    Assert.assertEquals(returnedContentOne, contentOne);

    final byte[] returnedContentTwo = recordMemoryCache.get(posTwo);
    Assert.assertEquals(returnedContentTwo, contentTwo);
  }

  public void testAddThreeItems() {
    final byte[] contentOne = new byte[] { 1, 2, 3, 4 };
    final byte[] contentTwo = new byte[] { 5, 6, 7, 8 };
    final byte[] contentThree = new byte[] { 5, 6, 7, 8, 9 };

    final long posOne = 2;
    final long posTwo = 3;
    final long posThree = 4;

    recordMemoryCache.put(1, posOne, contentOne, ORecordMemoryCache.RecordState.NEW);
    recordMemoryCache.put(1, posTwo, contentTwo, ORecordMemoryCache.RecordState.NEW);
    recordMemoryCache.put(1, posThree, contentThree, ORecordMemoryCache.RecordState.NEW);

    final byte[] returnedContentOne = recordMemoryCache.get(posOne);
    Assert.assertEquals(returnedContentOne, contentOne);

    final byte[] returnedContentTwo = recordMemoryCache.get(posTwo);
    Assert.assertEquals(returnedContentTwo, contentTwo);

    final byte[] returnedContentThree = recordMemoryCache.get(posThree);
    Assert.assertEquals(returnedContentThree, contentThree);
  }

  public void testAddThreeItemsReplaceOne() {
    final byte[] contentOne = new byte[] { 1, 2, 3, 4 };
    final byte[] contentTwo = new byte[] { 5, 6, 7, 8 };
    final byte[] contentThree = new byte[] { 5, 6, 7, 8, 9 };

    final byte[] contentTwoUpdated = new byte[] { 5, 6, 7, 10 };

    final long posOne = 2;
    final long posTwo = 3;
    final long posThree = 4;

    recordMemoryCache.put(1, posOne, contentOne, ORecordMemoryCache.RecordState.NEW);
    recordMemoryCache.put(1, posTwo, contentTwo, ORecordMemoryCache.RecordState.NEW);
    recordMemoryCache.put(1, posThree, contentThree, ORecordMemoryCache.RecordState.NEW);

    recordMemoryCache.put(1, posTwo, contentTwoUpdated, ORecordMemoryCache.RecordState.MODIFIED);

    final byte[] returnedContentOne = recordMemoryCache.get(posOne);
    Assert.assertEquals(returnedContentOne, contentOne);

    final byte[] returnedContentTwo = recordMemoryCache.get(posTwo);
    Assert.assertEquals(returnedContentTwo, contentTwoUpdated);

    final byte[] returnedContentThree = recordMemoryCache.get(posThree);
    Assert.assertEquals(returnedContentThree, contentThree);
  }

  public void testAddFourItemsReplaceTwo() {
    final byte[] contentOne = new byte[] { 1, 2, 3, 4 };
    final byte[] contentTwo = new byte[] { 5, 6, 7, 8 };
    final byte[] contentThree = new byte[] { 5, 6, 7, 8, 9 };
    final byte[] contentFour = new byte[] { 5, 6, 7, 8, 9, 10 };

    final byte[] contentTwoUpdated = new byte[] { 5, 6, 7, 10 };
    final byte[] contentFourUpdated = new byte[] { 5, 6, 7, 8, 9, 10, 100 };

    final long posOne = 2;
    final long posTwo = 3;
    final long posThree = 4;
    final long posFour = 5;

    recordMemoryCache.put(1, posOne, contentOne, ORecordMemoryCache.RecordState.NEW);
    recordMemoryCache.put(1, posTwo, contentTwo, ORecordMemoryCache.RecordState.NEW);
    recordMemoryCache.put(1, posThree, contentThree, ORecordMemoryCache.RecordState.NEW);
    recordMemoryCache.put(1, posFour, contentFour, ORecordMemoryCache.RecordState.NEW);

    recordMemoryCache.put(1, posTwo, contentTwoUpdated, ORecordMemoryCache.RecordState.MODIFIED);
    recordMemoryCache.put(1, posFour, contentFourUpdated, ORecordMemoryCache.RecordState.MODIFIED);

    final byte[] returnedContentOne = recordMemoryCache.get(posOne);
    Assert.assertEquals(returnedContentOne, contentOne);

    final byte[] returnedContentTwo = recordMemoryCache.get(posTwo);
    Assert.assertEquals(returnedContentTwo, contentTwoUpdated);

    final byte[] returnedContentThree = recordMemoryCache.get(posThree);
    Assert.assertEquals(returnedContentThree, contentThree);

    final byte[] returnedContentFour = recordMemoryCache.get(posFour);
    Assert.assertEquals(returnedContentFour, contentFourUpdated);
  }

  public void testAdd10000Items() {
    final Map<Long, byte[]> positionContentMap = new HashMap<Long, byte[]>();

    for (long i = 0; i < 10000; i++) {
      final byte[] content = new byte[(int) i + 1];
      for (int n = 0; n < content.length; n++) {
        content[n] = (byte) (n % 125);
      }

      positionContentMap.put(i, content);
      Assert.assertTrue(recordMemoryCache.put(1, i, content, ORecordMemoryCache.RecordState.NEW), "Put of " + i
          + " item was failed");
    }

    for (Map.Entry<Long, byte[]> entry : positionContentMap.entrySet())
      Assert.assertEquals(recordMemoryCache.get(entry.getKey()), entry.getValue());

  }

  public void testAdd10000ItemsReplaceHalf() {
    final Map<Long, byte[]> positionContentMap = new HashMap<Long, byte[]>();

    for (long i = 0; i < 10000; i++) {
      byte[] content = new byte[(int) i + 1];

      for (int n = 0; n < content.length; n++) {
        content[n] = (byte) (n % 125);
      }

      Assert.assertTrue(recordMemoryCache.put(1, i, content, ORecordMemoryCache.RecordState.NEW), "Put of " + i
          + " item was failed");

      if (i % 2 == 0) {
        content = new byte[(int) i / 2 + 1];
        for (int n = 0; n < content.length; n++) {
          content[n] = (byte) ((n % 125) - 10);
        }
      }

      positionContentMap.put(i, content);
      Assert.assertTrue(recordMemoryCache.put(1, i, content, ORecordMemoryCache.RecordState.MODIFIED), "Put of " + i
          + " item was failed");
    }

    for (Map.Entry<Long, byte[]> entry : positionContentMap.entrySet())
      Assert.assertEquals(recordMemoryCache.get(entry.getKey()), entry.getValue());

  }
}
