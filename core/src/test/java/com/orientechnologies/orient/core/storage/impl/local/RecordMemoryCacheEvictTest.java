package com.orientechnologies.orient.core.storage.impl.local;

import java.util.concurrent.atomic.AtomicInteger;

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
public class RecordMemoryCacheEvictTest {
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

  public void testAdd100RemainLast10() {
    recordMemoryCache.setEvictionSize(10);
    recordMemoryCache.setDefaultEvictionPercent(30);

    final AtomicInteger callCount = new AtomicInteger();

    final int beforeMemory = memory.freeSpace();
    for (int i = 0; i < 100; i++) {
      if (!recordMemoryCache.put(1, i, new byte[] { 1 }, ORecordMemoryCache.RecordState.NEW)) {
        recordMemoryCache.evict(new ORecordMemoryCacheFlusher() {
          public void flushRecord(int clusterId, long clusterPosition, int dataSegmentId, byte[] content,
              ORecordMemoryCache.RecordState recordState) {
            Assert.assertEquals(12, clusterId);
            Assert.assertEquals(1, dataSegmentId);
            Assert.assertEquals(new byte[] { 1 }, content);
            Assert.assertEquals(ORecordMemoryCache.RecordState.NEW, recordState);

            callCount.getAndIncrement();
          }
        });
        recordMemoryCache.put(1, i, new byte[] { 1 }, ORecordMemoryCache.RecordState.NEW);

      }
    }

    Assert.assertEquals(10, recordMemoryCache.size());
    for (int i = 99; i >= 90; i--)
      Assert.assertTrue(recordMemoryCache.remove(i));

    Assert.assertEquals(0, recordMemoryCache.size());
    Assert.assertEquals(beforeMemory, memory.freeSpace());

    Assert.assertTrue(callCount.intValue() > 0);
  }

  public void testAdd10RemainFirst6Get() {
    recordMemoryCache.setEvictionSize(10);
    recordMemoryCache.setDefaultEvictionPercent(30);

    final int beforeMemory = memory.freeSpace();
    for (int i = 0; i < 10; i++)
      recordMemoryCache.put(1, i, new byte[] { 1 }, ORecordMemoryCache.RecordState.NEW);

    for (int i = 0; i < 6; i++)
      recordMemoryCache.get(i);

    if (!recordMemoryCache.put(1, 10, new byte[] { 1 }, ORecordMemoryCache.RecordState.NEW))
      recordMemoryCache.evict(new ORecordMemoryCacheFlusherStub());

    Assert.assertEquals(7, recordMemoryCache.size());

    for (int i = 5; i >= 0; i--)
      Assert.assertTrue(recordMemoryCache.remove(i));

    Assert.assertTrue(recordMemoryCache.remove(9));

    Assert.assertEquals(0, recordMemoryCache.size());
    Assert.assertEquals(beforeMemory, memory.freeSpace());
  }

  public void testAdd100RemainFirst3Get() {
    recordMemoryCache.setEvictionSize(10);
    recordMemoryCache.setDefaultEvictionPercent(30);

    final int beforeMemory = memory.freeSpace();

    for (int n = 0; n < 10; n++) {
      for (int i = 0; i < 3; i++)
        if (!recordMemoryCache.put(1, n * 10 + i, new byte[] { 1 }, ORecordMemoryCache.RecordState.NEW)) {
          recordMemoryCache.evict(new ORecordMemoryCacheFlusherStub());
          recordMemoryCache.put(1, n * 10 + i, new byte[] { 1 }, ORecordMemoryCache.RecordState.NEW);
        }

      for (int j = 0; j < 3; j++)
        recordMemoryCache.get(j);

      for (int i = 3; i < 6; i++)
        if (!recordMemoryCache.put(1, n * 10 + i, new byte[] { 1 }, ORecordMemoryCache.RecordState.NEW)) {
          recordMemoryCache.evict(new ORecordMemoryCacheFlusherStub());
          recordMemoryCache.put(1, n * 10 + i, new byte[] { 1 }, ORecordMemoryCache.RecordState.NEW);
        }

      for (int j = 0; j < 3; j++)
        recordMemoryCache.get(j);

      for (int i = 6; i < 9; i++)
        if (!recordMemoryCache.put(1, n * 10 + i, new byte[] { 1 }, ORecordMemoryCache.RecordState.NEW)) {
          recordMemoryCache.evict(new ORecordMemoryCacheFlusherStub());
          recordMemoryCache.put(1, n * 10 + i, new byte[] { 1 }, ORecordMemoryCache.RecordState.NEW);
        }

      for (int j = 0; j < 3; j++)
        recordMemoryCache.get(j);

      if (!recordMemoryCache.put(1, n * 10 + 9, new byte[] { 1 }, ORecordMemoryCache.RecordState.NEW)) {
        recordMemoryCache.evict(new ORecordMemoryCacheFlusherStub());
        recordMemoryCache.put(1, n * 10 + 9, new byte[] { 1 }, ORecordMemoryCache.RecordState.NEW);
      }
    }

    Assert.assertEquals(10, recordMemoryCache.size());

    for (int i = 0; i < 3; i++)
      Assert.assertTrue(recordMemoryCache.remove(i));

    for (int i = 99; i >= 93; i--)
      Assert.assertTrue(recordMemoryCache.remove(i));

    Assert.assertEquals(recordMemoryCache.size(), 0);
    Assert.assertEquals(beforeMemory, memory.freeSpace());
  }

  public void testAdd10RemainFirst6Update() {
    recordMemoryCache.setEvictionSize(10);
    recordMemoryCache.setDefaultEvictionPercent(30);

    final int beforeMemory = memory.freeSpace();
    for (int i = 0; i < 10; i++)
      if (!recordMemoryCache.put(1, i, new byte[] { 1 }, ORecordMemoryCache.RecordState.NEW)) {
        recordMemoryCache.evict(new ORecordMemoryCacheFlusherStub());
        recordMemoryCache.put(1, i, new byte[] { 1 }, ORecordMemoryCache.RecordState.NEW);
      }

    for (int i = 0; i < 7; i++)
      if (!recordMemoryCache.put(1, i, new byte[] { 1 }, ORecordMemoryCache.RecordState.MODIFIED)) {
        recordMemoryCache.evict(new ORecordMemoryCacheFlusherStub());
        recordMemoryCache.put(1, i, new byte[] { 1 }, ORecordMemoryCache.RecordState.MODIFIED);
      }

    if (!recordMemoryCache.put(1, 10, new byte[] { 1 }, ORecordMemoryCache.RecordState.NEW)) {
      recordMemoryCache.evict(new ORecordMemoryCacheFlusherStub());
      recordMemoryCache.put(1, 10, new byte[] { 1 }, ORecordMemoryCache.RecordState.NEW);
    }

    Assert.assertEquals(recordMemoryCache.size(), 8);
    for (int i = 6; i >= 0; i--)
      Assert.assertTrue(recordMemoryCache.remove(i));

    Assert.assertTrue(recordMemoryCache.remove(10));

    Assert.assertEquals(0, recordMemoryCache.size());
    Assert.assertEquals(beforeMemory, memory.freeSpace());
  }

  public void testAdd10RemainLast8OnlySharedTest() {
    recordMemoryCache.setEvictionSize(10);
    recordMemoryCache.setDefaultEvictionPercent(30);

    final int beforeMemory = memory.freeSpace();

    for (int i = 0; i < 2; i++)
      if (!recordMemoryCache.put(1, i, new byte[] { 1 }, ORecordMemoryCache.RecordState.SHARED)) {
        recordMemoryCache.evict(new ORecordMemoryCacheFlusherStub());
        recordMemoryCache.put(1, i, new byte[] { 1 }, ORecordMemoryCache.RecordState.SHARED);
      }

    if (!recordMemoryCache.put(1, 2, new byte[] { 1 }, ORecordMemoryCache.RecordState.NEW)) {
      recordMemoryCache.evict(new ORecordMemoryCacheFlusherStub());
      recordMemoryCache.put(1, 2, new byte[] { 1 }, ORecordMemoryCache.RecordState.NEW);
    }

    for (int i = 3; i < 10; i++)
      if (!recordMemoryCache.put(1, i, new byte[] { 1 }, ORecordMemoryCache.RecordState.SHARED)) {
        recordMemoryCache.evict(new ORecordMemoryCacheFlusherStub());
        recordMemoryCache.put(1, i, new byte[] { 1 }, ORecordMemoryCache.RecordState.SHARED);
      }

    if (!recordMemoryCache.put(1, 11, new byte[] { 1 }, ORecordMemoryCache.RecordState.SHARED))
      recordMemoryCache.evictSharedRecordsOnly();

    Assert.assertEquals(recordMemoryCache.size(), 7);

    Assert.assertTrue(recordMemoryCache.remove(2));

    for (int i = 4; i < 10; i++)
      Assert.assertTrue(recordMemoryCache.remove(i));

    Assert.assertEquals(0, recordMemoryCache.size());
    Assert.assertEquals(beforeMemory, memory.freeSpace());
  }

  private static class ORecordMemoryCacheFlusherStub implements ORecordMemoryCacheFlusher {
    public void flushRecord(int clusterId, long clusterPosition, int dataSegmentId, byte[] content,
        ORecordMemoryCache.RecordState recordState) {
    }
  }
}
