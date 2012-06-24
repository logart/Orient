package com.orientechnologies.orient.test.internal.index;

import org.testng.annotations.Test;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;
import com.orientechnologies.orient.core.type.tree.OBuddyMemory;
import com.orientechnologies.orient.core.type.tree.OMemory;
import com.orientechnologies.orient.core.type.tree.OMemoryFirstLevelCache;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 05.06.12
 */
public class OffHeapTreeCacheBufferSpeedTest extends SpeedTestMonoThread {

  private OMemory                         offheapMemory   = new OBuddyMemory(60000000, 32);
  private OMemoryFirstLevelCache<Integer> firstLevelCache = new OMemoryFirstLevelCache<Integer>(offheapMemory,
                                                              OIntegerSerializer.INSTANCE);

  private int                             key;

  public OffHeapTreeCacheBufferSpeedTest() {
    super(50000000);
    firstLevelCache.setEvictionSize(100001);
  }

  @Override
  @Test(enabled = false)
  public void cycle() throws Exception {
    firstLevelCache.add(createCacheEntry(key));
    firstLevelCache.get(key);

    key++;
  }

  @Override
  @Test(enabled = false)
  public void deinit() throws Exception {
    System.out.println();
    System.out.println("Cache size : " + firstLevelCache.size());
    System.out.println("Bytes per item : " + ((offheapMemory.capacity() - offheapMemory.freeSpace()) / firstLevelCache.size()));
  }

  private OMemoryFirstLevelCache.CacheEntry<Integer> createCacheEntry(int key) {
    return new OMemoryFirstLevelCache.CacheEntry<Integer>(key, 1, new ORecordId(1, 1), new ORecordId(1, 2), new ORecordId(1, 3),
        new ORecordId(1, 4));
  }

}
