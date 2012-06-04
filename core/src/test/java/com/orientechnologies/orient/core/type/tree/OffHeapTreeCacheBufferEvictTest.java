package com.orientechnologies.orient.core.type.tree;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;
import junit.framework.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class OffHeapTreeCacheBufferEvictTest {
  private OOffHeapMemory memory = new OOffHeapMemory(2000000, 20);
  private OOffHeapTreeCacheBuffer<Integer> treeCacheBuffer;

  @BeforeMethod
  public void setUp() {
    treeCacheBuffer =
            new OOffHeapTreeCacheBuffer<Integer>(memory, OIntegerSerializer.INSTANCE);
  }

  @AfterMethod
  public void tearDown() {
    memory.clear();
  }

  public void testAdd100RemainLast7() {
    treeCacheBuffer.setEvictionSize(10);
    treeCacheBuffer.setDefaultEvictionPercent(30);

    for(int i = 0; i < 100; i++) {
      treeCacheBuffer.add(createCacheEntry(i));
    }

    Assert.assertEquals(7, treeCacheBuffer.size());
    for(int i = 99; i >= 93; i--)
     Assert.assertEquals( i + " -key is absent", createCacheEntry(i), treeCacheBuffer.remove(i));

    Assert.assertEquals(0, treeCacheBuffer.size());
    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

  public void testAdd10RemainFirst6Get() {
    treeCacheBuffer.setEvictionSize(10);
    treeCacheBuffer.setDefaultEvictionPercent(30);

    for (int i = 0; i < 9; i++)
      treeCacheBuffer.add(createCacheEntry(i));

    for (int i = 0; i < 6; i++)
      treeCacheBuffer.get(i);

    treeCacheBuffer.add(createCacheEntry(9));

    Assert.assertEquals(7, treeCacheBuffer.size());
    for (int i = 5; i >=0 ; i--)
      Assert.assertEquals(i + " -key is absent", createCacheEntry(i), treeCacheBuffer.remove(i));

		Assert.assertEquals("9 -key is absent", createCacheEntry(9), treeCacheBuffer.remove(9));

    Assert.assertEquals(0, treeCacheBuffer.size());
    Assert.assertEquals(memory.capacity(), memory.freeSpace());
  }

	public void testAdd100RemainFirst3Get() {
		treeCacheBuffer.setEvictionSize(10);
		treeCacheBuffer.setDefaultEvictionPercent(30);

		for (int n = 0; n < 10; n++) {
			for(int i = 0; i < 3; i++)
				treeCacheBuffer.add(createCacheEntry(n*10 + i));

			for (int j = 0; j < 3; j++)
				treeCacheBuffer.get(j);

			for(int i = 3; i < 6; i++)
				treeCacheBuffer.add(createCacheEntry(n*10 + i));

			for (int j = 0; j < 3; j++)
				treeCacheBuffer.get(j);

			for(int i = 6; i < 9; i++)
				treeCacheBuffer.add(createCacheEntry(n*10 + i));

			for (int j = 0; j < 3; j++)
				treeCacheBuffer.get(j);

			treeCacheBuffer.add(createCacheEntry(n * 10 + 9));
		}

		Assert.assertEquals(7, treeCacheBuffer.size());

		for(int i = 0; i < 3; i++)
			Assert.assertEquals( i + " -key is absent", createCacheEntry(i), treeCacheBuffer.remove(i));

		for(int i = 99; i >= 96; i--)
			Assert.assertEquals( i + " -key is absent", createCacheEntry(i), treeCacheBuffer.remove(i));

		Assert.assertEquals(0, treeCacheBuffer.size());
		Assert.assertEquals(memory.capacity(), memory.freeSpace());
	}

	public void testAdd10RemainFirst6GetCeiling() {
		treeCacheBuffer.setEvictionSize(10);
		treeCacheBuffer.setDefaultEvictionPercent(30);

		for (int i = 0; i < 9; i++)
			treeCacheBuffer.add(createCacheEntry(i));

		for (int i = 0; i < 6; i++)
			treeCacheBuffer.getCeiling(i);

		treeCacheBuffer.add(createCacheEntry(9));

		Assert.assertEquals(7, treeCacheBuffer.size());
		for (int i = 5; i >=0 ; i--)
			Assert.assertEquals(i + " -key is absent", createCacheEntry(i), treeCacheBuffer.remove(i));

		Assert.assertEquals("9 -key is absent", createCacheEntry(9), treeCacheBuffer.remove(9));

		Assert.assertEquals(0, treeCacheBuffer.size());
		Assert.assertEquals(memory.capacity(), memory.freeSpace());
	}

	public void testAdd10RemainFirst6GetFloor() {
		treeCacheBuffer.setEvictionSize(10);
		treeCacheBuffer.setDefaultEvictionPercent(30);

		for (int i = 0; i < 9; i++)
			treeCacheBuffer.add(createCacheEntry(i));

		for (int i = 0; i < 6; i++)
			treeCacheBuffer.getFloor(i);

		treeCacheBuffer.add(createCacheEntry(9));

		Assert.assertEquals(7, treeCacheBuffer.size());
		for (int i = 5; i >=0 ; i--)
			Assert.assertEquals(i + " -key is absent", createCacheEntry(i), treeCacheBuffer.remove(i));

		Assert.assertEquals("9 -key is absent", createCacheEntry(9), treeCacheBuffer.remove(9));

		Assert.assertEquals(0, treeCacheBuffer.size());
		Assert.assertEquals(memory.capacity(), memory.freeSpace());
	}

	public void testAdd10RemainFirst6Update() {
		treeCacheBuffer.setEvictionSize(10);
		treeCacheBuffer.setDefaultEvictionPercent(30);

		for (int i = 0; i < 9; i++)
			treeCacheBuffer.add(createCacheEntry(i));

		for (int i = 0; i < 6; i++)
			treeCacheBuffer.update(createUpdateCacheEntry(i));

		treeCacheBuffer.add(createCacheEntry(9));

		Assert.assertEquals(7, treeCacheBuffer.size());
		for (int i = 5; i >=0 ; i--)
			Assert.assertEquals(i + " -key is absent", createUpdateCacheEntry(i), treeCacheBuffer.remove(i));

		Assert.assertEquals("9 -key is absent", createCacheEntry(9), treeCacheBuffer.remove(9));

		Assert.assertEquals(0, treeCacheBuffer.size());
		Assert.assertEquals(memory.capacity(), memory.freeSpace());
	}


	private OOffHeapTreeCacheBuffer.CacheEntry<Integer> createCacheEntry(int key) {
    return new OOffHeapTreeCacheBuffer.CacheEntry<Integer>(key,1, new ORecordId(1, 1),
            new ORecordId(1, 2), new ORecordId(1, 3), new ORecordId(1, 4) );
  }

	private OOffHeapTreeCacheBuffer.CacheEntry<Integer> createUpdateCacheEntry(int key) {
		return new OOffHeapTreeCacheBuffer.CacheEntry<Integer>(key, 2, new ORecordId(2, 1),
						new ORecordId(2, 2), new ORecordId(2, 3), new ORecordId(2, 4));
	}
}
