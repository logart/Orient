package com.orientechnologies.orient.test.internal.index;

import com.orientechnologies.common.test.SpeedTestMonoThread;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;
import com.orientechnologies.orient.core.type.tree.OOffHeapMemory;
import com.orientechnologies.orient.core.type.tree.OOffHeapTreeCacheBuffer;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 05.06.12
 */
public class OffHeapTreeCacheBufferSpeedTest  extends SpeedTestMonoThread {

	private OOffHeapMemory offheapMemory= new OOffHeapMemory(32000400, 64);
	private OOffHeapTreeCacheBuffer<Integer> treeCacheBuffer =
					new OOffHeapTreeCacheBuffer<Integer>(offheapMemory, OIntegerSerializer.INSTANCE);

	private int	key;

	public OffHeapTreeCacheBufferSpeedTest() {
		super(5000000);
	}

	@Override
	public void cycle() throws Exception {
		treeCacheBuffer.add(createCacheEntry(key));
		treeCacheBuffer.get(key);

		key++;
	}

	@Override
	public void deinit() throws Exception {
		System.out.println();
		System.out.println("Cache size : " + treeCacheBuffer.size());
		System.out.println("Bytes per item : " +
						((offheapMemory.capacity() - offheapMemory.freeSpace()) / treeCacheBuffer.size()));
	}

	private OOffHeapTreeCacheBuffer.CacheEntry<Integer> createCacheEntry(int key) {
		return new OOffHeapTreeCacheBuffer.CacheEntry<Integer>(key,1, new ORecordId(1, 1),
						new ORecordId(1, 2), new ORecordId(1, 3), new ORecordId(1, 4) );
	}

}