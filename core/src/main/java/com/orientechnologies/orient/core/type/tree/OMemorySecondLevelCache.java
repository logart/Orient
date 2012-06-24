package com.orientechnologies.orient.core.type.tree;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OBooleanSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;

/**
 * @author LomakiA <a href="mailto:Andrey.Lomakin@exigenservices.com">Andrey Lomakin</a>
 * @since 24.06.12
 */
public class OMemorySecondLevelCache {
  private final OMemory         memory;
  private final ODatabaseRecord databaseRecord;

  private final int             clusterId;

  private int                   lruHeader              = OMemory.NULL_POINTER;
  private int                   lruTail                = OMemory.NULL_POINTER;

  private int                   evictionSize           = -1;
  private int                   defaultEvictionPercent = 20;

  private long                  size                   = 0;

  public OMemorySecondLevelCache(OMemory memory, int clusterId, ODatabaseRecord databaseRecord) {
    this.memory = memory;
    this.databaseRecord = databaseRecord;
    this.clusterId = clusterId;
  }

  public boolean put(ORecordId rid, byte[] content, boolean isDirty) {
    final int dataPointer = memory.allocate(content.length);
    if (dataPointer == OMemory.NULL_POINTER)
      return evict() && put(rid, content, isDirty);

    return false;
  }

  private int storeEntry(ORecordId rid, int dataPointer, boolean isDirty) {
    final int entryPointer = memory.allocate(OLinkSerializer.RID_SIZE + 3 * OIntegerSerializer.INT_SIZE
        + OBooleanSerializer.BOOLEAN_SIZE);

    if (entryPointer == OMemory.NULL_POINTER)
      return OMemory.NULL_POINTER;

    if (rid.isNew())
      rid.clusterId = -3;

    int offset = 0;
    memory.set(entryPointer, offset, rid, OLinkSerializer.INSTANCE);

    return entryPointer;
  }

  private boolean evict() {
    return false;
  }
}
