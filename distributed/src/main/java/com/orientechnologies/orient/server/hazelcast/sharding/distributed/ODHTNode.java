package com.orientechnologies.orient.server.hazelcast.sharding.distributed;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;

/**
 * @author Andrey Lomakin
 * @since 17.08.12
 */
public interface ODHTNode {
	public long getNodeId();
	public long getSuccessor();
	public Long getPredecessor();
	public void notify(long node);
	public boolean join(long node);
	public long findSuccessor(long id);
	public void put(Long id, String data);
	public String get(Long id);
	public boolean remove(Long id);
	public int size();
	public void notifyMigrationEnd(long nodeId);
	public void requestMigration(long requesterId);

    OPhysicalPosition createRecord(String storageName, ORecordId iRecordId, byte[] iContent, int iRecordVersion, byte iRecordType);
    ORawBuffer readRecord(String storageName, ORID iRid);
    int updateRecord(String storageName, ORecordId iRecordId, byte[] iContent, int iVersion, byte iRecordType);
    boolean deleteRecord(String storageName, ORecordId iRecordId, int iVersion);
}
