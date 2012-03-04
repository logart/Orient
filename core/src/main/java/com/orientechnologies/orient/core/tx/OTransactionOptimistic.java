/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.tx;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseComplex.OPERATION_MODE;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexMVRBTreeAbstract;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;

public class OTransactionOptimistic extends OTransactionRealAbstract {
	private List<ORID> lockedRids = new ArrayList<ORID>();
	private boolean								usingLog;
	private static AtomicInteger	txSerial	= new AtomicInteger();

	public OTransactionOptimistic(final ODatabaseRecordTx iDatabase) {
		super(iDatabase, txSerial.incrementAndGet());
		usingLog = OGlobalConfiguration.TX_USE_LOG.getValueAsBoolean();
	}

	public void begin() {
		status = TXSTATUS.BEGUN;
	}

	public void commit() {
		checkTransaction();

		if (!(database.getStorage() instanceof OStorageEmbedded)) {
			status = TXSTATUS.COMMITTING;
			database.getStorage().commit(this);
		} else {
			final OStorageEmbedded storageEmbedded = (OStorageEmbedded) database.getStorage();

			final List<ORecordOperation> tmpEntries = new ArrayList<ORecordOperation>();

			//Gather all RIDs before commit
			while (getCurrentRecordEntries().iterator().hasNext()) {
				for (ORecordOperation txEntry : getCurrentRecordEntries())
					tmpEntries.add(txEntry);

				clearRecordEntries();

				if (!tmpEntries.isEmpty()) {
					for (ORecordOperation txEntry : tmpEntries)
						txEntry.getRecord().toStream();
				}
			}

			List<ORID> ids = new ArrayList<ORID>();
			for (ORecordOperation recordOperation : getAllRecordEntries()) {
				ids.add(recordOperation.getRecord().getIdentity());
			}

			Collections.sort(ids);
			List<OIndexMVRBTreeAbstract<?>> lockedIndexes = null;

			try {
				status = TXSTATUS.COMMITTING;

				for (final ORID ridToLock : ids) {
					storageEmbedded.lockRecord(ridToLock, true);
					lockedRids.add(ridToLock);
				}

				database.getStorage().commit(OTransactionOptimistic.this);

				final List<String> involvedIndexes = getInvolvedIndexes();

				// LOCK INVOLVED INDEXES
				if (involvedIndexes != null) {
					final List<String> indexesToLock = new ArrayList<String>(involvedIndexes);

					Collections.sort(indexesToLock);

					for (String indexName : indexesToLock) {
						if (lockedIndexes == null)
							lockedIndexes = new ArrayList<OIndexMVRBTreeAbstract<?>>();

						final OIndexMVRBTreeAbstract<?> index = (OIndexMVRBTreeAbstract<?>) database.getMetadata().getIndexManager()
										.getIndexInternal(indexName);

						index.acquireExclusiveLock();
						lockedIndexes.add(index);
   				}
				}
				// COMMIT INDEX CHANGES
				final ODocument indexEntries = getIndexChanges();
				if (indexEntries != null) {
					for (Entry<String, Object> indexEntry : indexEntries) {
						final OIndex<?> index = database.getMetadata().getIndexManager().getIndexInternal(indexEntry.getKey());
						index.commit((ODocument) indexEntry.getValue());
					}
				}
			} catch (RuntimeException e) {
				// WE NEED TO CALL ROLLBACK HERE, IN THE LOCK
				rollback();
				throw e;
			} finally {
				// RELEASE INDEX LOCKS IF ANY
				if (lockedIndexes != null)
					// DON'T USE GENERICS TO AVOID OpenJDK CRASH :-(
					for (OIndexMVRBTreeAbstract<?> index : lockedIndexes) {
						index.releaseExclusiveLock();
					}

				for (final ORID rid : lockedRids)
					storageEmbedded.unlockRecord(rid, true);

				lockedRids.clear();
			}
		}
	}

	public void rollback() {
		checkTransaction();

		status = TXSTATUS.ROLLBACKING;

		// CLEAR THE CACHE MOVING GOOD RECORDS TO LEVEL-2 CACHE
		database.getLevel1Cache().clear();

		// REMOVE ALL THE ENTRIES AND INVALIDATE THE DOCUMENTS TO AVOID TO BE RE-USED DIRTY AT USER-LEVEL. IN THIS WAY RE-LOADING MUST
		// EXECUTED
		for (ORecordOperation v : recordEntries.values())
			v.getRecord().unload();

		for (ORecordOperation v : allEntries.values())
			v.getRecord().unload();

		indexEntries.clear();
	}

	public ORecordInternal<?> loadRecord(final ORID iRid, final ORecordInternal<?> iRecord, final String iFetchPlan) {
		checkTransaction();

		final ORecordInternal<?> txRecord = getRecord(iRid);
		if (txRecord == OTransactionRealAbstract.DELETED_RECORD)
			// DELETED IN TX
			return null;

		if (txRecord != null)
			return txRecord;

		// DELEGATE TO THE STORAGE
		return database.executeReadRecord((ORecordId) iRid, iRecord, iFetchPlan, false);
	}

	public void deleteRecord(final ORecordInternal<?> iRecord, final OPERATION_MODE iMode) {
		if (!iRecord.getIdentity().isValid())
			return;

		addRecord(iRecord, ORecordOperation.DELETED, null);
	}

	public void saveRecord(final ORecordInternal<?> iRecord, final String iClusterName, final OPERATION_MODE iMode) {
		addRecord(iRecord, iRecord.getIdentity().isValid() ? ORecordOperation.UPDATED : ORecordOperation.CREATED, iClusterName);
	}

	private void addRecord(final ORecordInternal<?> iRecord, final byte iStatus, final String iClusterName) {
		checkTransaction();

		if ((status == OTransaction.TXSTATUS.COMMITTING) && database.getStorage() instanceof OStorageEmbedded) {
			// I'M COMMITTING: BYPASS LOCAL BUFFER
			//RECORDS ARE NOT LOCKED HERE BECAUSE ONLY INDEX RECORDS ARE HANDLED HERE BUY THEY ARE LOCKED IN #commit method.

			switch (iStatus) {
			case ORecordOperation.CREATED:
			case ORecordOperation.UPDATED:
				database
						.executeSaveRecord(iRecord, iClusterName, iRecord.getVersion(), iRecord.getRecordType(), OPERATION_MODE.SYNCHRONOUS);
				break;
			case ORecordOperation.DELETED:
				database.executeDeleteRecord(iRecord, iRecord.getVersion(), false, OPERATION_MODE.SYNCHRONOUS);
				break;
			}
		} else {
			final ORecordId rid = (ORecordId) iRecord.getIdentity();

			if (!rid.isValid()) {
				// // TODO: NEED IT FOR REAL?
				// // NEW RECORD: CHECK IF IT'S ALREADY IN
				// for (OTransactionRecordEntry entry : recordEntries.values()) {
				// if (entry.getRecord() == iRecord)
				// return;
				// }

				iRecord.onBeforeIdentityChanged(rid);

				// ASSIGN A UNIQUE SERIAL TEMPORARY ID
				if (rid.clusterId == ORID.CLUSTER_ID_INVALID)
					rid.clusterId = iClusterName != null ? database.getClusterIdByName(iClusterName) : database.getDefaultClusterId();
				rid.clusterPosition = newObjectCounter--;

				iRecord.onAfterIdentityChanged(iRecord);
			} else
				// REMOVE FROM THE DB'S CACHE
				database.getLevel1Cache().freeRecord(rid);

			ORecordOperation txEntry = getRecordEntry(rid);

			if (txEntry == null) {
				// NEW ENTRY: JUST REGISTER IT
				txEntry = new ORecordOperation(iRecord, iStatus);

				recordEntries.put(rid, txEntry);
			} else {
				// UPDATE PREVIOUS STATUS
				txEntry.record = iRecord;

				switch (txEntry.type) {
				case ORecordOperation.LOADED:
					switch (iStatus) {
					case ORecordOperation.UPDATED:
						txEntry.type = ORecordOperation.UPDATED;
						break;
					case ORecordOperation.DELETED:
						txEntry.type = ORecordOperation.DELETED;
						break;
					}
					break;
				case ORecordOperation.UPDATED:
					switch (iStatus) {
					case ORecordOperation.DELETED:
						txEntry.type = ORecordOperation.DELETED;
						break;
					}
					break;
				case ORecordOperation.DELETED:
					break;
				case ORecordOperation.CREATED:
					switch (iStatus) {
					case ORecordOperation.DELETED:
						recordEntries.remove(rid);
						break;
					}
					break;
				}
			}
		}
	}

	@Override
	public String toString() {
		return "OTransactionOptimistic [id=" + id + ", status=" + status + ", recEntries=" + recordEntries.size() + ", idxEntries="
				+ indexEntries.size() + "]";
	}

	public boolean isUsingLog() {
		return usingLog;
	}

	public void setUsingLog(final boolean useLog) {
		this.usingLog = useLog;
	}

	public ISOLATION_LEVEL getIsolationLevel() {
		return isolation_level;
	}
}
