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

import java.util.List;

import com.orientechnologies.orient.core.db.ODatabaseComplex.OPERATION_MODE;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

public interface OTransaction {
	public enum TXTYPE {
		NOTX, OPTIMISTIC, PESSIMISTIC
	}

	public enum TXSTATUS {
		INVALID, BEGUN, COMMITTING, ROLLBACKING
	}

	public enum ISOLATION_LEVEL {
		READ_UNCOMMITTED, READ_COMMITTED
	}

	public void begin();

	public void commit();

	public void rollback();

	public ODatabaseRecordTx getDatabase();

	public void clearRecordEntries();

	public ORecordInternal<?> loadRecord(ORID iRid, ORecordInternal<?> iRecord, String iFetchPlan);

	public void saveRecord(ORecordInternal<?> iContent, String iClusterName, OPERATION_MODE iMode);

	public void deleteRecord(ORecordInternal<?> iRecord, OPERATION_MODE iMode);

	public int getId();

	public TXSTATUS getStatus();

	public Iterable<? extends ORecordOperation> getCurrentRecordEntries();

	public Iterable<? extends ORecordOperation> getAllRecordEntries();

	public List<ORecordOperation> getRecordEntriesByClass(String iClassName);

	public List<ORecordOperation> getRecordEntriesByClusterIds(int[] iIds);

	public ORecordInternal<?> getRecord(ORID iRid);

	public ORecordOperation getRecordEntry(ORID rid);

	public List<String> getInvolvedIndexes();

	public ODocument getIndexChanges();

	public void addIndexEntry(OIndex<?> delegate, final String iIndexName, final OTransactionIndexChanges.OPERATION iStatus,
			final Object iKey, final OIdentifiable iValue);

	public void clearIndexEntries();

	public OTransactionIndexChanges getIndexChanges(String iName);

	/**
	 * Tells if the transaction is active.
	 * 
	 * @return
	 */
	public boolean isActive();

	public boolean isUsingLog();

	public void setUsingLog(boolean useLog);

	public void close();

	public ISOLATION_LEVEL getIsolationLevel();

	/**
	 * When commit in transaction is performed all new records will change their identity, but index values will contain staile links,
	 * to fix them given method will be called for each entry.
	 *
	 * @param oldRid   Record identity before commit.
	 * @param newRid   Record identity after commit.
	 */
	public void updateIndexIdentityAfterCommit(final ORID oldRid, final ORID newRid);

	public void addRecordListener(final OTxRecordListener listener);

	public void removeRecordListener(final OTxRecordListener listener);

}
