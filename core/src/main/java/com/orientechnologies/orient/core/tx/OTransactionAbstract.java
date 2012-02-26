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

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.WeakHashMap;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.cache.OLevel1RecordCache;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.storage.OStorage;

public abstract class OTransactionAbstract implements OTransaction {
	protected final ODatabaseRecordTx	database;
	protected TXSTATUS								status	= TXSTATUS.INVALID;
	protected Set<OTxRecordListener> recordListeners;

	protected OTransactionAbstract(final ODatabaseRecordTx iDatabase) {
		database = iDatabase;
	}

	public boolean isActive() {
		return status != TXSTATUS.INVALID;
	}

	public TXSTATUS getStatus() {
		return status;
	}

	public ODatabaseRecordTx getDatabase() {
		return database;
	}

	public static void updateCacheFromEntries(final OStorage iStorage, final OTransaction iTx,
			final Iterable<? extends ORecordOperation> iEntries, final boolean iUpdateStrategy) throws IOException {
		final OLevel1RecordCache dbCache = (OLevel1RecordCache) iTx.getDatabase().getLevel1Cache();

		for (ORecordOperation txEntry : iEntries) {
			if (!iUpdateStrategy)
				// ALWAYS REMOVE THE RECORD FROM CACHE
				dbCache.deleteRecord(txEntry.getRecord().getIdentity());
			else if (txEntry.type == ORecordOperation.DELETED)
				// DELETION
				dbCache.deleteRecord(txEntry.getRecord().getIdentity());
			else if (txEntry.type == ORecordOperation.UPDATED || txEntry.type == ORecordOperation.CREATED)
				// UDPATE OR CREATE
				dbCache.updateRecord(txEntry.getRecord());
		}
	}

	protected void invokeCommitAgainstListeners() {
		// WAKE UP LISTENERS
		for (ODatabaseListener listener : ((ODatabaseRaw) database.getUnderlying()).getListeners())
			try {
				listener.onBeforeTxCommit(database.getUnderlying());
			} catch (Throwable t) {
				OLogManager.instance().error(this, "Error on commit callback against listener: " + listener, t);
			}
	}

	protected void invokeRollbackAgainstListeners() {
		// WAKE UP LISTENERS
		for (ODatabaseListener listener : ((ODatabaseRaw) database.getUnderlying()).getListeners())
			try {
				listener.onBeforeTxRollback(database.getUnderlying());
			} catch (Throwable t) {
				OLogManager.instance().error(this, "Error on rollback callback against listener: " + listener, t);
			}
	}

	public void addRecordListener(OTxRecordListener listener) {
		if(recordListeners == null)
			recordListeners = new HashSet<OTxRecordListener>();

		recordListeners.add(listener);
	}

	public void removeRecordListener(OTxRecordListener listener) {
		if(recordListeners == null)
			return;
		recordListeners.remove(listener);
	}

	protected void invokeBeforeRecordListener(final byte operation, final ORecord<?> record) {
		if(recordListeners == null || recordListeners.isEmpty())
			return;

		for(final OTxRecordListener listener : recordListeners) {
			switch (operation) {
				case ORecordOperation.CREATED:
					listener.onBeforeCreateRecordTx(record);
					break;
				case ORecordOperation.UPDATED:
					listener.onBeforeUpdateRecordTx(record);
					break;
				case ORecordOperation.DELETED:
					listener.onBeforeDeleteRecordTx(record);
					break;
				case ORecordOperation.LOADED:
					listener.onBeforeLoadRecordTx(record);
					break;
			}
		}
	}

	protected void invokeAfterRecordListener(final byte operation, final ORecord<?> record) {
		if(recordListeners == null || recordListeners.isEmpty())
			return;

		for(final OTxRecordListener listener : recordListeners) {
			switch (operation) {
				case ORecordOperation.CREATED:
					listener.onAfterCreateRecordTx(record);
					break;
				case ORecordOperation.UPDATED:
					listener.onAfterUpdateRecordTx(record);
					break;
				case ORecordOperation.DELETED:
					listener.onAfterDeleteRecordTx(record);
					break;
				case ORecordOperation.LOADED:
					listener.onAfterLoadRecordTx(record);
					break;
			}
		}
	}

}
