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
package com.orientechnologies.orient.core.storage;

import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Interface for embedded storage.
 * 
 * @see OStorageLocal, OStorageMemory
 * @author Luca Garulli
 * 
 */
public abstract class OStorageEmbedded extends OStorageAbstract {
	protected final ORecordLockManager	lockManager;

	public OStorageEmbedded(final String iName, final String iFilePath, final String iMode) {
		super(iName, iFilePath, iMode);
		lockManager = new ORecordLockManager(OGlobalConfiguration.STORAGE_RECORD_LOCK_TIMEOUT.getValueAsInteger());
	}

	protected abstract ORawBuffer readRecord(final OCluster iClusterSegment, final ORecordId iRid, boolean iAtomicLock);

	public abstract OCluster getClusterByName(final String iClusterName);

	/**
	 * Executes the command request and return the result back.
	 */
	public Object command(final OCommandRequestText iCommand) {
		final OCommandExecutor executor = OCommandManager.instance().getExecutor(iCommand);
		executor.setProgressListener(iCommand.getProgressListener());
		executor.parse(iCommand);
		try {
			final Object result = executor.execute(iCommand.getParameters());
			iCommand.setContext( executor.getContext() );
			return result;
		} catch (OException e) {
			// PASS THROUGHT
			throw e;
		} catch (Exception e) {
			throw new OCommandExecutionException("Error on execution of command: " + iCommand, e);
		}
	}

	/**
	 * Checks if the storage is open. If it's closed an exception is raised.
	 */
	protected void checkOpeness() {
		if (status != STATUS.OPEN)
			throw new OStorageException("Storage " + name + " is not opened.");
	}

	public <V> V callInLock(Callable<V> iCallable, boolean iExclusiveLock, List<ORID> ids) {
		final OLockManager.LOCK iLockType = (iExclusiveLock) ? OLockManager.LOCK.EXCLUSIVE : OLockManager.LOCK.SHARED;
		Collections.sort(ids);
		final List<ORID> lockedIds = new ArrayList<ORID>(ids.size());
		try {
			for (ORID id : ids) {
				lockManager.acquireLock(Thread.currentThread(), id, iLockType);
				lockedIds.add(id.copy());
			}
			return iCallable.call();
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new OException("Error on nested call in lock", e);
		} finally {
			for (ORID id : lockedIds) {
				lockManager.releaseLock(Thread.currentThread(), id, iLockType);
			}
		}
	}
}
