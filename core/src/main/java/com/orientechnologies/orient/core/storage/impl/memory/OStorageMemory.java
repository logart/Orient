/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.storage.impl.memory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.concur.lock.OLockManager.LOCK;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.OFastConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.ODataSegment;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.core.storage.impl.local.OStorageConfigurationSegment;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionAbstract;
import com.orientechnologies.orient.core.tx.OTxListener;

/**
 * Memory implementation of storage. This storage works only in memory and has the following features:
 * <ul>
 * <li>The name is "Memory"</li>
 * <li>Has a unique Data Segment</li>
 * </ul>
 * 
 * @author Luca Garulli
 * 
 */
public class OStorageMemory extends OStorageEmbedded {
  private final List<ODataSegmentMemory>    dataSegments     = new ArrayList<ODataSegmentMemory>();
  private final List<OClusterMemory>        clusters         = new ArrayList<OClusterMemory>();
  private final Map<String, OClusterMemory> clusterMap       = new HashMap<String, OClusterMemory>();
  private int                               defaultClusterId = 0;

  public OStorageMemory(final String iURL) {
    super(iURL, iURL, "rw");
    configuration = new OStorageConfiguration(this);
  }

  public void create(final Map<String, Object> iOptions) {
    addUser();

    lock.acquireExclusiveLock();
    try {

      addDataSegment(OStorage.DATA_DEFAULT_NAME);

      // ADD THE METADATA CLUSTER TO STORE INTERNAL STUFF
      addCluster(CLUSTER_TYPE.PHYSICAL.toString(), OStorage.CLUSTER_INTERNAL_NAME, null, null);

      // ADD THE INDEX CLUSTER TO STORE, BY DEFAULT, ALL THE RECORDS OF INDEXING
      addCluster(CLUSTER_TYPE.PHYSICAL.toString(), OStorage.CLUSTER_INDEX_NAME, null, null);

      // ADD THE DEFAULT CLUSTER
      defaultClusterId = addCluster(CLUSTER_TYPE.PHYSICAL.toString(), OStorage.CLUSTER_DEFAULT_NAME, null, null);

      configuration.create();

      status = STATUS.OPEN;

    } catch (OStorageException e) {
      close();
      throw e;

    } catch (IOException e) {
      close();
      throw new OStorageException("Error on creation of storage: " + name, e);

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public void open(final String iUserName, final String iUserPassword, final Map<String, Object> iOptions) {
    addUser();

    if (status == STATUS.OPEN)
      // ALREADY OPENED: THIS IS THE CASE WHEN A STORAGE INSTANCE IS
      // REUSED
      return;

    lock.acquireExclusiveLock();
    try {

      if (!exists())
        throw new OStorageException("Cannot open the storage '" + name + "' because it does not exist in path: " + url);

      status = STATUS.OPEN;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public void close(final boolean iForce) {
    lock.acquireExclusiveLock();
    try {

      if (!checkForClose(iForce))
        return;

      status = STATUS.CLOSING;

      // CLOSE ALL THE CLUSTERS
      for (OClusterMemory c : clusters)
        if (c != null)
          c.close();
      clusters.clear();
      clusterMap.clear();

      // CLOSE THE DATA SEGMENTS
      for (ODataSegmentMemory d : dataSegments)
        if (d != null)
          d.close();
      dataSegments.clear();

      level2Cache.shutdown();

      super.close(iForce);

      Orient.instance().unregisterStorage(this);
      status = STATUS.CLOSED;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public void delete() {
    close(true);
  }

  public void reload() {
  }

  public int addCluster(final String iClusterType, String iClusterName, final String iLocation, final String iDataSegmentName,
      final Object... iParameters) {
    iClusterName = iClusterName.toLowerCase();
    lock.acquireExclusiveLock();
    try {
      int clusterId = clusters.size();
      for (int i = 0; i < clusters.size(); ++i) {
        if (clusters.get(i) == null) {
          clusterId = i;
          break;
        }
      }

      final OClusterMemory cluster = (OClusterMemory) Orient.instance().getClusterFactory().createCluster(OClusterMemory.TYPE);
      cluster.configure(this, clusterId, iClusterName, iLocation, getDataSegmentIdByName(iDataSegmentName), iParameters);

      if (clusterId == clusters.size())
        // APPEND IT
        clusters.add(cluster);
      else
        // RECYCLE THE FREE POSITION
        clusters.set(clusterId, cluster);
      clusterMap.put(iClusterName, cluster);

      return clusterId;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public boolean dropCluster(final int iClusterId) {
    lock.acquireExclusiveLock();
    try {

      final OCluster c = clusters.get(iClusterId);
      if (c != null) {
        c.delete();
        clusters.set(iClusterId, null);
        getLevel2Cache().freeCluster(iClusterId);
        clusterMap.remove(c.getName());
      }

    } catch (IOException e) {
    } finally {

      lock.releaseExclusiveLock();
    }

    return false;
  }

  public boolean dropDataSegment(final String iName) {
    lock.acquireExclusiveLock();
    try {

      final int id = getDataSegmentIdByName(iName);
      final ODataSegment data = dataSegments.get(id);
      if (data == null)
        return false;

      data.drop();

      dataSegments.set(id, null);

      // UPDATE CONFIGURATION
      configuration.dropCluster(id);

      return true;
    } catch (Exception e) {
      OLogManager.instance().exception("Error while removing data segment '" + iName + "'", e, OStorageException.class);

    } finally {
      lock.releaseExclusiveLock();
    }

    return false;
  }

  public int addDataSegment(final String iDataSegmentName) {
    lock.acquireExclusiveLock();
    try {
      int pos = -1;
      for (int i = 0; i < dataSegments.size(); ++i) {
        if (dataSegments.get(i) == null) {
          pos = i;
          break;
        }
      }

      if (pos == -1)
        pos = dataSegments.size();

      final ODataSegmentMemory dataSegment = new ODataSegmentMemory(iDataSegmentName, pos);

      if (pos == dataSegments.size())
        dataSegments.add(dataSegment);
      else
        dataSegments.set(pos, dataSegment);

      return pos;

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public int addDataSegment(final String iSegmentName, final String iLocation) {
    return addDataSegment(iSegmentName);
  }

  public OPhysicalPosition createRecord(final int iDataSegmentId, final ORecordId iRid, final byte[] iContent, int iRecordVersion,
      final byte iRecordType, final int iMode, ORecordCallback<Long> iCallback) {
    final long timer = OProfiler.getInstance().startChrono();

    lock.acquireSharedLock();
    try {
      final ODataSegmentMemory data = (ODataSegmentMemory) getDataSegmentById(iDataSegmentId);

      final long offset = data.createRecord(iContent);
      final OCluster cluster = getClusterById(iRid.clusterId);

      // ASSIGN THE POSITION IN THE CLUSTER
      final OPhysicalPosition ppos = new OPhysicalPosition(iDataSegmentId, offset, iRecordType);
      cluster.addPhysicalPosition(ppos);
      iRid.clusterPosition = ppos.clusterPosition;

      if (iCallback != null)
        iCallback.call(iRid, iRid.clusterPosition);

      return ppos;
    } catch (IOException e) {
      throw new OStorageException("Error on create record in cluster: " + iRid.clusterId, e);

    } finally {
      lock.releaseSharedLock();
      OProfiler.getInstance().stopChrono(PROFILER_CREATE_RECORD, timer);
    }
  }

  public ORawBuffer readRecord(final ORecordId iRid, String iFetchPlan, boolean iIgnoreCache, ORecordCallback<ORawBuffer> iCallback) {
    return readRecord(getClusterById(iRid.clusterId), iRid, true);
  }

  @Override
  protected ORawBuffer readRecord(final OCluster iClusterSegment, final ORecordId iRid, final boolean iAtomicLock) {
    final long timer = OProfiler.getInstance().startChrono();

    lock.acquireSharedLock();

    try {

      lockManager.acquireLock(Thread.currentThread(), iRid, LOCK.SHARED);

      try {
        final long lastPos = iClusterSegment.getLastEntryPosition();

        if (iRid.clusterPosition > lastPos)
          throw new ORecordNotFoundException("Record " + iRid + " is outside cluster size. Valid range for cluster '"
              + iClusterSegment.getName() + "' is 0-" + lastPos);

        final OPhysicalPosition ppos = iClusterSegment.getPhysicalPosition(new OPhysicalPosition(iRid.clusterPosition));

        if (ppos == null)
          return null;

        final ODataSegmentMemory dataSegment = (ODataSegmentMemory) getDataSegmentById(ppos.dataSegmentId);

        return new ORawBuffer(dataSegment.readRecord(ppos.dataSegmentPos), ppos.recordVersion, ppos.recordType);

      } finally {
        lockManager.releaseLock(Thread.currentThread(), iRid, LOCK.SHARED);
      }
    } catch (IOException e) {
      throw new OStorageException("Error on read record in cluster: " + iClusterSegment.getId(), e);

    } finally {
      lock.releaseSharedLock();
      OProfiler.getInstance().stopChrono(PROFILER_READ_RECORD, timer);
    }
  }

  public int updateRecord(final ORecordId iRid, final byte[] iContent, final int iVersion, final byte iRecordType, final int iMode,
      ORecordCallback<Integer> iCallback) {
    final long timer = OProfiler.getInstance().startChrono();

    final OCluster cluster = getClusterById(iRid.clusterId);

    lock.acquireSharedLock();
    try {

      lockManager.acquireLock(Thread.currentThread(), iRid, LOCK.EXCLUSIVE);
      try {

        final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(iRid.clusterPosition));
        if (ppos == null) {
          if (iCallback != null)
            iCallback.call(iRid, -1);
          return -1;
        }

        if (iVersion != -1) {
          if (iVersion > -1) {
            // MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
            if (iVersion != ppos.recordVersion)
              if (OFastConcurrentModificationException.enabled())
                throw OFastConcurrentModificationException.instance();
              else
                throw new OConcurrentModificationException(iRid, ppos.recordVersion, iVersion);

            ++ppos.recordVersion;
          } else
            --ppos.recordVersion;
        }

        final ODataSegmentMemory dataSegment = (ODataSegmentMemory) getDataSegmentById(ppos.dataSegmentId);
        dataSegment.updateRecord(ppos.dataSegmentPos, iContent);

        if (iCallback != null)
          iCallback.call(null, ppos.recordVersion);

        return ppos.recordVersion;

      } finally {
        lockManager.releaseLock(Thread.currentThread(), iRid, LOCK.EXCLUSIVE);
      }
    } catch (IOException e) {
      throw new OStorageException("Error on update record " + iRid, e);

    } finally {
      lock.releaseSharedLock();
      OProfiler.getInstance().stopChrono(PROFILER_UPDATE_RECORD, timer);
    }
  }

  public boolean deleteRecord(final ORecordId iRid, final int iVersion, final int iMode, ORecordCallback<Boolean> iCallback) {
    final long timer = OProfiler.getInstance().startChrono();

    final OCluster cluster = getClusterById(iRid.clusterId);

    lock.acquireSharedLock();
    try {

      lockManager.acquireLock(Thread.currentThread(), iRid, LOCK.EXCLUSIVE);
      try {

        final OPhysicalPosition ppos = cluster.getPhysicalPosition(new OPhysicalPosition(iRid.clusterPosition));

        if (ppos == null) {
          if (iCallback != null)
            iCallback.call(iRid, false);
          return false;
        }

        // MVCC TRANSACTION: CHECK IF VERSION IS THE SAME
        if (iVersion > -1 && ppos.recordVersion != iVersion)
          if (OFastConcurrentModificationException.enabled())
            throw OFastConcurrentModificationException.instance();
          else
            throw new OConcurrentModificationException(iRid, ppos.recordVersion, iVersion);

        cluster.removePhysicalPosition(iRid.clusterPosition);

        final ODataSegmentMemory dataSegment = (ODataSegmentMemory) getDataSegmentById(ppos.dataSegmentId);
        dataSegment.deleteRecord(ppos.dataSegmentPos);

        if (iCallback != null)
          iCallback.call(null, true);

        return true;

      } finally {
        lockManager.releaseLock(Thread.currentThread(), iRid, LOCK.EXCLUSIVE);
      }

    } catch (IOException e) {
      throw new OStorageException("Error on delete record " + iRid, e);

    } finally {
      lock.releaseSharedLock();
      OProfiler.getInstance().stopChrono(PROFILER_DELETE_RECORD, timer);
    }
  }

  public long count(final int iClusterId) {
    final OCluster cluster = getClusterById(iClusterId);

    lock.acquireSharedLock();
    try {

      return cluster.getEntries();

    } finally {
      lock.releaseSharedLock();
    }
  }

  public long[] getClusterDataRange(final int iClusterId) {
    final OCluster cluster = getClusterById(iClusterId);
    lock.acquireSharedLock();
    try {

      return new long[] { cluster.getFirstEntryPosition(), cluster.getLastEntryPosition() };

    } finally {
      lock.releaseSharedLock();
    }
  }

  public long count(final int[] iClusterIds) {
    lock.acquireSharedLock();
    try {

      long tot = 0;
      for (int i = 0; i < iClusterIds.length; ++i) {
        final OCluster cluster = clusters.get(iClusterIds[i]);

        if (cluster != null)
          tot += cluster.getEntries();
      }
      return tot;

    } finally {
      lock.releaseSharedLock();
    }
  }

  public OCluster getClusterByName(final String iClusterName) {
    lock.acquireSharedLock();
    try {

      return clusterMap.get(iClusterName.toLowerCase());

    } finally {
      lock.releaseSharedLock();
    }
  }

  public int getClusterIdByName(String iClusterName) {
    iClusterName = iClusterName.toLowerCase();

    lock.acquireSharedLock();
    try {

      final OCluster cluster = clusterMap.get(iClusterName.toLowerCase());
      if (cluster == null)
        return -1;
      return cluster.getId();

    } finally {
      lock.releaseSharedLock();
    }
  }

  public String getClusterTypeByName(final String iClusterName) {
    return OClusterMemory.TYPE;
  }

  public String getPhysicalClusterNameById(final int iClusterId) {
    lock.acquireSharedLock();
    try {

      for (int i = 0; i < clusters.size(); ++i) {
        final OCluster cluster = clusters.get(i);

        if (cluster != null && cluster.getId() == iClusterId)
          return cluster.getName();
      }
      return null;

    } finally {
      lock.releaseSharedLock();
    }
  }

  public Set<String> getClusterNames() {
    lock.acquireSharedLock();
    try {

      return new HashSet<String>(clusterMap.keySet());

    } finally {
      lock.releaseSharedLock();
    }
  }

  public void commit(final OTransaction iTx) {
    lock.acquireExclusiveLock();
    try {

      final List<ORecordOperation> tmpEntries = new ArrayList<ORecordOperation>();

      while (iTx.getCurrentRecordEntries().iterator().hasNext()) {
        for (ORecordOperation txEntry : iTx.getCurrentRecordEntries())
          tmpEntries.add(txEntry);

        iTx.clearRecordEntries();

        for (ORecordOperation txEntry : tmpEntries)
          // COMMIT ALL THE SINGLE ENTRIES ONE BY ONE
          commitEntry(iTx, txEntry);

        tmpEntries.clear();
      }

      // UPDATE THE CACHE ONLY IF THE ITERATOR ALLOWS IT
      OTransactionAbstract.updateCacheFromEntries(this, iTx, iTx.getAllRecordEntries(), true);
    } catch (IOException e) {
      rollback(iTx);

    } finally {
      lock.releaseExclusiveLock();
    }
  }

  public void rollback(final OTransaction iTx) {
  }

  public void synch() {
  }

  public boolean exists() {
    lock.acquireSharedLock();
    try {

      return !clusters.isEmpty();

    } finally {
      lock.releaseSharedLock();
    }
  }

  public ODataSegment getDataSegmentById(int iDataId) {
    lock.acquireSharedLock();
    try {

      if (iDataId < 0 || iDataId > dataSegments.size() - 1)
        throw new IllegalArgumentException("Invalid data segment id " + iDataId + ". Range is 0-" + (dataSegments.size() - 1));

      return dataSegments.get(iDataId);

    } finally {
      lock.releaseSharedLock();
    }
  }

  public int getDataSegmentIdByName(final String iDataSegmentName) {
    if (iDataSegmentName == null)
      return 0;

    lock.acquireSharedLock();
    try {

      for (ODataSegmentMemory d : dataSegments)
        if (d != null && d.getName().equalsIgnoreCase(iDataSegmentName))
          return d.getId();

      throw new IllegalArgumentException("Data segment '" + iDataSegmentName + "' does not exist in storage '" + name + "'");

    } finally {
      lock.releaseSharedLock();
    }
  }

  public OCluster getClusterById(int iClusterId) {
    lock.acquireSharedLock();
    try {

      if (iClusterId == ORID.CLUSTER_ID_INVALID)
        // GET THE DEFAULT CLUSTER
        iClusterId = defaultClusterId;

      return clusters.get(iClusterId);

    } finally {
      lock.releaseSharedLock();
    }
  }

  public int getClusters() {
    lock.acquireSharedLock();
    try {

      return clusterMap.size();

    } finally {
      lock.releaseSharedLock();
    }
  }

  public Collection<? extends OCluster> getClusterInstances() {
    lock.acquireSharedLock();
    try {

      return Collections.unmodifiableCollection(clusters);

    } finally {
      lock.releaseSharedLock();
    }
  }

  public int getDefaultClusterId() {
    return defaultClusterId;
  }

  public long getSize() {
    long size = 0;

    lock.acquireSharedLock();
    try {
      for (ODataSegmentMemory d : dataSegments)
        if (d != null)
          size += d.getSize();

    } finally {
      lock.releaseSharedLock();
    }
    return size;
  }

  @Override
  public boolean checkForRecordValidity(final OPhysicalPosition ppos) {
    if (ppos.dataSegmentId > 0)
      return false;

    lock.acquireSharedLock();
    try {
      final ODataSegmentMemory dataSegment = (ODataSegmentMemory) getDataSegmentById(ppos.dataSegmentId);
      if (ppos.dataSegmentPos >= dataSegment.count())
        return false;

    } finally {
      lock.releaseSharedLock();
    }
    return true;
  }

  private void commitEntry(final OTransaction iTx, final ORecordOperation txEntry) throws IOException {

    final ORecordId rid = (ORecordId) txEntry.getRecord().getIdentity();

    final OCluster cluster = getClusterById(rid.clusterId);
    rid.clusterId = cluster.getId();

    if (txEntry.getRecord() instanceof OTxListener)
      ((OTxListener) txEntry.getRecord()).onEvent(txEntry, OTxListener.EVENT.BEFORE_COMMIT);

    switch (txEntry.type) {
    case ORecordOperation.LOADED:
      break;

    case ORecordOperation.CREATED:
      if (rid.isNew()) {
        // CHECK 2 TIMES TO ASSURE THAT IT'S A CREATE OR AN UPDATE BASED ON RECURSIVE TO-STREAM METHOD
        byte[] stream = txEntry.getRecord().toStream();

        if (rid.isNew()) {
          txEntry.getRecord().onBeforeIdentityChanged(rid);
          final OPhysicalPosition ppos = createRecord(txEntry.dataSegmentId, rid, stream, 0, txEntry.getRecord().getRecordType(),
              0, null);
          txEntry.getRecord().setVersion(ppos.recordVersion);
          txEntry.getRecord().onAfterIdentityChanged(txEntry.getRecord());
        } else {
          txEntry.getRecord().setVersion(
              updateRecord(rid, stream, txEntry.getRecord().getVersion(), txEntry.getRecord().getRecordType(), 0, null));
        }
      }
      break;

    case ORecordOperation.UPDATED:
      byte[] stream = txEntry.getRecord().toStream();

      txEntry.getRecord().setVersion(
          updateRecord(rid, stream, txEntry.getRecord().getVersion(), txEntry.getRecord().getRecordType(), 0, null));
      break;

    case ORecordOperation.DELETED:
      deleteRecord(rid, txEntry.getRecord().getVersion(), 0, null);
      break;
    }

    txEntry.getRecord().unsetDirty();

    if (txEntry.getRecord() instanceof OTxListener)
      ((OTxListener) txEntry.getRecord()).onEvent(txEntry, OTxListener.EVENT.AFTER_COMMIT);
  }

  @Override
  public String getURL() {
    return OEngineMemory.NAME + ":" + url;
  }

  public OStorageConfigurationSegment getConfigurationSegment() {
    return null;
  }

  public void renameCluster(final String iOldName, final String iNewName) {
    final OClusterMemory cluster = (OClusterMemory) getClusterByName(iOldName);
    if (cluster != null)
      try {
        cluster.set(com.orientechnologies.orient.core.storage.OCluster.ATTRIBUTES.NAME, iNewName);
      } catch (IOException e) {
      }
  }

  public void setDefaultClusterId(int defaultClusterId) {
    this.defaultClusterId = defaultClusterId;
  }
}
