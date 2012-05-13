/*
 * Copyright 1999-2011 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.server.replication;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.server.clustering.OClusterLogger;
import com.orientechnologies.orient.server.clustering.OClusterLogger.DIRECTION;
import com.orientechnologies.orient.server.clustering.OClusterLogger.TYPE;
import com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.STATUS_TYPE;
import com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.SYNCH_TYPE;

/**
 * Represents a member of the cluster.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ODistributedNode {
  private final OReplicator                     replicator;
  private final String                          id;
  public String                                 networkAddress;
  public int                                    networkPort;
  public Date                                   connectedOn;
  private Map<String, ODistributedDatabaseInfo> databases = new HashMap<String, ODistributedDatabaseInfo>();
  protected OClusterLogger                      logger    = new OClusterLogger();

  public ODistributedNode(final OReplicator iReplicator, final String iId) throws IOException {
    replicator = iReplicator;
    id = iId;

    final String[] parts = iId.split(":");
    networkAddress = parts[0];
    networkPort = Integer.parseInt(parts[1]);
    logger.setNode(iId);
  }

  public ODistributedDatabaseInfo getDatabase(final String iDatabaseName) {
    return databases.get(iDatabaseName);
  }

  public ODistributedDatabaseInfo removeDatabase(final String iDatabaseName) throws IOException {
    final ODistributedDatabaseInfo db = databases.remove(iDatabaseName);
    if (db != null)
      db.close();
    return db;
  }

  public ODistributedDatabaseInfo getOrCreateDatabaseEntry(final String iDatabaseName) throws IOException {
    ODistributedDatabaseInfo db = databases.get(iDatabaseName);
    if (db == null)
      db = createDatabaseEntry(iDatabaseName, SYNCH_TYPE.SYNCH);
    return db;
  }

  protected ODistributedDatabaseInfo createDatabaseEntry(final String dbName, SYNCH_TYPE iSynchType) throws IOException {
    ODistributedDatabaseInfo obj = databases.get(dbName);
    if (obj != null) {
      // CLOSE AND REMOVE IT BEFORE TO RESTART
      removeDatabase(dbName);
    }

    obj = new ODistributedDatabaseInfo(id, dbName, replicator.getReplicatorUser().name, replicator.getReplicatorUser().password,
        iSynchType, STATUS_TYPE.OFFLINE);
    databases.put(dbName, obj);

    return obj;
  }

  public void startDatabaseReplication(final ODistributedDatabaseInfo iDatabase) throws IOException {
    if (iDatabase == null)
      throw new IllegalArgumentException("Database is null");

    logger.setDatabase(iDatabase.databaseName);

    synchronized (this) {
      logger.log(this, Level.WARNING, TYPE.REPLICATION, DIRECTION.OUT, "starting replication against distributed node");

      try {
        databases.put(iDatabase.databaseName, iDatabase);

        if (iDatabase.connection == null)
          iDatabase.connection = new ONodeConnection(replicator, id, replicator.getConflictResolver());

        iDatabase.connection.synchronize(iDatabase.databaseName, replicator.getLocalDatabaseConfiguration(iDatabase.databaseName));
        iDatabase.setOnline();

      } catch (Exception e) {
        removeDatabase(iDatabase.databaseName);
        logger.log(this, Level.WARNING, TYPE.REPLICATION, DIRECTION.NONE,
            "cannot find database on remote server. Removing it from shared list", e);
      }
    }
  }

  public void stopDatabaseReplication(final ODistributedDatabaseInfo iDatabase) {
    synchronized (this) {
      logger.setDatabase(iDatabase.databaseName);

      iDatabase.setOffline();

      logger.log(this, Level.WARNING, TYPE.REPLICATION, DIRECTION.OUT, "stopped replication against distributed node");
    }
  }

  public void startDatabaseAlignment(final ODistributedDatabaseInfo iDatabase, final String iOptions) throws IOException {
    if (iDatabase == null)
      throw new IllegalArgumentException("Database is null");

    logger.setDatabase(iDatabase.databaseName);

    synchronized (this) {
      if (!iDatabase.isOnline())
        throw new IllegalArgumentException("Database '" + iDatabase.databaseName + "' is not replicated");

      logger.log(this, Level.WARNING, TYPE.REPLICATION, DIRECTION.OUT, "starting alignment against distributed node");

      try {
        databases.put(iDatabase.databaseName, iDatabase);

        if (iDatabase.connection == null)
          iDatabase.connection = new ONodeConnection(replicator, id, replicator.getConflictResolver());

        iDatabase.setSynchronizing();
        iDatabase.connection.align(iDatabase.databaseName, iOptions);
        iDatabase.setOnline();

      } catch (Exception e) {
        removeDatabase(iDatabase.databaseName);
        logger.log(this, Level.WARNING, TYPE.REPLICATION, DIRECTION.NONE,
            "cannot find database on remote server. Removing it from shared list", e);
      }
    }
  }

  public void propagateChange(final ORecordOperation iRequest, final SYNCH_TYPE iRequestType, final boolean iResume)
      throws IOException {
    final ORecordInternal<?> record = iRequest.getRecord();
    if (record == null)
      // RECORD DOESN'T EXIST ANYMORE
      return;

    final ODistributedDatabaseInfo databaseEntry = databases.get(record.getDatabase().getName());
    if (databaseEntry == null)
      return;

    try {
      databaseEntry.connection.propagateChange(databaseEntry, iRequest, iRequestType, record);

    } catch (Exception e) {
      handleError(iRequest, iRequestType, e, iResume);
    }
  }

  public ORecord<?> requestRecord(final String iDatabaseName, final ORecordId rid) {
    final ODistributedDatabaseInfo databaseEntry = databases.get(iDatabaseName);
    if (databaseEntry != null)
      try {
        return databaseEntry.connection.requestRecord(databaseEntry, rid);

      } catch (Exception e) {
        logger.log(this, Level.INFO, TYPE.REPLICATION, DIRECTION.IN, "Error on retrieving record %s from remote server", rid);
      }
    return null;
  }

  public ODistributedDatabaseInfo copyDatabase(final ODatabaseRecord iDb, final String iRemoteEngine) throws IOException {
    ODistributedDatabaseInfo db = getDatabase(iDb.getName());

    if (db != null && db.isOnline())
      throw new ODistributedSynchronizationException("Database '" + iDb.getName() + "' is already shared on remote server node '"
          + id + "'");

    db = createDatabaseEntry(iDb.getName(), SYNCH_TYPE.SYNCH);

    final long time = System.currentTimeMillis();

    try {
      logger.log(this, Level.INFO, TYPE.REPLICATION, DIRECTION.OUT,
          "copying database to the remote server via streaming across the network...");

      db.setSynchronizing();
      if (db.connection == null)
        db.connection = new ONodeConnection(replicator, id, replicator.getConflictResolver());
      db.connection.copy(iDb, db.databaseName, db.userName, db.userPassword, iRemoteEngine);

    } catch (IOException e) {
      // ERROR
      removeDatabase(iDb.getName());
      throw e;

    } catch (Exception e) {
      // ERROR
      removeDatabase(iDb.getName());

      logger.log(this, Level.INFO, TYPE.REPLICATION, DIRECTION.OUT, "Error on copying database");
      throw new OIOException("Error on copying database", e);
    }

    logger.log(this, Level.INFO, TYPE.REPLICATION, DIRECTION.NONE, "sharing completed (%dms)", System.currentTimeMillis() - time);

    return db;
  }

  @Override
  public String toString() {
    return id;
  }

  public String getName() {
    return id;
  }

  /**
   * Closes all the opened databases
   * 
   * @throws IOException
   */
  public void disconnect() throws IOException {
    for (ODistributedDatabaseInfo db : databases.values()) {
      db.close();
      if (db.connection != null)
        db.connection.disconnect();
    }
    databases.clear();
  }

  public long[] getLogRange(final String iDatabaseName) throws IOException {
    return new long[] { databases.get(iDatabaseName).getLog().getFirstOperationId(),
        databases.get(iDatabaseName).getLog().getLastOperationId() };
  }

  protected void handleError(final ORecordOperation iRequest, final SYNCH_TYPE iRequestType, final Exception iException,
      final boolean iResume) throws RuntimeException, IOException {

    final Set<ODistributedDatabaseInfo> currentDbList = new HashSet<ODistributedDatabaseInfo>(databases.values());

    disconnect();

    // ERROR
    logger.log(this, Level.WARNING, TYPE.REPLICATION, DIRECTION.NONE, "seems down, retrying to connect...");

    // RECONNECT ALL DATABASES
    if (iResume)
      try {
        for (ODistributedDatabaseInfo dbEntry : currentDbList) {
          startDatabaseReplication(dbEntry);
        }
      } catch (IOException e) {
        // IO ERROR: THE NODE SEEMED ALWAYS MORE DOWN: START TO COLLECT DATA FOR IT WAITING FOR A FUTURE RE-CONNECTION
        logger.log(this, Level.WARNING, TYPE.REPLICATION, DIRECTION.NONE, "is down, remove it from replication");
      }

    if (iRequestType == SYNCH_TYPE.SYNCH) {
      // SYNCHRONOUS CASE: RE-THROW THE EXCEPTION NOW TO BEING PROPAGATED UP TO THE CLIENT
      if (iException instanceof RuntimeException)
        throw (RuntimeException) iException;
    }
  }

  public void registerDatabase(final ODistributedDatabaseInfo iDatabaseEntry) throws IOException {
    databases.put(iDatabaseEntry.databaseName, iDatabaseEntry);
  }
}
