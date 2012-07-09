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
package com.orientechnologies.orient.core.engine.local;

import java.util.Map;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.engine.OEngineAbstract;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.core.type.tree.OBuddyMemory;
import com.orientechnologies.orient.core.type.tree.OMemory;

public class OEngineLocal extends OEngineAbstract {
  public static final String   NAME = "local";

  private static final OMemory MEMORY;

  static {
    final long maxMemory = Runtime.getRuntime().maxMemory();
    final int maxBuddyMemory;
    if (maxMemory * 0.25 > Integer.MAX_VALUE)
      maxBuddyMemory = Integer.MAX_VALUE;
    else
      maxBuddyMemory = (int) (0.25 * maxMemory);

    MEMORY = new OBuddyMemory(maxBuddyMemory, 32);
  }

  public OStorage createStorage(final String iDbName, final Map<String, String> iConfiguration) {
    try {
      // GET THE STORAGE
      return new OStorageLocal(iDbName, iDbName, getMode(iConfiguration), MEMORY);

    } catch (Throwable t) {
      OLogManager.instance().error(this,
          "Error on opening database: " + iDbName + ". Current location is: " + new java.io.File(".").getAbsolutePath(), t,
          ODatabaseException.class);
    }
    return null;
  }

  public String getName() {
    return NAME;
  }

  public boolean isShared() {
    return true;
  }
}
