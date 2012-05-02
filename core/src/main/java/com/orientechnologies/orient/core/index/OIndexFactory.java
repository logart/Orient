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
package com.orientechnologies.orient.core.index;

import java.util.Set;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OConfigurationException;

public interface OIndexFactory {

  /**
   * @return List of supported indexes of this factory
   */
  Set<String> getTypes();

  /**
   * 
   * @param iDatabase
   * @param iIndexType
   *          index type
   * @return OIndexInternal
   * @throws OConfigurationException
   *           if index creation failed
   */
  OIndexInternal<?> createIndex(ODatabaseRecord iDatabase, String iIndexType) throws OConfigurationException;

}
