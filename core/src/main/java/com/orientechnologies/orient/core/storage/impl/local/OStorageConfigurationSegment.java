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
package com.orientechnologies.orient.core.storage.impl.local;

import java.io.IOException;

import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageFileConfiguration;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.storage.ORawBuffer;

/**
 * Handles the database configuration in one big record.
 */
@SuppressWarnings("serial")
public class OStorageConfigurationSegment extends OStorageConfiguration {
  private static final int   START_SIZE = 10000;
  private OSingleFileSegment segment;

  public OStorageConfigurationSegment(final OStorageLocal iStorage) throws IOException {
    super(iStorage);
    segment = new OSingleFileSegment((OStorageLocal) storage, new OStorageFileConfiguration(null, getDirectory() + "/database.ocf",
        "classic", fileTemplate.maxSize, fileTemplate.fileIncrementSize));
  }

  public void close() throws IOException {
    segment.close();
  }

  public void create() throws IOException {
    segment.create(START_SIZE);
    super.create();
  }

  @Override
  public OStorageConfiguration load() throws OSerializationException {
    try {
      if (segment.getFile().exists())
        segment.open();
      else {
        segment.create(START_SIZE);

        // @COMPATIBILITY0.9.25
        // CHECK FOR OLD VERSION OF DATABASE
        final ORawBuffer rawRecord = storage.readRecord(CONFIG_RID, null, false, null);
        if (rawRecord != null)
          fromStream(rawRecord.buffer);

        update();
        return this;
      }

      final int size = segment.getFile().readInt(0);
      byte[] buffer = new byte[size];
      segment.getFile().read(OBinaryProtocol.SIZE_INT, buffer, size);

      fromStream(buffer);
    } catch (Exception e) {
      throw new OSerializationException("Cannot load database's configuration. The database seems to be corrupted.", e);
    }
    return this;
  }

  @Override
  public void update() throws OSerializationException {
    try {
      if (!segment.getFile().isOpen())
        return;

      final byte[] buffer = toStream();

      final int len = buffer.length + OBinaryProtocol.SIZE_INT;

      if (len > segment.getFile().getFilledUpTo())
        segment.getFile().allocateSpace(len - segment.getFile().getFilledUpTo());

      segment.getFile().writeInt(0, buffer.length);
      segment.getFile().write(OBinaryProtocol.SIZE_INT, buffer);
    } catch (Exception e) {
      throw new OSerializationException("Error on update storage configuration", e);
    }
  }

  public void synch() throws IOException {
    segment.getFile().synch();
  }

  @Override
  public void setSoftlyClosed(boolean softlyClosed) throws IOException {
    segment.getFile().setSoftlyClosed(softlyClosed);
  }
}
