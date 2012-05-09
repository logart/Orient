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
package com.orientechnologies.orient.core.sql;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

/**
 * SQL TRUNCATE RECORD command: Truncates a record without loading it. Useful when the record is dirty in any way and cannot be
 * loaded correctly.
 * 
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorSQLTruncateRecord extends OCommandExecutorSQLAbstract {
  public static final String KEYWORD_TRUNCATE = "TRUNCATE";
  public static final String KEYWORD_RECORD   = "RECORD";
  private Set<String>        records          = new HashSet<String>();

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLTruncateRecord parse(final OCommandRequest iRequest) {
    getDatabase().checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    init(((OCommandRequestText) iRequest).getText());

    StringBuilder word = new StringBuilder();

    int oldPos = 0;
    int pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_TRUNCATE))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_TRUNCATE + " not found. Use " + getSyntax(), text, oldPos);

    oldPos = pos;
    pos = OSQLHelper.nextWord(text, textUpperCase, oldPos, word, true);
    if (pos == -1 || !word.toString().equals(KEYWORD_RECORD))
      throw new OCommandSQLParsingException("Keyword " + KEYWORD_RECORD + " not found. Use " + getSyntax(), text, oldPos);

    oldPos = pos;
    pos = OSQLHelper.nextWord(text, text, oldPos, word, true);
    if (pos == -1)
      throw new OCommandSQLParsingException("Expected one or more records. Use " + getSyntax(), text, oldPos);

    if (word.charAt(0) == '[')
      // COLLECTION
      OStringSerializerHelper.getCollection(text, oldPos, records);
    else {
      records.add(word.toString());
    }

    if (records.isEmpty())
      throw new OCommandSQLParsingException("Missed record(s). Use " + getSyntax(), text, oldPos);
    return this;
  }

  /**
   * Execute the command.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (records.isEmpty())
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    final ODatabaseRecord database = getDatabase();
    for (String rec : records) {
      try {
        final ORecordId rid = new ORecordId(rec);
        database.getStorage().deleteRecord(rid, -1, 0, null);
      } catch (Throwable e) {
        throw new OCommandExecutionException("Error on executing command", e);
      }
    }

    return records.size();
  }

  @Override
  public String getSyntax() {
    return "TRUNCATE RECORD <rid>*";
  }
}
