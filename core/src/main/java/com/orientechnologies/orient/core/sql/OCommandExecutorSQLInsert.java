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
package com.orientechnologies.orient.core.sql;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

/**
 * SQL INSERT command.
 * 
 * @author Luca Garulli
 * @author Johann Sorel (Geomatys)
 */
public class OCommandExecutorSQLInsert extends OCommandExecutorSQLSetAware {
  public static final String        KEYWORD_INSERT = "INSERT";
  private static final String       KEYWORD_VALUES = "VALUES";
  private static final String       KEYWORD_INTO   = "INTO";
  private static final String       KEYWORD_SET    = "SET";
  private String                    className      = null;
  private String                    clusterName    = null;
  private String                    indexName      = null;
  private List<Map<String, Object>> newRecords;

  @SuppressWarnings("unchecked")
  public OCommandExecutorSQLInsert parse(final OCommandRequest iRequest) {
    final ODatabaseRecord database = getDatabase();
    database.checkSecurity(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);

    init(((OCommandRequestText) iRequest).getText());

    className = null;
    newRecords = null;

    parseRequiredWords("INSERT", "INTO");

    String subjectName = parseRequiredWord(true, "Invalid subject name. Expected cluster, class or index");
    if (subjectName.startsWith(OCommandExecutorSQLAbstract.CLUSTER_PREFIX))
      // CLUSTER
      clusterName = subjectName.substring(OCommandExecutorSQLAbstract.CLUSTER_PREFIX.length());

    else if (subjectName.startsWith(OCommandExecutorSQLAbstract.INDEX_PREFIX))
      // INDEX
      indexName = subjectName.substring(OCommandExecutorSQLAbstract.INDEX_PREFIX.length());

    else {
      // CLASS
      if (subjectName.startsWith(OCommandExecutorSQLAbstract.CLASS_PREFIX))
        subjectName = subjectName.substring(OCommandExecutorSQLAbstract.CLASS_PREFIX.length());

      final OClass cls = database.getMetadata().getSchema().getClass(subjectName);
      if (cls == null)
        throw new OCommandSQLParsingException("Class " + subjectName + " not found in database", text, currentPos);

      className = cls.getName();
    }

    final int beginFields = OStringParser.jumpWhiteSpaces(text, currentPos);
    if (beginFields == -1 || (text.charAt(beginFields) != '(' && !text.startsWith(KEYWORD_SET, beginFields)))
      throw new OCommandSQLParsingException("Set of fields is missed. Example: (name, surname) or SET name = 'Bill'. Use "
          + getSyntax(), text, currentPos);

    newRecords = new ArrayList<Map<String, Object>>();
    if (text.charAt(beginFields) == '(') {
      parseBracesFields(beginFields);
    } else {
      final LinkedHashMap<String, Object> fields = new LinkedHashMap<String, Object>();
      newRecords.add(fields);

      // ADVANCE THE GET KEYWORD
      parseRequiredWord(false);

      parseSetFields(fields);
    }

    return this;
  }

  protected void parseBracesFields(final int beginFields) {
    int pos;
    final int endFields = text.indexOf(')', beginFields + 1);
    if (endFields == -1)
      throw new OCommandSQLParsingException("Missed closed brace. Use " + getSyntax(), text, beginFields);

    final ArrayList<String> fieldNames = new ArrayList<String>();
    OStringSerializerHelper.getParameters(text, beginFields, endFields, fieldNames);
    if (fieldNames.size() == 0)
      throw new OCommandSQLParsingException("Set of fields is empty. Example: (name, surname). Use " + getSyntax(), text, endFields);

    // REMOVE QUOTATION MARKS IF ANY
    for (int i = 0; i < fieldNames.size(); ++i)
      fieldNames.set(i, OStringSerializerHelper.removeQuotationMarks(fieldNames.get(i)));

    pos = OSQLHelper.nextWord(text, textUpperCase, endFields + 1, tempParseWord, true);
    if (pos == -1 || !tempParseWord.toString().equals(KEYWORD_VALUES))
      throw new OCommandSQLParsingException("Missed VALUES keyword. Use " + getSyntax(), text, endFields);

    int beginValues = OStringParser.jumpWhiteSpaces(text, pos);
    if (pos == -1 || text.charAt(beginValues) != '(') {
      throw new OCommandSQLParsingException("Set of values is missed. Example: ('Bill', 'Stuart', 300). Use " + getSyntax(), text,
          pos);
    }

    final int textEnd = text.lastIndexOf(')');

    int blockStart = beginValues;
    int blockEnd = beginValues;
    while (blockStart != textEnd) {
      // skip coma between records
      blockStart = text.indexOf('(', blockStart - 1);

      blockEnd = findRecordEnd(text, '(', ')', blockStart);
      if (blockEnd == -1)
        throw new OCommandSQLParsingException("Missed closed brace. Use " + getSyntax(), text, blockStart);

      final List<String> values = OStringSerializerHelper.smartSplit(text, new char[] { ',' }, blockStart + 1, blockEnd - 1, true);

      if (values.isEmpty()) {
        throw new OCommandSQLParsingException("Set of values is empty. Example: ('Bill', 'Stuart', 300). Use " + getSyntax(), text,
            blockStart);
      }

      if (values.size() != fieldNames.size()) {
        throw new OCommandSQLParsingException("Fields not match with values", text, blockStart);
      }

      // TRANSFORM FIELD VALUES
      final Map<String, Object> fields = new LinkedHashMap<String, Object>();
      for (int i = 0; i < values.size(); ++i) {
        fields.put(fieldNames.get(i), OSQLHelper.parseValue(this, OStringSerializerHelper.decode(values.get(i).trim()), context));
      }
      newRecords.add(fields);
      blockStart = blockEnd;
    }

  }

  /**
   * Find closing character, skips text elements.
   */
  private static final int findRecordEnd(String candidate, char start, char end, int startIndex) {
    int inc = 0;

    for (int i = startIndex; i < candidate.length(); i++) {
      char c = candidate.charAt(i);
      if (c == '\'') {
        // skip to text end
        int tend = i;
        while (true) {
          tend = candidate.indexOf('\'', tend + 1);
          if (tend < 0) {
            throw new OCommandSQLParsingException("Could not find end of text area.");
          }

          if (candidate.charAt(tend - 1) == '\\') {
            // inner quote, skip it
            continue;
          } else {
            break;
          }
        }
        i = tend;
        continue;
      }

      if (c != start && c != end)
        continue;

      if (c == start) {
        inc++;
      } else if (c == end) {
        inc--;
        if (inc == 0) {
          return i;
        }
      }
    }

    return -1;
  }

  /**
   * Execute the INSERT and return the ODocument object created.
   */
  public Object execute(final Map<Object, Object> iArgs) {
    if (newRecords == null)
      throw new OCommandExecutionException("Cannot execute the command because it has not been parsed yet");

    if (indexName != null) {
      final OIndex<?> index = getDatabase().getMetadata().getIndexManager().getIndex(indexName);
      if (index == null)
        throw new OCommandExecutionException("Target index '" + indexName + "' not found");

      // BIND VALUES
      Map<String, Object> result = null;
      for (Map<String, Object> candidate : newRecords) {
        index.put(candidate.get(KEYWORD_KEY), (OIdentifiable) candidate.get(KEYWORD_RID));
        result = candidate;
      }

      // RETURN LAST ENTRY
      return new ODocument(result);
    } else {

      // CREATE NEW DOCUMENTS
      final List<ODocument> docs = new ArrayList<ODocument>();
      for (Map<String, Object> candidate : newRecords) {
        final ODocument doc = className != null ? new ODocument(className) : new ODocument();
        OSQLHelper.bindParameters(doc, candidate, new OCommandParameters(iArgs));

        if (clusterName != null) {
          doc.save(clusterName);
        } else {
          doc.save();
        }
        docs.add(doc);
      }

      if (docs.size() == 1) {
        return docs.get(0);
      } else {
        return docs;
      }
    }
  }

  @Override
  public String getSyntax() {
    return "INSERT INTO <Class>|cluster:<cluster>|index:<index> [(<field>[,]*) VALUES (<expression>[,]*)[,]*]|[SET <field> = <expression>[,]*]";
  }

}
