package com.orientechnologies.orient.core.serialization.serializer.binary;

import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;

import java.util.Set;

/**
 * This interface will give ability to serialize and deserialize subset of object fields
 *
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public interface OPartialRecordSerializer extends ORecordSerializer {

    /**
     * This method will allow to deserialize subset of the fields of the document
     */
    public ORecordInternal<?> fromStream(byte[] iSource, ORecordInternal<?> iRecord, Set<String> fieldsToRead);
}
