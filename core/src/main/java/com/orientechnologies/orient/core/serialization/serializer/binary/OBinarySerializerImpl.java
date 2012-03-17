package com.orientechnologies.orient.core.serialization.serializer.binary;

import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OEmbeddedSerializer;

import java.util.Set;

/**
 * This class is the implementation of the record serialization
 *
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class OBinarySerializerImpl implements OPartialRecordSerializer {

    private static final String NAME = "ORecordDocument2binary";

    private static final OEmbeddedSerializer SERIALIZER = new OEmbeddedSerializer();
    
    public ORecordInternal<?> fromStream(final byte[] iSource, final ORecordInternal<?> iRecord) {

        if (!(iRecord instanceof ODocument)) {
            throw new OSerializationException("Cannot marshall a record of type " + iRecord.getClass().getSimpleName() + " to binary");
        }

        final ODocument doc = (ODocument) iRecord;
        return SERIALIZER.deserialize(iSource, 0, doc);
    }

    public ORecordInternal<?> fromStream(final byte[] iSource, final ORecordInternal<?> iRecord, final Set<String> fieldsToRead) {

        throw new UnsupportedOperationException();
    }

    public byte[] toStream(final ORecordInternal<?> iSource, final boolean iOnlyDelta) {

        final ODocument doc = (ODocument) iSource;
        final byte[] stream = new byte[SERIALIZER.getObjectSize(doc)];
        SERIALIZER.serialize(doc, stream, 0);
        return stream;
    }

    @Override
    public String toString() {
        return NAME;
    }
}
