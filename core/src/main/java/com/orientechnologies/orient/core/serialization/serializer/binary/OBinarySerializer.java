package com.orientechnologies.orient.core.serialization.serializer.binary;

import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * This class is the implementation of the record serialization
 *
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class OBinarySerializer implements OPartialRecordSerializer {

    private static final ObjectSerializerFactory OBJECT_SERIALIZER_FACTORY = new ObjectSerializerFactory();

    private void readMetadata() {
        //TODO implement
    }

    private void writeMetadata() {
        //TODO implement
    }


    public ORecordInternal<?> fromStream(final byte[] iSource, final ORecordInternal<?> iRecord) {

        final ODocument doc = (ODocument) iRecord;

        //TODO implement
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public byte[] toStream(final ORecordInternal<?> iSource, final boolean iOnlyDelta) {
        //TODO implement
        return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
    }
}
