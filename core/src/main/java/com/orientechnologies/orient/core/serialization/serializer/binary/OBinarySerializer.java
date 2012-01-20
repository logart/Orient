package com.orientechnologies.orient.core.serialization.serializer.binary;

import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class is the implementation of the record serialization
 *
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class OBinarySerializer implements OPartialRecordSerializer {

    private static final String NAME = "ORecordDocument2binary";

    @Override
    public String toString() {
        return NAME;
    }

    @Override
    public ORecordInternal<?> fromStream(final byte[] iSource, final ORecordInternal<?> iRecord) {

        if (!(iRecord instanceof ODocument)) {
            throw new OSerializationException("Cannot marshall a record of type " + iRecord.getClass().getSimpleName() + " to binary");
        }

        final ODocument doc = (ODocument) iRecord;
        final OMetadata metadata = OMetadata.createFromBytes(iSource);
        doc.setMetadata(metadata);
        doc.setClassName(metadata.getDocumentClassName());

        return doc;
    }

    @Override
    public ORecordInternal<?> fromStream(final byte[] iSource, final ORecordInternal<?> iRecord, final Set<String> fieldsToRead) {

        if (!(iRecord instanceof ODocument)) {
            throw new OSerializationException("Cannot marshall a record of type " + iRecord.getClass().getSimpleName() + " to binary");
        }

        final ObjectSerializerFactory osf = ObjectSerializerFactory.INSTANCE;

        final ODocument doc = (ODocument) iRecord;
        final OMetadata metadata = doc.getMetadata();

        //obtain metadata size for using as offset
        final int metadataOffset = metadata.getMetaSize();
        //read data
        for(final Map.Entry<String, Integer> entry : metadata.getMetadataSet()) {
            if(fieldsToRead.contains(entry.getKey())){
                //calculate offset of the record
                final int offset = metadataOffset+entry.getValue();
                //read field type identifier
                final byte fieldType = iSource[offset];
                //read field data
                doc.field(entry.getKey(), osf.getObjectSerializer(fieldType).deserialize(iSource, offset+1));
            }
        }

        return doc;
    }

    @Override
    public byte[] toStream(final ORecordInternal<?> iSource, final boolean iOnlyDelta) {
        final ObjectSerializerFactory osf = ObjectSerializerFactory.INSTANCE;

        final ODocument doc = (ODocument) iSource;

        if (doc.getPureSource() == null || doc.getPureSource().length == 0) {
            //object has not been serialized ever before
            final OMetadata metadata = OMetadata.createFromDocument(doc);
            final int metadataSize = metadata.getMetaSize();
            final byte[] stream = new byte[metadataSize + metadata.getDataSize()];
            metadata.toBytes(stream);

            //write data
            for (final Map.Entry<String, Integer> entry : metadata.getMetadataSet()) {
                //calculate offset of the record
                final int offset = metadataSize + entry.getValue();
                final String name = entry.getKey();
                final Object value = doc.field(name);
                //obtain serializer and write type identifier
                final byte typeId = value != null ? doc.fieldType(name).getByteId() : ObjectSerializerFactory.NULL_TYPE;
                stream[offset] = typeId;
                osf.getObjectSerializer(typeId).serialize(value, stream, offset+1);
            }

            return stream;
        } else if (!doc.isDirty()) {
            //object has been serialized before and has not been changed after that
            return doc.getPureSource();
        } else {
            //object has been serialized before and has been changed after
            final OMetadata oldMetadata = doc.getMetadata();
            final OMetadata newMetadata = OMetadata.createFromDocument(doc);
            final int metadataSize = newMetadata.getMetaSize();
            final byte[] stream = new byte[metadataSize + newMetadata.getDataSize()];
            final byte[] oldStream = doc.getPureSource();
            newMetadata.toBytes(stream);

            final Set<String> dirty = new HashSet<String>(Arrays.asList(doc.getDirtyFields()));

            for (final Map.Entry<String, Integer> entry : newMetadata.getMetadataSet()) {
                //calculate offset of the record
                final int offset = metadataSize + entry.getValue();
                final String name = entry.getKey();
                final Object value = doc.field(name);
                if (dirty.contains(name)) {
                    //obtain serializer and write type identifier
                    final byte typeId = value != null ? doc.fieldType(name).getByteId() : ObjectSerializerFactory.NULL_TYPE;
                    stream[offset] = typeId;
                    osf.getObjectSerializer(typeId).serialize(value, stream, offset+1);
                } else {
                    final int oldOffset = oldMetadata.getMetadata().get(name);
                    final int sizeToCopy = osf.getObjectSerializer(doc.fieldType(name)).getFieldSize(value) + 1;
                    System.arraycopy(oldStream, oldOffset, stream, offset, sizeToCopy);
                }
            }

            return stream;
        }
    }
}
