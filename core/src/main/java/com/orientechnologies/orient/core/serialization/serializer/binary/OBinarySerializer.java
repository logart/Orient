package com.orientechnologies.orient.core.serialization.serializer.binary;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Map;
import java.util.Set;

/**
 * This class is the implementation of the record serialization
 *
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class OBinarySerializer implements OPartialRecordSerializer {

    @Override
    public ORecordInternal<?> fromStream(final byte[] iSource, final ORecordInternal<?> iRecord) {

        final ObjectSerializerFactory osf = ObjectSerializerFactory.INSTANCE;
        final ODocument doc = (ODocument) iRecord;

        //read metadata
        final OMetadata metadata = OMetadata.fromBytes(iSource);
        //obtain metadata size for using as offset
        final int metadataOffset = metadata.getMetadataSize();
        //read data
        for(final OMetadata.OMetadataEntry entry : metadata) {
            //calculate offset of the record
            final int offset = metadataOffset+entry.getFieldOffset();
            //read field type identifier
            final byte fieldType = iSource[offset];
            //read field data
            doc.field(entry.getFieldName(), osf.getObjectSerializer(fieldType).deserialize(iSource, offset+1));
        }

        return doc;
    }

    @Override
    public ORecordInternal<?> fromStream(final byte[] iSource, final ORecordInternal<?> iRecord, final Set<String> fieldsToRead) {

        final ObjectSerializerFactory osf = ObjectSerializerFactory.INSTANCE;
        final ODocument doc = (ODocument) iRecord;

        //read metadata
        final OMetadata metadata = OMetadata.fromBytes(iSource);
        //obtain metadata size for using as offset
        final int metadataOffset = metadata.getMetadataSize();
        //read data
        for(final OMetadata.OMetadataEntry entry : metadata) {
            if(fieldsToRead.contains(entry.getFieldName())){
                //calculate offset of the record
                final int offset = metadataOffset+entry.getFieldOffset();
                //read field type identifier
                final byte fieldType = iSource[offset];
                //read field data
                doc.field(entry.getFieldName(), osf.getObjectSerializer(fieldType).deserialize(iSource, offset+1));
            }
        }

        return doc;
    }

    @Override
    public byte[] toStream(final ORecordInternal<?> iSource, final boolean iOnlyDelta) {

        final ObjectSerializerFactory osf = ObjectSerializerFactory.INSTANCE;
        final ObjectSerializer<Byte> typeIdentifierSerializer = osf.getObjectSerializer(OType.BYTE);

        final ODocument doc = (ODocument) iSource;//TODO apply onlyDelta to OMetadataAwareDocument
        final OMetadata metadata = OMetadata.createForDocument(doc);//will obtain metadata from document if it implements OMetadataAwareDocument
        final byte[] stream = new byte[metadata.getMetadataSize()+metadata.getDataSize()];
        final int metadataOffset = metadata.getMetadataSize();

        //write metadata
        metadata.toBytes(stream);
        //write data
        for (final OMetadata.OMetadataEntry entry : metadata) {
            //calculate offset of the record
            final int offset = metadataOffset+entry.getFieldOffset();
            //write field type identifier
            typeIdentifierSerializer.serialize(entry.getFieldType().getId(), stream, offset);
            //write field data
            osf.getObjectSerializer(entry.getFieldType()).serialize(entry.getFieldValue(), stream, offset+1);
        }

        return stream;
    }
}
