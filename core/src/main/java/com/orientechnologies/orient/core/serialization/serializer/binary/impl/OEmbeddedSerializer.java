package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * This class perform serialization for ODocument instances
 *
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class OEmbeddedSerializer implements ObjectSerializer<ODocument> {

    @Override
    public int getObjectSize(ODocument doc) {
        final int typeIdSize = ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE;
        final ObjectSerializerFactory osf = ObjectSerializerFactory.INSTANCE;
        //size of metadata block
        int size = 0;
        //size of classname
        final String className = doc.getClassName();
        size += typeIdSize + osf.getObjectSerializer(OType.STRING, className).getObjectSize(className);
        //size of metadata
        final BinaryMetadata metadata = createMetadata(doc);
        final Map<String, Integer> rawMetadata = metadata.asMap();
        size += typeIdSize + osf.getObjectSerializer(OType.EMBEDDEDMAP, rawMetadata).getObjectSize(rawMetadata);
        //size of data fields
        size += metadata.getDataSize();
        return size;
    }

    @Override
    public void serialize(ODocument doc, byte[] stream, int startPosition) {
        final int typeIdSize = ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE;
        final ObjectSerializerFactory osf = ObjectSerializerFactory.INSTANCE;
        //metadata preparation
        final BinaryMetadata metadata = createMetadata(doc);
        final Map<String, Integer> rawMetadata = metadata.asMap();
        final String className = doc.getClassName();
        //sizes calculations
        final int classNameSize = osf.getObjectSerializer(OType.STRING, className).getObjectSize(className);
        final int metadataSize = osf.getObjectSerializer(OType.EMBEDDEDMAP, rawMetadata).getObjectSize(rawMetadata);
        //serialization itself
        int position = startPosition;
        stream[position] = osf.getSerializerId(OType.STRING, className);
        osf.getObjectSerializer(OType.STRING, className).serialize(className, stream, position + typeIdSize);
        position += typeIdSize + classNameSize;
        stream[position] = osf.getSerializerId(OType.EMBEDDEDMAP, rawMetadata);
        osf.getObjectSerializer(OType.EMBEDDEDMAP, rawMetadata).serialize(rawMetadata, stream, position + typeIdSize);
        position += typeIdSize + metadataSize;
        for (final Map.Entry<String, BinaryMetadata.BinaryMetadataEntry> metaEntry : metadata) {
            final byte serializerTypeId = osf.getSerializerId(metaEntry.getValue().getType(), metaEntry.getValue().getValue());
            final int offset = position + metaEntry.getValue().getOffset();
            stream[offset] = serializerTypeId;
            osf.getObjectSerializer(serializerTypeId).serialize(metaEntry.getValue().getValue(), stream, offset + typeIdSize);
        }
    }

    @Override
    public ODocument deserialize(byte[] stream, int startPosition) {
        final ODocument doc = new ODocument();
        return deserialize(stream, startPosition, doc);
    }

    /**
     * Perform deserialization into given document
     *
     * @param stream        is the stream from object will be read
     * @param startPosition is the position to start reading from
     * @param target        is the target document to perform deserialization into
     * @return instance of the deserialized object
     */
    public ODocument deserialize(byte[] stream, int startPosition, ODocument target) {
        //TODO reset document?
        final int typeIdSize = ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE;
        final ObjectSerializerFactory osf = ObjectSerializerFactory.INSTANCE;
        int position = startPosition;
        final String className = (String) osf.getObjectSerializer(stream[position]).deserialize(stream, position + typeIdSize);
        position += typeIdSize + osf.getObjectSerializer(stream[position]).getObjectSize(className);
        final Map<String, Integer> metadata = (Map<String, Integer>) osf.getObjectSerializer(stream[position]).deserialize(stream, position + typeIdSize);
        position += typeIdSize + osf.getObjectSerializer(stream[position]).getObjectSize(metadata);
        target.setClassName(className);
        for (final Map.Entry<String, Integer> metaEntry : metadata.entrySet()) {
            final int offset = position + metaEntry.getValue();
            final String fieldName = metaEntry.getKey();
            final byte serializerTypeId = stream[offset];
            final Object value = osf.getObjectSerializer(serializerTypeId).deserialize(stream, offset + typeIdSize);
            target.field(fieldName, value);
        }
        return target;
    }

    private BinaryMetadata createMetadata(ODocument doc) {
        final int typeIdSize = ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE;
        final ObjectSerializerFactory osf = ObjectSerializerFactory.INSTANCE;
        int position = 0;
        final String[] fieldNames = doc.fieldNames();
        final BinaryMetadata metadata = new BinaryMetadata();
        for (final String fieldName : fieldNames) {
            final Object fieldValue = doc.field(fieldName);
            final OType fieldType = doc.fieldType(fieldName) != null ?
                    doc.fieldType(fieldName) : OType.getTypeByClass(fieldValue.getClass());
            metadata.put(fieldName, fieldType, fieldValue, position);
            position += typeIdSize + osf.getObjectSerializer(fieldType, fieldValue).getObjectSize(fieldValue);
        }
        metadata.setDataSize(position);
        return metadata;
    }

    protected class BinaryMetadata implements Iterable<Map.Entry<String, BinaryMetadata.BinaryMetadataEntry>> {

        private final Map<String, BinaryMetadataEntry> metadata = new HashMap<String, BinaryMetadataEntry>();
        private int dataSize = 0;

        public void put(String fieldName, OType fieldType, Object fieldValue, int fieldOffset) {
            metadata.put(fieldName, new BinaryMetadataEntry(fieldOffset, fieldType, fieldValue));
        }

        public int getDataSize() {
            return dataSize;
        }

        public void setDataSize(int dataSize) {
            this.dataSize = dataSize;
        }

        @Override
        public Iterator<Map.Entry<String, BinaryMetadataEntry>> iterator() {
            return metadata.entrySet().iterator();
        }

        public Map<String, Integer> asMap() {
            final Map<String, Integer> asMap = new HashMap<String, Integer>();
            for (Map.Entry<String, BinaryMetadataEntry> entry : metadata.entrySet()) {
                asMap.put(entry.getKey(), entry.getValue().offset);
            }
            return asMap;
        }

        protected class BinaryMetadataEntry {
            private final int offset;
            private final OType type;
            private final Object value;

            protected BinaryMetadataEntry(int offset, OType type, Object value) {
                this.offset = offset;
                this.type = type;
                this.value = value;
            }

            public int getOffset() {
                return offset;
            }

            public OType getType() {
                return type;
            }

            public Object getValue() {
                return value;
            }
        }
    }
}
