package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: gman
 * Date: 04.03.12
 * Time: 21:06
 * To change this template use File | Settings | File Templates.
 */
public class OEmbeddedSerializer implements ObjectSerializer<ODocument> {

    @Override
    public int getObjectSize(ODocument doc) {
        final int typeIdSize = ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE;
        final ObjectSerializerFactory osf = ObjectSerializerFactory.INSTANCE;
        //size of metadata block
        int size = osf.getIndexSize();
        //size of classname
        final String className = doc.getClassName();
        size += typeIdSize + osf.getObjectSerializer(OType.STRING, className).getObjectSize(className);
        //size of metadata
        final Map<String, Integer> metadata = createMetadata(doc);
        size += typeIdSize + osf.getObjectSerializer(OType.EMBEDDEDMAP, metadata).getObjectSize(metadata);
        //size of data fields
        final String[] fieldNames = doc.fieldNames();
        for(final String fieldName : fieldNames) {
            final OType fieldType = doc.fieldType(fieldName);
            final Object  fieldValue = doc.field(fieldName);
            size += typeIdSize + osf.getObjectSerializer(fieldType, fieldValue).getObjectSize(fieldValue);
        }
        return size;
    }

    @Override
    public void serialize(ODocument doc, byte[] stream, int startPosition) {
        final int typeIdSize = ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE;
        final ObjectSerializerFactory osf = ObjectSerializerFactory.INSTANCE;
        final Map<String, Integer> metadata = createMetadata(doc);
        final String className = doc.getClassName();
        final int classNameSize = osf.getObjectSerializer(OType.STRING, className).getObjectSize(className);
        final int metadataSize = osf.getObjectSerializer(OType.EMBEDDEDMAP, metadata).getObjectSize(metadata);
        final int metaSize = osf.getIndexSize() +
                typeIdSize + classNameSize +
                typeIdSize + metadataSize;
        int position = startPosition;
        osf.getIndexSerializer().serialize(metaSize, stream, position);
        position += osf.getIndexSize();
        stream[position] = osf.getSerializerId(OType.STRING, className);
        osf.getObjectSerializer(OType.STRING, className).serialize(className, stream, position+typeIdSize);
        position += typeIdSize + classNameSize;
        stream[position] = osf.getSerializerId(OType.EMBEDDEDMAP, metadata);
        osf.getObjectSerializer(OType.EMBEDDEDMAP, metadata).serialize(metadata, stream, position+typeIdSize);
        position += typeIdSize + metadataSize;
        for(final Map.Entry<String, Integer> metaEntry : metadata.entrySet()) {
            final String fieldName = metaEntry.getKey();
            //TODO reuse type and value from metadata creation??
            final Object fieldValue = doc.field(fieldName);
            final OType fieldType = doc.fieldType(fieldName) != null ?
                    doc.fieldType(fieldName) : OType.getTypeByClass(fieldValue.getClass());
            final byte serializerTypeId = osf.getSerializerId(fieldType, fieldValue);
            final int offset = position + metaEntry.getValue();
            stream[offset] = serializerTypeId;
            osf.getObjectSerializer(serializerTypeId).serialize(fieldValue, stream, offset+typeIdSize);
        }
    }

    @Override
    public ODocument deserialize(byte[] stream, int startPosition) {
        final int typeIdSize = ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE;
        final ObjectSerializerFactory osf = ObjectSerializerFactory.INSTANCE;
        int position = startPosition;
        final int metaSize = osf.getIndexSerializer().deserialize(stream, position);
        position += osf.getIndexSize();
        final String className = (String) osf.getObjectSerializer(stream[position]).deserialize(stream, position+typeIdSize);
        position += typeIdSize + osf.getObjectSerializer(stream[position]).getObjectSize(className);
        final Map<String, Integer> metadata = (Map<String, Integer>) osf.getObjectSerializer(stream[position]).deserialize(stream, position+typeIdSize);
        position += typeIdSize + osf.getObjectSerializer(stream[position]).getObjectSize(metadata);
        final ODocument doc = new ODocument(className);
        for(final Map.Entry<String, Integer> metaEntry : metadata.entrySet()) {
            final int offset = position + metaEntry.getValue();
            final String fieldName = metaEntry.getKey();
            final byte serializerTypeId = stream[offset];
            final Object value = osf.getObjectSerializer(serializerTypeId).deserialize(stream, offset + typeIdSize);
            doc.field(fieldName, value);
        }
        return doc;
    }
    
    private Map<String, Integer> createMetadata(ODocument doc) {
        //TODO update metadata if exists
        final int typeIdSize = ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE;
        final ObjectSerializerFactory osf = ObjectSerializerFactory.INSTANCE;
        int position = 0;
        final String[] fieldNames = doc.fieldNames();
        final Map<String, Integer> metadata = new HashMap<String, Integer>();
        for(final String fieldName: fieldNames) {
            final Object fieldValue = doc.field(fieldName);
            final OType fieldType = doc.fieldType(fieldName) != null ?
                    doc.fieldType(fieldName) : OType.getTypeByClass(fieldValue.getClass());
            metadata.put(fieldName, position);
            position += typeIdSize + osf.getObjectSerializer(fieldType, fieldValue).getObjectSize(fieldValue);
        }
        return metadata;
    }
}
