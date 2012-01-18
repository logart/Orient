package com.orientechnologies.orient.core.serialization.serializer.binary;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Map;

/**
 *
 *
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class OMetadataAwareDocument extends ODocument {

    private OMetadata metadata;

    public OMetadataAwareDocument() {
    }

    public OMetadataAwareDocument(byte[] iSource) {
        super(iSource);
    }

    public OMetadataAwareDocument(ODatabaseRecord iDatabase) {
        super(iDatabase);
    }

    public OMetadataAwareDocument(ODatabaseRecord iDatabase, ORID iRID) {
        super(iDatabase, iRID);
    }

    public OMetadataAwareDocument(ORID iRID) {
        super(iRID);
    }

    public OMetadataAwareDocument(ODatabaseRecord iDatabase, String iClassName, ORID iRID) {
        super(iDatabase, iClassName, iRID);
    }

    public OMetadataAwareDocument(String iClassName, ORID iRID) {
        super(iClassName, iRID);
    }

    public OMetadataAwareDocument(ODatabaseRecord iDatabase, String iClassName) {
        super(iDatabase, iClassName);
    }

    public OMetadataAwareDocument(String iClassName) {
        super(iClassName);
    }

    public OMetadataAwareDocument(OClass iClass) {
        super(iClass);
    }

    public OMetadataAwareDocument(Object[] iFields) {
        super(iFields);
    }

    public OMetadataAwareDocument(Map<? extends Object, Object> iFieldMap) {
        super(iFieldMap);
    }

    public OMetadataAwareDocument(String iFieldName, Object iFieldValue, Object... iFields) {
        super(iFieldName, iFieldValue, iFields);
    }

    public OMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(OMetadata metadata) {
        this.metadata = metadata;
    }
}
