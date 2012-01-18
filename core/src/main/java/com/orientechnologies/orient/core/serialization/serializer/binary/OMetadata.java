package com.orientechnologies.orient.core.serialization.serializer.binary;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This class will store some addition information and direct links to the fields about
 * ODocument while serializing and deserializing to speed up process
 *
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class OMetadata implements Iterable<OMetadata.OMetadataEntry> {

    private final List<OMetadataEntry> entries = new ArrayList<OMetadataEntry>();

    private int metadataSize;
    private int dataSize;
    private String documentClassName;

    /**
     * Serialize metadata into bytes array
     * This operation is responsible to serialize metadata block size and document class name
     *
     * @return serialized metadata
     */
    public void toBytes(final byte[] source) {
        //TODO implement
    }

    /**
     * Deserialize metadata from the byte array
     * This operation is responsible for restoring metadata block size and document class name
     *
     * @return deserialized metadata
     */
    public static OMetadata fromBytes(final byte[] source) {
        //TODO implement
        return new OMetadata();
    }

    /**
     * Creates metadata for the ODocument that caches fields names, types and values and contains
     * fields data offset in data stream and offset for the previous offsets to provide fastest changes of structure
     *
     * @param doc is the document to extract data
     * @return the instance of the metadata
     */
    public static OMetadata createForDocument(final ODocument doc) {

        if(doc instanceof OMetadataAwareDocument){
            //TODO update cached values
            return ((OMetadataAwareDocument) doc).getMetadata();
        } else {
            //TODO implement

            return null;
        }
    }

    public int getMetadataSize() {
        return metadataSize;
    }

    public void setMetadataSize(int metadataSize) {
        this.metadataSize = metadataSize;
    }

    public int getDataSize() {
        return dataSize;
    }

    public void setDataSize(int dataSize) {
        this.dataSize = dataSize;
    }

    public String getDocumentClassName() {
        return documentClassName;
    }

    public void setDocumentClassName(String documentClassName) {
        this.documentClassName = documentClassName;
    }

    public void addEntry(final OMetadataEntry entry) {
        entries.add(entry);
    }

    public Iterator<OMetadataEntry> iterator() {
        return entries.iterator();
    }

    public static class OMetadataEntry {

        private final String fieldName;
        private final OType fieldType;
        private final Object fieldValue;
        private int fieldOffsetLocation;
        private int fieldOffset;

        public OMetadataEntry(final String fieldName, final int fieldOffset) {
            this(fieldName, null, null, -1, fieldOffset);
        }

        public OMetadataEntry(final String fieldName, final int fieldOffsetLocation, final int fieldOffset) {
            this(fieldName, null, null, fieldOffsetLocation, fieldOffset);
        }

        public OMetadataEntry(final String fieldName, final OType fieldType, final Object fieldValue,
                              final int fieldOffsetLocation, final int fieldOffset) {
            this.fieldName = fieldName;
            this.fieldType = fieldType;
            this.fieldValue = fieldValue;
            this.fieldOffsetLocation = fieldOffsetLocation;
            this.fieldOffset = fieldOffset;
        }

        public String getFieldName() {
            return fieldName;
        }

        public OType getFieldType() {
            return fieldType;
        }

        public Object getFieldValue() {
            return fieldValue;
        }

        public int getFieldOffsetLocation() {
            return fieldOffsetLocation;
        }

        public int getFieldOffset() {
            return fieldOffset;
        }

        public void setFieldOffsetLocation(int fieldOffsetLocation) {
            this.fieldOffsetLocation = fieldOffsetLocation;
        }

        public void setFieldOffset(int fieldOffset) {
            this.fieldOffset = fieldOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final OMetadataEntry that = (OMetadataEntry) o;

            return fieldName != null && fieldName.equals(that.fieldName);
        }

        @Override
        public int hashCode() {
            return fieldName != null ? fieldName.hashCode() : 0;
        }
    }
}
