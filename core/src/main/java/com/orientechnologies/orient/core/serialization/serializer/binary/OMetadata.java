package com.orientechnologies.orient.core.serialization.serializer.binary;

import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OMemoryInputStream;
import com.orientechnologies.orient.core.serialization.OMemoryStream;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * This class represents metadata block. Contains its serialized size, size of serialized data,
 * name of the document class and offsets after metadata block for each field
 *
 * Structure of the metadata block:
 * |-serialized-data-offset-|-document-class-name-(nullable)-|-information-about-fields-offset-as-map-|
 *
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class OMetadata {

    /**
     * Contains size that is required for serializing metadata
     */
//    private int metaSize;
    /**
     * Contains size that is required for serializing all fields
     * data + 1 byte for each field to determine type
     */
    private int dataSize;
    /**
     * Contains document class name
     */
    private final String documentClassName;
    /**
     * Contains fields serialized data offsets after metadata
     */
    private final Map<String, Integer> fieldOffsets = new HashMap<String, Integer>();

    /**
     * Constructor for the class
     *
     * @param dataSize is the size of the data block
     * @param metaSize is the size of the metadata block
     * @param documentClassName is the document class name (may be null)
     * @param fieldOffsets is the fields offset in stream
     */
    public OMetadata(final int metaSize, final int dataSize, final String documentClassName, final Map<String, Integer> fieldOffsets) {
        this.documentClassName = documentClassName;
        this.fieldOffsets.putAll(fieldOffsets);
        this.dataSize = dataSize;
//        this.metaSize = countMetadataSize();
    }

    /**
     * @return document class name
     */
    public String getDocumentClassName() {
        return documentClassName;
    }

    /**
     * @return size of the metadata block
     */
    public int getMetaSize() {
        return countMetadataSize();
    }

    /**
     * @return size of the data block
     */
    public int getDataSize() {
        return dataSize;
    }

     /**
     * @return offsets of the fields after metadata block
     */
    public Map<String, Integer> getMetadata() {
        return fieldOffsets;
    }

    /**
     * @return offsets of the fields after metadata block
     */
    public Set<Map.Entry<String, Integer>> getMetadataSet() {
        return fieldOffsets.entrySet();
    }

    /**
     * Serialize metadata to the begin of the source
     *
     * @param source is the stream to serialize into
     */
    public void toBytes(final byte[] source) {
        final ObjectSerializerFactory osf = ObjectSerializerFactory.INSTANCE;
        final ObjectSerializer<Object> indexSerializer = osf.getObjectSerializer(OType.INTEGER);

        indexSerializer.serialize(countMetadataSize(), source, 0);
        int position = indexSerializer.getFieldSize(null);
        //write documentClassName type identifier (string or null)
        source[position] = documentClassName != null ? OType.STRING.getByteId() : ObjectSerializerFactory.NULL_TYPE;
        position += ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE;
        //serialize string if not null
        if(documentClassName != null) {
            final ObjectSerializer nameSerializer = osf.getObjectSerializer(OType.STRING);
            nameSerializer.serialize(documentClassName, source, position);
            position += nameSerializer.getFieldSize(documentClassName);
        }

        //serialize hash map
        OMemoryStream mem = null;
        ObjectOutputStream os = null;
        try {
            mem = new OMemoryStream();
            os = new ObjectOutputStream(mem);
            os.writeObject(fieldOffsets);
            final byte[] map = mem.toByteArray();
            System.arraycopy(map, 0, source, position, map.length);
        } catch (final IOException e) {
            throw new OSerializationException("Problem while writing metadata", e);
        } finally {
            try{
                if(os != null) {
                    os.close();
                }
            } catch (final IOException e) {/*will newer happen*/}
            if(mem != null) {
                mem.close();
            }
        }
    }

    /**
     * Deserialize metadata from the begin of the source
     *
     * @param source is the stream to create metadata from
     */
    public static OMetadata createFromBytes(final byte[] source) {
        final ObjectSerializerFactory osf = ObjectSerializerFactory.INSTANCE;
        final ObjectSerializer<Object> indexSerializer = osf.getObjectSerializer(OType.INTEGER);

        final int metaSize = (Integer) indexSerializer.deserialize(source, 0);
        final int dataSize = source.length - metaSize;
        int position = indexSerializer.getFieldSize(null);

        //obtain serializer for documentClassName (string or null serializer)
        final ObjectSerializer nameSerializer = osf.getObjectSerializer(source[position]);
        position += ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE;
        final String documentClassName = (String) nameSerializer.deserialize(source, position);
        position += nameSerializer.getFieldSize(documentClassName);

        final Map<String, Integer> fieldOffsets = new HashMap<String, Integer>();
        OMemoryInputStream mem = null;
        ObjectInputStream obj = null;
        try {
            final byte[] metaBytes = new byte[metaSize-position];
            System.arraycopy(source, position, metaBytes, 0, metaSize-position);
            mem = new OMemoryInputStream(metaBytes);
            obj = new ObjectInputStream(mem);
            fieldOffsets.putAll((Map<String, Integer>) obj.readObject());
        } catch (final IOException e) {
            throw new OSerializationException("Problem while reading metadata", e);
        } catch (final ClassNotFoundException e) {
            throw new OSerializationException("Problem while reading metadata", e);
        } finally {
            try{
                if(obj != null){
                    obj.close();
                }
            } catch (final IOException e) {/*will newer happen*/}
            if(mem != null) {
                mem.close();
            }
        }

        return new OMetadata(metaSize, dataSize, documentClassName, fieldOffsets);
    }

    /**
     * Create metadata for the document
     */
    public static OMetadata createFromDocument(final ODocument doc) {
        final ObjectSerializerFactory osf = ObjectSerializerFactory.INSTANCE;
        final int typeIdentifierSize = ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE;

        int position = 0;
        final String[] fields = doc.fieldNames();
        final Map<String, Integer> fieldsOffsets = new HashMap<String, Integer>();
        for(final String field : fields) {
            final OType type = doc.fieldType(field);
            final Object value = doc.field(field);
            //adding record to the metadata
            fieldsOffsets.put(field, position);
            //calculate next position
            final ObjectSerializer ser = osf.getObjectSerializer(type, value);
            position += ser.getFieldSize(value) + typeIdentifierSize;
        }

        //TODO put real metadata size when it will be possible
        return new OMetadata(-1, position, doc.getClassName(), fieldsOffsets);
    }

    private int countMetadataSize() {
        final ObjectSerializerFactory osf = ObjectSerializerFactory.INSTANCE;
        int size = osf.getObjectSerializer(OType.INTEGER).getFieldSize(null) +
                ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE +
                osf.getObjectSerializer(OType.STRING, documentClassName).getFieldSize(documentClassName);

        OMemoryStream mem = null;
        ObjectOutputStream obj = null;
        try {
            mem = new OMemoryStream();
            obj = new ObjectOutputStream(mem);
            obj.writeObject(fieldOffsets);
            size += mem.size();
        } catch (final IOException e) {
            throw new OSerializationException("Problem while evaluating metadata", e);
        } finally {
            try{
                if(obj != null){
                    obj.close();
                }
            } catch (final IOException e) {/*will newer happen*/}
            if(mem != null) {
                mem.close();
            }
        }

        return size;
    }
}
