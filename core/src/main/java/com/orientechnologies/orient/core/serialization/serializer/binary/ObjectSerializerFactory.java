package com.orientechnologies.orient.core.serialization.serializer.binary;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.db.record.OBinaryLazyList;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OBinarySerializableDelegate;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OBinarySerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OBooleanSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OByteSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.ODateSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.ODateTimeSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.ODoubleSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OEmbeddedSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OFloatSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLongSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OMapSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.ONullSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OShortSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OStringSerializer;

/**
 * This class is responsible for obtaining ObjectSerializer realization, that fits for the next bytes in the stream
 *
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class ObjectSerializerFactory {

    private final Map<Byte, ObjectSerializer<?>> serializerMap = new HashMap<Byte, ObjectSerializer<?>>();
    private final ObjectSerializer<Integer> indexSerializer;

    /**
     * Instance of the factory
     */
    public static final ObjectSerializerFactory INSTANCE = new ObjectSerializerFactory();
    /**
     * Size of the type identifier block size
     */
    public static final int TYPE_IDENTIFIER_SIZE = 1;
    /**
     * Type identifier for the null fields
     */
    public static final byte NULL_TYPE = -1;

    private static final int INDEX_SIZE = 4;

    private ObjectSerializerFactory() {
        final OBinarySerializableDelegate delegate = new OBinarySerializableDelegate();

        //TODO populate map with objectSerializerImplementations
        serializerMap.put(NULL_TYPE, new ONullSerializer());

        serializerMap.put(OType.BOOLEAN.getByteId(), new OBooleanSerializer());
        serializerMap.put(OType.INTEGER.getByteId(), new OIntegerSerializer());
        serializerMap.put(OType.SHORT.getByteId(), new OShortSerializer());
        serializerMap.put(OType.LONG.getByteId(), new OLongSerializer());
        serializerMap.put(OType.FLOAT.getByteId(), new OFloatSerializer());
        serializerMap.put(OType.DOUBLE.getByteId(), new ODoubleSerializer());
        serializerMap.put(OType.DATETIME.getByteId(), new ODateTimeSerializer());
        serializerMap.put(OType.STRING.getByteId(), new OStringSerializer());
        serializerMap.put(OType.BINARY.getByteId(), new OBinarySerializer());

        serializerMap.put(OType.BYTE.getByteId(), new OByteSerializer());

        serializerMap.put(OType.DATE.getByteId(), new ODateSerializer());

        serializerMap.put(OType.EMBEDDEDLIST.getByteId(), delegate);
        serializerMap.put(OType.EMBEDDEDMAP.getByteId(), new OMapSerializer());

        serializerMap.put(OType.EMBEDDED.getByteId(), new OEmbeddedSerializer());

        indexSerializer = new OIntegerSerializer();
    }

    /**
     * Obtain serializer that is responsible for serializing indexes and sizes (currently is OIntegerSerializer)
     *
     * @return index serializer
     */
    public ObjectSerializer<Integer> getIndexSerializer() {
        return indexSerializer;
    }

    /**
     * Obtain amount of bytes that is required for storing index or size record
     *
     * @return size of index in bytes
     */
    public int getIndexSize() {
        return INDEX_SIZE;
    }

    /**
     * Obtain ObjectSerializer realization for the next bytes in the stream
     *
     * @param typeIdentifier is the type identifier that located before the data
     * @return ObjectSerializer realization that fits for the next data
     */
    public ObjectSerializer getObjectSerializer(final byte typeIdentifier) {
        return serializerMap.get(typeIdentifier);
    }

    /**
     * Obtain ObjectSerializer realization for the OType
     * !!!!WARNING!!!!
     * will obtain wrong serializer for the null objects
     *
     * @param type is the OType to obtain serializer algorithm for
     * @return ObjectSerializer realization that fits OType
     */
    @Deprecated
    public ObjectSerializer getObjectSerializer(final OType type) {
        return serializerMap.get(type.getByteId());
    }

    /**
     * Obtain ObjectSerializer realization for the OType and the Object.
     * Will return NULL serializer for the null objects, prevent null pointer
     *
     * @param type is the OType to obtain serializer algorithm for
     * @param obj  is the object to serialize
     * @return OBjectSerializer realization that fits OType or NULL serializer if object is null
     */
    public ObjectSerializer getObjectSerializer(final OType type, final Object obj) {
        if (obj != null) {
            return getObjectSerializer(type);
        } else {
            return serializerMap.get(NULL_TYPE);
        }
    }

    public byte getSerializerId(final OType type, final Object obj) {
        if (obj != null) {
            return type.getByteId();
        } else {
            return NULL_TYPE;
        }
    }

    /**
     * Obtain implementation of the object to be deserilized by given serializer`s id
     *
     * @param typeIdentifier is the identifier of the type
     * @return OBinarySerializable implementation
     */
    public OBinarySerializable getInstanceOf(final byte typeIdentifier) {
        if(typeIdentifier == OType.EMBEDDEDLIST.getByteId()){
            return new OBinaryLazyList();
        }
        return null;//TODO
    }
}
