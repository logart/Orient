package com.orientechnologies.orient.core.serialization.serializer.binary;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OBooleanSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OByteSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.ODateSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.ODateTimeSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.ODoubleSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OFloatSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLongSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.ONullSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OShortSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OStringSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OBinarySerializer;


import java.lang.Object;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is responsible for obtaining ObjectSerializer realization, that fits for the next bytes in the stream
 *
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class ObjectSerializerFactory {

    private final Map<Byte, ObjectSerializer<?>> serializerMap = new HashMap<Byte, ObjectSerializer<?>>();

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

    private ObjectSerializerFactory() {
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
     *
     * @param type is the OType to obtain serializer algorithm for
     * @return ObjectSerializer realization that fits OType
     */
    public ObjectSerializer getObjectSerializer(final OType type) {
        return serializerMap.get(type.getByteId());
    }

    /**
     * Obtain ObjectSerializer realization for the OType and the Object.
     * Will return NULL serializer for the null objects, prevent null pointer
     *
     * @param type is the OType to obtain serializer algorithm for
     * @param obj is the object to serialize
     * @return OBjectSerializer realization that fits OType or NULL serializer if object is null
     */
    public ObjectSerializer getObjectSerializer(final OType type, final Object obj) {
        if(obj != null) {
            return getObjectSerializer(type);
        } else {
            return serializerMap.get(NULL_TYPE);
        }
    }


}
