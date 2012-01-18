package com.orientechnologies.orient.core.serialization.serializer.binary;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OBooleanSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OByteSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.ODoubleSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OFloatSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OIntegerSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OShortSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * This class is responsible for obtaining ObjectSerializer realization, that fits for the next bytes in the stream
 *
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class ObjectSerializerFactory {

    private final Map<Byte, ObjectSerializer<?>> serializerMap = new HashMap<Byte, ObjectSerializer<?>>();

    public final static ObjectSerializerFactory INSTANCE = new ObjectSerializerFactory();

    private ObjectSerializerFactory() {
        //TODO populate map with objectSerializerImplementations
        serializerMap.put(OType.BOOLEAN.getId(), new OBooleanSerializer());
        serializerMap.put(OType.BYTE.getId(), new OByteSerializer());
        serializerMap.put(OType.SHORT.getId(), new OShortSerializer());
        serializerMap.put(OType.INTEGER.getId(), new OIntegerSerializer());
        serializerMap.put(OType.LONG.getId(), new OIntegerSerializer());
        serializerMap.put(OType.FLOAT.getId(), new OFloatSerializer());
        serializerMap.put(OType.DOUBLE.getId(), new ODoubleSerializer());
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
     * Obtain ObjectSerializer realization for the next OType from metadata
     *
     * @param type is the OType to obtain serializer algorithm for
     * @return byte type identifier to write into stream
     */
    public ObjectSerializer getObjectSerializer(final OType type) {
        return serializerMap.get(type.getId());
    }
}
