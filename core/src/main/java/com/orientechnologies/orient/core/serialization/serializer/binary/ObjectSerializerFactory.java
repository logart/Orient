package com.orientechnologies.orient.core.serialization.serializer.binary;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OBinarySerializer;
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

    public ObjectSerializer getIndexSerializer() {
        return serializerMap.get(OType.INTEGER);
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

    public OType getType(Object o) {
        OType type = null;
        if (o.getClass() == byte[].class)
            type = OType.BINARY;
/*        else if (ODatabaseRecordThreadLocal.INSTANCE.isDefined() && o instanceof ORecord<?>) {
            if (type == null)
                // DETERMINE THE FIELD TYPE
                if (o instanceof ODocument && ((ODocument) o).hasOwners())
                    type = OType.EMBEDDED;
                else
                    type = OType.LINK;
        } */else if (o instanceof ORID)
            type = OType.LINK;
        else if (o instanceof Date)
            type = OType.DATETIME;
        else if (o instanceof String)
            type = OType.STRING;
        else if (o instanceof Integer)
            type = OType.INTEGER;
        else if (o instanceof Long)
            type = OType.LONG;
        else if (o instanceof Float)
            type = OType.FLOAT;
        else if (o instanceof Short)
            type = OType.SHORT;
        else if (o instanceof Byte)
            type = OType.BYTE;
        else if (o instanceof Double)
            type = OType.DOUBLE;
/*        else
            type = OType.LINK;*/

        return type;
    }


}
