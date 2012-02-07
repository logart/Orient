package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializable;
import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializerFactory;

/**
 *
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class OBinarySerializableDelegate implements ObjectSerializer<OBinarySerializable> {

    public int getObjectSize(OBinarySerializable object) {
        return object.getBinarySize();
    }

    public void serialize(OBinarySerializable object, byte[] stream, int startPosition) {
        object.serialize(stream, startPosition);
    }

    public OBinarySerializable deserialize(byte[] stream, int startPosition) {
        final OBinarySerializable obj = ObjectSerializerFactory.INSTANCE.getInstanceOf(stream[startPosition-1]);
        obj.deserialize(stream, startPosition);
        return obj;
    }
}
