package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;

import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.bytes2int;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.int2bytes;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OFloatSerializer implements ObjectSerializer<Float>{
    /**
     *  size of float value in bytes
     */
    public static final int FLOAT_SIZE = 4;

    public int getFieldSize(Float object) {
        return FLOAT_SIZE;
    }

    public void serialize(Float object, byte[] stream, int startPosition) {
        int2bytes(Float.floatToIntBits(object), stream, startPosition);
    }

    public Float deserialize(byte[] stream, int startPosition) {
        return Float.intBitsToFloat(bytes2int(stream, startPosition));
    }
}
