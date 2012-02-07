package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;

import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.bytes2long;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.long2bytes;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 17.01.12
 */
public class ODoubleSerializer implements ObjectSerializer <Double>{
     /**
     *  size of double value in bytes
     */
    public static final int DOUBLE_SIZE = 8;

    public int getObjectSize(Double object) {
        return DOUBLE_SIZE;
    }

    public void serialize(Double object, byte[] stream, int startPosition) {
        long2bytes(Double.doubleToLongBits(object), stream, startPosition);
    }

    public Double deserialize(byte[] stream, int startPosition) {
        return Double.longBitsToDouble(bytes2long(stream, startPosition));
    }
}
