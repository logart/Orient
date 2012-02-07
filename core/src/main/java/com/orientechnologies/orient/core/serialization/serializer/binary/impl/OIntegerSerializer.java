package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;

import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.bytes2int;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.int2bytes;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 17.01.12
 */
public class OIntegerSerializer implements ObjectSerializer<Integer>{
     /**
     *  size of int value in bytes
     */
    public static final int INT_SIZE = 4;

    public int getObjectSize(Integer object) {
        return INT_SIZE;
    }

    public void serialize(Integer object, byte[] stream, int startPosition) {
        int2bytes(object, stream, startPosition);
    }

    public Integer deserialize(byte[] stream, int startPosition) {
        return bytes2int(stream, startPosition);
    }
}
