package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import java.util.Arrays;
import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;

import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.bytes2int;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.int2bytes;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 20.01.12
 */
public class OBinarySerializer implements ObjectSerializer<byte[]>{
    public int getFieldSize(byte[] object) {
        return object.length + OIntegerSerializer.INT_SIZE;
    }

    public void serialize(byte[] object, byte[] stream, int startPosition) {
        int len = object.length;
        int2bytes(len, stream, startPosition);
        System.arraycopy(object, 0, stream, startPosition + OIntegerSerializer.INT_SIZE, len);
    }

    public byte[] deserialize(byte[] stream, int startPosition) {
        int len = bytes2int(stream, startPosition);
        return Arrays.copyOfRange(stream, startPosition + OIntegerSerializer.INT_SIZE, startPosition + OIntegerSerializer.INT_SIZE + len);
    }
}
