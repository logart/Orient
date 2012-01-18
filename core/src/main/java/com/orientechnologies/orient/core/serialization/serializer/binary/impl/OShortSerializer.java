package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;

import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.bytes2short;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.short2bytes;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OShortSerializer implements ObjectSerializer<Short> {

    public int getFieldSize(Short object) {
        return 2;
    }

    public void serialize(Short object, byte[] stream, int startPosition) {
        short2bytes(object, stream, startPosition);
    }

    public Short deserialize(byte[] stream, int startPosition) {
        return bytes2short(stream, startPosition);
    }
}
