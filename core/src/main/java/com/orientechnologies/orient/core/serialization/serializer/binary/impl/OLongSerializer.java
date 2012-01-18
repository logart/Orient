package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;

import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.bytes2long;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.long2bytes;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OLongSerializer implements ObjectSerializer<Long> {
    public int getFieldSize(Long object) {
        return 8;
    }

    public void serialize(Long object, byte[] stream, int startPosition) {
        long2bytes(object, stream, startPosition);
    }

    public Long deserialize(byte[] stream, int startPosition) {
        return bytes2long(stream, startPosition);
    }
}
