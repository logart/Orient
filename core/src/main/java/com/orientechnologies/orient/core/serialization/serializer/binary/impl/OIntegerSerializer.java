package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;

import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.bytes2int;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.int2bytes;

/**
 * @author ibershadskiy <a href="mailto:Ilya.Bershadskiy@exigenservices.com">Ilya Bershadskiy</a>
 * @since 17.01.12
 */
public class OIntegerSerializer implements ObjectSerializer<Integer>{
    public int getFieldSize(Integer object) {
        return 4;
    }

    public void serialize(Integer object, byte[] stream, int startPosition) {
        stream = int2bytes(object, stream, startPosition);
    }

    public Integer deserialize(byte[] stream, int startPosition) {
        return bytes2int(stream, startPosition);
    }
}
