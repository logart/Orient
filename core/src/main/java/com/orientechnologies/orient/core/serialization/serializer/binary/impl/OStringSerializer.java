package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;

import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.bytes2int;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.int2bytes;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OStringSerializer implements ObjectSerializer<String>{
    public int getFieldSize(String object) {
        return object.length() * 2 + OIntegerSerializer.INT_SIZE;
    }

    public void serialize(String object, byte[] stream, int startPosition) {
        OCharSerializer charSerializer = new OCharSerializer();
        int length = object.length();
        int2bytes(length, stream, startPosition);
        for(int i = 0; i < length; i++) {
            charSerializer.serialize(object.charAt(i), stream, startPosition + OIntegerSerializer.INT_SIZE + i * 2);
        }
    }

    public String deserialize(byte[] stream, int startPosition) {
        OCharSerializer charSerializer = new OCharSerializer();
        int len = bytes2int(stream, startPosition);
        StringBuilder stringBuilder = new StringBuilder();
        for(int i = 0; i < len; i++) {
            stringBuilder.append(charSerializer.deserialize(stream, startPosition + OIntegerSerializer.INT_SIZE + i * 2));
        }
        return stringBuilder.toString();
    }
}
