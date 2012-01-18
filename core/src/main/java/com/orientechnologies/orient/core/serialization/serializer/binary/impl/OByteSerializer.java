package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OByteSerializer implements ObjectSerializer<Byte>{
    public int getFieldSize(Byte object) {
        return 1;
    }

    public void serialize(Byte object, byte[] stream, int startPosition) {
        stream[startPosition] = object;
    }

    public Byte deserialize(byte[] stream, int startPosition) {
        return stream[startPosition];
    }
}
