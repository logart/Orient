package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OBooleanSerializer implements ObjectSerializer<Boolean>{
    public int getFieldSize(Boolean object) {
        return 1;
    }

    public void serialize(Boolean object, byte[] stream, int startPosition) {
        if(object)
            stream[startPosition] = (byte) 1;
        else
            stream[startPosition] = (byte) 0;
    }

    public Boolean deserialize(byte[] stream, int startPosition) {
        return stream[startPosition] == 1;
    }
}
