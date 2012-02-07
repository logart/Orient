package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OCharSerializer implements ObjectSerializer<Character> {
     /**
     *  size of char value in bytes
     */
    public static final int CHAR_SIZE = 2;

    public int getObjectSize(Character object) {
        return CHAR_SIZE;
    }

    public void serialize(Character object, byte[] stream, int startPosition) {
        stream[startPosition] = (byte) (object >>> 8);
        stream[startPosition + 1] = (byte) (object.charValue());
    }

    public Character deserialize(byte[] stream, int startPosition) {
        return (char) (
                ((stream[startPosition] & 0xFF) << 8) +
                        (stream[startPosition + 1] & 0xFF)
        );
    }
}
