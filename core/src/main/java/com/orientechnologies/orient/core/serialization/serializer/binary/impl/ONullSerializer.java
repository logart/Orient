package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;

/**
 * Serialize and deserialize null values
 *
 * <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class ONullSerializer implements ObjectSerializer<Object> {

    public int getFieldSize(final Object object) {
        return 0;
    }

    public void serialize(final Object object, final byte[] stream, final int startPosition) {
        //nothing to serialize
    }

    public Object deserialize(final byte[] stream, final int startPosition) {
        //nothing to deserialize
        return null;
    }
}
