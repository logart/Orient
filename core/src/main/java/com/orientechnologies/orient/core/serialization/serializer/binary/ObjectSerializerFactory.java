package com.orientechnologies.orient.core.serialization.serializer.binary;

import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * This class is responsible for obtaining ObjectSerializer realization, that fits for the next bytes in the stream
 *
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class ObjectSerializerFactory {



    /**
     * Obtain ObjectSerializer realization for the next bytes in the stream
     *
     * @param typeIdentifier is the type identifier that located before the data
     * @return ObjectSerializer realization that fits for the next data
     */
    public ObjectSerializer getObjectSerializer(final byte typeIdentifier) {
        //TODO implement
        return null;
    }

    /**
     * Obtain byte type identifier for writing serialized object
     *
     * @param type is the type to obtain identifier
     * @return byte type identifier to write into stream
     */
    public byte getTypeIdentifier(final OType type) {

        return (byte)0;
    }
}
