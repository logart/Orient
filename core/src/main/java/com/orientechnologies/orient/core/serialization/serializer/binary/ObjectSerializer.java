package com.orientechnologies.orient.core.serialization.serializer.binary;

/**
 * This interface is used for serializing OrientDB datatypes
 *
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public interface ObjectSerializer<T> {

    /**
     * Obtain size of the serialized object
     * Size is the amount of bites that required for storing object (for example: for storing integer we need 4 bytes)
     *
     * @param object is the object to measure its size
     * @return size of the serialized object
     */
    int getFieldSize(T object);

    /**
     * Writes object to the stream starting from the startPosition
     *
     * @param object is the object to serialize
     * @param stream is the stream where object will be written
     * @param startPosition is the position to start writing from
     */
    void serialize(T object, byte[] stream, int startPosition);

    /**
     * Reads object from the stream starting from the startPosition
     *
     * @param stream is the stream from object will be read
     * @param startPosition is the position to start reading from
     * @return instance of the deserialized object
     */
    T deserialize(byte[] stream, int startPosition);
}
