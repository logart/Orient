package com.orientechnologies.orient.core.serialization.serializer.binary;

/**
 * This interface indicate that object is self-responsible for its serialization and deserialization
 *
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public interface OBinarySerializable {

    /**
     * Size of the object in binary representation
     *
     * @return
     */
    int getBinarySize();

    /**
     * Writes object into the stream starting from the given position
     *
     * @param stream to write object
     * @param startPosition is the position to start
     */
    void serialize(byte[] stream, int startPosition);

    /**
     * Reads object from the stream starting from the given position
     * @param stream
     * @param startPosition
     */
    void deserialize(byte[] stream, int startPosition);
}
