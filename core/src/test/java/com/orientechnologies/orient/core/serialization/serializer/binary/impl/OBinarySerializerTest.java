package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 20.01.12
 */
public class OBinarySerializerTest {
    private int FIELD_SIZE;
    private byte[] OBJECT;
    private OBinarySerializer binarySerializer;
    byte[] stream;

    @BeforeClass
    public void beforeClass() {
        binarySerializer = new OBinarySerializer();
        OBJECT = new byte[]{1,2,3,4,5,6};
        FIELD_SIZE = OBJECT.length + OIntegerSerializer.INT_SIZE;
        stream = new byte[FIELD_SIZE];
    }

    @Test
    public void testFieldSize() {
        Assert.assertEquals(binarySerializer.getFieldSize(OBJECT), FIELD_SIZE);
    }

    @Test
    public void testSerialize() {
        binarySerializer.serialize(OBJECT, stream, 0);
        Assert.assertEquals(binarySerializer.deserialize(stream, 0), OBJECT);
    }
}
