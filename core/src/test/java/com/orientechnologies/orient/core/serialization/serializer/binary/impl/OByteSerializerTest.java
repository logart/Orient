package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OByteSerializerTest {
    private static final int FIELD_SIZE = 1;
    private static final Byte OBJECT = 1;
    private OByteSerializer byteSerializer;
    byte[] stream = new byte[FIELD_SIZE];

    @BeforeClass
    public void beforeClass() {
        byteSerializer = new OByteSerializer();
    }

    @Test
    public void testFieldSize() {
        Assert.assertEquals(byteSerializer.getFieldSize(null), FIELD_SIZE);
    }

    @Test
    public void testSerialize() {
        byteSerializer.serialize(OBJECT, stream, 0);
        Assert.assertEquals(byteSerializer.deserialize(stream, 0), OBJECT);
    }
}
