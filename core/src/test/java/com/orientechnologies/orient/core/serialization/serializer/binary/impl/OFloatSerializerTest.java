package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OFloatSerializerTest {
    private static final int FIELD_SIZE = 4;
    private static final Float OBJECT = 3.14f;
    private OFloatSerializer floatSerializer;
    byte[] stream = new byte[FIELD_SIZE];

    @BeforeClass
    public void beforeClass() {
        floatSerializer = new OFloatSerializer();
    }

    @Test
    public void testFieldSize() {
        Assert.assertEquals(floatSerializer.getObjectSize(null), FIELD_SIZE);
    }

    @Test
    public void testSerialize() {
        floatSerializer.serialize(OBJECT, stream, 0);
        Assert.assertEquals(floatSerializer.deserialize(stream, 0), OBJECT);
    }
}
