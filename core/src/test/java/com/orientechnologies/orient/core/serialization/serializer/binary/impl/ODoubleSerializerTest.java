package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class ODoubleSerializerTest {
    private static final int FIELD_SIZE = 8;
    private static final Double OBJECT = Math.PI;
    private ODoubleSerializer doubleSerializer;
    byte[] stream = new byte[FIELD_SIZE];

    @BeforeClass
    public void beforeClass() {
        doubleSerializer = new ODoubleSerializer();
    }

    @Test
    public void testFieldSize() {
        Assert.assertEquals(doubleSerializer.getObjectSize(null), FIELD_SIZE);
    }

    @Test
    public void testSerialize() {
        doubleSerializer.serialize(OBJECT, stream, 0);
        Assert.assertEquals(doubleSerializer.deserialize(stream, 0), OBJECT);
    }
}
