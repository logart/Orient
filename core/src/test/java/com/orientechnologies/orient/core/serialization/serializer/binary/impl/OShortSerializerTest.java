package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OShortSerializerTest {
    private static final int FIELD_SIZE = 2;
    private static final Short OBJECT = 1;
    private OShortSerializer shortSerializer;
    byte[] stream = new byte[FIELD_SIZE];

    @BeforeClass
    public void beforeClass() {
        shortSerializer = new OShortSerializer();
    }

    @Test
    public void testFieldSize() {
        Assert.assertEquals(shortSerializer.getObjectSize(null), FIELD_SIZE);
    }

    @Test
    public void testSerialize() {
        shortSerializer.serialize(OBJECT, stream, 0);
        Assert.assertEquals(shortSerializer.deserialize(stream, 0), OBJECT);
    }
}
