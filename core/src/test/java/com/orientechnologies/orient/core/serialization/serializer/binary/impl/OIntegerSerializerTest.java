package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 17.01.12
 */
@Test
public class OIntegerSerializerTest {
    private static final int FIELD_SIZE = 4;
    private static final Integer OBJECT = 1;
    private OIntegerSerializer integerSerializer;
    byte[] stream = new byte[FIELD_SIZE];

    @BeforeClass
    public void beforeClass() {
        integerSerializer = new OIntegerSerializer();
    }

    @Test
    public void testFieldSize() {
        Assert.assertEquals(integerSerializer.getObjectSize(null), FIELD_SIZE);
    }

    @Test
    public void testSerialize() {
        integerSerializer.serialize(OBJECT, stream, 0);
        Assert.assertEquals(integerSerializer.deserialize(stream, 0), OBJECT);
    }
}
