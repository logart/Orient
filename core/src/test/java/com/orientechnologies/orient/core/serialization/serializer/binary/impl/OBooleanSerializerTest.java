package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OBooleanSerializerTest {
    private static final int FIELD_SIZE = 1;
    private static final Boolean OBJECT_TRUE = true;
    private static final Boolean OBJECT_FALSE = false;
    private OBooleanSerializer booleanSerializer;
    byte[] stream = new byte[FIELD_SIZE];

    @BeforeClass
    public void beforeClass() {
        booleanSerializer = new OBooleanSerializer();
    }

    @Test
    public void testFieldSize() {
        Assert.assertEquals(booleanSerializer.getFieldSize(null), FIELD_SIZE);
    }

    @Test
    public void testSerialize() {
        booleanSerializer.serialize(OBJECT_TRUE, stream, 0);
        Assert.assertEquals(booleanSerializer.deserialize(stream, 0), OBJECT_TRUE);
        booleanSerializer.serialize(OBJECT_FALSE, stream, 0);
        Assert.assertEquals(booleanSerializer.deserialize(stream, 0), OBJECT_FALSE);
    }
}
