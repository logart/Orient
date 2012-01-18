package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 18.01.12
 */
public class OLongSerializerTest {
    private static final int FIELD_SIZE = 8;
    private static final Long OBJECT = 999999999999999999L;
    private OLongSerializer longSerializer;
    byte[] stream = new byte[FIELD_SIZE];

    @BeforeClass
    public void beforeClass() {
        longSerializer = new OLongSerializer();
    }

    @Test
    public void testFieldSize() {
        Assert.assertEquals(longSerializer.getFieldSize(null), FIELD_SIZE);
    }

    @Test
    public void testSerialize() {
        longSerializer.serialize(OBJECT, stream, 0);
        Assert.assertEquals(longSerializer.deserialize(stream, 0), OBJECT);
    }
}
