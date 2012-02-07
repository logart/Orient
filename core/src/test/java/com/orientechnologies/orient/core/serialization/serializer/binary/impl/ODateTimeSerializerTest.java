package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import java.util.Date;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 20.01.12
 */
public class ODateTimeSerializerTest {
    private final static int FIELD_SIZE = 8;
    private static final Date OBJECT = new Date();
    private ODateTimeSerializer dateTimeSerializer;
    private static final byte[] stream = new byte[FIELD_SIZE];

    @BeforeClass
    public void beforeClass() {
        dateTimeSerializer = new ODateTimeSerializer();
    }

    @Test
    public void testFieldSize() {
        Assert.assertEquals(dateTimeSerializer.getObjectSize(OBJECT), FIELD_SIZE);
    }

    @Test
    public void testSerialize() {
        dateTimeSerializer.serialize(OBJECT, stream, 0);
        Assert.assertEquals(dateTimeSerializer.deserialize(stream, 0), OBJECT);
    }
}
