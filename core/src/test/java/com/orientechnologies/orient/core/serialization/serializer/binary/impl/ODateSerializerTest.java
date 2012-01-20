package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import java.util.Calendar;
import java.util.Date;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 20.01.12
 */
public class ODateSerializerTest {
    private final static int FIELD_SIZE = 8;
    private final Date OBJECT = new Date();
    private ODateSerializer dateSerializer;
    private final byte[] stream = new byte[FIELD_SIZE];

    @BeforeClass
    public void beforeClass() {
        dateSerializer = new ODateSerializer();
    }

    @Test
    public void testFieldSize() {
        Assert.assertEquals(dateSerializer.getFieldSize(OBJECT), FIELD_SIZE);
    }

    @Test()
    public void testSerialize() {
        dateSerializer.serialize(OBJECT, stream, 0);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(OBJECT);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        Assert.assertEquals(dateSerializer.deserialize(stream, 0), calendar.getTime());
    }
}
