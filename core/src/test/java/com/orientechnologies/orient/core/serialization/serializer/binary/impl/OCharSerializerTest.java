package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 19.01.12
 */
public class OCharSerializerTest {
    private static final int FIELD_SIZE = 2;
    private static final Character OBJECT = (char) (new Random()).nextInt();
    private OCharSerializer charSerializer;
    byte[] stream = new byte[FIELD_SIZE];

    @BeforeClass
    public void beforeClass() {
        charSerializer = new OCharSerializer();
    }

    @Test
    public void testFieldSize() {
        Assert.assertEquals(charSerializer.getFieldSize(null), FIELD_SIZE);
    }

    @Test
    public void testSerialize() {
        charSerializer.serialize(OBJECT, stream, 0);
        Assert.assertEquals(charSerializer.deserialize(stream, 0), OBJECT);
    }
}
