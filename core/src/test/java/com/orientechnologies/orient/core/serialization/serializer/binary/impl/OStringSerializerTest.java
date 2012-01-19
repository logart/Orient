package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 19.01.12
 */
public class OStringSerializerTest {
    private int FIELD_SIZE;
    private String OBJECT;
    private OStringSerializer stringSerializer;
    byte[] stream;

    @BeforeClass
    public void beforeClass() {
        stringSerializer = new OStringSerializer();
        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        for(int i = 0; i < 100; i++) {
            sb.append((char) random.nextInt());
        }
        OBJECT = sb.toString();
        FIELD_SIZE = OBJECT.length() * 2 + 4;
        stream = new byte[FIELD_SIZE];
    }

    @Test
    public void testFieldSize() {
        Assert.assertEquals(stringSerializer.getFieldSize(OBJECT), FIELD_SIZE);
    }

    @Test
    public void testSerialize() {
        stringSerializer.serialize(OBJECT, stream, 0);
        Assert.assertEquals(stringSerializer.deserialize(stream, 0), OBJECT);
    }
}
