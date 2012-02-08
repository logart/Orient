package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.orientechnologies.orient.core.id.ORecordId;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 07.02.12
 */
public class OLinkSerializerTest {
    private static final int FIELD_SIZE = OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE;
    private ORecordId OBJECT;
    private static final int clusterId = 5;
    private static final long position = 100500L;
    private OLinkSerializer linkSerializer;
    byte[] stream = new byte[FIELD_SIZE];

    @BeforeClass
    public void beforeClass() {
        OBJECT = new ORecordId(clusterId, position);
        linkSerializer = new OLinkSerializer();
    }

    @Test
    public void testFieldSize() {
        Assert.assertEquals(linkSerializer.getObjectSize(null), FIELD_SIZE);
    }

    @Test
    public void testSerialize() {
        linkSerializer.serialize(OBJECT, stream, 0);
        Assert.assertEquals(linkSerializer.deserialize(stream, 0), OBJECT);
    }
}
