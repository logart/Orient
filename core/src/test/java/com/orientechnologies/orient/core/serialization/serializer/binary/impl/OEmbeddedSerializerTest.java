package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Created by IntelliJ IDEA.
 * User: gman
 * Date: 04.03.12
 * Time: 21:07
 * To change this template use File | Settings | File Templates.
 */
public class OEmbeddedSerializerTest {

    private ObjectSerializer<ODocument> serializer = ObjectSerializerFactory.INSTANCE.getObjectSerializer(OType.EMBEDDED);
    private byte[] stream = new byte[10*1024];

    @Test
    public void testDocumentSerializationWithoutClass() {
        final int initialOffset = 10;

        final ODocument original = new ODocument();
        original.field("id", Integer.valueOf(10));
        original.field("name", "myObjectName");
        
        serializer.serialize(original, stream, initialOffset);

        final ODocument restored = serializer.deserialize(stream, initialOffset);

        assertNotNull(restored);
        assertNotNull(restored.fieldNames());
        assertEquals(restored.fieldNames().length, original.fieldNames().length);
        final String[] fields = original.fieldNames();
        for(final String fieldName : fields) {
            assertEquals(restored.fieldType(fieldName), original.fieldType(fieldName));
            assertEquals(restored.field(fieldName), original.field(fieldName));
        }
    }
}
