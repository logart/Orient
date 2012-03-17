package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class OEmbeddedSerializerTest {

    private ObjectSerializer<ODocument> serializer = ObjectSerializerFactory.INSTANCE.getObjectSerializer(OType.EMBEDDED);

    @Test
    public void testDocumentSerializationWithoutClass() {

        final ODocument original = new ODocument();
        original.field("id", Integer.valueOf(10));
        original.field("name", "myObjectName");

        final int initialOffset = 10;
        final int objectSize = serializer.getObjectSize(original);
        final byte[] stream = new byte[initialOffset + objectSize];

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
