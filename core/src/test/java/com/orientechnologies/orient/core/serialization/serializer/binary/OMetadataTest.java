package com.orientechnologies.orient.core.serialization.serializer.binary;

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Test ma sure {@link OMetadata} serialize and deserialize just right
 *
 * @author gman
 */
public class OMetadataTest {

    @Test
    public void testSerializingAndDeserializingMetadata() {
        
        final Map<String, Integer> fields = new HashMap<String, Integer>();
        fields.put("fieldA",0);
        fields.put("fieldB",5);
        fields.put("fieldC",10);

        final OMetadata original = new OMetadata(-1, 15, "documentClassName", fields);
        final byte[] stream = new byte[original.getMetaSize() + original.getDataSize()];
        original.toBytes(stream);

        final OMetadata restored = OMetadata.createFromBytes(stream);

        assertEquals(restored.getDocumentClassName(), original.getDocumentClassName(), "DocumentClassName mismatch");
        assertEquals(restored.getMetaSize(), original.getMetaSize(), "MetaSize mismatch");
        assertEquals(restored.getDataSize(), original.getDataSize(), "DataSize mismatch");
        assertEquals(restored.getMetadata(), original.getMetadata(), "Metadata mismatch");
    }
    
    @Test
    public void testCreationMetadataFromDocument() {

        final ODocument doc = new ODocument();
        doc.field("fieldA", 5, OType.INTEGER);
        doc.field("fieldB", 10, OType.INTEGER);
        doc.field("fieldC", 15, OType.INTEGER);

        final OMetadata metadata = OMetadata.createFromDocument(doc);

        assertEquals(metadata.getDataSize(), 15, "Data size has been calculated wrong");//3 * (integer size + type identifier size)
        assertEquals(metadata.getDocumentClassName(), null, "Document must not have schema class");
        assertEquals(metadata.getMetadata().size(), 3, "Document contains 3 fields");

        assertEquals(metadata.getMetadata().get("fieldA"), Integer.valueOf(0), "Field offset has been calculated wrong");
        assertEquals(metadata.getMetadata().get("fieldB"), Integer.valueOf(5), "Field offset has been calculated wrong");
        assertEquals(metadata.getMetadata().get("fieldC"), Integer.valueOf(10), "Field offset has been calculated wrong");
    }
}
