package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializerFactory;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class OBinaryLazyListTest {


    @Test
    public void testSerializationDeserialization() {
        final int size = 10;

        final OBinaryLazyList<Integer> lazyList = new OBinaryLazyList<Integer>();
        for(int i=0; i<size; i++){
            lazyList.add(i);
        }
        assertTrue(lazyList.isDirty(), "Collection must be dirty as newly created");
        assertEquals(lazyList.getStreamSize(),
                (size+1)* ObjectSerializerFactory.INSTANCE.getIndexSerializer().getFieldSize(null) +
                        size * (ObjectSerializerFactory.INSTANCE.getObjectSerializer(OType.INTEGER).getFieldSize(null) + ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE),
                "Trying to allocate wrong size"
        );

        final int offset = 10;
        final byte[] stream = new byte[lazyList.getStreamSize()+offset];

        Arrays.fill(stream, (byte) 99);

        lazyList.toStream(stream, offset);

        System.out.println(Arrays.toString(stream));

        final OBinaryLazyList<Integer> restoredLazyList = new OBinaryLazyList<Integer>(stream, offset);
        assertFalse(restoredLazyList.isDirty(), "Collection must not be dirty as restored and not changed");
        assertEquals(restoredLazyList, lazyList, "Restored lazyList must be equal to original one");


    }

    @Test
    public void testSerializationDeserializationWithDirtyRecords() {

    }
}
