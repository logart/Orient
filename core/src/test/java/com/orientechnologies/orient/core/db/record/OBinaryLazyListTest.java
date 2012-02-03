package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author <a href="mailto:gmandnepr@gmail.com">Evgeniy Degtiarenko</a>
 */
public class OBinaryLazyListTest {

    private static final int SIZE = 10;
    private static final int OFFSET = 10;
    private OBinaryLazyList<Integer> lazyList;
    private byte[] stream;

    @BeforeMethod
    public void setUpMethod() {
        lazyList = new OBinaryLazyList<Integer>();
        for(int i=0; i<SIZE; i++){
            lazyList.add(i);
        }
        stream = new byte[lazyList.getStreamSize()+OFFSET];
        lazyList.toStream(stream, OFFSET);
    }

    @Test
    public void testInitialization() {

        assertTrue(lazyList.isDirty(), "Collection must be dirty as newly created");
        assertEquals(lazyList.getStreamSize(),
                (SIZE+1)* ObjectSerializerFactory.INSTANCE.getIndexSerializer().getFieldSize(null) +
                        SIZE * (ObjectSerializerFactory.INSTANCE.getObjectSerializer(OType.INTEGER).getFieldSize(null) + ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE),
                "Trying to allocate wrong size"
        );

        final OBinaryLazyList<Integer> restoredLazyList = new OBinaryLazyList<Integer>(stream, OFFSET);

        assertFalse(restoredLazyList.isDirty(), "Collection must not be dirty as restored and not changed");
        assertEquals(restoredLazyList, lazyList, "Restored lazyList must be equal to original one");
    }

    @Test
    public void testSerealizationDeserializationOfCopy() {

        final OBinaryLazyList<Integer> restoredLazyList = new OBinaryLazyList<Integer>(stream, OFFSET);
        assertFalse(restoredLazyList.isDirty(), "Collection must not be dirty as restored and not changed");
        assertEquals(restoredLazyList, lazyList, "Restored lazyList must be equal to original one");

        final byte[] stream2 = new byte[lazyList.getStreamSize()];
        restoredLazyList.toStream(stream2,  0);

        //make sure copying is right
        final OBinaryLazyList<Integer> secondRestoredLazyList = new OBinaryLazyList<Integer>(stream2, 0);

        assertFalse(secondRestoredLazyList.isDirty(), "Collection must not be dirty as restored and not changed");
        assertEquals(secondRestoredLazyList, lazyList, "Restored lazyList must be equal to original one");
    }

    @Test
    public void testSerializationDeserializationWithDirtyRecords() {

        final OBinaryLazyList<Integer> restoredLazyList = new OBinaryLazyList<Integer>(stream, OFFSET);
        assertFalse(restoredLazyList.isDirty(), "Collection must not be dirty as restored and not changed");
        assertEquals(restoredLazyList, lazyList, "Restored lazyList must be equal to original one");

        restoredLazyList.remove(9);
        restoredLazyList.set(5, 255);
        restoredLazyList.add(8, 255);

        assertTrue(restoredLazyList.isDirty(), "Collection has changes and must be dirty");

        final byte[] stream2 = new byte[lazyList.getStreamSize()];
        restoredLazyList.toStream(stream2,  0);

        //make sure serialization forks fine for changed collections
        final OBinaryLazyList<Integer> secondRestoredLazyList = new OBinaryLazyList<Integer>(stream2, 0);
        assertFalse(secondRestoredLazyList.isDirty(), "Collection must not be dirty as restored and not changed");
        assertEquals(secondRestoredLazyList, restoredLazyList, "Restored lazyList must be equal to original one");
    }
}
