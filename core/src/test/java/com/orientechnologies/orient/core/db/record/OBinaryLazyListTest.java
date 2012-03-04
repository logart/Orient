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
        stream = new byte[lazyList.getBinarySize()+OFFSET];
        lazyList.serialize(stream, OFFSET);
    }

    @Test
    public void testInitialization() {

        assertFalse(lazyList.hasChanges(), "Just serialized list has no changes");
        assertTrue(lazyList.isDirty(), "But collection must be dirty as newly created and without parent");
        assertEquals(lazyList.getBinarySize(),
                (SIZE+1)* ObjectSerializerFactory.INSTANCE.getIndexSize() +
                        SIZE * (ObjectSerializerFactory.INSTANCE.getObjectSerializer(OType.INTEGER).getObjectSize(null) + ObjectSerializerFactory.TYPE_IDENTIFIER_SIZE),
                "Trying to allocate wrong size"
        );

        final OBinaryLazyList<Integer> restoredLazyList = new OBinaryLazyList<Integer>();
        restoredLazyList.deserialize(stream, OFFSET);

        assertFalse(restoredLazyList.hasChanges(), "Collection must not have changes as restored and not changed");
        assertEquals(restoredLazyList, lazyList, "Restored lazyList must be equal to original one");
    }

    @Test
    public void testSerealizationDeserializationOfCopy() {

        final OBinaryLazyList<Integer> restoredLazyList = new OBinaryLazyList<Integer>();
        restoredLazyList.deserialize(stream, OFFSET);

        assertFalse(restoredLazyList.hasChanges(), "Collection must not has changes as restored and not changed");
        assertEquals(restoredLazyList, lazyList, "Restored lazyList must be equal to original one");

        final byte[] stream2 = new byte[lazyList.getBinarySize()];
        restoredLazyList.serialize(stream2,  0);

        //make sure copying is right
        final OBinaryLazyList<Integer> secondRestoredLazyList = new OBinaryLazyList<Integer>();
        secondRestoredLazyList.deserialize(stream2, 0);

        assertFalse(secondRestoredLazyList.hasChanges(), "Collection must not has changes as restored and not changed");
        assertEquals(secondRestoredLazyList, lazyList, "Restored lazyList must be equal to original one");
    }

    @Test
    public void testSerializationDeserializationWithDirtyRecords() {

        final OBinaryLazyList<Integer> restoredLazyList = new OBinaryLazyList<Integer>();
        restoredLazyList.deserialize(stream, OFFSET);

        assertFalse(restoredLazyList.hasChanges(), "Collection must not has changes as restored and not changed");
        assertEquals(restoredLazyList, lazyList, "Restored lazyList must be equal to original one");

        restoredLazyList.remove(9);
        restoredLazyList.set(5, 255);
        restoredLazyList.add(8, 255);

        assertTrue(restoredLazyList.isDirty(), "Collection has changes and must be dirty");

        final byte[] stream2 = new byte[lazyList.getBinarySize()];
        restoredLazyList.serialize(stream2,  0);

        //make sure serialization forks fine for changed collections
        final OBinaryLazyList<Integer> secondRestoredLazyList = new OBinaryLazyList<Integer>();
        secondRestoredLazyList.deserialize(stream2, 0);

        assertFalse(secondRestoredLazyList.hasChanges(), "Collection must not has changes as restored and not changed");
        assertEquals(secondRestoredLazyList, restoredLazyList, "Restored lazyList must be equal to original one");
    }
}
