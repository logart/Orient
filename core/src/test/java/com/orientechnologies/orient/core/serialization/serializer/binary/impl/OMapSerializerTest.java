package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Created by IntelliJ IDEA.
 * User: gman
 * Date: 04.03.12
 * Time: 21:01
 * To change this template use File | Settings | File Templates.
 */
public class OMapSerializerTest {

    private final OMapSerializer mapSerializer = new OMapSerializer();
    private final byte[] stream = new byte[1024];
    
    @Test
    public void testMap() {
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put("a", 1);
        map.put("b", 2);
        map.put("c", 3);
        map.put("d", 4);
        
        mapSerializer.serialize(map, stream, 0);
        
        Map<String, Integer> restored = mapSerializer.deserialize(stream, 0);

        assertEquals(restored.size(), 4);
        assertTrue(restored.containsKey("a"));
        assertEquals(restored.get("a"), Integer.valueOf(1));
        assertTrue(restored.containsKey("b"));
        assertEquals(restored.get("b"), Integer.valueOf(2));
        assertTrue(restored.containsKey("c"));
        assertEquals(restored.get("c"), Integer.valueOf(3));
        assertTrue(restored.containsKey("d"));
        assertEquals(restored.get("d"), Integer.valueOf(4));
    }
}
