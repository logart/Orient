package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.serialization.OMemoryInputStream;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: gman
 * Date: 04.03.12
 * Time: 20:44
 * To change this template use File | Settings | File Templates.
 */
public class OMapSerializer implements ObjectSerializer<Map> {

    @Override
    public int getObjectSize(Map object) {
        OMemoryStream memoryStream = null;
        ObjectOutputStream objectStream = null;
        try{
            memoryStream = new OMemoryStream();
            objectStream = new ObjectOutputStream(memoryStream);
            objectStream.writeObject(object);
            return memoryStream.toByteArray().length;
        } catch (IOException e) {
            throw new OSerializationException("Fail", e);
        } finally {
            if (objectStream != null) {
                try {
                    objectStream.close();
                } catch (IOException e) {
                    //should never happened
                }
            }
            if (memoryStream != null) {
                memoryStream.close();
            }
        }
    }

    @Override
    public void serialize(Map object, byte[] stream, int startPosition) {
        OMemoryStream memoryStream = null;
        ObjectOutputStream objectStream = null;
        try{
            memoryStream = new OMemoryStream();
            objectStream = new ObjectOutputStream(memoryStream);
            objectStream.writeObject(object);
            final byte[] bytes = memoryStream.toByteArray();
            System.arraycopy(bytes, 0, stream, startPosition, bytes.length);
        } catch (IOException e) {
            throw new OSerializationException("Fail", e);
        } finally {
            if (objectStream != null) {
                try {
                    objectStream.close();
                } catch (IOException e) {
                    //should never happened
                }
            }
            if (memoryStream != null) {
                memoryStream.close();
            }
        }        
    }

    @Override
    public Map deserialize(byte[] stream, int startPosition) {
        OMemoryInputStream memoryStream = null;
        ObjectInputStream objectStream = null;
        try {
            final byte[] actual = new byte[stream.length - startPosition];
            System.arraycopy(stream, startPosition, actual, 0, actual.length);
            memoryStream = new OMemoryInputStream(actual);
            objectStream = new ObjectInputStream(memoryStream);
            return (Map) objectStream.readObject();
        } catch (IOException e) {
            throw new OSerializationException("Fail", e);
        } catch (ClassNotFoundException e) {
            throw new OSerializationException("Fail", e);
        } finally {
            if (objectStream != null) {
                try {
                    objectStream.close();
                } catch (IOException e) {
                    //should never happened
                }
            }
            if (memoryStream != null) {
                memoryStream.close();
            }
        }
    }
}
