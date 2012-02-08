package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;

import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.bytes2int;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.bytes2long;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.int2bytes;
import static com.orientechnologies.orient.core.serialization.OBinaryProtocol.long2bytes;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 07.02.12
 */
public class OLinkSerializer implements ObjectSerializer<Object> {
    public int getObjectSize(Object object) {
        return OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE;
    }

    public void serialize(Object object, byte[] stream, int startPosition) {
        ORID rid = (ORID) object;
        int2bytes(rid.getClusterId(), stream, startPosition);
        long2bytes(rid.getClusterPosition(), stream, startPosition + OIntegerSerializer.INT_SIZE);
    }

    public Object deserialize(byte[] stream, int startPosition) {
        return new ORecordId(bytes2int(stream, startPosition), bytes2long(stream, startPosition + OIntegerSerializer.INT_SIZE));
    }
}
