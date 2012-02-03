package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import java.util.Calendar;
import java.util.Date;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;
import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializerFactory;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 20.01.12
 */
public class ODateTimeSerializer implements ObjectSerializer<Date>{
    public int getFieldSize(Date object) {
        return OLongSerializer.LONG_SIZE;
    }

    public void serialize(Date object, byte[] stream, int startPosition) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(object);
        OLongSerializer longSerializer = (OLongSerializer) ObjectSerializerFactory.INSTANCE.getObjectSerializer(OType.LONG);
        longSerializer.serialize(calendar.getTimeInMillis(), stream, startPosition);
    }

    public Date deserialize(byte[] stream, int startPosition) {
        Calendar calendar = Calendar.getInstance();
        OLongSerializer longSerializer = (OLongSerializer) ObjectSerializerFactory.INSTANCE.getObjectSerializer(OType.LONG);
        calendar.setTimeInMillis(longSerializer.deserialize(stream, startPosition));
        return calendar.getTime();
    }
}
