package com.orientechnologies.orient.core.serialization.serializer.binary.impl;

import java.util.Calendar;
import java.util.Date;
import com.orientechnologies.orient.core.serialization.serializer.binary.ObjectSerializer;

/**
 * @author ibershadskiy <a href="mailto:ibersh20@gmail.com">Ilya Bershadskiy</a>
 * @since 20.01.12
 */
public class ODateSerializer implements ObjectSerializer<Date> {
    public int getFieldSize(Date object) {
        return OLongSerializer.LONG_SIZE;
    }

    public void serialize(Date object, byte[] stream, int startPosition) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(object);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        ODateTimeSerializer dateTimeSerializer = new ODateTimeSerializer(); //(ODateSerializer) ObjectSerializerFactory.INSTANCE.getObjectSerializer(OType.DATE);
        dateTimeSerializer.serialize(calendar.getTime(), stream, startPosition);
/*        OLongSerializer longSerializer = new OLongSerializer();//(OLongSerializer) ObjectSerializerFactory.INSTANCE.getObjectSerializer(OType.LONG);
        longSerializer.serialize(calendar.getTimeInMillis(), stream, startPosition);*/
    }

    public Date deserialize(byte[] stream, int startPosition) {
/*        Calendar calendar = Calendar.getInstance();
        OLongSerializer longSerializer = new OLongSerializer();//(OLongSerializer) ObjectSerializerFactory.INSTANCE.getObjectSerializer(OType.LONG);
        calendar.setTimeInMillis(longSerializer.deserialize(stream, startPosition));
        return calendar.getTime();*/
        ODateTimeSerializer dateTimeSerializer = new ODateTimeSerializer(); //(ODateSerializer) ObjectSerializerFactory.INSTANCE.getObjectSerializer(OType.DATE);
        return dateTimeSerializer.deserialize(stream, startPosition);
    }
}
