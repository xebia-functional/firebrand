package org.firebrandocm.dao.impl;

import me.prettyprint.cassandra.serializers.StringSerializer;
import org.firebrandocm.dao.TypeConverter;

import java.nio.ByteBuffer;

import static java.lang.Enum.valueOf;


public class EnumTypeConverter implements TypeConverter<Enum> {

    @Override
    public Enum fromValue(ByteBuffer value, Class<Enum> targetType) throws Exception {
        String stringValue = StringSerializer.get().fromByteBuffer(value);
        return stringValue != null ? valueOf(targetType, stringValue) : null;
    }

    @Override
    public ByteBuffer toValue(Enum value) throws Exception {
        String stringValue = value.name();
        return StringSerializer.get().toByteBuffer(stringValue);
    }
}
