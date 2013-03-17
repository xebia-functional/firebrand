package org.firebrandocm.dao.impl;

import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import org.firebrandocm.dao.TypeConverter;

import java.nio.ByteBuffer;

/**
 * byte[] Type converter to serialize to and from ByteBuffer's
 */
public class ByteArrayTypeConverter implements TypeConverter<byte[]> {
    @Override
    public ByteBuffer toValue(byte[] value) throws Exception {
        return BytesArraySerializer.get().toByteBuffer(value);
    }

    @Override
    public byte[] fromValue(ByteBuffer value) throws Exception {
        return BytesArraySerializer.get().fromByteBuffer(value);
    }
}
