package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerBigDecimal extends GroupSerializerObjectArray<BigDecimal> {
    @Override
    public void serialize(DataOutput2 out, BigDecimal value) throws IOException {
        BYTE_ARRAY.serialize(out, value.unscaledValue().toByteArray());
        out.packInt(value.scale());
    }

    @Override
    public BigDecimal deserialize(DataInput2 in, int available) throws IOException {
        return new BigDecimal(new BigInteger(
                BYTE_ARRAY.deserialize(in, -1)),
                in.unpackInt());
    }

    @Override
    public boolean isTrusted() {
        return true;
    }
}
