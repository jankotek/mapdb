package org.mapdb.ser;

import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Created by jan on 2/28/16.
 */
public class BigDecimalSerializer extends DefaultGroupSerializer<BigDecimal> {
    @Override
    public void serialize(DataOutput2 out, BigDecimal value) {
        Serializers.BYTE_ARRAY.serialize(out, value.unscaledValue().toByteArray());
        out.packInt(value.scale());
    }

    @Override
    public BigDecimal deserialize(DataInput2 in) {
        return new BigDecimal(new BigInteger(
                Serializers.BYTE_ARRAY.deserialize(in, -1)),
                in.unpackInt());
    }

    @Nullable
    @Override
    public Class serializedType() {
        return BigDecimal.class;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }
}
