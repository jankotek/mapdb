package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.serializer.Serializers;

import java.io.IOException;
import java.math.BigInteger;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerBigInteger implements Serializer<BigInteger> {
    @Override
    public void serialize(DataOutput2 out, BigInteger value) throws IOException {
        Serializers.BYTE_ARRAY.serialize(out, value.toByteArray());
    }

    @Override
    public BigInteger deserialize(DataInput2 in, int available) throws IOException {
        return new BigInteger(Serializers.BYTE_ARRAY.deserialize(in, available));
    }

    @Override
    public boolean isTrusted() {
        return true;
    }
}
