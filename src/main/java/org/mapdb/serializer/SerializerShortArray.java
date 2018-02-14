package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.hasher.Hasher;
import org.mapdb.hasher.Hashers;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerShortArray implements  Serializer<short[]> {
    @Override
    public void serialize(DataOutput2 out, short[] value) throws IOException {
        out.packInt(value.length);
        for (short v : value) {
            out.writeShort(v);
        }
    }

    @Override
    public short[] deserialize(DataInput2 in, int available) throws IOException {
        short[] ret = new short[in.unpackInt()];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = in.readShort();
        }
        return ret;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public short[] nextValue(short[] value) {
        value = value.clone();

        for (int i = value.length-1; ;i--) {
            short b1 = value[i];
            if(b1==Short.MAX_VALUE){
                if(i==0)
                    return null;
                value[i]=Short.MIN_VALUE;
                continue;
            }
            value[i] = (short) (b1+1);
            return value;
        }
    }

    @Override
    public Hasher<short[]> defaultHasher() {
        return Hashers.SHORT_ARRAY;
    }
}
