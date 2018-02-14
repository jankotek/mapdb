package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.hasher.Hasher;
import org.mapdb.hasher.Hashers;

import java.io.IOException;

/**
 * Created by jan on 2/28/16.
 */
//TODO optimize
public class SerializerLongArray implements Serializer<long[]> {

    @Override
    public void serialize(DataOutput2 out, long[] value) throws IOException {
        out.packInt(value.length);
        for (long c : value) {
            out.writeLong(c);
        }
    }

    @Override
    public long[] deserialize(DataInput2 in, int available) throws IOException {
        final int size = in.unpackInt();
        long[] ret = new long[size];
        for (int i = 0; i < size; i++) {
            ret[i] = in.readLong();
        }
        return ret;
    }


    @Override
    public boolean isTrusted() {
        return true;
    }


    @Override
    public long[] nextValue(long[] value) {
        value = value.clone();

        for (int i = value.length-1; ;i--) {
            long b1 = value[i];
            if(b1==Long.MAX_VALUE){
                if(i==0)
                    return null;
                value[i]=Long.MIN_VALUE;
                continue;
            }
            value[i] = b1+1L;
            return value;
        }
    }

    @Override
    public Hasher<long[]> defaultHasher() {
        return Hashers.LONG_ARRAY;
    }
}
