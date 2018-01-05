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
public class SerializerIntArray implements Serializer<int[]> {

    @Override
    public void serialize(DataOutput2 out, int[] value) throws IOException {
        out.packInt(value.length);
        for (int c : value) {
            out.writeInt(c);
        }
    }

    @Override
    public int[] deserialize(DataInput2 in, int available) throws IOException {
        final int size = in.unpackInt();
        int[] ret = new int[size];
        for (int i = 0; i < size; i++) {
            ret[i] = in.readInt();
        }
        return ret;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }


    @Override
    public int[] nextValue(int[] value) {
        value = value.clone();

        for (int i = value.length-1; ;i--) {
            int b1 = value[i];
            if(b1==Integer.MAX_VALUE){
                if(i==0)
                    return null;
                value[i]=Integer.MIN_VALUE;
                continue;
            }
            value[i] = b1+1;
            return value;
        }
    }

    @Override
    public Hasher<int[]> defaultHasher() {
        return Hashers.INT_ARRAY;
    }
}
