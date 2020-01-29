package org.mapdb.ser;

import org.mapdb.io.DataIO;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;

/**
 * Created by jan on 2/28/16.
 */
public class RecidArraySerializer extends LongArraySerializer{

    @Override
    public void serialize(DataOutput2 out, long[] value) {
        out.packInt(value.length);
        for (long recid : value) {
            DataIO.packRecid(out, recid);
        }
    }

    @Override
    public long[] deserialize(DataInput2 in, int available) {
        int size = in.unpackInt();
        long[] ret = new long[size];
        for (int i = 0; i < size; i++) {
            ret[i] = DataIO.unpackRecid(in);
        }
        return ret;
    }


}
