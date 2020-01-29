package org.mapdb.ser;

import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataIO;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by jan on 2/28/16.
 */
public class RecidSerializer extends EightByteSerializer<Long> {

    @Override
    public void serialize(DataOutput2 out, Long value) {
        DataIO.packRecid(out, value);
    }

    @Override
    public Long deserialize(DataInput2 in) {
        return new Long(DataIO.unpackRecid(in));
    }

    @Nullable
    @Override
    public Class serializedType() {
        return Long.class;
    }

    @Override
    public int fixedSize() {
        return -1;
    }

    @Override
    protected Long unpack(long l) {
        return new Long(l);
    }

    @Override
    protected long pack(Long l) {
        return l;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }


    @Override
    public int valueArraySearch(long[] keys, Long key) {
        return Arrays.binarySearch((long[])keys, key);
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, long[] vals) {
        for (long o : (long[]) vals) {
            DataIO.packRecid(out, o);
        }
    }

    @Override
    public long[] valueArrayDeserialize(DataInput2 in, int size) {
        long[] ret = new long[size];
        for (int i = 0; i < size; i++) {
            ret[i] = DataIO.unpackRecid(in);
        }
        return ret;
    }

    @Override
    public Long valueArrayBinaryGet(DataInput2 input, int keysLen, int pos) {
        input.unpackLongSkip(pos);
        return deserialize(input,-1);
    }
}
