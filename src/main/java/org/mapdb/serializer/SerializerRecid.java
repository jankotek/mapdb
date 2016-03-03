package org.mapdb.serializer;

import org.mapdb.DBUtil;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerRecid extends SerializerEightByte<Long> {

    @Override
    public void serialize(DataOutput2 out, Long value) throws IOException {
        DBUtil.packRecid(out, value);
    }

    @Override
    public Long deserialize(DataInput2 in, int available) throws IOException {
        return new Long(DBUtil.unpackRecid(in));
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
    public int valueArraySearch(Object keys, Long key) {
        return Arrays.binarySearch((long[])keys, key);
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, Object vals) throws IOException {
        for (long o : (long[]) vals) {
            DBUtil.packRecid(out, o);
        }
    }

    @Override
    public long[] valueArrayDeserialize(DataInput2 in, int size) throws IOException {
        long[] ret = new long[size];
        for (int i = 0; i < size; i++) {
            ret[i] = DBUtil.unpackRecid(in);
        }
        return ret;
    }
}
