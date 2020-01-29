package org.mapdb.ser;

import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

/**
 * Created by jan on 2/28/16.
 */
public class DateSerializer extends EightByteSerializer<Date> {

    @Override
    public void serialize(DataOutput2 out, Date value) {
        out.writeLong(value.getTime());
    }

    @Override
    public Date deserialize(DataInput2 in) {
        return new Date(in.readLong());
    }

    @Nullable
    @Override
    public Class serializedType() {
        return Date.class;
    }

    @Override
    protected Date unpack(long l) {
        return new Date(l);
    }

    @Override
    protected long pack(Date l) {
        return l.getTime();
    }

    @Override
    final public int valueArraySearch(long[] keys, Date key) {
        //TODO valueArraySearch versus comparator test
        long time = key.getTime();
        return Arrays.binarySearch((long[])keys, time);
    }


}
