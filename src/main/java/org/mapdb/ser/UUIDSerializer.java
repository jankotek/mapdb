package org.mapdb.ser;

import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.UUID;

/**
 * Created by jan on 2/28/16.
 */
public class UUIDSerializer implements GroupSerializer<java.util.UUID, long[]> {
    @Override
    public void serialize(DataOutput2 out, UUID value) {
        out.writeLong(value.getMostSignificantBits());
        out.writeLong(value.getLeastSignificantBits());
    }

    @Override
    public UUID deserialize(DataInput2 in) {
        return new UUID(in.readLong(), in.readLong());
    }

    @Nullable
    @Override
    public Class serializedType() {
        return UUID.class;
    }

    @Override
    public int fixedSize() {
        return 16;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }


    @Override
    public int hashCode(UUID uuid, int seed) {
        //on java6 uuid.hashCode is not thread safe. This is workaround
        long a = uuid.getLeastSignificantBits() ^ uuid.getMostSignificantBits();
        return ((int) (a >> 32)) ^ (int) a;

    }


    @Override
    public int valueArraySearch(long[] keys, UUID key) {
        return Arrays.binarySearch(valueArrayToArray(keys), key); //TODO search
    }

    @Override
    public int valueArraySearch(long[] keys, UUID key, Comparator comparator) {
        return Arrays.binarySearch(valueArrayToArray(keys), key, comparator); //TODO search
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, long[] vals) {
        for (long o : (long[]) vals) {
            out.writeLong(o);
        }
    }

    @Override
    public long[] valueArrayDeserialize(DataInput2 in, int size) {
        size *= 2;
        long[] ret = new long[size];
        for (int i = 0; i < size; i++) {
            ret[i] = in.readLong();
        }
        return ret;
    }

    @Override
    public UUID valueArrayGet(long[] v, int pos) {
        pos *= 2;
        return new UUID(v[pos++], v[pos]);
    }

    @Override
    public int valueArraySize(long[] vals) {
        return vals.length / 2;
    }

    @Override
    public long[] valueArrayEmpty() {
        return new long[0];
    }

    @Override
    public long[] valueArrayPut(long[] vals, int pos, UUID newValue) {
        pos *= 2;

        long[] array = (long[]) vals;
        final long[] ret = Arrays.copyOf(array, array.length + 2);

        if (pos < array.length) {
            System.arraycopy(array, pos, ret, pos + 2, array.length - pos);
        }
        ret[pos++] = newValue.getMostSignificantBits();
        ret[pos] = newValue.getLeastSignificantBits();
        return ret;
    }

    @Override
    public long[] valueArrayUpdateVal(long[] vals, int pos, UUID newValue) {
        pos *= 2;
        long[] vals2 = ((long[]) vals).clone();
        vals2[pos++] = newValue.getMostSignificantBits();
        vals2[pos] = newValue.getLeastSignificantBits();
        return vals2;
    }


    @Override
    public long[] valueArrayFromArray(Object[] objects) {
        long[] ret = new long[objects.length * 2];
        int pos = 0;

        for (Object o : objects) {
            UUID uuid = (java.util.UUID) o;
            ret[pos++] = uuid.getMostSignificantBits();
            ret[pos++] = uuid.getLeastSignificantBits();
        }

        return ret;
    }

    @Override
    public long[] valueArrayCopyOfRange(long[] vals, int from, int to) {
        return Arrays.copyOfRange((long[]) vals, from * 2, to * 2);
    }

    @Override
    public long[] valueArrayDeleteValue(long[] vals, int pos) {
        pos *= 2;
        long[] valsOrig = (long[]) vals;
        long[] vals2 = new long[valsOrig.length - 2];
        System.arraycopy(vals, 0, vals2, 0, pos - 2);
        System.arraycopy(vals, pos, vals2, pos - 2, vals2.length - (pos - 2));
        return vals2;
    }

}
