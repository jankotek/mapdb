package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

public abstract class EightByteSerializer<E> implements GroupSerializer<E,long[]> {

    protected abstract E unpack(long l);
    protected abstract long pack(E l);

    @Override
    public E valueArrayGet(long[] vals, int pos){
        return unpack(((long[]) vals)[pos]);
    }


    @Override
    public int valueArraySize(long[] vals){
        return ((long[])vals).length;
    }

    @Override
    public long[] valueArrayEmpty(){
        return new long[0];
    }

    @Override
    public long[] valueArrayPut(long[] vals, int pos, E newValue) {

        long[] array = (long[]) vals;
        final long[] ret = Arrays.copyOf(array, array.length+1);
        if(pos<array.length){
            System.arraycopy(array, pos, ret, pos+1, array.length-pos);
        }
        ret[pos] = pack(newValue);
        return ret;
    }

    @Override
    public long[] valueArrayUpdateVal(long[] vals, int pos, E newValue) {
        long[] vals2 = ((long[])vals).clone();
        vals2[pos] = pack(newValue);
        return vals2;
    }

    @Override
    public long[] valueArrayFromArray(Object[] objects) {
        long[] ret = new long[objects.length];
        int pos=0;

        for(Object o:objects){
            ret[pos++] = pack((E) o);
        }

        return ret;
    }

    @Override
    public long[] valueArrayCopyOfRange(long[] vals, int from, int to) {
        return Arrays.copyOfRange((long[])vals, from, to);
    }

    @Override
    public long[] valueArrayDeleteValue(long[] vals, int pos) {
        long[] valsOrig = (long[]) vals;
        long[] vals2 = new long[valsOrig.length-1];
        System.arraycopy(vals,0,vals2, 0, pos-1);
        System.arraycopy(vals, pos, vals2, pos-1, vals2.length-(pos-1));
        return vals2;
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, long[] vals) {
        for(long o:(long[]) vals){
            out.writeLong(o);
        }
    }

    @Override
    public long[] valueArrayDeserialize(DataInput2 in, int size) {
        long[] ret = new long[size];
        for(int i=0;i<size;i++){
            ret[i] = in.readLong();
        }
        return ret;
    }

    @Override
    public E valueArrayBinaryGet(DataInput2 input, int keysLen, int pos) {
        input.skipBytes(pos*8);
        return unpack(input.readLong());
    }


    @Override
    public boolean isTrusted() {
        return true;
    }


    @Override
    public int fixedSize() {
        return 8;
    }

    @Override
    final public int valueArraySearch(long[] keys, E key, Comparator comparator) {
        if(comparator==this)
            return valueArraySearch(keys, key);

        long[] array = (long[]) keys;

        int lo = 0;
        int hi = array.length - 1;

        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int compare = comparator.compare(key, unpack(array[mid]));

            if (compare == 0)
                return mid;
            else if (compare < 0)
                hi = mid - 1;
            else
                lo = mid + 1;
        }
        return -(lo + 1);
    }

}
