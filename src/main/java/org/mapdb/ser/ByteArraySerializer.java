package org.mapdb.ser;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;
import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by jan on 2/28/16.
 */
public class ByteArraySerializer implements GroupSerializer<byte[], byte[][]> {

    //TODO thread safe?
    private static final XXHash32 HASHER =  XXHashFactory.fastestInstance().hash32();

    @Override
    public void serialize(DataOutput2 out, byte[] value) {
        out.packInt(value.length);
        out.write(value);
    }

    @Override
    public byte[] deserialize(DataInput2 in) {
        int size = in.unpackInt();
        byte[] ret = new byte[size];
        in.readFully(ret);
        return ret;
    }

    @Nullable
    @Override
    public Class serializedType() {
        return byte[].class;
    }


    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public boolean equals(byte[] a1, byte[] a2) {
        return Arrays.equals(a1, a2);
    }

    public int hashCode(byte[] bytes, int seed) {
        return HASHER.hash(bytes, 0, bytes.length, seed);
    }

    @Override
    public int compare(byte[] o1, byte[] o2) {
        if (o1 == o2) return 0;
        final int len = Math.min(o1.length, o2.length);
        for (int i = 0; i < len; i++) {
            int b1 = o1[i] & 0xFF;
            int b2 = o2[i] & 0xFF;
            if (b1 != b2)
                return b1 - b2;
        }
        return o1.length - o2.length;
    }

    @Override
    public int valueArraySearch(byte[][] keys, byte[] key) {
        return Arrays.binarySearch((byte[][])keys, key, Serializers.BYTE_ARRAY);
    }

    @Override
    public int valueArraySearch(byte[][] keys, byte[] key, Comparator comparator) {
        //TODO PERF optimize search
        Object[] v = valueArrayToArray(keys);
        return Arrays.binarySearch(v, key, comparator);
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, byte[][] vals) {
        byte[][] vals2 = (byte[][]) vals;
        out.packInt(vals2.length);
        for(byte[]b:vals2){
            Serializers.BYTE_ARRAY.serialize(out, b);
        }
    }

    @Override
    public byte[][] valueArrayDeserialize(DataInput2 in, int size) {
        int s = in.unpackInt();
        byte[][] ret = new byte[s][];
        for(int i=0;i<s;i++) {
            ret[i] = Serializers.BYTE_ARRAY.deserialize(in, -1);
        }
        return ret;
    }

    @Override
    public byte[] valueArrayGet(byte[][] vals, int pos) {
        return ((byte[][])vals)[pos];
    }

    @Override
    public int valueArraySize(byte[][] vals) {
        return ((byte[][])vals).length;
    }

    @Override
    public byte[][] valueArrayEmpty() {
        return new byte[0][];
    }

    @Override
    public byte[][] valueArrayPut(byte[][] vals, int pos, byte[] newValue) {
        byte[][] array = (byte[][])vals;
        final byte[][] ret = Arrays.copyOf(array, array.length+1);
        if(pos<array.length){
            System.arraycopy(array, pos, ret, pos+1, array.length-pos);
        }
        ret[pos] = newValue;
        return ret;
    }

    @Override
    public byte[][] valueArrayUpdateVal(byte[][] vals, int pos, byte[] newValue) {
        byte[][] vals2 = (byte[][]) vals;
        vals2 = vals2.clone();
        vals2[pos] = newValue;
        return vals2;
    }

    @Override
    public byte[][] valueArrayFromArray(Object[] objects) {
        byte[][] ret = new byte[objects.length][];
        for(int i=0;i<ret.length;i++){
            ret[i] = (byte[])objects[i];
        }
        return ret;
    }

    @Override
    public byte[][] valueArrayCopyOfRange(byte[][] vals, int from, int to) {
        return Arrays.copyOfRange((byte[][])vals, from, to);
    }

    @Override
    public byte[][] valueArrayDeleteValue(byte[][] vals, int pos) {
        byte[][] vals2 = new byte[((byte[][])vals).length-1][];
        System.arraycopy(vals,0,vals2, 0, pos-1);
        System.arraycopy(vals, pos, vals2, pos-1, vals2.length-(pos-1));
        return vals2;
    }

    @Override
    public byte[] nextValue(byte[] value) {
        value = value.clone();

        for (int i = value.length-1; ;i--) {
            int b1 = value[i] & 0xFF;
            if(b1==255){
                if(i==0)
                    return null;
                value[i]=0;
                continue;
            }
            value[i] = (byte) ((b1+1)&0xFF);
            return value;
        }
    }
}
