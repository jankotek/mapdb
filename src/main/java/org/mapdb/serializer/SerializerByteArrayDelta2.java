package org.mapdb.serializer;

import org.jetbrains.annotations.NotNull;
import static org.mapdb.serializer.SerializerStringDelta2.ByteArrayKeys;
import org.mapdb.DBUtil;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by jan on 2/29/16.
 */
public class SerializerByteArrayDelta2 implements GroupSerializer<byte[]> {


    @Override
    public int valueArraySearch(Object keys, byte[] key) {
        Object[] v = valueArrayToArray(keys);
        return Arrays.binarySearch(v, key, (Comparator)this);
    }

    @Override
    public int valueArraySearch(Object keys, byte[] key, Comparator comparator) {
        Object[] v = valueArrayToArray(keys);
        return Arrays.binarySearch(v, key, comparator);
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, Object keys2) throws IOException {
        ByteArrayKeys keys = (ByteArrayKeys) keys2;
        int offset = 0;
        //write sizes
        for(int o:keys.offset){
            out.packInt(o-offset);
            offset = o;
        }
        //$DELAY$
        //find and write common prefix
        int prefixLen = keys.commonPrefixLen();
        out.packInt(prefixLen);
        out.write(keys.array,0,prefixLen);
        //$DELAY$
        //write suffixes
        offset = prefixLen;
        for(int o:keys.offset){
            out.write(keys.array, offset, o-offset);
            offset = o+prefixLen;
        }
    }

    @Override
    public ByteArrayKeys valueArrayDeserialize(DataInput2 in, int size) throws IOException {
        //read data sizes
        int[] offsets = new int[size];
        int old=0;
        for(int i=0;i<size;i++){
            old+= in.unpackInt();
            offsets[i]=old;
        }
        byte[] bb = new byte[old];
        //$DELAY$
        //read and distribute common prefix
        int prefixLen = in.unpackInt();
        in.readFully(bb, 0, prefixLen);
        for(int i=0; i<offsets.length-1;i++){
            System.arraycopy(bb, 0, bb, offsets[i], prefixLen);
        }
        //$DELAY$
        //read suffixes
        int offset = prefixLen;
        for(int o:offsets){
            in.readFully(bb,offset,o-offset);
            offset = o+prefixLen;
        }

        return new ByteArrayKeys(offsets,bb);
    }

    @Override
    public byte[] valueArrayGet(Object keys, int pos) {
        return ((ByteArrayKeys)keys).getKey(pos);
    }

    @Override
    public int valueArraySize(Object keys) {
        return ((ByteArrayKeys)keys).length();
    }

    @Override
    public ByteArrayKeys valueArrayEmpty() {
        return new ByteArrayKeys(new int[0], new byte[0]);
    }

    @Override
    public ByteArrayKeys valueArrayPut(Object keys, int pos, byte[] newValue) {
        return ((ByteArrayKeys)keys).putKey(pos, newValue);
    }

    @Override
    public ByteArrayKeys valueArrayUpdateVal(Object vals, int pos, byte[] newValue) {
        //FIXME why is this not catched by tests?
        throw new NotImplementedException();
    }

    @Override
    public ByteArrayKeys valueArrayFromArray(Object[] keys) {
        //fill offsets
        int[] offsets = new int[keys.length];

        int old=0;
        for(int i=0;i<keys.length;i++){
            byte[] b = (byte[]) keys[i];
            old+=b.length;
            offsets[i]=old;
        }
        //$DELAY$
        //fill large array
        byte[] bb = new byte[old];
        old=0;
        for(int i=0;i<keys.length;i++){
            int curr = offsets[i];
            System.arraycopy(keys[i], 0, bb, old, curr - old);
            old=curr;
        }
        //$DELAY$
        return new ByteArrayKeys(offsets,bb);
    }

    @Override
    public ByteArrayKeys valueArrayCopyOfRange(Object keys, int from, int to) {
        return ((ByteArrayKeys)keys).copyOfRange(from,to);
    }

    @Override
    public ByteArrayKeys valueArrayDeleteValue(Object keys, int pos) {
        //return keys.deleteKey(pos);
        Object[] vv = valueArrayToArray(keys);
        vv = DBUtil.arrayDelete(vv, pos, 1);
        return valueArrayFromArray(vv);
    }

    @Override
    public void serialize(@NotNull DataOutput2 out, @NotNull byte[] value) throws IOException {
        Serializer.BYTE_ARRAY.serialize(out, value);
    }

    @Override
    public byte[] deserialize(@NotNull DataInput2 input, int available) throws IOException {
        return Serializer.BYTE_ARRAY.deserialize(input, available);
    }

    @Override
    public int compare(byte[] o1, byte[] o2) {
        return Serializer.BYTE_ARRAY.compare(o1, o2);
    }

    @Override
    public boolean equals(byte[] a1, byte[] a2) {
        return Serializer.BYTE_ARRAY.equals(a1, a2);
    }

    @Override
    public int hashCode(@NotNull byte[] bytes, int seed) {
        return Serializer.BYTE_ARRAY.hashCode(bytes, seed);
    }

    @Override
    public boolean isTrusted() {
        return true;
    }
}
