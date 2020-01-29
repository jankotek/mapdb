package org.mapdb.ser;

import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

public class StringSerializer implements GroupSerializer<String, char[][]> {

    @Override
    public void serialize(DataOutput2 out, String value) {
        out.writeUTF(value);
    }

    @Override
    public String deserialize(DataInput2 in) {
        return in.readUTF();
    }

    @Nullable
    @Override
    public Class serializedType() {
        return String.class;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }


    @Override
    public void valueArraySerialize(DataOutput2 out2, char[][] vals) {
        for(char[] v:(char[][])vals){
            out2.packInt(v.length);
            for(char c:v){
                out2.packInt(c);
            }
        }
    }

    @Override
    public char[][] valueArrayDeserialize(DataInput2 in2, int size) {
        char[][] ret = new char[size][];
        for(int i=0;i<size;i++){
            int size2 = in2.unpackInt();
            char[] cc = new char[size2];
            for(int j=0;j<size2;j++){
                cc[j] = (char) in2.unpackInt();
            }
            ret[i] = cc;
        }
        return ret;
    }

    @Override
    public int valueArraySearch(char[][] keys, String key) {
        char[] key2 = key.toCharArray();
        return Arrays.binarySearch((char[][])keys, key2, Serializers.CHAR_ARRAY);
    }

    @Override
    public int valueArraySearch(char[][] vals, String key, Comparator comparator) {
        char[][] array = (char[][]) vals;
        int lo = 0;
        int hi = array.length - 1;

        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int compare = comparator.compare(key, new String(array[mid]));

            if (compare == 0)
                return mid;
            else if (compare < 0)
                hi = mid - 1;
            else
                lo = mid + 1;
        }
        return -(lo + 1);
    }

    @Override
    public String valueArrayGet(char[][] vals, int pos) {
        return new String(((char[][])vals)[pos]);
    }

    @Override
    public int valueArraySize(char[][] vals) {
        return ((char[][])vals).length;
    }

    @Override
    public char[][] valueArrayEmpty() {
        return new char[0][];
    }

    @Override
    public char[][] valueArrayPut(char[][] vals, int pos, String newValue) {
        char[][] array = (char[][]) vals;
        final char[][] ret = Arrays.copyOf(array, array.length+1);
        if(pos<array.length){
            System.arraycopy(array, pos, ret, pos+1, array.length-pos);
        }
        ret[pos] = newValue.toCharArray();
        return ret;

    }

    @Override
    public char[][] valueArrayUpdateVal(char[][] vals, int pos, String newValue) {
        char[][] vals2 = (char[][]) vals;
        vals2 = vals2.clone();
        vals2[pos] = newValue.toCharArray();
        return vals2;
    }

    @Override
    public char[][] valueArrayFromArray(Object[] objects) {
        char[][] ret = new char[objects.length][];
        for(int i=0;i<ret.length;i++){
            ret[i] = ((String)objects[i]).toCharArray();
        }
        return ret;
    }

    @Override
    public char[][] valueArrayCopyOfRange(char[][] vals, int from, int to) {
        return Arrays.copyOfRange((char[][])vals, from, to);
    }

    @Override
    public char[][] valueArrayDeleteValue(char[][] vals, int pos) {
        char[][] vals2 = new char[((char[][])vals).length-1][];
        System.arraycopy(vals,0,vals2, 0, pos-1);
        System.arraycopy(vals, pos, vals2, pos-1, vals2.length-(pos-1));
        return vals2;
    }


    @Override
    public int hashCode(String s, int seed) {
        char[] c = s.toCharArray();
        return Serializers.CHAR_ARRAY.hashCode(c, seed);
    }

}
