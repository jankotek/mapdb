package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

public class SerializerString implements GroupSerializer<String> {

    @Override
    public void serialize(DataOutput2 out, String value) throws IOException {
        out.writeUTF(value);
    }

    @Override
    public String deserialize(DataInput2 in, int available) throws IOException {
        return in.readUTF();
    }

    @Override
    public boolean isTrusted() {
        return true;
    }


    @Override
    public void valueArraySerialize(DataOutput2 out2, Object vals) throws IOException {
        for(char[] v:(char[][])vals){
            out2.packInt(v.length);
            for(char c:v){
                out2.packInt(c);
            }
        }
    }

    @Override
    public char[][] valueArrayDeserialize(DataInput2 in2, int size) throws IOException {
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
    public int valueArraySearch(Object keys, String key) {
        char[] key2 = key.toCharArray();
        return Arrays.binarySearch((char[][])keys, key2, CHAR_ARRAY);
    }

    @Override
    public int valueArraySearch(Object vals, String key, Comparator comparator) {
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
    public String valueArrayGet(Object vals, int pos) {
        return new String(((char[][])vals)[pos]);
    }

    @Override
    public int valueArraySize(Object vals) {
        return ((char[][])vals).length;
    }

    @Override
    public char[][] valueArrayEmpty() {
        return new char[0][];
    }

    @Override
    public char[][] valueArrayPut(Object vals, int pos, String newValue) {
        char[][] array = (char[][]) vals;
        final char[][] ret = Arrays.copyOf(array, array.length+1);
        if(pos<array.length){
            System.arraycopy(array, pos, ret, pos+1, array.length-pos);
        }
        ret[pos] = newValue.toCharArray();
        return ret;

    }

    @Override
    public char[][] valueArrayUpdateVal(Object vals, int pos, String newValue) {
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
    public char[][] valueArrayCopyOfRange(Object vals, int from, int to) {
        return Arrays.copyOfRange((char[][])vals, from, to);
    }

    @Override
    public char[][] valueArrayDeleteValue(Object vals, int pos) {
        char[][] vals2 = new char[((char[][])vals).length-1][];
        System.arraycopy(vals,0,vals2, 0, pos-1);
        System.arraycopy(vals, pos, vals2, pos-1, vals2.length-(pos-1));
        return vals2;
    }


    @Override
    public int hashCode(String s, int seed) {
        char[] c = s.toCharArray();
        return CHAR_ARRAY.hashCode(c, seed);
    }

}
