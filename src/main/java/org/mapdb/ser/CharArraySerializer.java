package org.mapdb.ser;

import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by jan on 2/28/16.
 */
public class CharArraySerializer extends DefaultGroupSerializer<char[]> {

    @Override
    public void serialize(DataOutput2 out, char[] value) {
        out.packInt(value.length);
        for (char c : value) {
            out.writeChar(c);
        }
    }

    @Override
    public char[] deserialize(DataInput2 in) {
        final int size = in.unpackInt();
        char[] ret = new char[size];
        for (int i = 0; i < size; i++) {
            ret[i] = in.readChar();
        }
        return ret;
    }

    @Nullable
    @Override
    public Class serializedType() {
        return char[].class;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public boolean equals(char[] a1, char[] a2) {
        return Arrays.equals(a1, a2);
    }

    @Override
    public int hashCode(char[] chars, int seed) {
        int res = 0;
        for (char c : chars) {
            res = (res + c) * -1640531527 ;
        }
        return res;
    }

    @Override
    public int compare(char[] o1, char[] o2) {
        final int len = Math.min(o1.length, o2.length);
        for (int i = 0; i < len; i++) {
            int b1 = o1[i];
            int b2 = o2[i];
            if (b1 != b2)
                return b1 - b2;
        }
        return SerializerUtils.compareInt(o1.length, o2.length);
    }

    @Override
    public char[] nextValue(char[] value) {
        value = value.clone();

        for (int i = value.length-1; ;i--) {
            char b1 = value[i];
            if(b1==Character.MAX_VALUE){
                if(i==0)
                    return null;
                value[i]=Character.MIN_VALUE;
                continue;
            }
            value[i] = (char) (b1+1);
            return value;
        }
    }
}
