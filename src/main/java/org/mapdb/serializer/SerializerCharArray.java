package org.mapdb.serializer;

import org.mapdb.DBUtil;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerCharArray extends GroupSerializerObjectArray<char[]> {

    @Override
    public void serialize(DataOutput2 out, char[] value) throws IOException {
        out.packInt(value.length);
        for (char c : value) {
            out.writeChar(c);
        }
    }

    @Override
    public char[] deserialize(DataInput2 in, int available) throws IOException {
        final int size = in.unpackInt();
        char[] ret = new char[size];
        for (int i = 0; i < size; i++) {
            ret[i] = in.readChar();
        }
        return ret;
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
    public int hashCode(char[] bytes, int seed) {
        return DBUtil.longHash(
                DBUtil.hash(bytes, 0, bytes.length, seed));
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

}
