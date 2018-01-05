package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.hasher.Hasher;
import org.mapdb.hasher.Hashers;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerCharArray implements Serializer<char[]> {

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

    @Override
    public Hasher<char[]> defaultHasher() {
        return Hashers.CHAR_ARRAY;
    }
}
