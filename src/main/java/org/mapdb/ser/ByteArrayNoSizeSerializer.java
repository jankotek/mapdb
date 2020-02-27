package org.mapdb.ser;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.DBException;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import javax.print.DocFlavor;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by jan on 2/28/16.
 */
public class ByteArrayNoSizeSerializer implements Serializer<byte[]> {

    @Override
    public void serialize(DataOutput2 out, byte[] value) {
        out.write(value);
    }

    @Override
    public byte[] deserialize(@NotNull DataInput2 input) {
        int avail = input.available();
        byte[] ret = new byte[avail];
        input.readFully(ret, 0, avail);
        return ret;
    }

    @Nullable
    @Override
    public Class serializedType() {
        return null;
    }

    @Override
    public byte[] deserialize(DataInput2 in, int available) {
        byte[] ret = new byte[available];
        in.readFully(ret);
        return ret;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public boolean equals(byte[] a1, byte[] a2) {
        return Arrays.equals(a1, a2);
    }

    @Override
    public int hashCode(byte[] bytes, int seed) {
        return Serializers.BYTE_ARRAY.hashCode(bytes, seed);
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

}
