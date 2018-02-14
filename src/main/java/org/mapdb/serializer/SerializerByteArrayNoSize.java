package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.hasher.Hasher;
import org.mapdb.hasher.Hashers;
import org.mapdb.serializer.Serializers;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerByteArrayNoSize implements Serializer<byte[]> {

    @Override
    public void serialize(DataOutput2 out, byte[] value) throws IOException {
        out.write(value);
    }

    @Override
    public byte[] deserialize(DataInput2 in, int available) throws IOException {
        byte[] ret = new byte[available];
        in.readFully(ret);
        return ret;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public boolean needsAvailableSizeHint() {
        return true;
    }

    @Override
    public Hasher<byte[]> defaultHasher() {
        return Hashers.BYTE_ARRAY;
    }
}
