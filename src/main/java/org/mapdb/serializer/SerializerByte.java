package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;

/**
 * Single byte serializer
 */
//TODO optimize
public class SerializerByte implements Serializer<Byte> {
    @Override
    public void serialize(DataOutput2 out, Byte value) throws IOException {
        out.writeByte(value);
    }

    @Override
    public Byte deserialize(DataInput2 in, int available) throws IOException {
        return in.readByte();
    }

    //TODO value array operations

    @Override
    public int fixedSize() {
        return 1;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }

}
