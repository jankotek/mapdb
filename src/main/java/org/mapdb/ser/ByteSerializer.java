package org.mapdb.ser;

import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;

/**
 * Created by jan on 2/28/16.
 */
public class ByteSerializer extends DefaultGroupSerializer<Byte> {

    //TODO use byte[] group serializer

    @Override
    public void serialize(DataOutput2 out, Byte value) {
        out.writeByte(value);
    }

    @Override
    public Byte deserialize(DataInput2 in) {
        return in.readByte();
    }

    @Nullable
    @Override
    public Class serializedType() {
        return Byte.class;
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
