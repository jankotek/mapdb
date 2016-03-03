package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerShort extends GroupSerializerObjectArray<Short> {
    @Override
    public void serialize(DataOutput2 out, Short value) throws IOException {
        out.writeShort(value.shortValue());
    }

    @Override
    public Short deserialize(DataInput2 in, int available) throws IOException {
        return in.readShort();
    }

    //TODO value array operations

    @Override
    public int fixedSize() {
        return 2;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }

}
