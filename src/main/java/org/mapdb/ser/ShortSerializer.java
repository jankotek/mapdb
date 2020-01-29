package org.mapdb.ser;

import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;

/**
 * Created by jan on 2/28/16.
 */
public class ShortSerializer extends DefaultGroupSerializer<Short> {
    @Override
    public void serialize(DataOutput2 out, Short value) {
        out.writeShort(value.shortValue());
    }

    @Override
    public Short deserialize(DataInput2 in) {
        return in.readShort();
    }

    @Nullable
    @Override
    public Class serializedType() {
        return Short.class;
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
