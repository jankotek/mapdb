package org.mapdb.ser;

import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerIllegalAccess<E> extends DefaultGroupSerializer<E> {
    @Override
    public void serialize(DataOutput2 out, E value) {
        throw new IllegalAccessError();
    }

    @Override
    public E deserialize(DataInput2 in) {
        throw new IllegalAccessError();
    }

    @Nullable
    @Override
    public Class serializedType() {
        return Object.class;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }

}
