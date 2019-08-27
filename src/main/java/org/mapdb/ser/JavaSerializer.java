package org.mapdb.ser;

import org.jetbrains.annotations.Nullable;
import org.mapdb.CC;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataInputToStream;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

/**
 * Created by jan on 2/28/16.
 */
public class JavaSerializer<E> extends DefaultGroupSerializer<E> {
    @Override
    public void serialize(DataOutput2 out, E value) throws IOException {
        ObjectOutputStream out2 = new ObjectOutputStream((OutputStream) out);
        out2.writeObject(value);
        out2.flush();
    }

    @Override
    public E deserialize(DataInput2 in) throws IOException {
        try {
            ObjectInputStream in2 = new ObjectInputStream(new DataInputToStream(in));
            return (E) in2.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    @Nullable
    @Override
    public Class serializedType() {
        return Object.class;
    }

    @Override
    public Object[] valueArrayDeserialize(DataInput2 in, int size) throws IOException {
        try {
            ObjectInputStream in2 = new ObjectInputStream(new DataInputToStream(in));
            Object[] ret = (Object[]) in2.readObject();
            if(CC.PARANOID && size!=valueArraySize(ret))
                throw new AssertionError();
            return ret;
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, Object[] vals) throws IOException {
        ObjectOutputStream out2 = new ObjectOutputStream((OutputStream) out);
        out2.writeObject(vals);
        out2.flush();
    }
}
