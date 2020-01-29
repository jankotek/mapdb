package org.mapdb.ser;

import org.jetbrains.annotations.Nullable;
import org.mapdb.CC;
import org.mapdb.DBException;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataInputToStream;
import org.mapdb.io.DataOutput2;

import java.io.*;

/**
 * Created by jan on 2/28/16.
 */
public class JavaSerializer<E> extends DefaultGroupSerializer<E> {
    @Override
    public void serialize(DataOutput2 out, E value) {
        ObjectOutputStream out2 = null;
        try {
            out2 = new ObjectOutputStream((OutputStream) out);
            out2.writeObject(value);
            out2.flush();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    public E deserialize(DataInput2 in) {
        try {
            ObjectInputStream in2 = new ObjectInputStream(new DataInputToStream(in));
            return (E) in2.readObject();
        } catch (ClassNotFoundException e) {
            throw new DBException.SerializationError(e);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Nullable
    @Override
    public Class serializedType() {
        return Object.class;
    }

    @Override
    public Object[] valueArrayDeserialize(DataInput2 in, int size) {
        try {
            ObjectInputStream in2 = new ObjectInputStream(new DataInputToStream(in));
            Object[] ret = (Object[]) in2.readObject();
            if(CC.PARANOID && size!=valueArraySize(ret))
                throw new AssertionError();
            return ret;
        } catch (ClassNotFoundException e) {
            throw new DBException.SerializationError(e);
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, Object[] vals) {
        try{
            ObjectOutputStream out2 = new ObjectOutputStream((OutputStream) out);
            out2.writeObject(vals);
            out2.flush();
        } catch (IOException e) {
            throw new IOError(e);
        }
    }
}
