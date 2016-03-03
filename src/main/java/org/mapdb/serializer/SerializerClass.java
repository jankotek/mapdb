package org.mapdb.serializer;

import org.mapdb.DBException;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerClass extends GroupSerializerObjectArray<Class<?>> {

    @Override
    public void serialize(DataOutput2 out, Class<?> value) throws IOException {
        out.writeUTF(value.getName());
    }

    @Override
    public Class<?> deserialize(DataInput2 in, int available) throws IOException {
        //TODO this should respect registered ClassLoaders from DBMaker.serializerRegisterClasses()
        try {
            return Thread.currentThread().getContextClassLoader().loadClass(in.readUTF());
        } catch (ClassNotFoundException e) {
            throw new DBException.SerializationError(e);
        }
    }

    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public boolean equals(Class<?> a1, Class<?> a2) {
        return a1 == a2 || (a1.toString().equals(a2.toString()));
    }

    @Override
    public int hashCode(Class<?> aClass, int seed) {
        //class does not override identity hash code
        return aClass.toString().hashCode();
    }
}
