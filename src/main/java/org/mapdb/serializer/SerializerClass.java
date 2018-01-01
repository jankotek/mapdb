package org.mapdb.serializer;

import org.mapdb.DBException;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.hasher.Hasher;
import org.mapdb.hasher.Hashers;

import java.io.IOException;

/**
 * Serialier for class. It takes a class loader as constructor param, by default it uses
 * {@code Thread.currentThread().getContextClassLoader()}
 */
public class SerializerClass extends GroupSerializerObjectArray<Class<?>> {

    protected final ClassLoader classLoader;

    public SerializerClass(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    public SerializerClass(){
        this(Thread.currentThread().getContextClassLoader());
    }

    @Override
    public void serialize(DataOutput2 out, Class<?> value) throws IOException {
        out.writeUTF(value.getName());
    }

    @Override
    public Class<?> deserialize(DataInput2 in, int available) throws IOException {
        try {
            return classLoader.loadClass(in.readUTF());
        } catch (ClassNotFoundException e) {
            throw new DBException.SerializationException(e);
        }
    }

    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public Hasher<Class<?>> defaultHasher() {
        return Hashers.CLASS;
    }
}
