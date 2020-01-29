package org.mapdb.ser;

import org.jetbrains.annotations.Nullable;
import org.mapdb.DBException;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;

/**
 * Serialier for class. It takes a class loader as constructor param, by default it uses
 * {@code Thread.currentThread().getContextClassLoader()}
 */
public class ClassSerializer extends DefaultGroupSerializer<Class> {

    protected final ClassLoader classLoader;

    public ClassSerializer(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    public ClassSerializer(){
        this(Thread.currentThread().getContextClassLoader());
    }

    @Override
    public void serialize(DataOutput2 out, Class value) {
        out.writeUTF(value.getName());
    }

    @Override
    public Class deserialize(DataInput2 in) {
        try {
            return classLoader.loadClass(in.readUTF());
        } catch (ClassNotFoundException e) {
            throw new DBException.SerializationError(e);
        }
    }

    @Nullable
    @Override
    public Class serializedType() {
        return Class.class;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public boolean equals(Class a1, Class a2) {
        return a1 == a2 || (a1.toString().equals(a2.toString()));
    }

    @Override
    public int hashCode(Class aClass, int seed) {
        //class does not override identity hash code
        return aClass.toString().hashCode();
    }
}
