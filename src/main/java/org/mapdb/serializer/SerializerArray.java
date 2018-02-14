package org.mapdb.serializer;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.hasher.ArrayHasher;
import org.mapdb.hasher.Hasher;

import java.io.IOException;
import java.lang.reflect.Array;

/**
 * Serializes an object array of non-primitive objects.
 * This serializer takes two parameters:
 *
 * - serializer used for each component
 *
 * - componentType is class used to instantiate arrays. Generics are erased at runtime,
 *   this class controls what type of array will be instantiated.
 *   See {@link java.lang.reflect.Array#newInstance(Class, int)}
 *
 */
public class SerializerArray<T> extends GroupSerializerObjectArray<T[]> implements DB.DBAware {

    private static final long serialVersionUID = -982394293898234253L;
    protected Serializer<T> serializer;

    protected Hasher hasher;
    protected final Class<T> componentType;


    public SerializerArray(){
        this.serializer = null;
        this.componentType = (Class<T>)Object.class;
    }

    /**
     * Wraps given serializer and produces Object[] serializer.
     * To produce array with different component type, specify extra class.
      */
    public SerializerArray(Serializer<T> serializer) {
        this(serializer, null);
    }


    /**
     * Wraps given serializer and produces array serializer.
     *
     * @param serializer
     * @param componentType type of array which will be created on deserialization
     */
    public SerializerArray(Serializer<T> serializer, Class<T>  componentType) {
        if (serializer == null)
            throw new NullPointerException("null serializer");
        this.serializer = serializer;
        this.componentType = componentType!=null
                ? componentType
                : (Class<T>)Object.class;
       this.hasher = new ArrayHasher(serializer.defaultHasher());
    }

//        /** used for deserialization */
//        @SuppressWarnings("unchecked")
//		protected Array(SerializerBase serializerBase, DataInput2 is, SerializerBase.FastArrayList<Object> objectStack) throws IOException {
//            objectStack.add(this);
//            this.serializer = (Serializer<T>) serializerBase.deserialize(is,objectStack);
//        }


    @Override
    public void serialize(DataOutput2 out, T[] value) throws IOException {
        out.packInt(value.length);
        for (T a : value) {
            serializer.serialize(out, a);
        }
    }

    @Override
    public T[] deserialize(DataInput2 in, int available) throws IOException {
        int size = in.unpackInt();
        T[] ret = (T[]) Array.newInstance(componentType, size);
        for (int i = 0; i < size; i++) {
            ret[i] = serializer.deserialize(in, -1);
        }
        return ret;

    }

    @Override
    public boolean isTrusted() {
        return serializer.isTrusted();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return serializer.equals(((SerializerArray<?>) o).serializer);
    }

    @Override
    public int hashCode() {
        return serializer.hashCode();
    }


    @Override
    public void callbackDB(@NotNull DB db) {
        if(this.serializer==null)
            this.serializer = (Serializer<T>) db.getDefaultSerializer();
    }


    @Override
    public Hasher<T[]> defaultHasher() {
        return (Hasher<T[]>) hasher;
    }
}
