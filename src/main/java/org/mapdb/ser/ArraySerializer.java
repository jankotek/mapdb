package org.mapdb.ser;

import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

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
public class ArraySerializer<T> extends DefaultGroupSerializer<T[]> {

    private static final long serialVersionUID = -982394293898234253L;
    protected final Serializer<T> serializer;
    protected final Class<T> componentType;


    /**
     * Wraps given serializer and produces Object[] serializer.
     * To produce array with different component type, specify extra class.
      */
    public ArraySerializer(Serializer<T> serializer) {
        this(serializer, null);
    }


    /**
     * Wraps given serializer and produces array serializer.
     *
     * @param serializer
     * @param componentType type of array which will be created on deserialization
     */
    public ArraySerializer(Serializer<T> serializer, Class<T>  componentType) {
        if (serializer == null)
            throw new NullPointerException("null serializer");
        this.serializer = serializer;
        this.componentType = componentType!=null
                ? componentType
                : (Class<T>)Object.class;
    }

//        /** used for deserialization */
//        @SuppressWarnings("unchecked")
//		protected Array(SerializerBase serializerBase, DataInput2 is, SerializerBase.FastArrayList<Object> objectStack) {
//            objectStack.add(this);
//            this.serializer = (Serializer<T>) serializerBase.deserialize(is,objectStack);
//        }


    @Override
    public void serialize(DataOutput2 out, T[] value) {
        out.packInt(value.length);
        for (T a : value) {
            serializer.serialize(out, a);
        }
    }

    @Override
    public T[] deserialize(DataInput2 in) {
        int size = in.unpackInt();
        T[] ret = (T[]) Array.newInstance(componentType, size);
        for (int i = 0; i < ret.length; i++) {
            ret[i] = serializer.deserialize(in, -1);
        }
        return ret;

    }

    @Nullable
    @Override
    public Class serializedType() {
        return Object[].class;
    }

    @Override
    public boolean isTrusted() {
        return serializer.isTrusted();
    }

    @Override
    public boolean equals(T[] a1, T[] a2) {
        if (a1 == a2)
            return true;
        if (a1 == null || a1.length != a2.length)
            return false;

        for (int i = 0; i < a1.length; i++) {
            if (!serializer.equals(a1[i], a2[i]))
                return false;
        }
        return true;
    }

    @Override
    public int hashCode(T[] objects, int seed) {
        seed += objects.length;
        for (T a : objects) {
            seed = (-1640531527) * seed + serializer.hashCode(a, seed);
        }
        return seed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return serializer.equals(((ArraySerializer<?>) o).serializer);
    }

    @Override
    public int hashCode() {
        return serializer.hashCode();
    }


    @Override
    public int compare(Object[] o1, Object[] o2) {
        int len = Math.min(o1.length, o2.length);
        int r;
        for (int i = 0; i < len; i++) {
            Object a1 = o1[i];
            Object a2 = o2[i];

            if (a1 == a2) { //this case handles both nulls
                r = 0;
            } else if (a1 == null) {
                r = 1; //null is positive infinity, always greater than anything else
            } else if (a2 == null) {
                r = -1;
            } else {
                r = serializer.compare((T) a1, (T) a2);
                ;
            }
            if (r != 0)
                return r;
        }
        return SerializerUtils.compareInt(o1.length, o2.length);
    }

}
