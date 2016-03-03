package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.io.Serializable;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerArray<T> extends GroupSerializerObjectArray<T[]>{

    private static final long serialVersionUID = -7443421486382532062L;
    protected final Serializer<T> serializer;

    public SerializerArray(Serializer<T> serializer) {
        if (serializer == null)
            throw new NullPointerException("null serializer");
        this.serializer = serializer;
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
        T[] ret = (T[]) new Object[in.unpackInt()];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = serializer.deserialize(in, -1);
        }
        return ret;

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
        return serializer.equals(((SerializerArray<?>) o).serializer);
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
