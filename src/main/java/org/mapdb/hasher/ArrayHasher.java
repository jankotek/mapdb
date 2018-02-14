package org.mapdb.hasher;

import org.mapdb.serializer.SerializerUtils;

import java.util.Comparator;

/** compares content of two arrays. Given comparator compares content of both arrays, until difference is found or end is reached */
public class ArrayHasher implements Hasher<Object[]> {


    public ArrayHasher(Hasher hasher) {
        this.hasher = hasher;
    }

    protected final Hasher hasher;

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
                r = hasher.compare(a1,  a2);
                ;
            }
            if (r != 0)
                return r;
        }
        return SerializerUtils.compareInt(o1.length, o2.length);
    }


    @Override
    public boolean equals(Object[] a1, Object[] a2) {
        if (a1 == a2)
            return true;
        if (a1 == null || a1.length != a2.length)
            return false;

        for (int i = 0; i < a1.length; i++) {
            if (!hasher.equals(a1[i], a2[i]))
                return false;
        }
        return true;
    }

    @Override
    public int hashCode(Object[] objects, int seed) {
        seed += objects.length;
        for (Object a : objects) {
            seed = (-1640531527) * seed + hasher.hashCode(a, seed);
        }
        return seed;
    }
}
