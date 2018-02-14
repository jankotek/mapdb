package org.mapdb.hasher;

import net.jpountz.xxhash.XXHash32;
import org.jetbrains.annotations.NotNull;
import org.mapdb.CC;
import org.mapdb.serializer.SerializerUtils;
import org.mapdb.serializer.Serializers;
import org.mapdb.util.DataIO;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

public final class Hashers {


    /** hasher that uses {@link Object#hashCode()} to calculate hash,
     * {@link Object#equals(Object)} to provide equality and {@link Comparable} for comparation*/
    public static final Hasher JAVA = new Hasher<Object>() {


        @Override
        public int compare(Object o1, Object o2) {
            if(o1==o2) //handles both null
                return 0;
            return ((Comparable)o1).compareTo(o2);
        }

        @Override
        public boolean equals(Object first, Object second) {
            return Objects.equals(first, second);
        }
        @Override
        public int hashCode(@NotNull Object o, int seed) {
            //TODO is DataIO.intHash needed here? any effect on performance?
            return DataIO.intHash(o.hashCode() + seed);
        }

    };



    public static final Hasher<long[]> LONG_ARRAY = new Hasher<long[]>(){

        @Override
        public boolean equals(long[] first, long[] second) {
            return Arrays.equals(first, second);
        }


        @Override
        public int compare(long[] o1, long[] o2) {
            if (o1 == o2) return 0;
            final int len = Math.min(o1.length, o2.length);
            for (int i = 0; i < len; i++) {
                if (o1[i] == o2[i])
                    continue;
                if (o1[i] > o2[i])
                    return 1;
                return -1;
            }
            return SerializerUtils.compareInt(o1.length, o2.length);
        }


        @Override
        public int hashCode(long[] a, int seed) {
            for (long element : a) {
                int elementHash = (int) (element ^ (element >>> 32));
                seed = (-1640531527) * seed + elementHash;
            }
            return seed;
        }

    };

    public static final Hasher<String> STRING = new Hasher<String>() {

        @Override
        public boolean equals(String first, String second) {
            return Objects.equals(first,second);
        }


        @Override
        public int hashCode(String s, int seed) {
            char[] c = s.toCharArray();
            return CHAR_ARRAY.hashCode(c, seed);
        }

        @Override
        public int compare(String o1, String o2) {
            return o1.compareTo(o2);
        }
    };

    //TODO remove external dep?
    private static final XXHash32 HASHER = CC.HASH_FACTORY.hash32();


    public static final Hasher<byte[]> BYTE_ARRAY = new Hasher<byte[]>(){
        @Override
        public boolean equals(byte[] a1, byte[] a2) {
            return Arrays.equals(a1, a2);
        }

        public int hashCode(byte[] bytes, int seed) {
            return HASHER.hash(bytes, 0, bytes.length, seed);
        }

        @Override
        public int compare(byte[] o1, byte[] o2) {
            if (o1 == o2) return 0;
            final int len = Math.min(o1.length, o2.length);
            for (int i = 0; i < len; i++) {
                int b1 = o1[i] & 0xFF;
                int b2 = o2[i] & 0xFF;
                if (b1 != b2)
                    return b1 - b2;
            }
            return o1.length - o2.length;
        }

    };
    public static final Hasher<char[]> CHAR_ARRAY = new Hasher<char[]>() {


        @Override
        public int hashCode(char[] chars, int seed) {
            for (char c : chars) {
                seed = (seed + c) * (-1640531527) ;
            }
            return seed;
        }

        @Override
        public boolean equals(char[] first, char[] second) {
            return Arrays.equals(first, second);
        }

        @Override
        public int compare(char[] o1, char[] o2) {
            final int len = Math.min(o1.length, o2.length);
            for (int i = 0; i < len; i++) {
                int b1 = o1[i];
                int b2 = o2[i];
                if (b1 != b2)
                    return b1 - b2;
            }
            return SerializerUtils.compareInt(o1.length, o2.length);
        }

    };

    public static final Hasher<Class<?>> CLASS = new Hasher<Class<?>>() {

        @Override
        public int compare(Class<?> o1, Class<?> o2) {
            return o1.getName().compareTo(o2.getName());
        }

        @Override
        public boolean equals(Class<?> a1, Class<?> a2) {
            return a1.getName().equals(a2.getName());
        }

        @Override
        public int hashCode(Class<?> aClass, int seed) {
            //class does not override identity hash code
            return STRING.hashCode(aClass.getName(), seed);
        }


    };

    public static final Hasher<double[]> DOUBLE_ARRAY = new Hasher<double[]>() {

        @Override
        public boolean equals(double[] a1, double[] a2) {
            return Arrays.equals(a1, a2);
        }

        @Override
        public int hashCode(double[] bytes, int seed) {
            for (double element : bytes) {
                long bits = Double.doubleToLongBits(element);
                seed = (-1640531527) * seed + (int) (bits ^ (bits >>> 32));
            }
            return seed;
        }

        @Override
        public int compare(double[] o1, double[] o2) {
            if (o1 == o2) return 0;
            final int len = Math.min(o1.length, o2.length);
            for (int i = 0; i < len; i++) {
                if (o1[i] == o2[i])
                    continue;
                if (o1[i] > o2[i])
                    return 1;
                return -1;
            }
            return SerializerUtils.compareInt(o1.length, o2.length);
        }
    };

    public static final Hasher<float[]> FLOAT_ARRAY = new Hasher<float[]>() {

        @Override
        public boolean equals(float[] a1, float[] a2) {
            return Arrays.equals(a1, a2);
        }

        @Override
        public int hashCode(float[] floats, int seed) {
            for (float element : floats)
                seed = (-1640531527) * seed + Float.floatToIntBits(element);
            return seed;
        }

        @Override
        public int compare(float[] o1, float[] o2) {
            if (o1 == o2) return 0;
            final int len = Math.min(o1.length, o2.length);
            for (int i = 0; i < len; i++) {
                if (o1[i] == o2[i])
                    continue;
                if (o1[i] > o2[i])
                    return 1;
                return -1;
            }
            return SerializerUtils.compareInt(o1.length, o2.length);
        }
    };

    public static final Hasher<int[]> INT_ARRAY = new Hasher<int[]>() {

        @Override
        public boolean equals(int[] a1, int[] a2) {
            return Arrays.equals(a1, a2);
        }

        @Override
        public int hashCode(int[] bytes, int seed) {
            for (int i : bytes) {
                seed = (-1640531527) * seed + i;
            }
            return seed;
        }

        @Override
        public int compare(int[] o1, int[] o2) {
            if (o1 == o2) return 0;
            final int len = Math.min(o1.length, o2.length);
            for (int i = 0; i < len; i++) {
                if (o1[i] == o2[i])
                    continue;
                if (o1[i] > o2[i])
                    return 1;
                return -1;
            }
            return SerializerUtils.compareInt(o1.length, o2.length);
        }
    };

    public static final Hasher<short[]> SHORT_ARRAY = new Hasher<short[]>() {

        @Override
        public boolean equals(short[] a1, short[] a2) {
            return Arrays.equals(a1, a2);
        }

        @Override
        public int hashCode(short[] shorts, int seed) {
            for (short element : shorts)
                seed = (-1640531527) * seed + element;
            return seed;
        }

        @Override
        public int compare(short[] o1, short[] o2) {
            if (o1 == o2) return 0;
            final int len = Math.min(o1.length, o2.length);
            for (int i = 0; i < len; i++) {
                if (o1[i] == o2[i])
                    continue;
                if (o1[i] > o2[i])
                    return 1;
                return -1;
            }
            return SerializerUtils.compareInt(o1.length, o2.length);
        }

    };

    public static final Hasher FAIL = new Hasher() {
        @Override
        public boolean equals(Object first, Object second) {
            throw new AssertionError("Hasher should not be called");
        }

        @Override
        public int hashCode(@NotNull Object o, int seed) {
            throw new AssertionError("Hasher should not be called");
        }

        @Override
        public int compare(Object o1, Object o2) {

            throw new AssertionError("Hasher should not be called");
        }
    };
}
