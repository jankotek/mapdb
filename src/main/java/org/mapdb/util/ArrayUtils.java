/*
 *  Copyright (c) 2012 Jan Kotek
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Adopted from Apache Harmony with following copyright:
 *
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */
package org.mapdb.util;

import java.util.Arrays;

/**
 * <p>
 * A small toolkit of classes that support lock-free thread-safe
 * programming on single records.  In essence, the classes here
 * provide provide an atomic conditional update operation of the form:
 * </p>
 *
 * <pre>
 *   boolean compareAndSet(expectedValue, updateValue);
 * </pre>
 *
 * <p>
 * This method (which varies in argument types across different
 * classes) atomically sets a record to the {@code updateValue} if it
 * currently holds the {@code expectedValue}, reporting {@code true} on
 * success. Classes jere also contain methods to get and
 * unconditionally set values.
 * </p><p>
 *
 * The specifications of these methods enable to
 * employ more efficient internal DB locking. CompareAndSwap
 * operation is typically faster than using transactions, global lock or other
 * concurrent protection.
 *
 * </p><p>
 * Instances of classes
 * {@link Atomic.Boolean},
 * {@link Atomic.Integer},
 * {@link Atomic.Long},
 * {@link Atomic.String} and
 * {@link Atomic.Var}
 * each provide access and updates to a single record of the
 * corresponding type.  Each class also provides appropriate utility
 * methods for that type.  For example, classes {@code Atomic.Long} and
 * {@code Atomic.Integer} provide atomic increment methods.  One
 * application is to generate unique keys for Maps:
 * </p>
 * <pre>
 *    Atomic.Long id = Atomic.getLong("mapId");
 *    map.put(id.getAndIncrement(), "something");
 * </pre>
 *
 * <p>
 * Atomic classes are designed primarily as building blocks for
 * implementing non-blocking data structures and related infrastructure
 * classes.  The {@code compareAndSet} method is not a general
 * replacement for locking.  It applies only when critical updates for an
 * object are confined to a <em>single</em> record.
 *</p><p>
 *
 * Atomic classes are not general purpose replacements for
 * {@code java.lang.Integer} and related classes.  They do <em>not</em>
 * define methods such as {@code hashCode} and
 * {@code compareTo}.  (Because atomic records are expected to be
 * mutated, they are poor choices for hash table keys.)  Additionally,
 * classes are provided only for those types that are commonly useful in
 * intended applications. Other types has to be wrapped into general {@link Atomic.Var}
 * </p><p>
 *
 * You can also hold floats using
 * {@link java.lang.Float#floatToIntBits} and
 * {@link java.lang.Float#intBitsToFloat} conversions, and doubles using
 * {@link java.lang.Double#doubleToLongBits} and
 * {@link java.lang.Double#longBitsToDouble} conversions.
 * </p>
 *
 */
public final class ArrayUtils {

    private ArrayUtils() {
    }

    /**
     * <p>
     * Return a copy of original array and handling {@code null}.
     * </p>
     * 
     * @param array
     *            the array to be copy, may be {@code null}
     * @return a copy of the original array, {@code null} if {@code null} input
     */
    public static boolean[] copyOf(final boolean[] array) {
        if (array == null) {
            return null;
        }
        return Arrays.copyOf(array, array.length);
    }

    /**
     * <p>
     * Return a copy of original array and handling {@code null}.
     * </p>
     * 
     * @param array
     *            the array to be copy, may be {@code null}
     * @return a copy of the original array, {@code null} if {@code null} input
     */
    public static byte[] copyOf(final byte[] array) {
        if (array == null) {
            return null;
        }
        return Arrays.copyOf(array, array.length);
    }

    /**
     * <p>
     * Return a copy of original array and handling {@code null}.
     * </p>
     * 
     * @param array
     *            the array to be copy, may be {@code null}
     * @return a copy of the original array, {@code null} if {@code null} input
     */
    public static short[] copyOf(final short[] array) {
        if (array == null) {
            return null;
        }
        return Arrays.copyOf(array, array.length);
    }

    /**
     * <p>
     * Return a copy of original array and handling {@code null}.
     * </p>
     * 
     * @param array
     *            the array to be copy, may be {@code null}
     * @return a copy of the original array, {@code null} if {@code null} input
     */
    public static int[] copyOf(final int[] array) {
        if (array == null) {
            return null;
        }
        return Arrays.copyOf(array, array.length);
    }

    /**
     * <p>
     * Return a copy of original array and handling {@code null}.
     * </p>
     * 
     * @param array
     *            the array to be copy, may be {@code null}
     * @return a copy of the original array, {@code null} if {@code null} input
     */
    public static long[] copyOf(final long[] array) {
        if (array == null) {
            return null;
        }
        return Arrays.copyOf(array, array.length);
    }

    /**
     * <p>
     * Return a copy of original array and handling {@code null}.
     * </p>
     * 
     * @param array
     *            the array to be copy, may be {@code null}
     * @return a copy of the original array, {@code null} if {@code null} input
     */
    public static float[] copyOf(final float[] array) {
        if (array == null) {
            return null;
        }
        return Arrays.copyOf(array, array.length);
    }

    /**
     * <p>
     * Return a copy of original array and handling {@code null}.
     * </p>
     * 
     * @param array
     *            the array to be copy, may be {@code null}
     * @return a copy of the original array, {@code null} if {@code null} input
     */
    public static double[] copyOf(final double[] array) {
        if (array == null) {
            return null;
        }
        return Arrays.copyOf(array, array.length);
    }

    /**
     * <p>
     * Return a copy of original array and handling {@code null}.
     * </p>
     * 
     * @param array
     *            the array to be copy, may be {@code null}
     * @return a copy of the original array, {@code null} if {@code null} input
     */
    public static char[] copyOf(final char[] array) {
        if (array == null) {
            return null;
        }
        return Arrays.copyOf(array, array.length);
    }

    /**
     * <p>
     * Return a copy of original array and handling {@code null}.
     * </p>
     * 
     * @param array
     *            the array to be copy, may be {@code null}
     * @return a copy of the original array, {@code null} if {@code null} input
     */
    public static <T> T[] copyOf(final T[] array) {
        if (array == null) {
            return null;
        }
        return Arrays.copyOf(array, array.length);
    }

}