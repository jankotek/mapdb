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

package org.mapdb;

import java.util.Arrays;

/**
 * Calculates hash from an object. It also provides `equals` method.
 * Provides an alternative hashing method for {@link org.mapdb.HTreeMap}
 *
 * @author Jan Kotek
 */
public interface  Hasher<K> {

    int hashCode(K k);

    boolean equals(K k1, K k2);


    Hasher BASIC = new Hasher() {
        @Override
        public final int hashCode(  Object k) {
            return k.hashCode();
        }

        @Override
        public boolean equals(Object k1, Object k2) {
            return k1.equals(k2);
        }
    };

    Hasher<byte[]> BYTE_ARRAY = new Hasher<byte[]>() {
        @Override
        public final int hashCode(  byte[] k) {
            return Arrays.hashCode(k);
        }

        @Override
        public boolean equals(byte[] k1, byte[] k2) {
            return Arrays.equals(k1,k2);
        }
    };

    Hasher<char[]> CHAR_ARRAY = new Hasher<char[]>() {
        @Override
        public final int hashCode(  char[] k) {
            return Arrays.hashCode(k);
        }

        @Override
        public boolean equals(char[] k1, char[] k2) {
            return Arrays.equals(k1,k2);
        }
    };

    Hasher<int[]> INT_ARRAY = new Hasher<int[]>() {
        @Override
        public final int hashCode(  int[] k) {
            return Arrays.hashCode(k);
        }

        @Override
        public boolean equals(int[] k1, int[] k2) {
            return Arrays.equals(k1,k2);
        }
    };

    Hasher<long[]> LONG_ARRAY = new Hasher<long[]>() {
        @Override
        public final int hashCode(  long[] k) {
            return Arrays.hashCode(k);
        }

        @Override
        public boolean equals(long[] k1, long[] k2) {
            return Arrays.equals(k1,k2);
        }
    };

    Hasher<double[]> DOUBLE_ARRAY = new Hasher<double[]>() {
        @Override
        public final int hashCode(  double[] k) {
            return Arrays.hashCode(k);
        }

        @Override
        public boolean equals(double[] k1, double[] k2) {
            return Arrays.equals(k1,k2);
        }
    };


    Hasher<Object[]> ARRAY = new Hasher<Object[]>() {
        @Override
        public final int hashCode(  Object[] k) {
            return Arrays.hashCode(k);
        }

        @Override
        public boolean equals(Object[] k1, Object[] k2) {
            return Arrays.equals(k1,k2);
        }
    };


}