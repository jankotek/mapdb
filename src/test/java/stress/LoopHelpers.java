package stress;
/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */
/**
 * Misc utilities in JSR166 performance tests
 */

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

class LoopHelpers {

    static final SimpleRandom staticRNG = new SimpleRandom();

    // Some mindless computation to do between synchronizations...

    /**
     * generates 32 bit pseudo-random numbers.
     * Adapted from http://www.snippets.org
     */
    public static int compute1(int x) {
        int lo = 16807 * (x & 0xFFFF);
        int hi = 16807 * (x >>> 16);
        lo += (hi & 0x7FFF) << 16;
        if ((lo & 0x80000000) != 0) {
            lo &= 0x7fffffff;
            ++lo;
        }
        lo += hi >>> 15;
        if (lo == 0 || (lo & 0x80000000) != 0) {
            lo &= 0x7fffffff;
            ++lo;
        }
        return lo;
    }

    /**
     *  Computes a linear congruential random number a random number
     *  of times.
     */
    public static int compute2(int x) {
        int loops = (x >>> 4) & 7;
        while (loops-- > 0) {
            x = (x * 2147483647) % 16807;
        }
        return x;
    }

    /**
     * Yet another random number generator
     */
    public static int compute3(int x) {
        int t = (x % 127773) * 16807 - (x / 127773) * 2836;
        return (t > 0) ? t : t + 0x7fffffff;
    }

    /**
     * Yet another random number generator
     */
    public static int compute4(int x) {
        return x * 134775813 + 1;
    }


    /**
     * Yet another random number generator
     */
    public static int compute5(int x) {
        return 36969 * (x & 65535) + (x >> 16);
    }

    /**
     * Marsaglia xorshift (1, 3, 10)
     */
    public static int compute6(int seed) {
        seed ^= seed << 1;
        seed ^= seed >>> 3;
        seed ^= (seed << 10);
        return seed;
    }

    /**
     * Marsaglia xorshift (6, 21, 7)
     */
    public static int compute7(int y) {
        y ^= y << 6;
        y ^= y >>> 21;
        y ^= (y << 7);
        return y;
    }

    // FNV: (x ^ 0x811c9dc5) * 0x01000193;  15485863;

    /**
     * Marsaglia xorshift for longs
     */
    public static long compute8(long x) {
        x ^= x << 13;
        x ^= x >>> 7;
        x ^= (x << 17);
        return x;
    }

    public static final class XorShift32Random {
        static final AtomicInteger seq = new AtomicInteger(8862213);
        int x = -1831433054;
        public XorShift32Random(int seed) { x = seed; }
        public XorShift32Random() {
            this((int) System.nanoTime() + seq.getAndAdd(129));
        }
        public int next() {
            x ^= x << 6;
            x ^= x >>> 21;
            x ^= (x << 7);
            return x;
        }
    }


    /** Multiplication-free RNG from Marsaglia "Xorshift RNGs" paper */
    public static final class MarsagliaRandom {
        static final AtomicInteger seq = new AtomicInteger(3122688);
        int x;
        int y = 842502087;
        int z = -715159705;
        int w = 273326509;
        public MarsagliaRandom(int seed) { x = seed; }
        public MarsagliaRandom() {
            this((int) System.nanoTime() + seq.getAndAdd(129));
        }
        public int next() {
            int t = x ^ (x << 11);
            x = y;
            y = z;
            z = w;
            return w = (w ^ (w >>> 19) ^ (t ^ (t >>> 8)));
        }
    }

    /**
     * Unsynchronized version of java.util.Random algorithm.
     */
    public static final class SimpleRandom {
        private static final long multiplier = 0x5DEECE66DL;
        private static final long addend = 0xBL;
        private static final long mask = (1L << 48) - 1;
        static final AtomicLong seq = new AtomicLong( -715159705);
        private long seed;

        SimpleRandom(long s) {
            seed = s;
        }

        SimpleRandom() {
            seed = System.nanoTime() + seq.getAndAdd(129);
        }

        public void setSeed(long s) {
            seed = s;
        }

        public int next() {
            long nextseed = (seed * multiplier + addend) & mask;
            seed = nextseed;
            return ((int) (nextseed >>> 17)) & 0x7FFFFFFF;
        }
    }

    public static class BarrierTimer implements Runnable {
        volatile boolean started;
        volatile long startTime;
        volatile long endTime;
        public void run() {
            long t = System.nanoTime();
            if (!started) {
                started = true;
                startTime = t;
            } else
                endTime = t;
        }
        public void clear() {
            started = false;
        }
        public long getTime() {
            return endTime - startTime;
        }
    }

    public static String rightJustify(long n) {
        // There's probably a better way to do this...
        String field = "         ";
        String num = Long.toString(n);
        if (num.length() >= field.length())
            return num;
        StringBuffer b = new StringBuffer(field);
        b.replace(b.length()-num.length(), b.length(), num);
        return b.toString();
    }

}