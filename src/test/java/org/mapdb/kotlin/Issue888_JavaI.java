package org.mapdb.kotlin;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public interface Issue888_JavaI {

    default int aa(){
        return 1;
    }


    default int bb(){
        return 1;
    }

    class JJ implements Issue888_JavaI {
        @Override
        public int aa() {
            return 2;
        }
    }



    class JK implements Issue888_KotlinI {
        @Override
        public int aa() {
            return 2;
        }

        @Test public void test_override(){
            assertEquals(new JJ().aa(), 2);
            assertEquals(new JK().aa(), 2);
            assertEquals(new KJ().aa(), 2);
            assertEquals(new KK().aa(), 2);
        }


        @Test public void test_not_override(){
            assertEquals(new JJ().bb(), 1);
            assertEquals(new JK().bb(), 1);
            assertEquals(new KJ().bb(), 1);
            assertEquals(new KK().bb(), 1);
        }


        //See Issue 888, if this is removed, compilation fails
        // for now we define all interfaces in java
        @Override
        public int bb() {
            return 1;
        }
    }
}
