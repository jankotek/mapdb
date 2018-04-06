package org.mapdb.kotlin;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public interface Issue888_JavaI {

    default int aa(){
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

        @Test public void testw(){
            assertEquals(new JJ().aa(), 2);
            assertEquals(new JK().aa(), 2);
            assertEquals(new KJ().aa(), 2);
            assertEquals(new KK().aa(), 2);
        }
    }
}
