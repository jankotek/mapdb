package org.mapdb.kotlin

import org.junit.Assert.assertEquals
import org.junit.Test

interface Issue888_KotlinI{

    fun aa(): Int {
        return 1
    }


    //@JvmDefault
    fun bb(): Int {
        return 1
    }



    class KJ : Issue888_JavaI {
        override fun aa(): Int {
            return 2
        }
    }


    class KK : Issue888_KotlinI {
        override fun aa(): Int {
            return 2
        }
    }


    class Test2(){
        @Test
        fun test_override(){
            assertEquals(Issue888_JavaI.JJ().aa().toLong(), 2)
            assertEquals(Issue888_JavaI.JK().aa().toLong(), 2)
            assertEquals(KJ().aa().toLong(), 2)
            assertEquals(KK().aa().toLong(), 2)
        }


        @Test
        fun test_not_override() {
            assertEquals(Issue888_JavaI.JJ().bb(), 1)
            assertEquals(Issue888_JavaI.JK().bb().toLong(), 1)
            assertEquals(Issue888_KotlinI.KJ().bb(), 1)
            assertEquals(Issue888_KotlinI.KK().bb().toLong(), 1)
        }
    }
}

