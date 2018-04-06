package org.mapdb.kotlin

import org.junit.Test
import kotlin.test.assertEquals

interface Issue888_KotlinI{

    fun aa(): Int {
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
        fun test(){
            assertEquals(Issue888_JavaI.JJ().aa().toLong(), 2)
            assertEquals(Issue888_JavaI.JK().aa().toLong(), 2)
            assertEquals(KJ().aa().toLong(), 2)
            assertEquals(KK().aa().toLong(), 2)
        }
    }
}

