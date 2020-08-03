package org.mapdb.flat

import io.kotlintest.shouldBe
import org.junit.Test

class LongBufStackTest{

    @Test fun putGet(){
        val stack = LongBufStack()

        stack.pop() shouldBe 0L

        val count = 1000L
        for(i in 1L .. count){
            stack.push(i)
        }

        for( i in count  downTo 0){
            stack.pop() shouldBe  i
        }

        stack.pop() shouldBe 0L
    }

}

