package org.mapdb

import org.junit.Test
import kotlin.test.assertTrue
import java.util.Random
import java.util.ArrayList

class BTreeKeySerializerTest{

    Test fun long(){
       val m = DBMaker.newMemoryDB()
               .cacheDisable()
               .make()
               .createTreeMap<Long?,Long>("test",32,false,
               BTreeKeySerializer.ZERO_OR_POSITIVE_LONG,
               null, null );

        for(i in 1.toLong()..1000.toLong()){
            m.put(i*i,i*i+1);
        }

        for(i in 1.toLong()..1000.toLong()){
            assertTrue(m.get(i*i)==i*i+1);
        }
    }

    Test fun string(){
        val r = Random();

        val m = DBMaker.newMemoryDB()
                .cacheDisable()
                .make()
                .createTreeMap<String?,String>("test",32,false,
                BTreeKeySerializer.STRING,
                null, null );

        val list = ArrayList<String>()
        for(i in 1..1000){
            val s = ""+r.nextDouble()+(i*i*i)
            m.put(s,s+"aa");
        }

        for(s in list){
            assertTrue(m.get(s)==s+"aa")
        }
    }
}
