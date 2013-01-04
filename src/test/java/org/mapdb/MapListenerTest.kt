package org.mapdb

import kotlin.test.assertEquals
import org.junit.*
import kotlin.test.assertTrue


class MapListenerTest{

    Test fun hashMap(){
        tt(DBMaker.newMemoryDB().journalDisable().make().getHashMap("test"))
    }

    Test fun treeMap(){
        tt(DBMaker.newMemoryDB().journalDisable().make().getTreeMap("test"))
    }


    fun tt(m:Bind.MapWithModificationListener<Any?,Any?>){
        var key:Any? = null
        var newVal:Any? = null
        var oldVal:Any? = null
        var counter = 0;

        val listener = mapListener{(key2:Any?,oldVal2:Any?,newVal2:Any?) ->
           counter++;
           key = key2;
           oldVal = oldVal2;
           newVal = newVal2;
        }

        m.addModificationListener(listener);

        //check CRUD
        m.put("aa","bb")
        assertTrue(key=="aa" && newVal=="bb" && oldVal==null && counter==1);

        m.put("aa","cc")
        assertTrue(key=="aa" && newVal=="cc" && oldVal=="bb" && counter==2);

        m.remove("aa")
        assertTrue(key=="aa" && newVal==null && oldVal=="cc" && counter==3);

        //check clear()
        m.put("aa","bb")
        assertTrue(key=="aa" && newVal=="bb" && oldVal==null && counter==4);
        m.clear()
        assertTrue(key=="aa" && newVal==null && oldVal=="bb" && counter==5);


        //check it was unregistered
        counter = 0;
        m.removeModificationListener(listener);
        m.put("aa","bb");
        assertEquals(0, counter);
    }
}
