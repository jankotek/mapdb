package org.mapdb

import org.junit.*
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SnapshotEngineTest{

    val e = SnapshotEngine(StorageDirect(Volume.memoryFactory(false)),1024)

    Test fun update(){
        val recid = e.put(111, Serializer.INTEGER_SERIALIZER);
        val snapshot = e.snapshot();
        e.update(recid, 222, Serializer.INTEGER_SERIALIZER);
        assertTrue(111 == snapshot.get(recid, Serializer.INTEGER_SERIALIZER))
    }

    Test fun compareAndSwap(){
        val recid = e.put(111, Serializer.INTEGER_SERIALIZER);
        val snapshot = e.snapshot();
        e.compareAndSwap(recid, 111, 222, Serializer.INTEGER_SERIALIZER);
        assertTrue(111 == snapshot.get(recid, Serializer.INTEGER_SERIALIZER))
    }

    Test fun delete(){
        val recid = e.put(111, Serializer.INTEGER_SERIALIZER);
        val snapshot = e.snapshot();
        e.delete(recid);
        assertTrue(111 == snapshot.get(recid, Serializer.INTEGER_SERIALIZER))
    }

    Test fun notExist(){
        val snapshot = e.snapshot();
        val recid = e.put(111, Serializer.INTEGER_SERIALIZER);
        assertTrue(null == snapshot.get(recid, Serializer.INTEGER_SERIALIZER))
    }


    Test fun create_snapshot(){
        val e = DBMaker.newMemoryDB().makeEngine();
        val snapshot = SnapshotEngine.createSnapshotFor(e);
        assertTrue(snapshot!=null)
    }

    Test fun DB_snapshot(){
        val db = DBMaker.newMemoryDB().journalDisable().make();
        val recid = db.getEngine().put("aa",Serializer.STRING_SERIALIZER);
        val db2 = db.snapshot();
        assertEquals("aa", db2.getEngine().get(recid,Serializer.STRING_SERIALIZER) as String);
        db.getEngine().update(recid, "bb",Serializer.STRING_SERIALIZER);
        assertEquals("aa", db2.getEngine().get(recid,Serializer.STRING_SERIALIZER) as String);
    }

    Test fun BTreeMap_snapshot(){
        val map =
                DBMaker.newMemoryDB().journalDisable()
                  .make().getTreeMap<String,String>("aaa");
        map.put("aa","aa");
        val map2 = map.snapshot();
        map.put("aa","bb");
        assertEquals("aa",map2.get("aa"))
    }

    Test fun HTreeMap_snapshot(){
        val map =
                DBMaker.newMemoryDB().journalDisable()
                        .make().getHashMap<String,String>("aaa");
        map.put("aa","aa");
        val map2 = map.snapshot();
        map.put("aa","bb");
        assertEquals("aa",map2.get("aa"))
    }

}
