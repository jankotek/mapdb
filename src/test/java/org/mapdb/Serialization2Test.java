package org.mapdb;


import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class Serialization2Test extends TestFile {


    @Test public void test2() throws IOException {
        DB db = DBMaker.newFileDB(index).cacheDisable().asyncWriteDisable().journalDisable().make();

        Serialization2Bean processView = new Serialization2Bean();

        Map<Object, Object> map =  db.getHashMap("test2");

        map.put("abc", processView);

        db.commit();

        Serialization2Bean retProcessView = (Serialization2Bean)map.get("abc");
        assertEquals(processView, retProcessView);

        db.close();
    }


    @Test public void test2_engine() throws IOException {
        DB db = DBMaker.newFileDB(index).cacheDisable().asyncWriteDisable().make();

        Serialization2Bean processView = new Serialization2Bean();

        long recid = db.engine.put(processView, (Serializer<Object>) db.getDefaultSerializer());

        db.commit();

        Serialization2Bean retProcessView = (Serialization2Bean) db.engine.get(recid, db.getDefaultSerializer());
        assertEquals(processView, retProcessView);

        db.close();
    }


    @Test  public void test3() throws IOException {


        Serialized2DerivedBean att = new Serialized2DerivedBean();
        DB db = DBMaker.newFileDB(index).cacheDisable().asyncWriteDisable().make();

        Map<Object, Object> map =  db.getHashMap("test");

        map.put("att", att);
        db.commit();
        db.close();
        db = DBMaker.newFileDB(index).cacheDisable().asyncWriteDisable().make();
        map =  db.getHashMap("test");


        Serialized2DerivedBean retAtt = (Serialized2DerivedBean) map.get("att");
        assertEquals(att, retAtt);
    }



    static class AAA implements Serializable {
		
    	private static final long serialVersionUID = 632633199013551846L;
		
		String test  = "aa";
    }

// TODO skipped test
//    @Test  public void testReopenWithDefrag(){
//
//        String f = newTestFile();
//
//        DB db = DBMaker.openFile(f)
//                .disableTransactions()
//                .make();
//
//        Map<Integer,AAA> map = db.createTreeMap("test");
//        map.put(1,new AAA());
//
//        db.compact(true);
//        db.close();
//
//        db = DBMaker.openFile(f)
//                .disableTransactions()
//                .make();
//
//        map = db.getTreeMap("test");
//        assertNotNull(map.get(1));
//        assertEquals(map.get(1).test, "aa");
//
//
//        db.close();
//    }

}
