package org.mapdb;


import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class Serialization2Test{


    @Test public void test2() throws IOException {
        File index = TT.tempDbFile();
        DB db = DBMaker.fileDB(index).transactionDisable().make();

        Serialization2Bean processView = new Serialization2Bean();

        Map<Object, Object> map =  db.hashMap("test2");

        map.put("abc", processView);

        db.commit();

        Serialization2Bean retProcessView = (Serialization2Bean)map.get("abc");
        assertEquals(processView, retProcessView);

        db.close();
    }


    @Test public void test2_engine() throws IOException {
        File index = TT.tempDbFile();
        DB db = DBMaker.fileDB(index).make();

        Serialization2Bean processView = new Serialization2Bean();

        long recid = db.engine.put(processView, (Serializer<Object>) db.getDefaultSerializer());

        db.commit();

        Serialization2Bean retProcessView = (Serialization2Bean) db.engine.get(recid, db.getDefaultSerializer());
        assertEquals(processView, retProcessView);

        db.close();
    }


    @Test  public void test3() throws IOException {
        File index = TT.tempDbFile();

        Serialized2DerivedBean att = new Serialized2DerivedBean();
        DB db = DBMaker.fileDB(index).make();

        Map<Object, Object> map =  db.hashMap("test");

        map.put("att", att);
        db.commit();
        db.close();
        db = DBMaker.fileDB(index).make();
        map =  db.hashMap("test");


        Serialized2DerivedBean retAtt = (Serialized2DerivedBean) map.get("att");
        assertEquals(att, retAtt);
    }



    static class AAA implements Serializable {
		
    	private static final long serialVersionUID = 632633199013551846L;
		
		String test  = "aa";
    }


    @Test  public void testReopenWithDefrag(){

        File f = TT.tempDbFile();

        DB db = DBMaker.fileDB(f)
                .transactionDisable()
                .checksumEnable()
                .make();

        Map<Integer,AAA> map = db.treeMap("test");
        map.put(1,new AAA());

        db.compact();
        System.out.println(db.getEngine().get(Engine.RECID_CLASS_CATALOG, Serializer.RECID_ARRAY));
        db.close();

        db = DBMaker.fileDB(f)
                .transactionDisable()
                .checksumEnable()
                .make();

        map = db.treeMap("test");
        assertNotNull(map.get(1));
        assertEquals(map.get(1).test, "aa");


        db.close();
    }

}
