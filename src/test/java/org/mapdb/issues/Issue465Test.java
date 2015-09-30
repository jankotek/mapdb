package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TT;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class Issue465Test  {


    static  class ExtHashMap extends HashMap<String,String>{}


    @Test
    public void testExtHashMap(){
        File f = TT.tempDbFile();
        DB db = DBMaker.fileDB(f).make();
        Map<String,HashMap> map = db.treeMap("test");

        ExtHashMap ehm = new ExtHashMap();
        ehm.put("Key1", "Value1");
        ehm.put("Key2", "Value2");
        map.put("ehm", ehm);
        db.commit();
        assertEquals(2, map.get("ehm").size());


        ExtHashMap ehm2 = new ExtHashMap();
        ehm2.put("Key1",null);
        ehm2.put("Key2", null);
        map.put("ehm2", ehm2);
        db.commit();

        assertEquals(2, map.get("ehm").size());
        assertEquals(2, map.get("ehm2").size());
        assertTrue(map.get("ehm").toString().contains("Key1"));
        assertTrue(map.get("ehm2").toString().contains("Key1"));

        db.close();

        db = DBMaker.fileDB(f).make();
        map = db.treeMap("test");

        assertEquals(2, map.get("ehm").size());
        assertEquals(2, map.get("ehm2").size());
        assertTrue(map.get("ehm").toString().contains("Key1"));
        assertTrue(map.get("ehm2").toString().contains("Key1"));
        db.close();
        f.delete();
    }


    @Test
    public void testHashMap(){
        File f = TT.tempDbFile();
        DB db = DBMaker.fileDB(f).make();
        Map<String,HashMap> map = db.treeMap("test");

        HashMap ehm = new HashMap();
        ehm.put("Key1", "Value1");
        ehm.put("Key2", "Value2");
        map.put("ehm", ehm);
        db.commit();

        HashMap ehm2 = new HashMap();
        ehm2.put("Key1",null);
        ehm2.put("Key2", null);
        map.put("ehm2", ehm2);
        db.commit();


        assertEquals(2, map.get("ehm").size());
        assertEquals(2, map.get("ehm2").size());
        assertTrue(map.get("ehm").toString().contains("Key1"));
        assertTrue(map.get("ehm2").toString().contains("Key1"));

        db.close();

        db = DBMaker.fileDB(f).make();
        map = db.treeMap("test");

        assertEquals(2, map.get("ehm").size());
        assertEquals(2, map.get("ehm2").size());
        assertTrue(map.get("ehm").toString().contains("Key1"));
        assertTrue(map.get("ehm2").toString().contains("Key1"));

        db.close();
        f.delete();
    }

    @Test public void clone2() throws IOException, ClassNotFoundException {
        ExtHashMap ehm = new ExtHashMap();
        ehm.put("Key1", "Value1");
        ehm.put("Key2", "Value2");


        assertEquals(ehm, TT.cloneJavaSerialization(ehm));
    }

    @Test public void clone3() throws IOException, ClassNotFoundException {
        ExtHashMap ehm = new ExtHashMap();
        ehm.put("Key1", "Value1");
        ehm.put("Key2", "Value2");


        assertEquals(ehm, TT.clone(ehm, DBMaker.memoryDB().transactionDisable().make().getDefaultSerializer()));
    }


}