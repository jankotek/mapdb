package org.mapdb;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class JavaMethodsTest {

    @Test
    public void hashMap(){
        HTreeMap m = DBMaker.memoryDB().make().hashMap("aa").create();
        assertNotNull(m.keySet());
        assertNotNull(m.entrySet());
        assertNotNull(m.values());
    }

    @Test
    public void treeMap(){
        BTreeMap m = DBMaker.memoryDB().make().treeMap("aa").create();
        assertNotNull(m.keySet());
        assertNotNull(m.entrySet());
        assertNotNull(m.values());
    }

    @Test
    public void hashMap2(){
        Map m = DBMaker.memoryDB().make().hashMap("aa").create();
        assertNotNull(m.keySet());
        assertNotNull(m.entrySet());
        assertNotNull(m.values());
    }

    @Test
    public void treeMap2(){
        Map m = DBMaker.memoryDB().make().treeMap("aa").create();
        assertNotNull(m.keySet());
        assertNotNull(m.entrySet());
        assertNotNull(m.values());
    }
}
