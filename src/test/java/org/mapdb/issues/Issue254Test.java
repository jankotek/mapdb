package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TT;

import java.io.File;
import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.*;

@SuppressWarnings({"rawtypes","unchecked"})
public class Issue254Test {

    @Test
    public void test(){
        File f = TT.tempDbFile();
        DB db = DBMaker.fileDB(f)
                .transactionDisable()
                .make();

        db.atomicLong("long").set(1L);
        db.close();

        db = DBMaker.fileDB(f)
                .transactionDisable()
                .readOnly()
                .closeOnJvmShutdown()
                .make();

        assertEquals(0L, db.atomicLong("non-existing long").get());

        db.close();
    }


    DB ro;

    {
        File f = TT.tempDbFile();
        ro = DBMaker.fileDB(f).transactionDisable().make();
        ro.close();
        ro = DBMaker.fileDB(f).transactionDisable().readOnly().make();
    }

    @Test
    public void atomic_long(){
        Atomic.Long l = ro.atomicLong("non-existing");
        assertEquals(0L, l.get());
        try{
            l.set(1);
            fail();
        }catch(UnsupportedOperationException e){
            assertEquals("Read-only",e.getMessage());
        }
    }

    @Test
    public void atomic_int(){
        Atomic.Integer l = ro.atomicInteger("non-existing");
        assertEquals(0, l.get());
        try{
            l.set(1);
            fail();
        }catch(UnsupportedOperationException e){
            assertEquals("Read-only",e.getMessage());
        }
    }

    @Test
    public void atomic_boolean(){
        Atomic.Boolean l = ro.atomicBoolean("non-existing");
        assertEquals(false, l.get());
        try{
            l.set(true);
            fail();
        }catch(UnsupportedOperationException e){
            assertEquals("Read-only",e.getMessage());
        }
    }

    @Test
    public void atomic_string(){
        Atomic.String l = ro.atomicString("non-existing");
        assertEquals("", l.get());
        try{
            l.set("a");
            fail();
        }catch(UnsupportedOperationException e){
            assertEquals("Read-only",e.getMessage());
        }
    }

    @Test
    public void atomic_var(){
        Atomic.Var l = ro.atomicVar("non-existing");
        assertEquals(null, l.get());
        try{
            l.set("a");
            fail();
        }catch(UnsupportedOperationException e){
            assertEquals("Read-only",e.getMessage());
        }
    }

    @Test
    public void atomic_queue(){
        Collection l = ro.getQueue("non-existing");
        assertTrue(l.isEmpty());
        try{
            l.add("a");
            fail();
        }catch(UnsupportedOperationException e){
            assertEquals("Read-only",e.getMessage());
        }
    }

    @Test
    public void atomic_stack(){
        Collection l = ro.getStack("non-existing");
        assertTrue(l.isEmpty());
        try{
            l.add("a");
            fail();
        }catch(UnsupportedOperationException e){
            assertEquals("Read-only",e.getMessage());
        }
    }

    @Test
    public void atomic_circular_queue(){
        Collection l = ro.getCircularQueue("non-existing");
        assertTrue(l.isEmpty());
        try{
            l.add("a");
            fail();
        }catch(UnsupportedOperationException e){
            assertEquals("Read-only",e.getMessage());
        }
    }


    @Test
    public void atomic_tree_set(){
        Collection l = ro.treeSet("non-existing");
        assertTrue(l.isEmpty());
        try{
            l.add("a");
            fail();
        }catch(UnsupportedOperationException e){
            assertEquals("Read-only",e.getMessage());
        }
    }

    @Test
    public void atomic_hash_set(){
        Collection l = ro.hashSet("non-existing");
        assertTrue(l.isEmpty());
        try{
            l.add("a");
            fail();
        }catch(UnsupportedOperationException e){
            assertEquals("Read-only",e.getMessage());
        }
    }


    @Test
    public void atomic_tree_map(){
        Map l = ro.treeMap("non-existing");
        assertTrue(l.isEmpty());
        try{
            l.put("a", "a");
            fail();
        }catch(UnsupportedOperationException e){
            assertEquals("Read-only",e.getMessage());
        }
    }

    @Test
    public void atomic_hash_map(){
        Map l = ro.hashMap("non-existing");
        assertTrue(l.isEmpty());
        try{
            l.put("a","a");
            fail();
        }catch(UnsupportedOperationException e){
            assertEquals("Read-only",e.getMessage());
        }
    }






}
