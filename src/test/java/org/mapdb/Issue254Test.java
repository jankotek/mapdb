package org.mapdb;

import org.junit.Test;

import java.io.EOFException;
import java.io.File;
import java.io.IOError;
import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class Issue254Test {

    @Test
    public void test(){
        File f = Utils.tempDbFile();
        DB db = DBMaker.newFileDB(f)
                .transactionDisable()
                .make();

        db.getAtomicLong("long").set(1L);
        db.close();

        db = DBMaker.newFileDB(f)
                .transactionDisable()
                .readOnly()
                .closeOnJvmShutdown()
                .make();

        assertEquals(0L, db.getAtomicLong("non-existing long").get());

        db.close();
    }


    DB ro;

    {
        File f = Utils.tempDbFile();
        ro = DBMaker.newFileDB(f).transactionDisable().transactionDisable().make();
        ro = DBMaker.newFileDB(f).transactionDisable().transactionDisable().readOnly().make();
    }

    @Test
    public void atomic_long(){
        Atomic.Long l = ro.getAtomicLong("non-existing");
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
        Atomic.Integer l = ro.getAtomicInteger("non-existing");
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
        Atomic.Boolean l = ro.getAtomicBoolean("non-existing");
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
        Atomic.String l = ro.getAtomicString("non-existing");
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
        Atomic.Var l = ro.getAtomicVar("non-existing");
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
        Collection l = ro.getTreeSet("non-existing");
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
        Collection l = ro.getHashSet("non-existing");
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
        Map l = ro.getTreeMap("non-existing");
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
        Map l = ro.getHashMap("non-existing");
        assertTrue(l.isEmpty());
        try{
            l.put("a","a");
            fail();
        }catch(UnsupportedOperationException e){
            assertEquals("Read-only",e.getMessage());
        }
    }






}
