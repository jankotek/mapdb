package org.mapdb10;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Issue513Test {

    @Test
    public void atomicVar(){
        DB db = DBMaker.newMemoryDB().make();

        Atomic.Var  v = db.getAtomicVar("test");
        assertTrue(v!=null);
    }

    @Test
    public void atomicLong(){
        DB db = DBMaker.newMemoryDB().make();

        Atomic.Long  v = db.getAtomicLong("test");
        assertTrue(v!=null);
        assertEquals(0, v.get());
    }

    @Test
    public void atomicInt(){
        DB db = DBMaker.newMemoryDB().make();

        Atomic.Integer  v = db.getAtomicInteger("test");
        assertTrue(v!=null);
        assertEquals(0,v.get());
    }

    @Test
    public void atomicBoolean(){
        DB db = DBMaker.newMemoryDB().make();

        Atomic.Boolean  v = db.getAtomicBoolean("test");
        assertTrue(v!=null);
        assertEquals(false,v.get());
    }

    @Test
    public void atomicString(){
        DB db = DBMaker.newMemoryDB().make();

        Atomic.String  v = db.getAtomicString("test");
        assertTrue(v!=null);
        assertEquals("",v.get());
    }




}
