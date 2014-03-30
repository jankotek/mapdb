package org.mapdb;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * check that `IllegalAccessError` is thrown after DB was closed
 */
public abstract class ClosedThrowsExceptionTest {

    abstract DB db();

    DB db;


    @Before public void init(){
        db = db();
    }

    static public class Def extends ClosedThrowsExceptionTest{
        @Override DB db() {
            return DBMaker.newMemoryDB().make();
        }
    }

    static public class Async extends ClosedThrowsExceptionTest{
        @Override DB db() {
            return DBMaker.newMemoryDB().asyncWriteEnable().make();
        }
    }

    static public class NoCache extends ClosedThrowsExceptionTest{
        @Override DB db() {
            return DBMaker.newMemoryDB().cacheDisable().make();
        }
    }

    static public class HardRefCache extends ClosedThrowsExceptionTest{
        @Override DB db() {
            return DBMaker.newMemoryDB().cacheHardRefEnable().make();
        }
    }

    static public class TX extends ClosedThrowsExceptionTest{
        @Override DB db() {
            return DBMaker.newMemoryDB().makeTxMaker().makeTx();
        }
    }

    static public class storeHeap extends ClosedThrowsExceptionTest{
        @Override DB db() {
            return new DB(new StoreHeap());
        }
    }

    @Test(expected = IllegalAccessError.class)
    public void closed_getHashMap(){
        db.getHashMap("test");
        db.close();
        db.getHashMap("test");
    }

    @Test()
    public void closed_getNamed(){
        db.getHashMap("test");
        db.close();
        assertEquals(null, db.getNameForObject("test"));
    }


    @Test(expected = IllegalAccessError.class)
    public void closed_put(){
        Map m = db.getHashMap("test");
        db.close();
        m.put("aa","bb");
    }


    @Test(expected = IllegalAccessError.class)
    public void closed_remove(){
        Map m = db.getHashMap("test");
        m.put("aa","bb");
        db.close();
        m.remove("aa");
    }

    @Test(expected = IllegalAccessError.class)
    public void closed_close(){
        Map m = db.getHashMap("test");
        m.put("aa","bb");
        db.close();
        db.close();
    }

    @Test(expected = IllegalAccessError.class)
    public void closed_rollback(){
        Map m = db.getHashMap("test");
        m.put("aa","bb");
        db.close();
        db.rollback();
    }

    @Test(expected = IllegalAccessError.class)
    public void closed_commit(){
        Map m = db.getHashMap("test");
        m.put("aa","bb");
        db.close();
        db.commit();
    }

    @Test
    public void closed_is_closed(){
        Map m = db.getHashMap("test");
        m.put("aa","bb");
        db.close();
        assertEquals(true,db.isClosed());
    }

    @Test(expected = IllegalAccessError.class)
    public void closed_engine_get(){
        long recid = db.getEngine().put("aa",Serializer.STRING);
        db.close();
        db.getEngine().get(recid,Serializer.STRING);
    }

    @Test(expected = IllegalAccessError.class)
    public void closed_engine_put(){
        db.close();
        long recid = db.getEngine().put("aa",Serializer.STRING);
    }

    @Test(expected = IllegalAccessError.class)
    public void closed_engine_update(){
        long recid = db.getEngine().put("aa",Serializer.STRING);
        db.close();
        db.getEngine().update(recid, "aax", Serializer.STRING);
    }

    @Test(expected = IllegalAccessError.class)
    public void closed_engine_delete(){
        long recid = db.getEngine().put("aa",Serializer.STRING);
        db.close();
        db.getEngine().delete(recid, Serializer.STRING);
    }

}
