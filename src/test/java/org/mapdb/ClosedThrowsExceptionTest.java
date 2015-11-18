package org.mapdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/*
 * check that `IllegalAccessError` is thrown after DB was closed
 */
public abstract class ClosedThrowsExceptionTest {

    abstract DB db();

    DB db;


    @Before public void init(){
        db = db();
    }

    @After public void close(){
        db = null;
    }

    static public class Def extends ClosedThrowsExceptionTest{
        @Override DB db() {
            return DBMaker.memoryDB().make();
        }
    }

    static public class Async extends ClosedThrowsExceptionTest{
        @Override DB db() {
            return DBMaker.memoryDB().asyncWriteEnable().make();
        }
    }

    static public class NoCache extends ClosedThrowsExceptionTest{
        @Override DB db() {
            return DBMaker.memoryDB().make();
        }
    }

    static public class HardRefCache extends ClosedThrowsExceptionTest{
        @Override DB db() {
            return DBMaker.memoryDB().cacheHardRefEnable().make();
        }
    }

    static public class TX extends ClosedThrowsExceptionTest{
        @Override DB db() {
            return DBMaker.memoryDB().makeTxMaker().makeTx();
        }
    }

    static public class storeHeap extends ClosedThrowsExceptionTest{
        @Override DB db() {
            return new DB(new StoreHeap(true,CC.DEFAULT_LOCK_SCALE,0,false));
        }
    }

    @Test(expected = IllegalAccessError.class)
    public void closed_getHashMap(){
        db.hashMap("test");
        db.close();
        db.hashMap("test");
    }

    @Test()
    public void closed_getNamed(){
        db.hashMap("test");
        db.close();
        assertEquals(null, db.getNameForObject("test"));
    }


    @Test(expected = IllegalAccessError.class)
    public void closed_put(){
        Map m = db.hashMap("test");
        db.close();
        m.put("aa","bb");
    }


    @Test(expected = IllegalAccessError.class)
    public void closed_remove(){
        Map m = db.hashMap("test");
        m.put("aa","bb");
        db.close();
        m.remove("aa");
    }

    @Test(expected = IllegalAccessError.class)
    public void closed_close(){
        Map m = db.hashMap("test");
        m.put("aa","bb");
        db.close();
        db.close();
    }

    @Test(expected = IllegalAccessError.class)
    public void closed_rollback(){
        Map m = db.hashMap("test");
        m.put("aa","bb");
        db.close();
        db.rollback();
    }

    @Test(expected = IllegalAccessError.class)
    public void closed_commit(){
        Map m = db.hashMap("test");
        m.put("aa","bb");
        db.close();
        db.commit();
    }

    @Test
    public void closed_is_closed(){
        Map m = db.hashMap("test");
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
