package org.mapdb.issues;


import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;

public class Issue114Test {

    @Test
    public void test(){
        DB db = DBMaker.tempFileDB()
                //.randomAccessFileEnable()
                .transactionDisable().make();
        db.getCircularQueue("test");
    }
}
