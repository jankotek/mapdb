package org.mapdb20.issues;


import org.junit.Test;
import org.mapdb20.DB;
import org.mapdb20.DBMaker;

public class Issue114Test {

    @Test
    public void test(){
        DB db = DBMaker.tempFileDB()
                //.randomAccessFileEnable()
                .transactionDisable().make();
        db.getCircularQueue("test");
    }
}
