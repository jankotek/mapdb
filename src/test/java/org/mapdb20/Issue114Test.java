package org.mapdb20;


import org.junit.Test;

public class Issue114Test {

    @Test
    public void test(){
        DB db = DBMaker.tempFileDB()
                //.randomAccessFileEnable()
                .transactionDisable().make();
        db.getCircularQueue("test");
    }
}
