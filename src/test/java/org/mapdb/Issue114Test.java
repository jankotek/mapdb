package org.mapdb;


import org.junit.Test;

public class Issue114Test {

    @Test
    public void test(){
        DB db = DBMaker.newTempFileDB().randomAccessFileEnable().transactionDisable().make();
        db.getCircularQueue("test");
    }
}
