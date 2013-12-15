package org.mapdb;


import org.junit.Test;

import java.io.File;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class Issue247Test {

    @Test
    public void test(){
        File f = Utils.tempDbFile();
        DB db = DBMaker.newFileDB(f)
                .transactionDisable()
                .make();

        Map m = db.createTreeMap("test")
                .counterEnable()
                .valuesOutsideNodesEnable()
                .make();
        m.put("a","b");
        //db.commit();

        db.close();

        db = DBMaker.newFileDB(f)
                .readOnly()
                .make();
        m = db.getTreeMap("test");
        assertEquals("b",m.get("a"));
    }
}
