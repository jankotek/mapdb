package org.mapdb;


import org.junit.Test;

import java.io.File;
import java.util.Map;

public class Issue247Test {

        private Map getMap(DB db){
                return db.treeMapCreate("test")
                             .counterEnable()
                             .valuesOutsideNodesEnable()
                             .makeOrGet();
            }


    @Test
    public void test(){
        File f = UtilsTest.tempDbFile();
        DB db = DBMaker.fileDB(f)
                .transactionDisable()
                .make();

        getMap(db);
        //db.commit();

        db.close();

        db = DBMaker.fileDB(f)
                .readOnly()
                .make();
        getMap(db).size();
    }
}
