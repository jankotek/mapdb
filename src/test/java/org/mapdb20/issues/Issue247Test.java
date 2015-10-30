package org.mapdb20.issues;


import org.junit.Test;
import org.mapdb20.DB;
import org.mapdb20.DBMaker;
import org.mapdb20.TT;

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
        File f = TT.tempDbFile();
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
