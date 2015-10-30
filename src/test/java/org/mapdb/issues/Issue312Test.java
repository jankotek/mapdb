package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TT;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class Issue312Test {

    @Test
    public void test() throws IOException{
        if(TT.scale()==0)
            return;

        File f = File.createTempFile("mapdbTest","test");
        DB db = DBMaker.fileDB(f)
                .mmapFileEnableIfSupported()
                .transactionDisable()
                .make();

        Map<Long, String> map = db.treeMapCreate("data").make();
        for(long i = 0; i<100000;i++){
            map.put(i,i + "hi my friend " + i);
        }
        db.commit();
        db.close();

        db = DBMaker.fileDB(f)
                .mmapFileEnableIfSupported()
                .transactionDisable()
                .readOnly()
                .make();


    }
}
