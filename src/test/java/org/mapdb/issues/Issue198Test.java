package org.mapdb.issues;


import org.junit.Test;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TT;


public class Issue198Test {

    @Test public void main() {

        DB db = DBMaker.fileDB(TT.tempDbFile())
                .closeOnJvmShutdown()
                //.randomAccessFileEnable()
                .make();
        BTreeMap<Integer, Integer> map = db.treeMapCreate("testmap").makeOrGet();
        for(int i = 1; i <= 3000; ++i)
            map.put(i, i);
        db.commit();
        db.close();

    }
}
