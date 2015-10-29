package org.mapdb20.issues;


import org.junit.Test;
import org.mapdb20.BTreeMap;
import org.mapdb20.DB;
import org.mapdb20.DBMaker;
import org.mapdb20.TT;


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
