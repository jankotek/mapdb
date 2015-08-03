package org.mapdb20;


import org.junit.Test;


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
