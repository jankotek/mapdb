package org.mapdb10;


import org.junit.Test;


public class Issue198Test {

    @Test public void main() {

        DB db = DBMaker.newFileDB(UtilsTest.tempDbFile())
                .closeOnJvmShutdown()
                //.randomAccessFileEnable()
                .make();
        BTreeMap<Integer, Integer> map = db.createTreeMap("testmap").makeOrGet();
        for(int i = 1; i <= 3000; ++i)
            map.put(i, i);
        db.commit();
        db.close();

    }
}
