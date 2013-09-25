package org.mapdb;


import java.io.File;

public class Issue198Test {

    public static void main(String[] args) {

        DB db = DBMaker.newFileDB(Utils.tempDbFile())
                .closeOnJvmShutdown()
                .randomAccessFileEnable()
                .make();
        BTreeMap<Integer, Integer> map = db.createTreeMap("testmap").makeOrGet();
        for(int i = 1; i <= 3000; ++i)
            map.put(i, i);
        db.commit();
        db.close();

    }
}
