package org.mapdb;


import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.util.Random;
import java.util.concurrent.ConcurrentNavigableMap;

public class Issue77Test {
    private  Random random = new Random(1);
    private  File dir = new File(Utils.tempDbFile()+"aaa");

    @Test
    public void run(){
        create();
        read(); // UnsupportedOperationException
        read(); // InternalError
    }

    DB open(boolean readOnly) {
        // This works:
        // DBMaker maker = DBMaker.newFileDB(new File(dir + "/test"));
        // This is faster, but fails if read() is called for the second time:
        DBMaker maker = DBMaker.newAppendFileDB(new File(dir + "/test"));
        if (readOnly) {
            maker.readOnly();
        }
        maker.randomAccessFileEnableIfNeeded();
        maker.closeOnJvmShutdown();
        DB db = maker.make(); // InternalError, UnsupportedOperationException
        return db;
    }

     void create() {
         dir.mkdirs();
        DB db = open(false);
        ConcurrentNavigableMap<Integer, byte[]> map = db.getTreeMap("bytes");
        int n = 10;
        int m = 10;
        for (int i = 0; i < n; i++) {
            map.put(i, getRandomData(m));
        }
        db.commit();
        db.close();
    }

     void read() {
        DB db = open(true); // InternalError, UnsupportedOperationException
        db.close();
    }

     byte[] getRandomData(int n) {
        byte[] c = new byte[n];
        random.nextBytes(c);
        return c;
    }

    @After
    public void cleanup(){
        for (File f : dir.listFiles()) {
            f.delete();
        }

    }
}
