package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.TT;
import org.mapdb.tree.HTreeMap;

import java.io.File;

public class Issue746Test {

    File f = TT.tempFile();

    @Test public void test() {
        run();
        run();
        run();
        run();
        run();
        run();
    }

    protected void run(){
        DB db = DBMaker
                .fileDB(f)
                .closeOnJvmShutdown()
                .transactionEnable()
                .make();
        HTreeMap map = (HTreeMap) db
                .hashMap("map")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.JAVA)
                .createOrOpen();
        //Putting data in
//        System.out.println("Storing data");
        map.put("something", "here");
        for (int i = 0; i < 100; i++) {
            map.put(""+i, "value-a"+i);
        }
//        System.out.println("Commiting transaction");
        db.commit();  //Here I get the stacktrace the second time.
//        System.out.println("Loading data");
        for (int i = 0; i < 100; i++) {
            Object get = map.get(""+i);
//            System.out.println(get);
        }
//        System.out.println(map.get("something"));
//        System.out.println("Clearing data");
        map.clear();
//        System.out.println("Commiting transaction");
        db.commit();
//        System.out.println("Loading data");
        for (int i = 0; i < 1000; i++) {
            Object get = map.get(""+i);
//            System.out.println(get);
        }
        db.close();
//        System.out.println(map.get("something"));
//        System.out.println("Done");
    }

}
