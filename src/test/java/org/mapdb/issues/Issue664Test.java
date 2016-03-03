//TODO add this test at M3
/*
package org.mapdb.issues;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import org.mapdb.DB;
import org.mapdb.DBMaker;

public class Issue664Test {

    public static void main(String[] args) {
        for(int i =0;i<100;i++) {
            testing();
        }
    }

    private static void testing() {
        DBMaker m = DBMaker.newTempFileDB().deleteFilesAfterClose();
        m = m.transactionDisable();
        m = m.compressionEnable();
        m = m.cacheDisable();
        m = m.asyncWriteEnable();
        m = m.closeOnJvmShutdown();
        DB db = m.make();
        Map<Integer,HashMap> tmp = db.createTreeMap("test")
                .counterEnable()
                .makeOrGet();

        IntStream.rangeClosed(0, 49).parallel().forEach(i -> {
            System.out.println(i+" -> "+tmp.put(i, new HashMap<>()));
        });

        int n =tmp.size();
        System.out.println(n);
        if(n!=50) {
            throw new RuntimeException("The numbers don't match");
        }


        db.close();
    }
}

*/