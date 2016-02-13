//package org.mapdb;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.stream.IntStream;
//import org.mapdb.DB;
//import org.mapdb.DBMaker;
//
//public class Issue664Test {
//
//    public static void main(String[] args) {
//        for(int i =0;i<1000;i++) {
//            testing();
//        }
//    }
//
//    private static void testing() {
//        DBMaker m = DBMaker.newTempFileDB().deleteFilesAfterClose();
//        m = m.transactionDisable();
//        m = m.compressionEnable();
//        m = m.cacheDisable();
//        m = m.asyncWriteEnable();
//        m = m.closeOnJvmShutdown();
//        DB db = m.make();
//        Map<Integer,HashMap> tmp = db.createTreeMap("test")
//                        .counterEnable()
//                        .makeOrGet();
//
//        IntStream.rangeClosed(0, 499).parallel().forEach(i -> {
//            Object old = tmp.put(i, new HashMap<>());
//            //System.out.println("Old "+old);
//        });
//
//        int n =tmp.size();
//        //System.out.println(n);
//        if(n!=500) {
//            throw new RuntimeException("The numbers don't match");
//        }
//
//
//        db.close();
//    }
//}