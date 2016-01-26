package org.mapdb.benchmark;

import org.mapdb20.*;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Fills map until it runs out of memory
 */
public class InMemorySpaceUsage {

    static final int memUsage = 1;

    static final Map<String, Callable<Map<Long,String>>> fabs = new LinkedHashMap();
    static{
        fabs.put("ConcurrentHashMap", () -> new ConcurrentHashMap<Long, String>());

        fabs.put("ConcurrentSkipListMap", () -> new ConcurrentSkipListMap<Long, String>());

        fabs.put("HTreeMap_heap", () -> DBMaker.heapDB().transactionDisable().make()
                .hashMap("map", Serializer.LONG, Serializer.STRING));

        fabs.put("BTreeMap_heap", () -> DBMaker.heapDB().transactionDisable().make()
                .treeMap("map", Serializer.LONG, Serializer.STRING));

        fabs.put("HTreeMap_offheap", () -> DBMaker.memoryDB().transactionDisable().make()
                .hashMap("map", Serializer.LONG, Serializer.STRING));
//
//        fabs.put("BTreeMap_offheap", new Callable<Map<Long, String>>() {
//            @Override public Map<Long, String> call() throws Exception {
//                return DBMaker.memoryDB().asyncWriteEnable()
//                        .asyncWriteQueueSize(100)
//                        .transactionDisable().make()
//                        .treeMap("map", Serializer.LONG, Serializer.STRING);
//            }
//        });

        fabs.put("BTreeMap_offheap", () -> {
            Iterator<Fun.Pair<Long,String>> iter = new ReverseIter();

            return DBMaker.memoryDB()
                    .transactionDisable().make()
                    .treeMapCreate("map")
                    .keySerializer(Serializer.LONG)
                    .valueSerializer(Serializer.STRING)
                    .pumpSource(iter)
                    .make();
        });
//
//        fabs.put("BTreeMap-archive", new Callable<Map<Long, String>>() {
//            @Override public Map<Long, String> call() throws Exception {
//
//                Iterator iter = new ReverseIter();
//
//                Pump.archiveTreeMap(iter, null, Volume.ByteArrayVol.FACTORY,
//                        new DB.BTreeMapMaker("map")
//                                .keySerializer(Serializer.LONG)
//                                .valueSerializer(Serializer.STRING)
//                );
//
//                return new HashMap();
//            }
//        });
//

    }

    public static void main(String[] args) throws Throwable {
        String name = args[0];
        Throwable e;
        long counter = 0;

        Map map = fabs.get(name).call();
        for (; ; ) {
            map.put(counter++, "");
            if(counter%10000==0)
                System.out.println(counter);
        }

    }

    static class ReverseIter implements Iterator<Fun.Pair<Long,String>> {
            final long start = (long) 1e9;

            long counter = start;


            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Fun.Pair<Long, String> next() {
                long val = counter--;
                if(val%10000==0)
                    System.out.println(start-val);
                return new Fun.Pair(val,"");
            }

            @Override
            public void remove() {

            }
    }
}
