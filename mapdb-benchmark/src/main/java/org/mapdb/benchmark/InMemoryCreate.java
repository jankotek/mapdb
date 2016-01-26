package org.mapdb.benchmark;


import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Tests how long it takes to insert 100M items
 */
public class InMemoryCreate {

    static final int memUsage = 20;
    static final int max = (int) 100e6;

    static final Map<String, Callable<Map<Long,UUID>>> fabs = new LinkedHashMap(max);
    static{
        fabs.put("ConcurrentHashMap", () -> new ConcurrentHashMap<Long, UUID>());

        fabs.put("ConcurrentSkipListMap", () -> new ConcurrentSkipListMap<Long, UUID>());

        fabs.put("HTreeMap_heap", () -> org.mapdb20.DBMaker.heapDB().transactionDisable().make()
                .hashMap("map", org.mapdb20.Serializer.LONG, org.mapdb20.Serializer.UUID));

        fabs.put("BTreeMap_heap", () -> org.mapdb20.DBMaker.heapDB().transactionDisable().make()
                .treeMap("map", org.mapdb20.Serializer.LONG, org.mapdb20.Serializer.UUID));

        fabs.put("HTreeMap_offheap", () -> org.mapdb20.DBMaker.memoryDB().transactionDisable().asyncWriteEnable().make()
                .hashMap("map", org.mapdb20.Serializer.LONG, org.mapdb20.Serializer.UUID));

        fabs.put("BTreeMap_offheap", () -> org.mapdb20.DBMaker.memoryDB().asyncWriteEnable()
                .transactionDisable().make()
                .treeMap("map", org.mapdb20.Serializer.LONG, org.mapdb20.Serializer.UUID));
    }

    public static void main(String[] args) throws Throwable {
        long time = System.currentTimeMillis();
        String name = args[0];
        Map map = fabs.get(name).call();
        for (long i=0;i<max;i++) {
            UUID val = new UUID(org.mapdb20.DataIO.longHash(i),org.mapdb20.DataIO.longHash(i+1)); //Random is too slow, so use faster hash
            map.put(i, val);
        }
        System.out.println(System.currentTimeMillis()-time);
    }

}
