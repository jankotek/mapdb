package examples;

import org.mapdb.*;

import java.util.Map;

/**
 * Keep tracks of number of items in map.
 * <p/>
 * {@code Collections.size()} typically requires traversing entire collection in MapDB, but there is optional parameter
 * which controls if Map keeps track of its count.
 */
public class Map_Size_Counter {

    public static void main(String[] args) {

        //first option, create Map with counter (NOTE: counter is not on by default)
        DB db1 = DBMaker.newTempFileDB().make();
        //hashMap
        Map m = db1.createHashMap("map1a",true /**<<here is keepCounter argument*/,null, null);
        //treeMap
        m = db1.createTreeMap("map1b",32,false,true /**<<here is keepCounter argument*/,null, null,null);

        m.put("a","b");
        m.size();



        //second option, create external Atomic.Long and bind it to map */
        DB db2 = DBMaker.newTempFileDB().make();

        BTreeMap primary = db2.getTreeMap("map2");
        Atomic.Long sizeCounter = Atomic.getLong(db2, "mapSize");

        Bind.size(primary, sizeCounter);

        primary.put("111", "some value");

        sizeCounter.get();


    }

}