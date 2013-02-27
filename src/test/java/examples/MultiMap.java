package examples;

import org.mapdb.*;

import java.util.NavigableSet;

/**
 * Shows howto implement MultiMap (Map with more then one values for a singe key) correctly.
 * To do 1:N mapping most people would use Map[String, List[Long]], however MapDB
 * requires nodes to be immutable, so this is wrong.
 */
public class MultiMap {

    public static void main(String[] args) {
        DB db = DBMaker.newMemoryDB().make();

        // this is wrong, do not do it !!!
        //  Map<String,List<Long>> map

        //correct way is to use composite set, where 'map key' is primary key and 'map value' is secondary value
        NavigableSet<Fun.Tuple2<String,Long>> multiMap = db.getTreeSet("test");

        multiMap.add(Fun.t2("aa",1L));
        multiMap.add(Fun.t2("aa",2L));
        multiMap.add(Fun.t2("aa",3L));
        multiMap.add(Fun.t2("bb",1L));

        //find all values for a key
        for(Long l: Bind.findSecondaryKeys(multiMap, "aa")){
            System.out.println("value for key 'aa': "+l);
        }

        //check if pair exists

        boolean found = multiMap.contains(Fun.t2("bb",1L));
        System.out.println("Found: " + found);

        db.close();

    }
}
