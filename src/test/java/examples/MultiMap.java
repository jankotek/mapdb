package examples;

import org.mapdb10.BTreeKeySerializer;
import org.mapdb10.DB;
import org.mapdb10.DBMaker;
import org.mapdb10.Fun;

import java.util.NavigableSet;

/**
 * Shows howto implement MultiMap (Map with more then one values for a singe key) correctly.
 * To do 1:N mapping most people would use Map[String, List[Long]], however MapDB
 * requires nodes to be immutable, so this is wrong.
 */
public class MultiMap {

    public static void main(String[] args) {
        DB db = DBMaker.memoryDB().make();

        // this is wrong, do not do it !!!
        //  Map<String,List<Long>> map

        //correct way is to use composite set, where 'map key' is primary key and 'map value' is secondary value
        NavigableSet<Object[]> multiMap = db.treeSet("test");

        //optionally you can use set with Delta Encoding. This may save lot of space
        multiMap = db.treeSetCreate("test2")
                .serializer(BTreeKeySerializer.ARRAY2)
                .make();

        multiMap.add(new Object[]{"aa",1});
        multiMap.add(new Object[]{"aa",2});
        multiMap.add(new Object[]{"aa",3});
        multiMap.add(new Object[]{"bb",1});

        //find all values for a key
        for(Object[] l: Fun.filter(multiMap, "aa")){
            System.out.println("value for key 'aa': "+l[1]);
        }

        //check if pair exists

        boolean found = multiMap.contains(new Object[]{"bb",1});
        System.out.println("Found: " + found);

        db.close();

    }
}
