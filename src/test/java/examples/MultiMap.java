package examples;

import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;

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

        // Correct way is to use composite set, where 'map key' is primary key and 'map value' is secondary value
        // Composite keys are done with arrays.
        NavigableSet<Object[]> multiMap = db.treeSetCreate("test2")
                .serializer(BTreeKeySerializer.ARRAY2)
                .make();

        //TODO there is Pair class, update example to include it

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

        db.commit();
        db.close();

    }
}
