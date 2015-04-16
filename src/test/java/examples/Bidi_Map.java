package examples;

import org.mapdb.Bind;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.HTreeMap;

import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * Simple way to create  bidirectional map (can find key for given value) using Binding.
 */
public class Bidi_Map {

    public static void main(String[] args) {
        //primary map
        HTreeMap<Long,String> map = DBMaker.tempHashMap();

        // inverse mapping for primary map
        NavigableSet<Object[]> inverseMapping = new TreeSet<Object[]>(Fun.COMPARABLE_ARRAY_COMPARATOR);
        //NOTE: you may also use Set provided by MapDB to make it persistent

        // bind inverse mapping to primary map, so it is auto-updated
        Bind.mapInverse(map, inverseMapping);


        map.put(10L,"value2");
        map.put(1111L,"value");
        map.put(1112L,"value");
        map.put(11L,"val");

        //now find all keys for given value
        for(Object[] key: Fun.filter(inverseMapping, "value")){
            System.out.println("Key for 'value' is: "+key[1]);
        }

    }
}
