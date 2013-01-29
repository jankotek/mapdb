package examples;

import org.mapdb.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Shows how to create secondary map
 * which is synchronized with primary map
 */
public class Secondary_Map {

    public static void main(String[] args) {
        HTreeMap<Long, String> primary = DBMaker.newMemoryDB().make().getHashMap("test");

        // secondary map will hold String.size() from primary map as its value
        Map<Long,Integer> secondary = new HashMap(); //can be normal java map, or MapDB map


        //Bind maps together. It is one way binding, so changes in primary are reflected in secondary
        Bind.secondaryValue(primary, secondary, new Fun.Function2<Integer, Long, String>() {
            @Override public Integer run(Long key, String value) {
                return value.length();
            }
        });


        primary.put(111L, "just some chars");
        int strSize = secondary.get(111L);
        System.out.println(strSize);
    }
}
