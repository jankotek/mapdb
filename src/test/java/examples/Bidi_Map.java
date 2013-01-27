package examples;

import org.mapdb.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple way to create  bidirectional map (can find key for given value) using Binding.
 */
public class Bidi_Map {

    public static void main(String[] args) {
        HTreeMap<Long,String> map = DBMaker.newTempHashMap();

        Map<String, Long> inverseMapping = new HashMap<String, Long>(); // can be any map

        Bind.secondaryKey(map,inverseMapping, new Fun.Function2<String, Long, String>() {
            @Override public String run(Long key, String value) {
                return value;
            }
        });


        map.put(1111L,"value");
        Long keyFromValue = inverseMapping.get("value");
        System.out.println(keyFromValue);
    }
}
