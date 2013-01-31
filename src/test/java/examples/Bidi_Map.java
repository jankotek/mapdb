package examples;

import org.mapdb.*;

import javax.management.monitor.StringMonitor;
import javax.swing.plaf.basic.BasicInternalFrameTitlePane;
import java.util.*;

/**
 * Simple way to create  bidirectional map (can find key for given value) using Binding.
 */
public class Bidi_Map {

    public static void main(String[] args) {
        //primary map
        HTreeMap<Long,String> map = DBMaker.newTempHashMap();

        // inverse mapping for primary map
        NavigableSet<Fun.Tuple2<String, Long>> inverseMapping = new TreeSet<Fun.Tuple2<String, Long>>();

        // bind inverse mapping to primary map, so it is auto-updated
        Bind.secondaryKey(map,inverseMapping, new Fun.Function2<String, Long, String>() {
            @Override public String run(Long key, String value) {
                return value;
            }
        });


        map.put(10L,"value2");
        map.put(1111L,"value");
        map.put(1112L,"value");
        map.put(11L,"val");

        //use range query on inverse set, to find all keys associated with given value
        Set<Fun.Tuple2> keys = ((NavigableSet)inverseMapping)   //cast is workaround for broken Java generics
                .subSet(
                Fun.t2("value",null), //NULL represents lower bound, everything is larger than null
                Fun.t2("value",Fun.HI) // HI is upper bound everything is smaller then HI
        );

        for(Fun.Tuple2 t : keys){
            System.out.println("Key for 'value' is: "+t.b);
        }

    }
}
