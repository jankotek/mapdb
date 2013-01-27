package examples;

import org.mapdb.Bind;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.HTreeMap;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Shows how to split map into categories and count elements in each category
 *
 * Here we show histogram of an {@code Math.random()}.
 * We represent category as string for clarity, but any number or other type could be used
 */
public class Histogram {

    public static void main(String[] args) {
        HTreeMap<Long, Double> map = DBMaker.newTempHashMap();

        // histogram, category is a key, count is a value
        ConcurrentMap<String, Long> histogram = new ConcurrentHashMap<String, Long>(); //any map will do

        // bind histogram to primary map
        // we need function which returns category for each map entry
        Bind.histogram(map, histogram, new Fun.Function2<String, Long, Double>(){
            @Override
            public String run(Long key, Double value) {
                if(value<0.25) return "first quarter";
                else if(value<0.5) return "second quarter";
                else if(value<0.75) return "third quarter";
                else return "fourth quarter";
            }
        });

        //insert some random stuff
        for(long key=0;key<1e4;key++){
            map.put(key, Math.random());
        }

        System.out.println(histogram);
    }
}
