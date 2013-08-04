package examples;

import org.mapdb.*;

import java.io.File;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Demonstrate how-to create large BTreeMap using data pump.
 * Typical usage is to import data set from external source.
 *
 * @author Jan Kotek
 */
public class Huge_Insert {

    public static void main(String[] args){

        /** max number of elements to import */
        final long max = (int) 1e6;

        /**
         * Open database in temporary directory
         */
        File dbFile = Utils.tempDbFile();
        DB db = DBMaker
                .newFileDB(dbFile)
                /** disabling Write Ahead Log makes import much faster */
                .transactionDisable()
                .make();


        long time = System.currentTimeMillis();

        /**
         * Source of data which randomly generates strings.
         * In real world this would return data from file.
         */
        Iterator<String> source = new Iterator<String>() {

            long counter = 0;

            @Override public boolean hasNext() {
                return counter<max;
            }

            @Override
            public String next() {
                counter++;
                return Utils.randomString(10);
            }

            @Override public void remove() {}
        };

        /**
         * BTreeMap Data Pump requires data source to be presorted in reverse order (highest to lowest).
         * There is method in Data Pump we can use to sort data. It uses temporarly files and can handle fairly
         * large data sets.
         */
        source = Pump.sort(source,
                100000,
                Collections.reverseOrder(Utils.COMPARABLE_COMPARATOR), //reverse  order comparator
                db.getDefaultSerializer()
                );



        /**
         * Disk space used by serialized keys should be minimised.
         * Keys are sorted, so only difference between consequential keys is stored.
         * This method is called delta-packing and typically saves 60% of disk space.
         */
        BTreeKeySerializer<String> keySerializer = BTreeKeySerializer.STRING;


        /**
         * Translates Map Key into Map Value.
         */
        Fun.Function1<Integer,String> valueExtractor = new Fun.Function1<Integer, String>() {
            @Override public Integer run(String s) {
                return s.hashCode();
            }
        };

        /**
         * Create BTreeMap
         */
        Pump.buildTreeMap(source,db,"map",valueExtractor,32, false,false,keySerializer, null,null);



        /** after map has been created it can be retrieve as usual*/
        Map<String,Integer> map = db.getTreeMap("map");

        System.out.println("Finished; total time: "+(System.currentTimeMillis()-time)/1000+"s; there are "+map.size()+" items in map");
        db.close();


    }
}
