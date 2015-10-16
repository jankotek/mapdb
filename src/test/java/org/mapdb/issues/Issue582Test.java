package org.mapdb.issues;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.Pump;

public class Issue582Test {

    @Test
    public void test(){
        // make the features

        List<Fun.Pair<String, Integer>> features = new ArrayList<Fun.Pair<String, Integer>>();
        for (int i = 0 ; i < 6061 ; i++) {
            features.add(new Fun.Pair<String, Integer>("job_geomerror." + i, (Integer) i));
        }

        DB db = DBMaker.newTempFileDB().make();

        Iterator<Fun.Pair<String,Integer>> iter = Pump.sort(features.iterator(),
                true, 100000,
                Collections.reverseOrder(new Comparator<Fun.Pair<String,Integer>>() {
                    @Override
                    public int compare(Fun.Pair<String,Integer> o1, Fun.Pair<String,Integer> o2) {
                        return o1.compareTo(o2);
                    }
                }),
                db.getDefaultSerializer(),
                null
                );

        db.createTreeMap("test")
        .pumpSource(iter)
        // removing this line causes everything to work fine
        .keySerializer(BTreeKeySerializer.STRING)
        .make();

    }
}
