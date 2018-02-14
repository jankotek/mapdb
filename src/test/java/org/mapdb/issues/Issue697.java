package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.CC;
import org.mapdb.serializer.Serializers;
import org.mapdb.flat.SortedTableMap;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by jan on 5/2/16.
 */
public class Issue697 {
    @Test
    public void test(){

        SortedTableMap.Sink<Integer, String> sink = SortedTableMap.create(
                CC.DEFAULT_MEMORY_VOLUME_FACTORY.makeVolume(null, false),
                Serializers.INTEGER,
                Serializers.STRING)
                .createFromSink();

        for (int i = 0; i < 10; i++)
        {
            sink.put(i, "value" + i);
        }

        Map m = sink.create();
        for (int i = 0; i < 10; i++)
        {
            assertEquals("value" + i, m.get(i));
        }


    }
}
