package doc;

import org.junit.Test;
import org.mapdb.Serializer;
import org.mapdb.SortedTableMap;
import org.mapdb.Volume;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class sortedtablemap_params {

    @Test
    public void run() throws IOException {
        File file0 = File.createTempFile("mapdb","mapdb");
        file0.delete();
        String file = file0.getPath();
        //a
        //create memory mapped volume
        Volume volume = Volume.MappedFileVol.FACTORY.makeVolume(file, false);

        //open consumer which will feed map with content
        SortedTableMap.Consumer<Integer,String> consumer =
                SortedTableMap.create(
                        volume,
                        Serializer.INTEGER, // key serializer
                        Serializer.STRING   // value serializer
                )
                        .pageSize(64*1024) // set Page Size to 64KB
                        .nodeSize(8)       // set Node Size to 8 entries
                        .consumer();

        //feed content into consumer
        for(int key=0; key<100000; key++){
            consumer.take(key, "value"+key);
        }

        // finally open created map
        SortedTableMap<Integer, String> map = consumer.finish();
        volume.close();

        // Existing SortedTableMap can be reopened.
        // In that case only Serializers needs to be set,
        // other params are stored in file
        volume = Volume.MappedFileVol.FACTORY.makeVolume(file, true);
                                                             // read-only=true
        map = SortedTableMap.open(volume, Serializer.INTEGER, Serializer.STRING);
        //z

        assertEquals(100000, map.size());
        for(int key=0; key<100000; key++){
            assertEquals("value"+key, map.get(key));
        }
        assertEquals(64*1024, map.getPageSize());

        file0.delete();
    }

}
