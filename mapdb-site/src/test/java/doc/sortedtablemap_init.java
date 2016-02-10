package doc;

import org.junit.Test;
import org.mapdb.*;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class sortedtablemap_init {

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
                        Serializer.INTEGER,
                        Serializer.STRING
                ).consumer();

        //feed content into consumer
        for(int key=0; key<100000; key++){
            consumer.take(key, "value"+key);
        }

        // finally open created map
        SortedTableMap<Integer, String> map = consumer.finish();
        //z

        assertEquals(100000, map.size());
        for(int key=0; key<100000; key++){
            assertEquals("value"+key, map.get(key));
        }

        file0.delete();
    }

}
