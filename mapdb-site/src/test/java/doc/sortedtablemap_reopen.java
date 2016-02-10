package doc;

import org.junit.Test;
import org.mapdb.Serializer;
import org.mapdb.SortedTableMap;
import org.mapdb.Volume;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class sortedtablemap_reopen {

    @Test
    public void run() throws IOException {
        //
        // Fill Map with data
        //
        File file0 = File.createTempFile("mapdb","mapdb");
        file0.delete();
        String file = file0.getPath();
        {
            //create memory mapped file
            Volume volume = Volume.MappedFileVol.FACTORY.makeVolume(file, false);

            //open consumer which will feed map with content
            SortedTableMap.Consumer<Integer, String> consumer =
                    SortedTableMap.create(
                            volume,
                            Serializer.INTEGER,
                            Serializer.STRING
                    ).consumer();

            //feed content into consumer
            for (int key = 0; key < 100000; key++) {
                consumer.take(key, "value" + key);
            }

            // finally open created map
            consumer.finish();
            volume.close();
        }

        //
        // Now reopen Map
        //

        //a
        //open existing  memory-mapped file in read-only mode
        Volume volume = Volume.MappedFileVol.FACTORY.makeVolume(file, true);
                                                                 //read-only=true

        SortedTableMap<Integer,String> map =
                SortedTableMap.open(
                        volume,
                        Serializer.INTEGER,
                        Serializer.STRING
                        );
        //z

        assertEquals(100000, map.size());
        for(int key=0; key<100000; key++){
            assertEquals("value"+key, map.get(key));
        }

        file0.delete();
    }

}
