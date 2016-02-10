package doc;

import org.junit.Test;
import org.mapdb.Serializer;
import org.mapdb.SortedTableMap;
import org.mapdb.Volume;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class sortedtablemap_volume {

    @Test
    public void run() throws IOException {
        //a
        //create in-memory volume over byte[]
        Volume byteArrayVolume = Volume.ByteArrayVol.FACTORY.makeVolume(null, false);

        //create in-memory volume in direct memory using DirectByteByffer
        Volume offHeapVolume = Volume.MemoryVol.FACTORY.makeVolume(null, false);

        File file = File.createTempFile("mapdb","mapdb");
        //create memory mapped file volume
        Volume mmapVolume = Volume.MappedFileVol.FACTORY.makeVolume(file.getPath(), false);

        //or if data were already imported, create it read-only
        mmapVolume.close();
        mmapVolume = Volume.MappedFileVol.FACTORY.makeVolume(file.getPath(), true);
                                                                          //read-only=true
        //z
    }

}
