package org.mapdb.issues;


import org.junit.Test;
import org.mapdb.Serializer;
import org.mapdb.SortedTableMap;
import org.mapdb.TestWithTempDir;
import org.mapdb.volume.MappedFileVol;
import org.mapdb.volume.Volume;

import java.util.Map;

public class Issue_815 extends TestWithTempDir{

    @Test
    public void sliceSize_reopen(){
        String file = tempFile().getPath();
        Volume volume = MappedFileVol.FACTORY.makeVolume(file, false,0L,22,0,false);
        SortedTableMap.Sink<Integer, String> sink = SortedTableMap
                .create(volume, Serializer.INTEGER, // key serializer
                        Serializer.STRING) // value serializer
                .pageSize(4 * 1024 * 1024) // set Page Size to 4MB
                .nodeSize(8) // set Node Size to 8 entries
                .createFromSink();

        for(int i=0;i<1e6;i++){
            sink.put(i, ""+i);
        }

        Map m = sink.create();


    }


}
