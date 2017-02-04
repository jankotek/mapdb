package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.Serializer;
import org.mapdb.SortedTableMap;
import org.mapdb.TT;
import org.mapdb.volume.MappedFileVol;
import org.mapdb.volume.Volume;

import java.util.stream.Collectors;

public class Issue800 {

    @Test
    public void testNPE(){
        String file = TT.tempFile().getPath();
        Volume vol = MappedFileVol.FACTORY.makeVolume(file, false);
        SortedTableMap.Sink<byte[], byte[]> sink =
                SortedTableMap.create(
                    vol,
                    Serializer.BYTE_ARRAY,
                    Serializer.BYTE_ARRAY)
                .createFromSink();

        for(int b = 0; b<100; b++) {
            sink.put(new byte[]{1, (byte) b}, new byte[]{1, (byte) b});
        }
        sink.create().close();

        SortedTableMap<byte[], byte[]> stm = SortedTableMap.open(
                MappedFileVol.FACTORY.makeVolume(file, true),
                Serializer.BYTE_ARRAY,
                Serializer.BYTE_ARRAY);

        // producing a subMap that is 'after' all keys will cause trouble:

        stm.subMap(new byte[] { 3,3 }, new byte[] { 4 })
                .entrySet()
                .stream()
                .map(entry -> entry.getKey().length + entry.getValue().length)
                .collect(Collectors.toList());

        // it works fine if the subMap is 'before' all the keys, or covers some of them.
    }
}
