package doc;

import org.junit.Test;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.util.Map;


public class htreemap_segmented {

    @Test
    public void run() {
        //a
        Map<String, byte[]> map = DBMaker
               .heapSegmentedHashMap(4)
               .keySerializer(Serializer.STRING)
               .valueSerializer(Serializer.BYTE_ARRAY)
               .create();
        //z
    }
}
