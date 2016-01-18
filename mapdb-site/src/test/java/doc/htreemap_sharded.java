package doc;

import org.junit.Test;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;


public class htreemap_sharded {

    @Test
    public void run() {
        //a
        HTreeMap<String, byte[]> map = DBMaker
                //param is number of Stores (concurrency factor)
               .memoryShardedHashMap(8)
               .keySerializer(Serializer.STRING)
               .valueSerializer(Serializer.BYTE_ARRAY)
               .create();

        //DB does not exist, so close map directly
        map.close();
        //z
    }
}
