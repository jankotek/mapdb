package doc;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;


public class htreemap_value_creator {

    @Test
    public void run(){
        DB db = DBMaker.memoryDB().make();
        //a
        HTreeMap<String,Long> map = db.hashMap("map", Serializer.STRING, Serializer.LONG)
                .valueCreator(s -> 111L)
                .create();
        //z
    }
}
