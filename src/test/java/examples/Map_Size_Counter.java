package examples;

import org.mapdb.*;

import java.util.Map;

/**
 * Keep tracks of number of items in map.
 * <p/>
 * Note: {@code Collections.size()} typically requires traversing entire collection in MapDB.
 */
public class Map_Size_Counter {

    public static void main(String[] args) {

        DB db = DBMaker.newTempFileDB().make();

        BTreeMap primary = db.getTreeMap("map");
        Atomic.Long sizeCounter = Atomic.getLong(db, "mapSize");

        Bind.size(primary, sizeCounter);

        primary.put("111", "some value");


    }

}