package examples;

import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.util.Map;

/**
 * All collections are loaded lazily by default.
 * MapDB never loads full collection into heap memory,
 * only small currently used portion (typically single tree node) is loaded into heap.
 * But even single tree node can be too large (for example 32 key-value pairs),
 * and you may want to load even single values lazily.
 *
 */
public class Lazily_Loaded_Records {

    public static void main(String[] args) {

        DB db = DBMaker.newMemoryDB().make();
        //
        // TreeMap has build in support for lazily loaded values.
        // In that case each value are not stored inside node,
        // but in separate record.
        //
        // use DB.createTreeMap to create TreeMap with non-default parameters


        Map map = db.createTreeMap("name").valuesOutsideNodesEnable().make();
        map.put("key","this string is loaded lazily with 'map.get(key)' ");


        //
        // Other option for lazily loaded record is to use Atomic.Var.
        // In this case you have singleton record with name.
        // As bonus you can update reference in thread-safe atomic manner.
        //
        Atomic.Var<String> record =
                db.createAtomicVar("lazyRecord", "aaa", db.getDefaultSerializer());

        record.set("some value");
        System.out.println(record.get());


        // Last option is to use low level Engine storage directly.
        // Each stored record gets assigned unique recid (record id),
        // which is latter used to get or update record.
        // Your code should store only recid as reference to object.
        // All MapDB collections are written this way.

        //insert new record
        long recid = db.getEngine().put("something", Serializer.STRING_NOSIZE);

        //load record
        String lazyString = db.getEngine().get(recid, Serializer.STRING_NOSIZE);

        //update record
        db.getEngine().update(recid, "new value", Serializer.STRING_NOSIZE);


        //I hope this example helped!
        db.close();

    }
}
