package examples;

import org.mapdb.BTreeMap;
import org.mapdb.DBMaker;

import java.util.Map;

/**
 * Opens maps backed by file in temporary folder.
 * Quick and simple way to get Maps which can handle billions of items.
 * All files are deleted after Map is closed or JVM exits (using shutdown hook).
 */
public class _TempMap {
    public static void main(String[] args) {

        // open new empty map
        // DBMaker will create files in temporary folder and opens it
        Map<String, String> map = DBMaker.newTempTreeMap();

        //put some stuff into map
        //all data are stored in file in temp folder
        map.put("aa", "bb");
        map.put("cc", "dd");

        // Close map to release resources.
        // It is optional, there is JVM shutdown hook which deletes files on JVM exit.
        ((BTreeMap)map).close();

        // After JVM exits files are deleted.
        // This map was temporary, there is no way to recover its data !
    }
}
