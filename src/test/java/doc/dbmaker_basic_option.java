package doc;

import org.mapdb20.DB;
import org.mapdb20.DBMaker;

import java.io.File;


public class dbmaker_basic_option {

    public static void main(String[] args) {
        //a
        DB db = DBMaker
                .appendFileDB(new File("/some/file"))
                .encryptionEnable("password")
                .make();
        //z
    }
}
