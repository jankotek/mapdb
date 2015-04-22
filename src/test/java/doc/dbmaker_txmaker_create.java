package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TxMaker;

import java.util.concurrent.ConcurrentNavigableMap;


public class dbmaker_txmaker_create {

    public static void main(String[] args) {
        //a
        TxMaker txMaker = DBMaker
                .memoryDB()
                .makeTxMaker();
        //z
    }
}
