package doc;

import org.mapdb.DBMaker;
import org.mapdb.TxMaker;


public class dbmaker_txmaker_create {

    public static void main(String[] args) {
        //a
        TxMaker txMaker = DBMaker
                .memoryDB()
                .makeTxMaker();
        //z
    }
}
