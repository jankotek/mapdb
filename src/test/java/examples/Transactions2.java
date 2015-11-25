package examples;

import org.mapdb.*;

import java.util.Map;

/**
 * Demonstrates easier way to execute concurrent transactions.
 */
public class Transactions2 {

    public static void main(String[] args) {
        TxMaker txMaker = DBMaker.memoryDB().makeTxMaker();

        // Execute transaction within single block.
        txMaker.execute(new TxBlock(){
            @Override public void tx(DB db) throws TxRollbackException {
                Map m = db.hashMap("test");
                m.put("test","test");
            }
        });

        //show result of block execution
        DB tx1 = txMaker.makeTx();
        Object val = tx1.hashMap("test").get("test");
        System.out.println(val);

        tx1.close();
        txMaker.close();
    }

}
