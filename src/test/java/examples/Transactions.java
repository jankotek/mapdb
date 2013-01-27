package examples;

import org.mapdb.*;

import java.util.Map;

/**
 * MapDB provides concurrent transactions with Serialized Snapshot Isolation to manage MVCC.
 * This example shows how to invoke multiple transactions at the same time.
 * It also shows rollback in case of an concurrent update conflict
 */
public class Transactions {
    public static void main(String[] args) {


        //Open Transaction Factory. DBMaker shares most options with single-transaction mode.
        TxMaker txMaker = DBMaker
                .newMemoryDB()
                .makeTxMaker();

        // Now open first transaction and get map from first transaction
        DB tx1 = txMaker.makeTx();

        //create map from first transactions and fill it with data
        Map map1 = tx1.getTreeMap("testMap");
        for(int i=0;i<1e4;i++){
            map1.put(i,"aaa"+i);
        }

        //commit first transaction
        tx1.commit();

        // !! IMPORTANT !!
        // !! DB transaction can be used only once,
        // !! it throws an 'already closed' exception after it was commited/rolledback
        // !! IMPORTANT !!
        //map1.put(1111,"dqdqwd"); // this will fail

        //open second transaction
        DB tx2 = txMaker.makeTx();
        Map map2 = tx2.getTreeMap("testMap");

        //open third transaction
        DB tx3 = txMaker.makeTx();
        Map map3 = tx3.getTreeMap("testMap");

        //put some stuff into second transactions, observer third map size
        System.out.println("map3 size before insert: "+map3.size());
        map2.put(-10, "exists");
        System.out.println("map3 size after insert: "+map3.size());

        //put some stuff into third transactions, observer second map size
        System.out.println("map2 size before insert: "+map2.size());
        map3.put(100000, "exists");
        System.out.println("map2 size after insert: "+map2.size());

        // so far there was no conflict, since modified Map values lie far away from each other in tree.
        // `map2` has new key -10, so inserting -11 into map3 should update the same node and trigger rollback
        try{
            map3.put(-11, "exists"); //will throw TxRollbackException
            System.out.println("Insert -11 into map3 was fine"); //this will be skipped by exception
        }catch(TxRollbackException e){
            System.out.println("Could not insert -11 into map3 thanks to conflict, tx3 was rolled back");
        }

        //tx3 was rolled back and is now closed, lets save tx2
        tx2.commit();

        //create yet another transaction and observe result
        DB tx4 = txMaker.makeTx();
        Map map4 = tx4.getTreeMap("testMap");
        System.out.println("Map size after commits: "+map4.size());
        System.out.println("Value inserted into tx2 and successfully commited: "+map4.get(-10));
        System.out.println("Value inserted into tx3 before rollback: "+map4.get(100000));
        System.out.println("Value inserted into tx3 which triggered rollback: "+map4.get(-11));

        //close transaction without modifying anything
        tx4.close();

        //close the entire database
        txMaker.close();
    }
}
