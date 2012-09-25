package net.kotek.jdbm.examples;

import net.kotek.jdbm.DB;
import net.kotek.jdbm.DBMaker;

import java.io.File;
import java.util.Map;

/**
 * @author Jan Kotek
 */
public class E03_HugeRead {

    public static void main(String[] args){
    DB db = DBMaker.newFileDB(new File("/tmp/db2"))
            .transactionDisable() //transactions are not implemented yet
                    //.asyncWriteDisable()
            .make();

    Map<Integer, String> map = db.getTreeMap("map");

    long time = System.currentTimeMillis();
    long max = (int) 1e8;
    long step = max/100;
    for(int i=0;i<max;i++){
        map.get(i);
        if(i%step == 0){
            System.out.println(100.0 * i/max);
        }

    }

    System.out.println("Closing");
    db.close();

    System.out.println(System.currentTimeMillis() - time);
    }
}
