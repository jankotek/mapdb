package net.kotek.jdbm.examples;

import net.kotek.jdbm.*;


public class E01_BasicExample {

    public static void main(String[] args){

        DB db = DBMaker.newFileDB("filename")
                    .transactionDisable() //transactions are not implemented yet
                    .make();

        ConcurrentSortedMap<Integer, String> map = db.getTreeMap("treeMap");
        map.put(1,"some string");
        map.put(2,"some other string");

        db.close(); //make sure db is correctly closed!!

    }
}
