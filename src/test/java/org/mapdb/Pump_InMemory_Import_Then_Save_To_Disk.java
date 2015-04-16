package org.mapdb;

import java.io.File;
import java.util.Map;
import java.util.Random;

/*
 * This demonstrates using Data Pump to first create store in-memory at maximal speed,
 * and than copy the store into memory
 */
//TODO Pump between stores is disabled for now, copy this back to examples  once enabled
public class Pump_InMemory_Import_Then_Save_To_Disk {

    public static void main(String[] args) {
//        if(1==1) return;
//
//        //create inMemory store which does not use serialization,
//        //and has speed comparable to `java.util` collections
//        DB inMemory = new DB(new StoreHeap(transactionsDisabled));
//        Map m = inMemory.getTreeMap("test");
//
//        Random r = new Random();
//        //insert random stuff, keep on mind it needs to fit into memory
//        for(int i=0;i<10000;i++){
//            m.put(r.nextInt(),"dwqas"+i);
//        }
//
//        //now create on-disk store, it needs to be completely empty
//        File targetFile = UtilsTest.tempDbFile();
//        DB target = DBMaker.fileDB(targetFile).make();
//
//        Pump.copy(inMemory, target);
//
//        inMemory.close();
//        target.close();

    }
}
