package examples;

import org.mapdb.*;

import java.io.File;
import java.util.Map;

/**
 * This demonstrates using Data Pump to first create store in-memory at maximal speed,
 * and than copy the store into memory
 */
public class Pump_InMemory_Import_Than_Save_To_Disk {

    public static void main(String[] args) {

        //create inMemory store which does not use serialization,
        //and has speed comparable to `java.util` collections
        DB inMemory = new DB(new StoreHeap());
        Map m = inMemory.getTreeMap("test");

        //insert random stuff, keep on mind it needs to fit into memory
        for(int i=0;i<10000;i++){
            m.put(Utils.RANDOM.nextInt(),"dwqas"+i);
        }

        //now create on-disk store, it needs to be completely empty
        File targetFile = Utils.tempDbFile();
        DB target = DBMaker.newFileDB(targetFile).make();

        Pump.copy(inMemory, target);

        inMemory.close();
        target.close();

    }
}
