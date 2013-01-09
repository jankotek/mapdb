package org.mapdb;


import java.io.*;
import java.util.concurrent.*;

import org.junit.Test;

public class Issue37Test {


    @Test
    public void test2() throws IOException{

        /**
         * Tijdelijke on-disk database gebruiken.
         */
        DB db = DBMaker.newTempFileDB().cacheDisable().make();
        ConcurrentMap<String,String> testData = db.createHashMap("test",
                null, null);

        BufferedReader in = new BufferedReader(new FileReader("./src/test/resources/Issue37Data.txt"));
        String line = null;

        while ((line = in.readLine()) != null){
            testData.put(line,line);
        }

        db.commit();

        int printCount = 0;

        for(String key : testData.keySet()){
            printCount++;

            String data = testData.get(key);
            if(printCount%1000==0){
                System.out.println("Printed: " + printCount + " items from verloopData with size: " + testData.size());
            }

            if(printCount > 10000){
                break;
            }
        }
    }

}
