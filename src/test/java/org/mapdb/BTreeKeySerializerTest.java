package org.mapdb;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BTreeKeySerializerTest {

    @Test public void testLong(){
        DB db = DBMaker.newMemoryDB()
                .cacheDisable()
                .make();
        Map m = db.createTreeMap("test",32,false,false,
                BTreeKeySerializer.ZERO_OR_POSITIVE_LONG,
                null, null );

        for(long i = 0; i<1000;i++){
            m.put(i*i,i*i+1);
        }

        for(long i = 0; i<1000;i++){
            assertEquals(i * i + 1, m.get(i * i));
        }
    }


    @Test public void testString(){


        DB db = DBMaker.newMemoryDB()
                .cacheDisable()
                .make();
        Map m = db.createTreeMap("test",32,false, false,
                BTreeKeySerializer.STRING,
                null, null );

        List<String> list = new ArrayList <String>();
        for(long i = 0; i<1000;i++){
            String s = ""+ Math.random()+(i*i*i);
            m.put(s,s+"aa");
        }

        for(String s:list){
            assertEquals(s+"aa",m.get(s));
        }
    }

}
