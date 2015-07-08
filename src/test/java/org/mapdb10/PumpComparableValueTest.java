package org.mapdb10;


import java.util.Iterator;

import org.junit.Test;
import org.mapdb10.Fun.Tuple2;

import static org.junit.Assert.assertEquals;


public class PumpComparableValueTest {

        
        /**
         * Test mapDB data pump mechanize 
         * 
         */
        @Test
        public void run(){
                DBMaker dbMaker = DBMaker.newMemoryDB()
                                .transactionDisable();

                DB mapDBStore = dbMaker.make();

                
                
                final int max = 70000;
                
                final int pumpSize = max/10;
                
                // data source returning the same value max times values are NOT comparable
                Iterator<Tuple2<String, byte[]>> entriesSourceNonComp = new Iterator<Tuple2<String,byte[]>>() { 
                        int count = 0;
                        @Override
                        public void remove() {throw new IllegalArgumentException("NOT SUPPORTED");}
                        
                        @Override
                        public Tuple2<String, byte[]> next() {
                                count++;
                                
                                String key ="SAME KEY";
                                byte []value = {1};
                                
                                Tuple2 <String,byte[]>ret = new Tuple2<String,byte[]>(key,value);
                                return ret;
                        }
                        
                        @Override
                        public boolean hasNext() {
                                return count<max;
                        }
                }; 
                


                BTreeMap<String,String> map2 = mapDBStore.createTreeMap("non comparable values")
                                .pumpSource(entriesSourceNonComp)
                                .pumpPresort(pumpSize)
                                .pumpIgnoreDuplicates()
                                .counterEnable()
                                .makeStringMap();

                assertEquals(1,map2.size());

        }

    @Test
    public void run2(){
        DBMaker dbMaker = DBMaker.newMemoryDB()
                .transactionDisable();

        DB mapDBStore = dbMaker.make();



        final int max = 70000;

        final int pumpSize = max/10;

        // data source returning the same value max times values are NOT comparable
        Iterator<Tuple2<String, byte[]>> entriesSourceNonComp = new Iterator<Tuple2<String,byte[]>>() {
            int count = 0;
            @Override
            public void remove() {throw new IllegalArgumentException("NOT SUPPORTED");}

            @Override
            public Tuple2<String, byte[]> next() {
                count++;

                String key = ""+count;
                byte []value = {1};

                Tuple2 <String,byte[]>ret = new Tuple2<String,byte[]>(key,value);
                return ret;
            }

            @Override
            public boolean hasNext() {
                return count<max;
            }
        };



        BTreeMap<String,String> map2 = mapDBStore.createTreeMap("non comparable values")
                .pumpSource(entriesSourceNonComp)
                .pumpPresort(pumpSize)
                .pumpIgnoreDuplicates()
                .counterEnable()
                .makeStringMap();

        assertEquals(max,map2.size());


    }


}