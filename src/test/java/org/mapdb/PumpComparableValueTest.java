package org.mapdb;


import java.util.Iterator;

import org.junit.Test;
import org.mapdb.Fun.Pair;

import static org.junit.Assert.assertEquals;


public class PumpComparableValueTest {


        /*
         * Test mapDB data pump mechanize 
         * 
         */
        @Test
        public void run(){
                DB mapDBStore = DBMaker.memoryDB()
                                .transactionDisable()
                                .make();

                final int max = 70000;
                
                final int pumpSize = max/10;
                
                // data source returning the same value max times values are NOT comparable
                Iterator<Pair<String, byte[]>> entriesSourceNonComp = new Iterator<Pair<String,byte[]>>() {
                        int count = 0;
                        @Override
                        public void remove() {throw new IllegalArgumentException("NOT SUPPORTED");}

                        @Override
                        public Pair<String, byte[]> next() {
                                count++;

                                String key ="SAME KEY";
                                byte []value = {1};

                                Pair<String,byte[]> ret = new Pair<String,byte[]>(key,value);
                                return ret;
                        }

                        @Override
                        public boolean hasNext() {
                                return count<max;
                        }
                };



                BTreeMap<String,String> map2 = mapDBStore.treeMapCreate("non comparable values")
                                .pumpSource(entriesSourceNonComp)
                                .pumpPresort(pumpSize)
                                .pumpIgnoreDuplicates()
                                .counterEnable()
                                .makeStringMap();

                assertEquals(1,map2.size());

        }

    @Test
    public void run2(){
        DB db = DBMaker.memoryDB()
                .transactionDisable().make();


        final int max = 70000;

        final int pumpSize = max/10;

        // data source returning the same value max times values are NOT comparable
        Iterator<Pair<String, byte[]>> entriesSourceNonComp = new Iterator<Pair<String,byte[]>>() {
            int count = 0;
            @Override
            public void remove() {throw new IllegalArgumentException("NOT SUPPORTED");}

            @Override
            public Pair<String, byte[]> next() {
                count++;

                String key = ""+count;
                byte []value = {1};

                Pair<String,byte[]> ret = new Pair<String,byte[]>(key,value);
                return ret;
            }

            @Override
            public boolean hasNext() {
                return count<max;
            }
        };



        BTreeMap<String,String> map2 = db.treeMapCreate("non comparable values")
                .pumpSource(entriesSourceNonComp)
                .pumpPresort(pumpSize)
                .pumpIgnoreDuplicates()
                .counterEnable()
                .makeStringMap();

        assertEquals(max,map2.size());


    }


}