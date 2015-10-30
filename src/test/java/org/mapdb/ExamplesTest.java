package org.mapdb;


import examples.*;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class ExamplesTest {

    static final String[] args= new String[0];

    @Test public void all_examples_test(){
        File f = new File("src/test/java/examples");
        for(File ff:f.listFiles()){
            String n = ff.getName();
            if(!n.endsWith(".java"))
                continue;
            n = n.substring(0,n.length()-5);
            //check given method exists
            try {
                ExamplesTest.class.getMethod(n);
            } catch (NoSuchMethodException e) {
                throw new AssertionError("Example has no test "+n);
            }
        }


    }


    @Test public void _HelloWorld() throws IOException {
        _HelloWorld.main(args);
    }

    @Test public void _TempMap(){
        _TempMap.main(args);
    }

    @Test public void Cache(){
        if(TT.scale()==0)
            return;
        CacheEntryExpiry.main(args);
    }

    @Test public void CacheOverflow() throws InterruptedException {
        if(TT.scale()==0)
            return;
        CacheOverflow.main(args);
    }

    @Test public void Compression(){
        Compression.main(args);
    }

    @Test public void Huge_Insert() throws IOException {
        if(TT.scale()==0)
            return;

        Huge_Insert.main(args);
    }

    @Test public void Custom_Value() throws IOException {
        Custom_Value.main(args);
    }


    @Test public void Bidi_Map(){
        Bidi_Map.main(args);
    }

    @Test public void Histogram(){
        if(TT.scale()==0)
            return;

        Histogram.main(args);
    }

    @Test public void Lazily_Loaded_Records(){
        Lazily_Loaded_Records.main(args);
    }

    @Test public void Map_Size_Counter(){
        Map_Size_Counter.main(args);
    }

    @Test public void MultiMap(){
        MultiMap.main(args);
    }

    @Test public void Secondary_Key(){
        Secondary_Key.main(args);
    }

    @Test public void Secondary_Map(){
        Secondary_Map.main(args);
    }

    @Test public void Secondary_Values(){
        Secondary_Values.main(args);
    }

    @Test public void SQL_Auto_Incremental_Unique_Key(){
        SQL_Auto_Incremental_Unique_Key.main(args);
    }

    @Test public void Transactions(){
        Transactions.main(args);
    }

    @Test public void Transactions2(){
        Transactions2.main(args);
    }

    @Test public void TreeMap_Composite_Key(){
        TreeMap_Composite_Key.main(args);
    }

    @Test public void Pump_InMemory_Import_Than_Save_To_Disk(){
        Pump_InMemory_Import_Then_Save_To_Disk.main(args);
    }

    @Test public void CacheOffHeap(){
        if(TT.scale()==0)
            return;
        CacheOffHeap.main(args);
    }

    @Test public void CacheOffHeapAdvanced(){
        if(TT.scale()==0)
            return;
        CacheOffHeapAdvanced.main(args);
    }

    @Test public void TreeMap_Performance_Tunning(){
        if(TT.scale()==0)
            return;
        TreeMap_Performance_Tunning.main(args);
    }

    @Test public void CacheEntryExpiry(){
        if(TT.scale()==0)
            return;
        CacheEntryExpiry.main(args);
    }

    @Test public void TreeMap_Value_Compression(){
        TreeMap_Value_Compression.main(args);
    }

    @Test public void Backup() throws IOException {
        Backup.main(args);
    }
    @Test public void Backup_Incremental() throws IOException {
        Backup_Incremental.main(args);
    }


}


