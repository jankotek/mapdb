package org.mapdb;


import examples.*;
import org.junit.Test;

import java.io.IOException;

public class ExamplesTest {

    static final String[] args= new String[0];

    @Test public void _HelloWorld() throws IOException {
        _HelloWorld.main(args);
    }

    @Test public void _TempMap(){
        _TempMap.main(args);
    }

    @Test public void Cache(){
        CacheEntryExpiry.main(args);
    }

    @Test public void CacheOverflow() throws InterruptedException {
        CacheOverflow.main(args);
    }

    @Test public void Compression(){
        Compression.main(args);
    }

    @Test public void Huge_Insert() throws IOException {
        Huge_Insert.main(args);
    }



    @Test public void Custom_Value() throws IOException {
        Custom_Value.main(args);
    }


    @Test public void Bidi_Map(){
        Bidi_Map.main(args);
    }

    @Test public void Histogram(){
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



}
