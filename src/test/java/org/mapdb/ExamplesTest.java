package org.mapdb;


import examples.*;
import org.junit.Test;

public class ExamplesTest {

    static final String[] args= new String[0];

    @Test public void _HelloWorld(){
        _HelloWorld.main(args);
    }

    @Test public void _TempMap(){
        _TempMap.main(args);
    }

    @Test public void Compression(){
        Compression.main(args);
    }


    @Test public void Custom_Value(){
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




}
