package org.mapdb;

import org.junit.Test;

import java.util.NavigableSet;

public class Issue440Test {

    @Test
    public void first(){
        DB db = DBMaker.newMemoryDB().make();

        NavigableSet<Object[]> set1 = db.createTreeSet("set1")
                .serializer(BTreeKeySerializer.ARRAY2)
                .makeOrGet();

        db = DBMaker.newMemoryDB().transactionDisable().make();

        NavigableSet<Object[]> set2 = db.createTreeSet("set2")
                .serializer(BTreeKeySerializer.ARRAY2)
                .makeOrGet();
    }

    @Test public void second(){
        DB db = DBMaker.newTempFileDB().make();

        NavigableSet<Object[]> set1 = db.createTreeSet("set1")
                .serializer(BTreeKeySerializer.ARRAY2)
                .makeOrGet();

        db.commit();

    }

}
