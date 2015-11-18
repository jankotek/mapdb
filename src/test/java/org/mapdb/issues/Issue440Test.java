package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.util.NavigableSet;

public class Issue440Test {

    @Test
    public void first(){
        DB db = DBMaker.memoryDB().make();

        NavigableSet<Object[]> set1 = db.treeSetCreate("set1")
                .serializer(BTreeKeySerializer.ARRAY2)
                .makeOrGet();

        db = DBMaker.memoryDB().transactionDisable().make();

        NavigableSet<Object[]> set2 = db.treeSetCreate("set2")
                .serializer(BTreeKeySerializer.ARRAY2)
                .makeOrGet();
    }

    @Test public void second(){
        DB db = DBMaker.tempFileDB().make();

        NavigableSet<Object[]> set1 = db.treeSetCreate("set1")
                .serializer(BTreeKeySerializer.ARRAY2)
                .makeOrGet();

        db.commit();

    }

}
