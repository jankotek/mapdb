package org.mapdb20.issues;

import org.junit.Test;
import org.mapdb20.DB;
import org.mapdb20.DBMaker;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Issue321Test {

    @Test
    public void npe(){

        DB db = DBMaker.memoryDB().make();

        List l = Arrays.asList(19,10,9,8,2);

        Map m = db.treeMapCreate("aa")
                .pumpPresort(100)
                .make();

    }
}
