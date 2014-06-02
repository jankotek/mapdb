package org.mapdb;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Issue321Test {

    @Test
    public void npe(){

        DB db = DBMaker.newMemoryDB().make();

        List l = Arrays.asList(19,10,9,8,2);

        Map m = db.createTreeMap("aa")
                .pumpPresort(100)
                .make();

    }
}
