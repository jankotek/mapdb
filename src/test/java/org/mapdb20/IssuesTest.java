package org.mapdb20;

import org.junit.Test;

import java.util.Map;

public class IssuesTest {

    @Test public void issue130(){
        DB db = DBMaker.appendFileDB(UtilsTest.tempDbFile())
                .closeOnJvmShutdown()
                .make();

        Map store = db.treeMap("collectionName");


    }
}
