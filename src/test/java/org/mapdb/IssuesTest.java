package org.mapdb;

import org.junit.Test;

import java.util.Map;

public class IssuesTest {

    @Test public void issue130(){
        DB db = DBMaker.newAppendFileDB(UtilsTest.tempDbFile())
                .closeOnJvmShutdown()
                .make();

        Map store = db.getTreeMap("collectionName");


    }
}
