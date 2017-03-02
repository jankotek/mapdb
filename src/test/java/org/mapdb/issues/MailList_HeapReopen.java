package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.*;

import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;

public class MailList_HeapReopen {

    @Test
    public void test(){
        DB store = DBMaker.heapDB().make();

        ConcurrentMap map = store.hashMap("map").createOrOpen();
        map.put("fooKey","fooValue");

//get the map from store
        ConcurrentMap mapExisting = store.hashMap("map").createOrOpen();
        assertEquals("fooValue",mapExisting.get("fooKey"));
    }
}
