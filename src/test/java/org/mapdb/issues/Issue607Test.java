package org.mapdb.issues;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Ignore;
import org.junit.Test;
import org.mapdb.Bind.MapListener;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

public class Issue607Test {

    @Test
    public void testListenerDeadlock() {
        final DB db = DBMaker.memoryDB().make();
        HTreeMap map = db.hashMap("test");
        map.modificationListenerAdd(new MapListener() {
            @Override
            public void update(Object key, Object oldVal, Object newVal) {
               ((Map) newVal).put("xyz", "bar");
               db.commit();
            }
        });
        Map record = new HashMap();
        record.put("abc", "foo");
        map.put("key", record);
        db.commit();
        
    }
}
