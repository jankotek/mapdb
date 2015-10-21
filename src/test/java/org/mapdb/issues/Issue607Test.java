package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.Bind.MapListener;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

public class Issue607Test {

    @Test
    public void testListenerDeadlock() {
        final DB db = DBMaker.memoryDB().make();
        final HTreeMap<Object,Object> map = db.hashMap("test");
        map.modificationListenerAfterAdd(new MapListener<Object, Object>() {
            @Override
            public void update(Object key, Object oldVal, Object newVal) {
                if ("foo".equals(newVal)) {
                    map.put("xyz", "bar");
                }
                db.commit();
            }
        });
        map.put("abc", "foo");
    }
}
