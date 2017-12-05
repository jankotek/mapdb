package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.*;
import org.mapdb.serializer.Serializers;
import org.mapdb.tree.BTreeMap;

import static org.junit.Assert.assertEquals;

public class Issue743Test {


    @Test
    public void testAfterClear_Integer(){
        if(TT.shortTest())
            return;

        DB db = DBMaker.memoryDB()
                .closeOnJvmShutdown()
                .make();

        DBConcurrentNavigableMap<Integer,String> testMap = db.treeMap("test",
                Serializers.INTEGER,
                Serializers.JAVA )
                .counterEnable()
                .createOrOpen();

        int cnt = 3000;

        for(int i = 0; i < cnt; i++){
            testMap.put(i, "" + i);
        }
        testMap.clear();
        testMap.verify();
        for(int i = 0; i < cnt; i++){
            String toPut = "" + i;
            testMap.put(i, toPut);
            testMap.verify();
            String res = testMap.get(i);
            assertEquals(toPut, res);
        }


        for(int i = 0; i < cnt; i++){
            String toPut = "" + i;
            testMap.put(i, toPut);
            testMap.verify();
            String res = testMap.get(i);
            assertEquals(toPut, res);
        }

    }

    @Test
    public void testAfterClear_Long(){
        DB db = DBMaker.memoryDB()
                .closeOnJvmShutdown()
                .make();

        BTreeMap<Long,String> testMap = (BTreeMap<Long, String>) db.treeMap("test2",
                Serializers.LONG,
                Serializers.STRING )
                .counterEnable().createOrOpen();

        int cnt = 3000;

        for(int i = 0; i < cnt; i++){
            testMap.put((long)i, "" + i);
        }
        testMap.clear();

        for(int i = 0; i < cnt; i++){
            String toPut = "" + i;
            testMap.put((long)i, toPut);

            String res = testMap.get((long)i);
            assertEquals(toPut, res);
        }


        db.close();
    }
}
