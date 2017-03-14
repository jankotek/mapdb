package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.*;

import static org.junit.Assert.assertEquals;

public class Issue743Test {


    @Test
    public void testAfterClear_Integer(){
        DB db = DBMaker.memoryDB()
                .closeOnJvmShutdown()
                .make();

        BTreeMap<Integer,String> testMap = db.treeMap("test",
                Serializer.INTEGER,
                Serializer.JAVA )
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

        BTreeMap<Long,String> testMap = db.treeMap("test2",
                Serializer.LONG,
                Serializer.STRING )
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
