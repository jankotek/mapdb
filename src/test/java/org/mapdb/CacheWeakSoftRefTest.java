package org.mapdb;

public class CacheWeakSoftRefTest {

/* TODO reenable

    @Test
    public void weak_htree_inserts_delete() throws InterruptedException {
        DB db = DBMaker
                .memoryDB()
                .cacheWeakRefEnable()
                .make();
        testMap(db);
    }

    @Test
    public void soft_htree_inserts_delete() throws InterruptedException {
        DB db = DBMaker
                .memoryDB()
                .cacheSoftRefEnable()
                .make();
        testMap(db);
    }


    private void testMap(DB db) throws InterruptedException {
        Map<Integer, Integer> m = db.getHashMap("name");
        for(Integer i = 0;i<1000;i++){
            m.put(i,i);
        }
        Cache.WeakSoftRef engine = (Cache.WeakSoftRef)db.engine;
        assertTrue(engine.items.size()!=0);

        for(Integer i = 0;i<1000;i++){
            Integer a = m.remove(i);
            assertEquals(i, a);
        }
        db.close();
        int counter = 10000;
        while(engine.cleanerFinished.getCount()!=0 && counter>0){
            Thread.sleep(1);
            counter--;
        }
        assertEquals(0,engine.cleanerFinished.getCount());
    }
    */
}
