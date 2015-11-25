package org.mapdb;


import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

@SuppressWarnings({"rawtypes","unchecked"})
public class PumpTest {

    @Test
    public void copy(){
        DB db1 = new DB(new StoreHeap(true,CC.DEFAULT_LOCK_SCALE,0,false));
        Map m = db1.hashMap("test");
        for(int i=0;i<1000;i++){
            m.put(i, "aa"+i);
        }

        DB db2 = DBMaker.memoryDB().make();
        Pump.copy(db1,db2);

        Map m2 = db2.hashMap("test");
        for(int i=0;i<1000;i++){
            assertEquals("aa"+i, m.get(i));
        }

    }

    DB makeDB(int i){
        switch(i){
            case 0: return DBMaker.appendFileDB(TT.tempDbFile()).deleteFilesAfterClose().snapshotEnable().make();
            case 1: return DBMaker.memoryDB().snapshotEnable().make();
            case 2: return DBMaker.memoryDB().snapshotEnable().transactionDisable().make();
            case 3: return DBMaker.memoryDB().snapshotEnable().makeTxMaker().makeTx();
            case 4: return new DB(new StoreHeap(true,CC.DEFAULT_LOCK_SCALE,0,false));
        }
        throw new IllegalArgumentException(""+i);
    }
    final int dbmax = 5;


    @Test @Ignore
    public void copy_all_stores_simple(){
        for(int srcc=0;srcc<dbmax;srcc++){
            for(int targetc=0;targetc<dbmax;targetc++) try{

                DB src = makeDB(srcc);
                DB target = makeDB(targetc);

                long recid1 = src.engine.put("1", Serializer.STRING_NOSIZE);
                long recid2 = src.engine.put("2", Serializer.STRING_NOSIZE);

                Pump.copy(src,target);

                assertEquals("1", target.engine.get(recid1, Serializer.STRING_NOSIZE));
                assertEquals("2", target.engine.get(recid2, Serializer.STRING_NOSIZE));
                assertEquals("1", src.engine.get(recid1, Serializer.STRING_NOSIZE));
                assertEquals("2", src.engine.get(recid2, Serializer.STRING_NOSIZE));

                src.close();
                target.close();
            } catch(Throwable e){
                throw new RuntimeException("Failed with "+srcc+" - "+targetc,e);
            }
        }


    }

    @Test @Ignore
    public void copy_all_stores(){
        for(int srcc=0;srcc<dbmax;srcc++){
            for(int targetc=0;targetc<dbmax;targetc++) try{

                DB src = makeDB(srcc);
                DB target = makeDB(targetc);

                Map m = src.treeMap("test");
                for(int i=0;i<1000;i++) m.put(i,"99090adas d"+i);
                src.commit();

                Pump.copy(src, target);

                assertEquals(src.getCatalog(), target.getCatalog());
                Map m2 = target.treeMap("test");
                assertFalse(m2.isEmpty());
                assertEquals(m,m2);
                src.close();
                target.close();
            } catch(Throwable e){
                throw new RuntimeException("Failed with "+srcc+" - "+targetc,e);
            }
        }
    }

    @Test @Ignore
    public void copy_all_stores_with_snapshot(){
        for(int srcc=0;srcc<dbmax;srcc++){
            for(int targetc=0;targetc<dbmax;targetc++) try{

                DB src = makeDB(srcc);
                DB target = makeDB(targetc);

                Map m = src.treeMap("test");
                for(int i=0;i<1000;i++) m.put(i,"99090adas d"+i);
                src.commit();

                DB srcSnapshot = src.snapshot();

                for(int i=0;i<1000;i++) m.put(i,"aaaa"+i);

                Pump.copy(srcSnapshot, target);

                assertEquals(src.getCatalog(), target.getCatalog());
                Map m2 = target.treeMap("test");
                assertFalse(m2.isEmpty());
                assertEquals(m,m2);
                src.close();
                target.close();
            } catch(Throwable e){
                throw new RuntimeException("Failed with "+srcc+" - "+targetc,e);
            }
        }
    }

    @Test public void presort(){
        final int max = 10000;
        List<Integer> list = new ArrayList<Integer>(max);
        for(Integer i=0;i<max;i++) list.add(i);
        Collections.shuffle(list);

        Iterator<Integer> sorted = Pump.sort(list.iterator(),false, max/20,
                Fun.COMPARATOR, Serializer.INTEGER, null);

        int counter=0;
        while(sorted.hasNext()){
            assertEquals(counter++, (int)sorted.next());
        }
        assertEquals((Object)max,counter);
    }


    @Test public void presort_parallel(){
        final int max = 10000;
        List<Integer> list = new ArrayList<Integer>(max);
        for(Integer i=0;i<max;i++) list.add(i);
        Collections.shuffle(list);

        Iterator<Integer> sorted = Pump.sort(list.iterator(),false, max/20,
                Fun.COMPARATOR, Serializer.INTEGER,
                Executors.newCachedThreadPool());

        int counter=0;
        while(sorted.hasNext()){
            assertEquals(counter++, (int)sorted.next());
        }
        assertEquals((Object)max,counter);
    }


    @Test public void presort_duplicates(){
        final int max = 10000;
        List<Integer> list = new ArrayList<Integer>(max);
        for(Integer i=0;i<max;i++){
            list.add(i);
            list.add(i);
        }
        Collections.shuffle(list);

        Iterator<Integer> sorted = Pump.sort(list.iterator(),true, max/20,
                Fun.COMPARATOR, Serializer.INTEGER,null);

        int counter=0;
        while(sorted.hasNext()){
            Object v = sorted.next();
            assertEquals(counter++, v);
        }
        assertEquals((Object)max,counter);
    }

    @Test public void presort_duplicates_parallel(){
        final int max = 10000;
        List<Integer> list = new ArrayList<Integer>(max);
        for(Integer i=0;i<max;i++){
            list.add(i);
            list.add(i);
        }
        Collections.shuffle(list);

        Iterator<Integer> sorted = Pump.sort(list.iterator(),true, max/20,
                Fun.COMPARATOR, Serializer.INTEGER,Executors.newCachedThreadPool());

        int counter=0;
        while(sorted.hasNext()){
            Object v = sorted.next();
            assertEquals(counter++, v);
        }
        assertEquals((Object)max,counter);
    }


    @Test public void build_treeset(){
        final int max = 10000;
        List<Integer> list = new ArrayList<Integer>(max);
        for(Integer i=max-1;i>=0;i--) list.add(i);

        Engine e = new StoreHeap(true,CC.DEFAULT_LOCK_SCALE,0,false);
        DB db = new DB(e);

        Set s = db.treeSetCreate("test")
                .nodeSize(8)
                .pumpSource(list.iterator())
                .make();

        Iterator<Integer> iter =s.iterator();

        int count = 0;
        while(iter.hasNext()){
            assertEquals(count++, (int)iter.next());
        }

        for(Integer i:list){
            assertTrue(""+i,s.contains(i));
        }

        assertEquals(max, s.size());
    }


    @Test public void build_treeset_ignore_duplicates(){
        final int max = 10000;
        List<Integer> list = new ArrayList<Integer>(max);
        for(Integer i=max-1;i>=0;i--){
            list.add(i);
            list.add(i);
        }

        Engine e = new StoreHeap(true,CC.DEFAULT_LOCK_SCALE,0,false);
        DB db = new DB(e);

        Set s = db.treeSetCreate("test")
                .nodeSize(8)
                .pumpSource(list.iterator())
                .pumpIgnoreDuplicates()
                .make();

        Iterator<Integer> iter =s.iterator();

        int count = 0;
        while(iter.hasNext()){
            assertEquals(count++, (int)iter.next());
        }

        for(Integer i:list){
            assertTrue(""+i,s.contains(i));
        }

        assertEquals(max, s.size());
    }


    @Test public void build_treemap(){
        final int max = 10000;
        List<Integer> list = new ArrayList<Integer>(max);
        for(Integer i=max-1;i>=0;i--) list.add(i);

        Engine e = new StoreHeap(true,CC.DEFAULT_LOCK_SCALE,0,false);
        DB db = new DB(e);

        Fun.Function1<Object, Integer> valueExtractor = new Fun.Function1<Object, Integer>() {
            @Override
            public Object run(Integer integer) {
                return integer*100;
            }
        };


        Map s = db.treeMapCreate("test")
            .nodeSize(6)
            .pumpSource(list.iterator(), valueExtractor)
            .make();


        Iterator<Integer> iter =s.keySet().iterator();

        int count = 0;
        while(iter.hasNext()){
            assertEquals(count++, (int)iter.next());
        }

        for(Integer i:list){
            assertEquals(i * 100, s.get(i));
        }

        assertEquals(max, s.size());
    }


    @Test public void build_treemap_external(){
        final int max = 10000;
        List<Integer> list = new ArrayList<Integer>(max);
        for(Integer i=max-1;i>=0;i--) list.add(i);

        Engine e = new StoreHeap(true,CC.DEFAULT_LOCK_SCALE,0,false);
        DB db = new DB(e);

        Fun.Function1<Object, Integer> valueExtractor = new Fun.Function1<Object, Integer>() {
            @Override
            public Object run(Integer integer) {
                return integer*100;
            }
        };


        Map s = db.treeMapCreate("test")
                .nodeSize(6)
                .pumpSource(list.iterator(), valueExtractor)
                .valuesOutsideNodesEnable()
                .make();


        Iterator<Integer> iter =s.keySet().iterator();

        int count = 0;
        while(iter.hasNext()){
            assertEquals(count++, (int)iter.next());
        }

        for(Integer i:list){
            assertEquals(i * 100, s.get(i));
        }

        assertEquals(max, s.size());
    }


    @Test public void build_treemap_ignore_dupliates(){
        final int max = 10000;
        List<Integer> list = new ArrayList<Integer>(max);
        for(Integer i=max-1;i>=0;i--){
            list.add(i);
            list.add(i);
        }

        Engine e = new StoreHeap(true,CC.DEFAULT_LOCK_SCALE,0,false);
        DB db = new DB(e);

        Fun.Function1<Object, Integer> valueExtractor = new Fun.Function1<Object, Integer>() {
            @Override
            public Object run(Integer integer) {
                return integer*100;
            }
        };


        BTreeMap s = db.treeMapCreate("test")
                .nodeSize(6)
                .pumpSource(list.iterator(), valueExtractor)
                .pumpIgnoreDuplicates()
                .make();


        Iterator<Integer> iter =s.keySet().iterator();

        int count = 0;
        while(iter.hasNext()){
            assertEquals(count++, (int)iter.next());
        }

        for(Integer i:list){
            assertEquals(i * 100, s.get(i));
        }

        assertEquals(max, s.size());
    }



    @Test(expected = DBException.PumpSourceDuplicate.class)
    public void build_treemap_fails_with_unsorted(){
        List a = Arrays.asList(1, 2, 3, 4, 4, 5);
        Collections.reverse(a);
        DB db = DBMaker.memoryDB().transactionDisable().make();
        db.treeSetCreate("test").pumpSource(a.iterator()).make();
    }

    @Test(expected = DBException.PumpSourceNotSorted.class)
    public void build_treemap_fails_with_unsorted2(){
        List a = Arrays.asList(1,2,3,4,3,5);
        Collections.reverse(a);
        DB db = DBMaker.memoryDB().transactionDisable().make();
        db.treeSetCreate("test").pumpSource(a.iterator()).make();
    }


    @Test public void uuid_reversed(){
        int max = TT.scale()*10000+100;
        List<UUID> u = new ArrayList<UUID>();
        Random r = new Random();
        for(int i=0;i<max;i++) u.add(new UUID(r.nextLong(),r.nextLong()));
        Set<UUID> sorted = new TreeSet<UUID>(Collections.reverseOrder(Fun.COMPARATOR));
        sorted.addAll(u);

        Iterator<UUID> iter = u.iterator();
        iter = Pump.sort(iter,false, 10000,Collections.reverseOrder(Fun.COMPARATOR),Serializer.UUID,null);
        Iterator<UUID> iter2 = sorted.iterator();

        while(iter.hasNext()){
            assertEquals(iter2.next(), iter.next());
        }
        assertFalse(iter2.hasNext());
    }


    @Test public void merge_with_duplicates(){
        List<Long> u = new ArrayList<Long>();
        for(long i=0;i<100;i++){
            u.add(i);
        }

        Iterator res = Pump.sort(Fun.COMPARATOR, false, u.iterator(), u.iterator());

        for(long i=0;i<100;i++){
            assertTrue(res.hasNext());
            assertEquals(i, res.next());
            assertTrue(res.hasNext());
            assertEquals(i, res.next());
        }
        assertFalse(res.hasNext());
    }
    @Test public void merge_without_duplicates(){
        List<Long> u = new ArrayList<Long>();
        for(long i=0;i<100;i++){
            u.add(i);
        }

        Iterator res = Pump.sort(Fun.COMPARATOR, true, u.iterator(), u.iterator());

        for(long i=0;i<100;i++){
            assertTrue(res.hasNext());
            assertEquals(i, res.next());
        }
        assertFalse(res.hasNext());
    }


    @Test public void merge(){
        Iterator<String> i = Pump.merge(
                null,
                Arrays.asList("a","b").iterator(),
                Arrays.asList().iterator(),
                Arrays.asList("c","d").iterator(),
                Arrays.asList().iterator()
        );

        assertTrue(i.hasNext());
        assertEquals("a",i.next());
        assertTrue(i.hasNext());
        assertEquals("b",i.next());
        assertTrue(i.hasNext());
        assertEquals("c",i.next());
        assertTrue(i.hasNext());
        assertEquals("d",i.next());
        assertTrue(!i.hasNext());
    }

    @Test public void merge_parallel(){
        Iterator i = Pump.merge(
                Executors.newCachedThreadPool(),
                Arrays.asList("a","b").iterator(),
                Arrays.asList().iterator(),
                Arrays.asList("c","d").iterator(),
                Arrays.asList().iterator()
        );

        assertTrue(i.hasNext());
        assertEquals("a",i.next());
        assertTrue(i.hasNext());
        assertEquals("b",i.next());
        assertTrue(i.hasNext());
        assertEquals("c",i.next());
        assertTrue(i.hasNext());
        assertEquals("d",i.next());
        assertTrue(!i.hasNext());
    }



    @Test public void sorted(){
        if(TT.scale()==0)
            return;

        DB db = DBMaker.memoryDB()
                .transactionDisable()
                .cacheHashTableEnable()
                .make();

        class Source implements Iterator<Fun.Pair<Integer, String>> {
            int counter = 0;
            int mapIndex = Integer.MAX_VALUE;

            @Override public boolean hasNext()
            {

                return counter <= 16737175;
            }

            @Override
            public Fun.Pair<Integer, String> next()
            {
                counter++;
                mapIndex--;
                return new Fun.Pair(mapIndex, "foobar"+mapIndex);
            }

            @Override public void remove()
            {
            }
        }

        BTreeMap<Integer, String> csvContentMap = db.treeMapCreate("csvContentMap")
                .keySerializer(BTreeKeySerializer.INTEGER)
                .valueSerializer(Serializer.STRING)
                .pumpSource(new Source())
                .counterEnable()
                .make();

        Source s = new Source();
        while(s.hasNext()){
            Fun.Pair<Integer, String> next = s.next();
            assertEquals(next.b, csvContentMap.get(next.a));
        }

        int i = Integer.MAX_VALUE-16737175-1;
        for(Map.Entry e:csvContentMap.entrySet()){
            assertEquals(i++, e.getKey());
        }


//        csvContentMap.printTreeStructure();

        db.commit();
        db.close();
    }


    @Test public void empty_treemap(){
        BTreeMap m = DBMaker.memoryDB().transactionDisable()
                .make().treeMapCreate("map")
                .pumpSource(Fun.EMPTY_ITERATOR)
                .make();
        assertTrue(m.isEmpty());
    }


    @Test public void empty_iterator_issue452(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        Map m = db.treeMapCreate("m")
                .pumpSource(Fun.EMPTY_ITERATOR)
                .make();
        assertTrue(m.isEmpty());
        m = db.treeMapCreate("2m")
                .pumpSource(Fun.EMPTY_ITERATOR,Fun.extractNoTransform())
                .make();
        assertTrue(m.isEmpty());
    }

    @Test public void empty_iterator_set_issue452(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        Set m = db.treeSetCreate("m")
                .pumpSource(Fun.EMPTY_ITERATOR)
                .make();
        assertTrue(m.isEmpty());
    }

    @Test public void hash_empty_iterator_issue452(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        Map m = db.hashMapCreate("m")
                .pumpSource(Fun.EMPTY_ITERATOR)
                .make();
        assertTrue(m.isEmpty());
        m = db.hashMapCreate("2m")
                .pumpSource(Fun.EMPTY_ITERATOR,Fun.extractNoTransform())
                .make();
        assertTrue(m.isEmpty());
    }

    @Test public void hash_empty_iterator_set_issue452(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        Set m = db.hashSetCreate("m")
                .pumpSource(Fun.EMPTY_ITERATOR)
                .make();
        assertTrue(m.isEmpty());
    }

    @Test public void btreemap_pump_takes_navigablemap(){
        TreeMap m = new TreeMap();
        for(int i=0;i<10000;i++){
            m.put(i,i*111);
        }
        DB db = DBMaker.memoryDB().transactionDisable().make();
        Map m2 = db.treeMapCreate("map")
                .pumpSource(m)
                .make();
        assertEquals(m, m2);
    }


    @Test public void treemap_pump_takes_navigableset(){
        TreeSet m = new TreeSet();
        for(int i=0;i<10000;i++){
            m.add(i);
        }
        DB db = DBMaker.memoryDB().transactionDisable().make();
        Set m2 = db.treeSetCreate("map")
                .pumpSource(m)
                .make();
        assertEquals(m,m2);
    }
}
