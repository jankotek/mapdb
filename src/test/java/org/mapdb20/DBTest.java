package org.mapdb10;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.*;


@SuppressWarnings({ "unchecked", "rawtypes" })
public class DBTest {

    Store engine;
    DB db;


    @Before public void init(){
        engine = new StoreDirect(null);
        engine.init();
        db = new DB(engine);
    }


    @After
    public void close(){
        db = null;
    }

    @Test
    public void testGetHashMap() throws Exception {
        Map m1 = db.hashMap("test");
        m1.put(1,2);
        m1.put(3,4);
        assertTrue(m1 == db.hashMap("test"));
        assertEquals(m1, new DB(engine).hashMap("test"));
    }



    @Test
    public void testGetHashSet() throws Exception {
        Set m1 = db.hashSet("test");
        m1.add(1);
        m1.add(2);
        assertTrue(m1 == db.hashSet("test"));
        assertEquals(m1, new DB(engine).hashSet("test"));
    }

    @Test
    public void testGetTreeMap() throws Exception {
        Map m1 = db.treeMap("test");
        m1.put(1, 2);
        m1.put(3, 4);
        assertTrue(m1 == db.treeMap("test"));
        assertEquals(m1, new DB(engine).treeMap("test"));
    }

    @Test
    public void testGetTreeSet() throws Exception {
        Set m1 = db.treeSet("test");
        m1.add(1);
        m1.add(2);
        assertTrue(m1 == db.treeSet("test"));
        assertEquals(m1, new DB(engine).treeSet("test"));
    }

    @Test(expected = IllegalAccessError.class)
    public void testClose() throws Exception {
        db.close();
        db.hashMap("test");
    }


    @Test public void getAll(){
        db.atomicStringCreate("aa", "100");
        db.hashMap("zz").put(11,"12");
        Map all = db.getAll();

        assertEquals(2,all.size());
        assertEquals("100", ((Atomic.String) all.get("aa")).get());
        assertEquals("12", ((HTreeMap) all.get("zz")).get(11));

    }

    @Test public void rename(){
        db.hashMap("zz").put(11, "12");
        db.rename("zz", "aa");
        assertEquals("12", db.hashMap("aa").get(11));
    }


    @Test(expected = IllegalArgumentException.class)
    public void testCollectionExists(){
        db.hashMap("test");
        db.checkNameNotExists("test");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueueExists(){
        db.getQueue("test");
        db.checkNameNotExists("test");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAtomicExists(){
        db.atomicInteger("test");
        db.checkNameNotExists("test");
    }

    @Test
    public void test_issue_315() {
        DB db = DBMaker.memoryDB().make();

        final String item1 = "ITEM_ONE";
        final String item2 = "ITEM_ONE_TWO";
        final String item3 = "ITEM_ONETWO";
        final String item4 = "ITEM_ONE__TWO";
        final String item5 = "ITEM_ONE.TWO";
        final String item6 = "ITEM_ONE.__.TWO";


        db.treeMapCreate(item1).make();
        db.treeSetCreate(item2).make();
        db.treeSetCreate(item3).make();
        db.treeSetCreate(item4).make();
        db.treeSetCreate(item5).make();
        db.treeSetCreate(item6).make();


        db.delete(item1);

        assertTrue(db.get(item1) == null);
        assertTrue(db.get(item2) instanceof Set);
        assertTrue(db.get(item3) instanceof Set);
        assertTrue(db.get(item4) instanceof Set);
        assertTrue(db.get(item5) instanceof Set);
        assertTrue(db.get(item6) instanceof Set);

    }


    @Test public void basic_reopen(){
        File f = UtilsTest.tempDbFile();
        DB db = DBMaker.fileDB(f).make();
        Map map = db.treeMap("map");
        map.put("aa", "bb");

        db.commit();
        db.close();

        db = DBMaker.fileDB(f).deleteFilesAfterClose().make();
        map = db.treeMap("map");
        assertEquals(1, map.size());
        assertEquals("bb", map.get("aa"));
        db.close();
    }

    @Test public void basic_reopen_notx(){
        File f = UtilsTest.tempDbFile();
        DB db = DBMaker.fileDB(f).transactionDisable().make();
        Map map = db.treeMap("map");
        map.put("aa", "bb");

        db.commit();
        db.close();

        db = DBMaker.fileDB(f).deleteFilesAfterClose().transactionDisable().make();
        map = db.treeMap("map");
        assertEquals(1, map.size());
        assertEquals("bb", map.get("aa"));
        db.close();
    }

    @Test public void hashmap_executor(){
        ScheduledExecutorService s = Executors.newSingleThreadScheduledExecutor();
        DB db = DBMaker.memoryDB().make();

        HTreeMap m = db.hashMapCreate("aa").executorPeriod(1111).executorEnable(s).make();
        assertTrue(s == m.executor);
        db.close();

        assertTrue(s.isTerminated());
    }

    @Test public void hashset_executor(){
        ScheduledExecutorService s = Executors.newSingleThreadScheduledExecutor();
        DB db = DBMaker.memoryDB().make();

        HTreeMap.KeySet m = (HTreeMap.KeySet) db.hashSetCreate("aa").executorPeriod(1111).executorEnable(s).make();
        assertTrue(s == m.getHTreeMap().executor);
        db.close();

        assertTrue(s.isTerminated());
    }

    @Test public void treemap_infer_key_serializer(){
        DB db = DBMaker.memoryDB().make();
        BTreeMap m = db.treeMapCreate("test")
                .keySerializer(Serializer.LONG)
                .make();
        assertEquals(BTreeKeySerializer.LONG, m.keySerializer);

        BTreeMap m2 = db.treeMapCreate("test2")
                .keySerializer(Serializer.LONG)
                .comparator(Fun.REVERSE_COMPARATOR)
                .make();
        assertTrue(m2.keySerializer instanceof BTreeKeySerializer.BasicKeySerializer);
        assertEquals(m2.comparator(), Fun.REVERSE_COMPARATOR);
    }


    @Test public void treeset_infer_key_serializer(){
        DB db = DBMaker.memoryDB().make();
        BTreeMap.KeySet m = (BTreeMap.KeySet) db.treeSetCreate("test")
                .serializer(Serializer.LONG)
                .make();
        assertEquals(BTreeKeySerializer.LONG, ((BTreeMap)m.m).keySerializer);

        BTreeMap.KeySet m2 = (BTreeMap.KeySet) db.treeSetCreate("test2")
                .serializer(Serializer.LONG)
                .comparator(Fun.REVERSE_COMPARATOR)
                .make();
        assertTrue(((BTreeMap)m2.m).keySerializer instanceof BTreeKeySerializer.BasicKeySerializer);
        assertEquals(m2.comparator(), Fun.REVERSE_COMPARATOR);
    }

    public static final Serializer<Long> SER1 = new Serializer<Long>() {
        @Override
        public void serialize(DataOutput out, Long value) throws IOException {
            out.writeLong(value);
        }

        @Override
        public Long deserialize(DataInput in, int available) throws IOException {
            return in.readLong();
        }
    };

    public static final Serializer<String> SER2 = new Serializer<String>() {
        @Override
        public void serialize(DataOutput out, String value) throws IOException {
            out.writeUTF(value);
        }

        @Override
        public String deserialize(DataInput in, int available) throws IOException {
            return in.readUTF();
        }
    };

    @Test public void hashMap_serializers_non_serializable() throws IOException {
        File f = File.createTempFile("mapdb","mapdb");
        DB db = DBMaker
                .fileDB(f)
                .transactionDisable()
                .make();
        HTreeMap m = db
                .hashMapCreate("map")
                .keySerializer(SER1)
                .valueSerializer(SER2)
                .makeOrGet();
        assertEquals(SER1,m.keySerializer);
        assertEquals(SER2, m.valueSerializer);
        m.put(1L, "aaaaa");
        db.close();

        //reopen and supply serializers
        db = DBMaker
                .fileDB(f)
                .transactionDisable()
                .make();
        m = db
                .hashMapCreate("map")
                .keySerializer(SER1)
                .valueSerializer(SER2)
                .makeOrGet();
        assertEquals(SER1,m.keySerializer);
        assertEquals(SER2,m.valueSerializer);
        assertEquals("aaaaa", m.get(1L));
        db.close();

        //try to reopen with one unknown serializer, it should throw an exception
        //reopen and supply serializers
        db = DBMaker
                .fileDB(f)
                .transactionDisable()
                .make();
        try {
            db
                    .hashMapCreate("map")
                    //.keySerializer(SER1)
                    .valueSerializer(SER2)
                    .makeOrGet();
            fail();
        }catch(DBException.UnknownSerializer e){
            assertEquals(e.getMessage(),"Map 'map' has no keySerializer defined in Name Catalog nor constructor argument.");
        }

        try {
            db
                    .hashMapCreate("map")
                    .keySerializer(SER1)
                    //.valueSerializer(SER2)
                    .makeOrGet();
            fail();
        }catch(DBException.UnknownSerializer e){
            assertEquals(e.getMessage(),"Map 'map' has no valueSerializer defined in Name Catalog nor constructor argument.");
        }

        db.close();
    }

    @Test public void treeMap_serializers_non_serializable() throws IOException {
        File f = File.createTempFile("mapdb","mapdb");
        DB db = DBMaker
                .fileDB(f)
                .transactionDisable()
                .make();
        BTreeMap m = db
                .treeMapCreate("map")
                .keySerializer(SER1)
                .valueSerializer(SER2)
                .makeOrGet();
        assertEquals(SER1,((BTreeKeySerializer.BasicKeySerializer)m.keySerializer).serializer);
        assertEquals(SER2, m.valueSerializer);
        m.put(1L, "aaaaa");
        db.close();

        //reopen and supply serializers
        db = DBMaker
                .fileDB(f)
                .transactionDisable()
                .make();
        m = db
                .treeMapCreate("map")
                .keySerializer(SER1)
                .valueSerializer(SER2)
                .makeOrGet();
        assertEquals(SER1,((BTreeKeySerializer.BasicKeySerializer)m.keySerializer).serializer);
        assertEquals(SER2,m.valueSerializer);
        assertEquals("aaaaa", m.get(1L));
        db.close();

        //try to reopen with one unknown serializer, it should throw an exception
        //reopen and supply serializers
        db = DBMaker
                .fileDB(f)
                .transactionDisable()
                .make();
        try {
            db
                    .treeMapCreate("map")
                            //.keySerializer(SER1)
                    .valueSerializer(SER2)
                    .makeOrGet();
            fail();
        }catch(DBException.UnknownSerializer e){
            assertEquals(e.getMessage(),"Map 'map' has no keySerializer defined in Name Catalog nor constructor argument.");
        }

        try {
            db
                    .treeMapCreate("map")
                    .keySerializer(SER1)
                            //.valueSerializer(SER2)
                    .makeOrGet();
            fail();
        }catch(DBException.UnknownSerializer e){
            assertEquals(e.getMessage(),"Map 'map' has no valueSerializer defined in Name Catalog nor constructor argument.");
        }

        db.close();
    }

    @Test public void treeSet_serializers_non_serializable() throws IOException {
        File f = File.createTempFile("mapdb","mapdb");
        DB db = DBMaker
                .fileDB(f)
                .transactionDisable()
                .make();
        BTreeMap.KeySet m = (BTreeMap.KeySet) db
                .treeSetCreate("map")
                .serializer(SER1)
                .makeOrGet();
        assertEquals(SER1, ((BTreeKeySerializer.BasicKeySerializer) ((BTreeMap) m.m).keySerializer).serializer);
        m.add(1L);
        db.close();

        //reopen and supply serializers
        db = DBMaker
                .fileDB(f)
                .transactionDisable()
                .make();
        m = (BTreeMap.KeySet) db
                .treeSetCreate("map")
                .serializer(SER1)
                .makeOrGet();
        assertEquals(SER1,((BTreeKeySerializer.BasicKeySerializer)((BTreeMap)m.m).keySerializer).serializer);
        assertTrue(m.contains(1L));
        db.close();

        //try to reopen with one unknown serializer, it should throw an exception
        //reopen and supply serializers
        db = DBMaker
                .fileDB(f)
                .transactionDisable()
                .make();
        try {
            db
                    .treeSetCreate("map")
                    //.serializer(SER1)
                    .makeOrGet();
            fail();
        }catch(DBException.UnknownSerializer e){
            assertEquals(e.getMessage(),"Set 'map' has no serializer defined in Name Catalog nor constructor argument.");
        }

        db.close();
    }


    @Test public void hashSet_serializers_non_serializable() throws IOException {
        File f = File.createTempFile("mapdb","mapdb");
        DB db = DBMaker
                .fileDB(f)
                .transactionDisable()
                .make();
        HTreeMap.KeySet m = (HTreeMap.KeySet) db
                .hashSetCreate("map")
                .serializer(SER1)
                .makeOrGet();
        assertEquals(SER1, m.getHTreeMap().keySerializer);
        m.add(1L);
        db.close();

        //reopen and supply serializers
        db = DBMaker
                .fileDB(f)
                .transactionDisable()
                .make();
        m = (HTreeMap.KeySet) db
                .hashSetCreate("map")
                .serializer(SER1)
                .makeOrGet();
        assertEquals(SER1, m.getHTreeMap().keySerializer);
        assertTrue(m.contains(1L));
        db.close();

        //try to reopen with one unknown serializer, it should throw an exception
        //reopen and supply serializers
        db = DBMaker
                .fileDB(f)
                .transactionDisable()
                .make();
        try {
            db
                    .hashSetCreate("map")
                            //.serializer(SER1)
                    .makeOrGet();
            fail();
        }catch(DBException.UnknownSerializer e){
            assertEquals(e.getMessage(),"Set 'map' has no serializer defined in Name Catalog nor constructor argument.");
        }

        db.close();
    }

    @Test public void atomicvar_serializers_non_serializable() throws IOException {
        File f = File.createTempFile("mapdb","mapdb");
        DB db = DBMaker
                .fileDB(f)
                .transactionDisable()
                .make();
        Atomic.Var m = db
                .atomicVarCreate("map",1L,SER1);
        assertEquals(SER1, m.serializer);
        m.set(2L);
        db.close();

        //reopen and supply serializers
        db = DBMaker
                .fileDB(f)
                .transactionDisable()
                .make();
        m = db.atomicVarCreate("map",1L,SER1);

        assertEquals(SER1, m.serializer);
        assertEquals(2L, m.get());
        db.close();

        //try to reopen with one unknown serializer, it should throw an exception
        //reopen and supply serializers
        db = DBMaker
                .fileDB(f)
                .transactionDisable()
                .make();
        try {
            db.get("map");
            fail();
        }catch(DBException.UnknownSerializer e){
            assertEquals(e.getMessage(),"Atomic.Var 'map' has no serializer defined in Name Catalog nor constructor argument.");
        }

        db.close();
    }

    @Test public void issue540_btreemap_serializers(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        BTreeMap map = db.treeMap("test",BTreeKeySerializer.LONG,Serializer.BYTE_ARRAY);
        assertEquals(map.keySerializer,BTreeKeySerializer.LONG);
        assertEquals(map.valueSerializer,Serializer.BYTE_ARRAY);
    }

    @Test public void issue540_htreemap_serializers(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        Fun.Function1 f = new Fun.Function1(){
            @Override
            public Object run(Object o) {
                return "A";
            }
        };
        HTreeMap map = db.hashMap("test", Serializer.LONG, Serializer.BYTE_ARRAY, f);
        assertEquals(map.keySerializer,Serializer.LONG);
        assertEquals(map.valueSerializer,Serializer.BYTE_ARRAY);
        assertEquals(map.valueCreator,f);
    }


    @Test public void issue540_btreeset_serializers(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        BTreeMap.KeySet set = (BTreeMap.KeySet) db.treeSet("test", BTreeKeySerializer.LONG);
        assertEquals(((BTreeMap)set.m).keySerializer,BTreeKeySerializer.LONG);
    }


    @Test public void issue540_htreeset_serializers(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        HTreeMap.KeySet set = (HTreeMap.KeySet) db.hashSet("test", Serializer.LONG);
        assertEquals(set.getHTreeMap().keySerializer,Serializer.LONG);
    }

    @Test public void issue540_btreeset_serializers2(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        BTreeMap.KeySet set = (BTreeMap.KeySet) db.treeSet("test", Serializer.LONG);
        assertEquals(((BTreeMap)set.m).keySerializer,BTreeKeySerializer.LONG);
    }


    @Test public void issue540_btreemap_serializers2(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        BTreeMap map = db.treeMap("test",Serializer.LONG,Serializer.BYTE_ARRAY);
        assertEquals(map.keySerializer,BTreeKeySerializer.LONG);
        assertEquals(map.valueSerializer,Serializer.BYTE_ARRAY);
    }

}
