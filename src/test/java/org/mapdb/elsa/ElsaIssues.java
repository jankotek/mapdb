package org.mapdb.elsa;

import org.fest.reflect.core.Reflection;
import org.junit.Test;
import org.mapdb.*;

import java.io.File;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**Q
 * Serialization unit tests, moved from Elsa because it had dependency on mapdb
 */
public class ElsaIssues {


    @Test
    public void testIssue177() throws UnknownHostException {
        DB db = DBMaker.memoryDB().make();
        InetAddress value = InetAddress.getByName("127.0.0.1");
        long recid = db.getStore().put(value, db.getDefaultSerializer());
        Object value2 = db.getStore().get(recid,db.getDefaultSerializer());
        assertEquals(value,value2);
    }



    @Test public void test2() throws IOException {
        File index = TT.tempFile();
        DB db = DBMaker.fileDB(index).make();

        Serialization2Bean processView = new Serialization2Bean();

        Map<Object, Object> map =  db.hashMap("test2").create();

        map.put("abc", processView);

        db.commit();

        Serialization2Bean retProcessView = (Serialization2Bean)map.get("abc");
        assertEquals(processView, retProcessView);

        db.close();
    }


    @Test public void test2_engine() throws IOException {
        File index = TT.tempFile();
        DB db = DBMaker.fileDB(index).make();

        Serialization2Bean processView = new Serialization2Bean();

        long recid = db.getStore().put(processView, (Serializer<Object>) db.getDefaultSerializer());

        db.commit();

        Serialization2Bean retProcessView = (Serialization2Bean) db.getStore().get(recid, db.getDefaultSerializer());
        assertEquals(processView, retProcessView);

        db.close();
    }


    @Test  public void test3() throws IOException {
        File index = TT.tempFile();

        Serialized2DerivedBean att = new Serialized2DerivedBean();
        DB db = DBMaker.fileDB(index).make();

        Map<Object, Object> map =  db.hashMap("test").create();

        map.put("att", att);
        db.commit();
        db.close();
        db = DBMaker.fileDB(index).make();
        map =  db.hashMap("test").open();


        Serialized2DerivedBean retAtt = (Serialized2DerivedBean) map.get("att");
        assertEquals(att, retAtt);
    }



    static class AAA implements Serializable {

        private static final long serialVersionUID = 632633199013551846L;

        String test  = "aa";
    }


    @Test  public void testReopenWithDefrag(){

        File f = TT.tempFile();

        DB db = DBMaker.fileDB(f)
                .make();

        Map<Integer,AAA> map = db.treeMap("test", Integer.class, AAA.class).createOrOpen();
        map.put(1,new AAA());

        db.compact();
        System.out.println(db.getStore().get(CC.RECID_CLASS_INFOS, Serializer.RECID_ARRAY));
        db.close();

        db = DBMaker.fileDB(f)
                .make();

        map = db.treeMap("test", Integer.class, AAA.class).open();
        assertNotNull(map.get(1));
        assertEquals(map.get(1).test, "aa");


        db.close();
    }


    @Test public void testPersistedSimple() throws Exception {

        File f = TT.tempFile();
        DB r1 = DBMaker.fileDB(f).make();
        long recid = r1.getStore().put("AA",r1.getDefaultSerializer());
        r1.commit();
        r1.close();

        r1 = DBMaker.fileDB(f).make();

        String a2 = (String) r1.getStore().get(recid, r1.getDefaultSerializer());
        r1.close();
        assertEquals("AA", a2);

    }


    @Test public void testPersisted() throws Exception {
        Bean1 b1 = new Bean1("abc", "dcd");
        File f = TT.tempFile();
        DB r1 = DBMaker.fileDB(f).make();
        long recid = r1.getStore().put(b1, r1.getDefaultSerializer());
        r1.commit();
        r1.close();

        r1 = DBMaker.fileDB(f).make();

        Bean1 b2 = (Bean1) r1.getStore().get(recid,r1.getDefaultSerializer());
        r1.close();
        assertEquals(b1, b2);

    }


    @Test public void test_write_object_advanced_serializationm(){
        Object[] o = new Object[]{
                new GregorianCalendar(1,1,1),
                new Bean1("aa","bb")
        };

        for(Object oo:o){
            DB db = DBMaker.memoryDB().make();
            long recid = db.getStore().put(oo, db.getDefaultSerializer());
            assertEquals(oo, db.getStore().get(recid, db.getDefaultSerializer()));
        }

    }


    public static class test_pojo_reload_TestClass implements Serializable
    {
        private String name;

        public test_pojo_reload_TestClass(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            test_pojo_reload_TestClass that = (test_pojo_reload_TestClass) o;

            if (name != null ? !name.equals(that.name) : that.name != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }
    }


    @Test  public  void test_pojo_reload() throws IOException {

        File f = TT.tempFile();
        DB db = DBMaker.fileDB(f).make();
        Set set = db.hashSet("testSerializerPojo").create();
        set.add(new test_pojo_reload_TestClass("test"));
        db.commit();
//        System.out.println(((ElsaSerializerPojo)db.defaultSerializer).registered);
        int prevsize = loadClassInfos(db).length;

        db.close();

        db = DBMaker.fileDB(f).deleteFilesAfterClose().make();
        set = db.hashSet("testSerializerPojo").open();
        set.add(new test_pojo_reload_TestClass("test2"));
        db.commit();
        int newsize = loadClassInfos(db).length;
//        System.out.println(((ElsaSerializerPojo)db.defaultSerializer).registered);
        db.close();

        assertEquals(prevsize, newsize);
    }


    Set<Class> unknownClasses(DB db){
        return Reflection.field("unknownClasses").ofType(Set.class).in(db).get();
    }

    ElsaSerializerPojo.ClassInfo[] loadClassInfos(DB db){
        return Reflection.method("loadClassInfos").withReturnType(ElsaSerializerPojo.ClassInfo[].class).in(db).invoke();
    }

    @Test
    public void class_registered_after_commit(){
        DB db = DBMaker.memoryDB().make();

        assertEquals(0, loadClassInfos(db).length);
        assertEquals(0, unknownClasses(db).size());

        //add some unknown class, DB should be notified
        db.getStore().put(new Bean1("a","b"),db.getDefaultSerializer());
        assertEquals(0, loadClassInfos(db).length);
        assertEquals(1, unknownClasses(db).size());

        //commit, class should become known
        db.commit();
        assertEquals(1, loadClassInfos(db).length);
        assertEquals(0, unknownClasses(db).size());

    }



    @Test
    public void testWriteReplace2() throws IOException {
        File f = TT.tempFile();
        Map m = new MM();
        m.put("11", "111");
        DB db = DBMaker.fileDB(f).make();
        db.treeMap("map").create().put("key",m);
        db.commit();
        db.close();

        db = DBMaker.fileDB(f).make();

        assertEquals(new LinkedHashMap(m), db.treeMap("map").open().get("key"));
    }


    public static class SS implements Serializable{
        protected final Map mm;

        public SS(Map mm) {
            this.mm = mm;
        }
    }

    public static class MM extends AbstractMap implements Serializable{

        Map m = new HashMap();

        private Object writeReplace() throws ObjectStreamException {
            return new LinkedHashMap(this);
        }

        @Override
        public Set<Entry> entrySet() {
            return m.entrySet();
        }

        @Override
        public Object put(Object key, Object value) {
            return m.put(key,value);
        }
    }

    @Test
    public void testWriteReplace2Wrap() throws IOException {
        File f = TT.tempFile();
        SS m = new SS(new MM());
        m.mm.put("11", "111");
        DB db = DBMaker.fileDB(f).make();
        db.treeMap("map").create().put("key", m);
        db.commit();
        db.close();

        db = DBMaker.fileDB(f).make();

        assertEquals(new LinkedHashMap(m.mm), ((SS)db.treeMap("map").open().get("key")).mm);
    }


    @Test(expected = IllegalArgumentException.class)
    public void pojo_serialization_writeReplace_in_object_graph() throws IOException, ClassNotFoundException {
        DB db = DBMaker.heapDB().make();
        TT.clone(new WriteReplaceBB(), db.getDefaultSerializer(), new DataOutput2());
    }



    static class WriteReplaceAA implements Serializable{
        Object writeReplace() throws ObjectStreamException {
            return "";
        }

    }

    static class WriteReplaceBB implements Serializable{
        WriteReplaceAA aa = new WriteReplaceAA();
    }


}

