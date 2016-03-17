package org.mapdb;


import junit.framework.TestCase;

import javax.security.sasl.RealmCallback;
import javax.swing.*;
import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class SerializerPojoTest extends TestCase {

    SerializerPojo p = new SerializerPojo(new CopyOnWriteArrayList<SerializerPojo.ClassInfo>());

    enum Order
    {
        ASCENDING,
        DESCENDING
    }
    private byte[] serialize(Object i) throws IOException {
        DataOutput2 in = new DataOutput2();
        p.serialize(in, i);
        return in.copyBytes();
    }

    private Object deserialize(byte[] buf) throws IOException {
        return p.deserialize(new DataInput2(ByteBuffer.wrap(buf),0),-1);
    }


    public void testEnum() throws Exception{
        Order o = Order.ASCENDING;
        o = (Order) UtilsTest.clone(o, p);
        assertEquals(o,Order.ASCENDING );
        assertEquals(o.ordinal(),Order.ASCENDING .ordinal());
        assertEquals(o.name(),Order.ASCENDING .name());

        o = Order.DESCENDING;
        o = (Order) UtilsTest.clone(o, p);
        assertEquals(o,Order.DESCENDING );
        assertEquals(o.ordinal(),Order.DESCENDING .ordinal());
        assertEquals(o.name(),Order.DESCENDING .name());

    }


    static class Extr  implements  Externalizable{

        public Extr(){}

        int aaa = 11;
        String  l = "agfa";

        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(l);
            out.writeInt(aaa);

        }

        @Override  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            l = (String) in.readObject();
            aaa = in.readInt()+1;

        }
    }

    public void testExternalizable() throws Exception{
        Extr e = new Extr();
        e.aaa = 15;
        e.l = "pakla";

        e = (Extr) deserialize(serialize(e));
        assertEquals(e.aaa, 16); //was incremented during serialization
        assertEquals(e.l,"pakla");

    }


    static class Bean1 implements Serializable {

    	private static final long serialVersionUID = -2549023895082866523L;

		@Override
		public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Bean1 bean1 = (Bean1) o;

            if (Double.compare(bean1.doubleField, doubleField) != 0) return false;
            if (Float.compare(bean1.floatField, floatField) != 0) return false;
            if (intField != bean1.intField) return false;
            if (longField != bean1.longField) return false;
            if (field1 != null ? !field1.equals(bean1.field1) : bean1.field1 != null) return false;
            if (field2 != null ? !field2.equals(bean1.field2) : bean1.field2 != null) return false;

            return true;
        }


        protected String field1 = null;
        protected String field2 = null;

        protected int intField = Integer.MAX_VALUE;
        protected long longField = Long.MAX_VALUE;
        protected double doubleField = Double.MAX_VALUE;
        protected float floatField = Float.MAX_VALUE;

        transient int getCalled = 0;
        transient int setCalled = 0;

        public String getField2() {
            getCalled++;
            return field2;
        }

        public void setField2(String field2) {
            setCalled++;
            this.field2 = field2;
        }

        Bean1(String field1, String field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        Bean1() {
        }
    }

    static class Bean2 extends Bean1 {

		private static final long serialVersionUID = 8376654194053933530L;

		@Override
		public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;

            Bean2 bean2 = (Bean2) o;

            if (field3 != null ? !field3.equals(bean2.field3) : bean2.field3 != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return field3 != null ? field3.hashCode() : 0;
        }

        private String field3 = null;

        Bean2(String field1, String field2, String field3) {
            super(field1, field2);
            this.field3 = field3;
        }

        Bean2() {
        }
    }



    Bean1 b = new Bean1("aa", "bb");
    Bean2 b2 = new Bean2("aa", "bb", "cc");

    public void testGetFieldValue1() throws Exception {
        assertEquals("aa", p.getFieldValue(new SerializerPojo.FieldInfo("field1",false,String.class.getName(),b.getClass()), b));
    }

    public void testGetFieldValue2() throws Exception {
        assertEquals("bb", p.getFieldValue(new SerializerPojo.FieldInfo("field2",false,String.class.getName(),b.getClass()), b));
        assertEquals(0, b.getCalled);
    }

    public void testGetFieldValue3() throws Exception {
        assertEquals("aa", p.getFieldValue(new SerializerPojo.FieldInfo("field1",false,String.class.getName(),b2.getClass()), b2));
    }

    public void testGetFieldValue4() throws Exception {
        assertEquals("bb", p.getFieldValue(new SerializerPojo.FieldInfo("field2",false,String.class.getName(),b2.getClass()), b2));
        assertEquals(0, b2.getCalled);
    }

    public void testGetFieldValue5() throws Exception {
        assertEquals("cc", p.getFieldValue(new SerializerPojo.FieldInfo("field3",false,String.class.getName(),b2.getClass()), b2));
    }



    public void testSerializable() throws Exception {

        assertEquals(b, UtilsTest.clone(b, p));
    }


    public void testRecursion() throws Exception {
        AbstractMap.SimpleEntry b = new AbstractMap.SimpleEntry("abcd", null);
        b.setValue(b.getKey());

        AbstractMap.SimpleEntry bx = (AbstractMap.SimpleEntry) UtilsTest.clone(b, p);
        assertEquals(bx, b);
        assert (bx.getKey() == bx.getValue());

    }

    public void testRecursion2() throws Exception {
        AbstractMap.SimpleEntry b = new AbstractMap.SimpleEntry("abcd", null);
        b.setValue(b);

        AbstractMap.SimpleEntry bx = (AbstractMap.SimpleEntry) UtilsTest.clone(b, p);
        assertTrue(bx == bx.getValue());
        assertEquals(bx.getKey(), "abcd");

    }


    public void testRecursion3() throws Exception {
        ArrayList l = new ArrayList();
        l.add("123");
        l.add(l);

        ArrayList l2 = (ArrayList) UtilsTest.clone(l, p);

        assertTrue(l2.size() == 2);
        assertEquals(l2.get(0), "123");
        assertTrue(l2.get(1) == l2);
    }

    public void testPersistedSimple() throws Exception {

        File f = UtilsTest.tempDbFile();
        DB r1 = DBMaker.newFileDB(f).make();
        long recid = r1.engine.put("AA",r1.getDefaultSerializer());
        r1.commit();
        r1.close();

         r1 = DBMaker.newFileDB(f).make();

        String a2 = (String) r1.engine.get(recid, r1.getDefaultSerializer());
        r1.close();
        assertEquals("AA", a2);

    }


    public void testPersisted() throws Exception {
        Bean1 b1 = new Bean1("abc", "dcd");
        File f = UtilsTest.tempDbFile();
        DB r1 = DBMaker.newFileDB(f).make();
        long recid = r1.engine.put(b1, r1.getDefaultSerializer());
        r1.commit();
        r1.close();

        r1 = DBMaker.newFileDB(f).make();

        Bean1 b2 = (Bean1) r1.engine.get(recid,r1.getDefaultSerializer());
        r1.close();
        assertEquals(b1, b2);

    }


    public void test_write_object_advanced_serializationm(){
        Object[] o = new Object[]{
                new GregorianCalendar(1,1,1),
                new JLabel("aa")
        };

        for(Object oo:o){
            DB db = DBMaker.newMemoryDB().make();
            long recid = db.engine.put(oo, db.getDefaultSerializer());
            assertEquals(oo, db.engine.get(recid, db.getDefaultSerializer()));
        }

    }


    public static class test_pojo_reload_TestClass implements Serializable
    {
        private String name;

        public test_pojo_reload_TestClass(String name) {
            this.name = name;
        }

    }

    /** @author Jan Sileny */
    public  void test_pojo_reload() throws IOException {

        File f = UtilsTest.tempDbFile();
        DB db = DBMaker.newFileDB(f).make();
        Set set = db.getHashSet("testSerializerPojo");
        set.add(new test_pojo_reload_TestClass("test"));
        db.commit();
//        System.out.println(((SerializerPojo)db.defaultSerializer).registered);
        int prevsize = ((SerializerPojo)db.getDefaultSerializer()).registered.size();

        db.close();

        db = DBMaker.newFileDB(f).deleteFilesAfterClose().make();
        set = db.getHashSet("testSerializerPojo");
        set.add(new test_pojo_reload_TestClass("test2"));
        db.commit();
        int newsize = ((SerializerPojo)db.getDefaultSerializer()).registered.size();
//        System.out.println(((SerializerPojo)db.defaultSerializer).registered);
        db.close();

        assertEquals(prevsize, newsize);
    }


    public static class test_transient implements Serializable{
        transient int aa = 11;
        transient String ss = "aa";
        int bb = 11;
    }

    public void test_transient(){
        test_transient t = new test_transient();
        t.aa = 12;
        t.ss = "bb";
        t.bb = 13;
        t = (test_transient) UtilsTest.clone(t, p);
        assertEquals(0,t.aa);
        assertEquals(null,t.ss);
        assertEquals(13,t.bb);
    }

    public void test_transient2(){
        test_transient t = new test_transient();
        t.aa = 12;
        t.ss = "bb";
        t.bb = 13;

        t = outputStreamClone(t);
        assertEquals(0,t.aa);
        assertEquals(null,t.ss);
        assertEquals(13,t.bb);
    }

    /** clone value using serialization */
    public static <E> E outputStreamClone(E value){
        try{
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            new ObjectOutputStream(out).writeObject(value);
            ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(out.toByteArray()));
            return (E) in.readObject();
        }catch(Exception ee){
            throw new IOError(ee);
        }
    }


    public void testIssue177() throws UnknownHostException {
        DB db = DBMaker.newMemoryDB().cacheDisable().make();
        InetAddress value = InetAddress.getByName("127.0.0.1");
        long recid = db.engine.put(value, db.getDefaultSerializer());
        Object value2 = db.engine.get(recid,db.getDefaultSerializer());
        assertEquals(value,value2);
    }

    //this can not be serialized, it alwaes throws exception on serialization
    static final class RealClass implements Serializable, Externalizable{
        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            throw new Error();
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            throw new Error();
        }
    }

    //this is placeholder which gets serialized instead
    static final class PlaceHolder implements Serializable{

    }

    public void test_interlizeceptors(){
        final AtomicInteger counter = new AtomicInteger();
        Fun.Function1 ser = new Fun.Function1() {
            @Override
            public Object run(Object o) {
                if(o instanceof RealClass) {
                    counter.incrementAndGet();
                    return new PlaceHolder();
                }
                return o;
            }
        };
        Fun.Function1 deser = new Fun.Function1() {
            @Override
            public Object run(Object o) {
                if(o instanceof PlaceHolder) {
                    counter.incrementAndGet();
                    return new RealClass();
                }
                return o;
            }
        };


        p.serializerTransformAdd(ser,deser);

        Object o = UtilsTest.clone(new RealClass(), p);
        assertTrue(o instanceof RealClass);
        assertEquals(2,counter.get());
    }


    public void testSQLTimestamp(){
        Timestamp t = new Timestamp(111);
        t.setTime(111);
        t.setNanos(2222);
        Timestamp t2 = (Timestamp) UtilsTest.clone(t,p);
        assertEquals(2222, t2.getNanos());
        assertEquals(t,t2);
    }


    public static class SS implements Serializable{
        protected final Map mm;

        public SS(Map mm) {
            this.mm = mm;
        }
    }

    public static class MMCore extends AbstractMap implements Serializable{

        Map m = new HashMap();

        protected Object writeReplace() throws ObjectStreamException {
            return new LinkedHashMap(m);
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

    public static class MM extends MMCore {
        @Override
        public Set<Entry> entrySet() {
            return super.entrySet();
        }

        @Override
        public Object put(Object key, Object value) {
            return super.put(key, value);
        }
    }

    public void testWriteReplace_inheritance() throws ObjectStreamException {
        Map m = new MM();
        m.put("11","111");

        Object clone = UtilsTest.clone(m,p);
        assertEquals(LinkedHashMap.class, clone.getClass());
        assertEquals(new LinkedHashMap(m), clone);
    }

    public void testWriteReplace() throws ObjectStreamException {
        Map m = new MMCore();
        m.put("11","111");

        Object clone = UtilsTest.clone(m,p);
        assertEquals(LinkedHashMap.class, clone.getClass());
        assertEquals(new LinkedHashMap(m), clone);
    }


    public void testWriteReplace_inheritance_java() throws ObjectStreamException {
        Map m = new MM();
        m.put("11","111");

        Object clone = UtilsTest.clone(m,Serializer.JAVA);
        assertEquals(LinkedHashMap.class, clone.getClass());
        assertEquals(new LinkedHashMap(m), clone);
    }

    public void testWriteReplace_java() throws ObjectStreamException {
        Map m = new MMCore();
        m.put("11","111");

        Object clone = UtilsTest.clone(m,Serializer.JAVA);
        assertEquals(LinkedHashMap.class, clone.getClass());
        assertEquals(new LinkedHashMap(m), clone);
    }


    public void testWriteReplace2() throws IOException {
        File f = File.createTempFile("mapdb","mapdb");
        Map m = new MM();
        m.put("11", "111");
        DB db = DBMaker.newFileDB(f).transactionDisable().cacheDisable().make();
        db.getTreeMap("map").put("key",m);
        db.commit();
        db.close();

        db = DBMaker.newFileDB(f).transactionDisable().cacheDisable().make();

        assertEquals(new LinkedHashMap(m), db.getTreeMap("map").get("key"));
    }

    public void testWriteReplaceWrap() throws ObjectStreamException {
        Map m = new MM();
        m.put("11","111");
        assertEquals(new LinkedHashMap(m),UtilsTest.clone(m,p));
    }

    public void testWriteReplace2Wrap() throws IOException {
        File f = File.createTempFile("mapdb","mapdb");
        SS m = new SS(new MM());
        m.mm.put("11", "111");
        DB db = DBMaker.newFileDB(f).transactionDisable().cacheDisable().make();
        db.getTreeMap("map").put("key",m);
        db.commit();
        db.close();

        db = DBMaker.newFileDB(f).transactionDisable().cacheDisable().make();

        assertEquals(new LinkedHashMap(m.mm), ((SS)db.getTreeMap("map").get("key")).mm);
    }


}
