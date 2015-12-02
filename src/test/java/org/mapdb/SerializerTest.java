package org.mapdb;

import org.junit.Test;

import java.io.*;
import java.util.*;

import static org.junit.Assert.*;

@SuppressWarnings({"rawtypes","unchecked"})
public class SerializerTest {

    @Test public void UUID2(){
        UUID u = UUID.randomUUID();
        assertEquals(u, SerializerBaseTest.clone2(u,Serializer.UUID));
    }

    @Test public void string_ascii(){
        String s = "adas9 asd9009asd";
        assertEquals(s, SerializerBaseTest.clone2(s, Serializer.STRING_ASCII));
        s = "";
        assertEquals(s, SerializerBaseTest.clone2(s, Serializer.STRING_ASCII));
        s = "    ";
        assertEquals(s, SerializerBaseTest.clone2(s, Serializer.STRING_ASCII));
    }

    @Test public void compression_wrapper() throws IOException {
        byte[] b = new byte[100];
        new Random().nextBytes(b);
        Serializer<byte[]> ser = new Serializer.CompressionWrapper(Serializer.BYTE_ARRAY);
        assertTrue(Serializer.BYTE_ARRAY.equals(b, SerializerBaseTest.clone2(b, ser)));

        b = Arrays.copyOf(b, 10000);
        assertTrue(Serializer.BYTE_ARRAY.equals(b, SerializerBaseTest.clone2(b, ser)));

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        ser.serialize(out, b);
        assertTrue(out.pos < 1000);
    }

    @Test public void java_serializer_issue536(){
        Long l = 1111L;
        assertEquals(l, SerializerBaseTest.clone2(l, Serializer.JAVA));
    }


    @Test public void java_serializer_issue536_with_engine(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        Long l = 1111L;
        long recid = db.engine.put(l,Serializer.JAVA);
        assertEquals(l, db.engine.get(recid, Serializer.JAVA));
    }


    @Test public void java_serializer_issue536_with_map() {
        DB db = DBMaker.memoryDB().transactionDisable().make();
        Map m = db.hashMapCreate("map")
                .keySerializer(Serializer.JAVA)
                .make();
        Long l = 1111L;
        m.put(l, l);
        assertEquals(l, m.get(l));
    }

    @Test public void array(){
        Serializer.Array s = new Serializer.Array(Serializer.INTEGER);

        Object[] a = new Object[]{1,2,3,4};

        assertTrue(Arrays.equals(a, (Object[]) TT.clone(a, s)));
        assertEquals(s, TT.clone(s, Serializer.BASIC));
    }

    void testLong(Serializer<Long> ser){
        for(Long i= (long) -1e5;i<1e5;i++){
            assertEquals(i, TT.clone(i, ser));
        }

        for(Long i=0L;i>0;i+=1+i/10000){
            assertEquals(i, TT.clone(i, ser));
            assertEquals(new Long(-i), TT.clone(-i, ser));
        }

        Random r = new Random();
        for(int i=0;i<1e6;i++){
            Long a = r.nextLong();
            assertEquals(a, TT.clone(a, ser));
        }

    }

    @Test public void Long(){
        testLong(Serializer.LONG);
    }


    @Test public void Long_packed(){
        testLong(Serializer.LONG_PACKED);
    }



    void testInt(Serializer<Integer> ser){
        for(Integer i= (int) -1e5;i<1e5;i++){
            assertEquals(i, TT.clone(i, ser));
        }

        for(Integer i=0;i>0;i+=1+i/10000){
            assertEquals(i, TT.clone(i, ser));
            assertEquals(new Long(-i), TT.clone(-i, ser));
        }

        Random r = new Random();
        for(int i=0;i<1e6;i++){
            Integer a = r.nextInt();
            assertEquals(a, TT.clone(a, ser));
        }
    }

    @Test public void Int(){
        testInt(Serializer.INTEGER);
    }


    @Test public void Int_packed(){
        testInt(Serializer.INTEGER_PACKED);
    }

    @Test public void deflate_wrapper(){
        Serializer.CompressionDeflateWrapper c =
                new Serializer.CompressionDeflateWrapper(Serializer.BYTE_ARRAY, -1,
                        new byte[]{1,1,1,1,1,1,1,1,1,1,1,23,4,5,6,7,8,9,65,2});

        byte[] b = new byte[]{1,1,1,1,1,1,1,1,1,1,1,1,4,5,6,3,3,3,3,35,6,67,7,3,43,34};

        assertTrue(Arrays.equals(b, TT.<byte[]>clone(b, c)));
    }

    @Test public void deflate_wrapper_values(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        Map m = db.treeMapCreate("a")
                .valueSerializer(new Serializer.CompressionDeflateWrapper(Serializer.LONG))
                .keySerializer(Serializer.LONG)
                .make();

        for(long i=0;i<1000;i++){
            m.put(i,i*10);
        }

        for(long i=0;i<1000;i++){
            assertEquals(i*10,m.get(i));
        }
    }


    @Test public void compress_wrapper_values(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        Map m = db.treeMapCreate("a")
                .valueSerializer(new Serializer.CompressionWrapper(Serializer.LONG))
                .keySerializer(Serializer.LONG)
                .make();

        for(long i=0;i<1000;i++){
            m.put(i,i*10);
        }

        for(long i=0;i<1000;i++){
            assertEquals(i * 10, m.get(i));
        }
    }


    static final class StringS implements Comparable<StringS>{
        final String s;

        StringS(String s) {
            this.s = s;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            StringS stringS = (StringS) o;

            return !(s != null ? !s.equals(stringS.s) : stringS.s != null);

        }

        @Override
        public int hashCode() {
            return s != null ? s.hashCode() : 0;
        }

        @Override
        public int compareTo(StringS o) {
            return s.compareTo(o.s);
        }
    }

    static final class StringSSerializer extends Serializer<StringS> implements Serializable {

        @Override
        public void serialize(DataOutput out, StringS value) throws IOException {
            out.writeUTF(value.s);
        }

        @Override
        public StringS deserialize(DataInput in, int available) throws IOException {
            return new StringS(in.readUTF());
        }
    }
    @Test public void issue546() throws IOException {
        File f = File.createTempFile("mapdbTest","mapdb");
        DB db = DBMaker
                .fileDB(f)
                .transactionDisable()
                .make();



        BTreeKeySerializer XYZ_SERIALIZER = new BTreeKeySerializer.ArrayKeySerializer(
                new Comparator[]{Fun.COMPARATOR,Fun.COMPARATOR},
                new Serializer[]{new StringSSerializer(), new StringSSerializer()}
        );

        NavigableSet<Object[]> multiMap = db.treeSetCreate("xyz")
                .serializer(XYZ_SERIALIZER)
                .make();

        multiMap.add(new Object[]{new StringS("str1"), new StringS("str2")});
        db.close();

        db = DBMaker
                .fileDB(f)
                .transactionDisable()
                .asyncWriteEnable()
                .make();


        multiMap = db.treeSetCreate("xyz")
                .serializer(XYZ_SERIALIZER)
                .makeOrGet();

        assertEquals(1, multiMap.size());
        assertTrue(multiMap.contains(new Object[]{new StringS("str1"), new StringS("str2")}));
        db.close();

    }

}
