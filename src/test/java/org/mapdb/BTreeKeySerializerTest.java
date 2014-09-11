package org.mapdb;

import org.junit.Test;

import java.io.DataInput;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mapdb.BTreeKeySerializer.*;

@SuppressWarnings({"rawtypes","unchecked"})
public class BTreeKeySerializerTest {

    @Test public void testLong(){
        DB db = DBMaker.newMemoryDB()
                .cacheDisable()
                .make();
        Map m = db.createTreeMap("test")
                .keySerializer(BTreeKeySerializer.LONG)
                .make();

        for(long i = 0; i<1000;i++){
            m.put(i*i,i*i+1);
        }

        for(long i = 0; i<1000;i++){
            assertEquals(i * i + 1, m.get(i * i));
        }
    }


    void checkKeyClone(BTreeKeySerializer ser, Object[] keys) throws IOException {
        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        ser.serialize(out,ser.arrayToKeys(keys));
        DataIO.DataInputByteArray in = new DataIO.DataInputByteArray(out.copyBytes());

        Object[] keys2 = ser.keysToArray(ser.deserialize(in,keys.length));
        assertEquals(in.pos, out.pos);

        assertArrayEquals(keys,keys2);
    }

    @Test public void testLong2() throws IOException {
        Object[][] vals = new Object[][]{
                {Long.MIN_VALUE,Long.MAX_VALUE},
                {Long.MIN_VALUE,1L,Long.MAX_VALUE},
                {-1L,0L,1L},
                {-1L,Long.MAX_VALUE}
        };

        for(Object[] v:vals){
            checkKeyClone(BTreeKeySerializer.LONG, v);
        }
    }

    @Test public void testInt2() throws IOException {
        Object[][] vals = new Object[][]{
                {Integer.MIN_VALUE,Integer.MAX_VALUE},
                {Integer.MIN_VALUE,1,Integer.MAX_VALUE},
                {-1,0,1},
                {-1,Integer.MAX_VALUE}
        };

        for(Object[] v:vals){
            checkKeyClone(BTreeKeySerializer.INTEGER, v);
        }
    }


    @Test public void testString(){


        DB db = DBMaker.newMemoryDB()
                .cacheDisable()
                .make();
        Map m =  db.createTreeMap("test")
                .keySerializer(BTreeKeySerializer.STRING)
                .make();


        List<String> list = new ArrayList <String>();
        for(long i = 0; i<1000;i++){
            String s = ""+ Math.random()+(i*i*i);
            m.put(s,s+"aa");
        }

        for(String s:list){
            assertEquals(s+"aa",m.get(s));
        }
    }

    final BTreeKeySerializer.Tuple2KeySerializer tuple2_serializer = new BTreeKeySerializer.Tuple2KeySerializer(
            Fun.COMPARATOR, Fun.COMPARATOR,
            Serializer.BASIC, Serializer.BASIC);

    @Test public void tuple2_simple() throws IOException {
        List<Fun.Tuple2<String,Integer>> v = new ArrayList<Fun.Tuple2<String, Integer>>();


        v.add(Fun.t2("aa",1));
        v.add(Fun.t2("aa",2));
        v.add(Fun.t2("aa",3));
        v.add(Fun.t2("bb",3));
        v.add(Fun.t2("zz",1));
        v.add(Fun.t2("zz",2));
        v.add(Fun.t2("zz",3));


        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();

        tuple2_serializer.serialize(out, v.toArray(new Fun.Tuple2[0]));

        DataInput in = new DataIO.DataInputByteArray(out.copyBytes());
        Object[] nn = tuple2_serializer.deserialize(in, v.size());

        assertArrayEquals(v.toArray(), nn);

    }
    @Test public void tuple2() throws IOException {
        List<Fun.Tuple2<String,Integer>> v = new ArrayList<Fun.Tuple2<String, Integer>>();



        for(String s: new String[]{"aa","bb","oper","zzz"}){
            for(int i = 2;i<1000;i+=i/2){
                v.add(Fun.t2(s,i));
            }
        }



        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        tuple2_serializer.serialize(out, v.toArray(new Fun.Tuple2[0]));

        DataInput in = new DataIO.DataInputByteArray(out.copyBytes());
        Object[] nn = tuple2_serializer.deserialize(in, v.size());

        assertArrayEquals(v.toArray(), nn);

    }


    @Test public void tuple3_simple() throws IOException {
        List<Fun.Tuple3<String,Integer, Double>> v = new ArrayList<Fun.Tuple3<String, Integer, Double>>();


        v.add(Fun.t3("aa",1,1D));
        v.add(Fun.t3("aa",1,2D));
        v.add(Fun.t3("aa",2,2D));
        v.add(Fun.t3("aa",3,2D));
        v.add(Fun.t3("aa",3,3D));
        v.add(Fun.t3("zz",1,2D));
        v.add(Fun.t3("zz",2,2D));
        v.add(Fun.t3("zz",3,2D));

        Object[] vv = (Object[]) TUPLE3.arrayToKeys(v.toArray());

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();

        TUPLE3.serialize(out, vv);

        DataInput in = new DataIO.DataInputByteArray(out.copyBytes());
        Object[] nn = (Object[]) TUPLE3.deserialize(in, v.size());

        assertArrayEquals(vv, nn);

    }
    @Test public void tuple3() throws IOException {
        List<Fun.Tuple3<String,Integer,Double>> v = new ArrayList<Fun.Tuple3<String, Integer,Double>>();



        for(String s: new String[]{"aa","bb","oper","zzz"}){
            for(int i = 2;i<1000;i+=i/2){
                for(double d = 2D;i<1000;i+=i/2){
                    v.add(Fun.t3(s,i,d));
                }
            }
        }

        Object[] vv = (Object[]) TUPLE3.arrayToKeys(v.toArray());

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        TUPLE3.serialize(out,  vv);

        DataInput in = new DataIO.DataInputByteArray(out.copyBytes());
        Object[] nn = (Object[]) TUPLE3.deserialize(in,v.size());

        assertArrayEquals(vv, nn);

    }

    @Test public void tuple4_simple() throws IOException {
        List<Fun.Tuple4<String,Integer, Double, Long>> v = new ArrayList<Fun.Tuple4<String, Integer, Double,Long>>();


        v.add(Fun.t4("aa",1,1D,1L));
        v.add(Fun.t4("aa",1,1D,2L));
        v.add(Fun.t4("aa",1,2D,2L));
        v.add(Fun.t4("aa",2,2D,2L));
        v.add(Fun.t4("aa",3,2D,2L));
        v.add(Fun.t4("aa",3,3D,2L));
        v.add(Fun.t4("zz",1,2D,2L));
        v.add(Fun.t4("zz",2,2D,2L));
        v.add(Fun.t4("zz",3,2D,2L));

        Object[] vv = (Object[]) TUPLE4.arrayToKeys(v.toArray());

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();

        TUPLE4.serialize(out, vv);

        DataInput in = new DataIO.DataInputByteArray(out.copyBytes());
        Object[] nn = (Object[]) TUPLE4.deserialize(in, v.size());

        assertArrayEquals(vv, nn);

    }
    @Test public void tuple4() throws IOException {
        List<Fun.Tuple4<String,Integer,Double, Long>> v = new ArrayList<Fun.Tuple4<String, Integer,Double,Long>>();



        for(String s: new String[]{"aa","bb","oper","zzz"}){
            for(int i = 2;i<1000;i+=i/2){
                for(double d = 2D;i<1000;i+=i/2){
                    for(long l = 3L;i<10000;i+=i/2){
                        v.add(Fun.t4(s,i,d,l));
                    }
                }
            }
        }

        Object[] vv = (Object[]) TUPLE4.arrayToKeys(v.toArray());

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        TUPLE4.serialize(out, vv);

        DataInput in = new DataIO.DataInputByteArray(out.copyBytes());
        Object[] nn = (Object[]) TUPLE4.deserialize(in, v.size());

        assertArrayEquals(vv, nn);

    }

    @Test public void tuple5_simple() throws IOException {
        List<Fun.Tuple5<String,Integer, Double, Long,String>> v = new ArrayList<Fun.Tuple5<String, Integer, Double,Long,String>>();


        v.add(Fun.t5("aa",1,1D,1L,"zz"));
        v.add(Fun.t5("aa",1,1D,2L,"zz"));
        v.add(Fun.t5("aa",1,2D,2L,"zz"));
        v.add(Fun.t5("aa",2,2D,2L,"zz"));
        v.add(Fun.t5("aa",3,2D,2L,"zz"));
        v.add(Fun.t5("aa",3,3D,2L,"zz"));
        v.add(Fun.t5("zz",1,2D,2L,"zz"));
        v.add(Fun.t5("zz",2,2D,2L,"zz"));
        v.add(Fun.t5("zz",3,2D,2L,"zz"));

        Object[] vv = (Object[]) TUPLE5.arrayToKeys(v.toArray());
        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();

        TUPLE5.serialize(out,vv);

        DataInput in = new DataIO.DataInputByteArray(out.copyBytes());
        Object[] nn = (Object[]) TUPLE5.deserialize(in, v.size());

        assertArrayEquals(vv, nn);

    }
    @Test public void tuple5() throws IOException {
        List<Fun.Tuple5<String,Integer,Double, Long,String>> v = new ArrayList<Fun.Tuple5<String, Integer,Double,Long,String>>();



        String[] ss = new String[]{"aa","bb","oper","zzz"};
        for(String s: ss){
            for(int i = 2;i<1000;i+=i/2){
                for(double d = 2D;i<1000;i+=i/2){
                    for(long l = 3L;i<10000;i+=i/2){
                        for(String s2: ss){
                            v.add(Fun.t5(s,i,d,l,s2));
                        }
                    }
                }
            }
        }

        Object[] vv = (Object[]) TUPLE5.arrayToKeys(v.toArray());

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        TUPLE5.serialize(out, vv);

        DataInput in = new DataIO.DataInputByteArray(out.copyBytes());
        Object[] nn = (Object[]) TUPLE5.deserialize(in,v.size());

        assertArrayEquals(vv, nn);

    }

    @Test public void tuple6_simple() throws IOException {
        List<Fun.Tuple6<String,Integer, Double, Long,String,String>> v = new ArrayList<Fun.Tuple6<String, Integer, Double,Long,String,String>>();


        v.add(Fun.t6("aa",1,1D,1L,"zz","asd"));
        v.add(Fun.t6("aa",1,1D,2L,"zz","asd"));
        v.add(Fun.t6("aa",1,2D,2L,"zz","asd"));
        v.add(Fun.t6("aa",2,2D,2L,"zz","asd"));
        v.add(Fun.t6("aa",3,2D,2L,"zz","asd"));
        v.add(Fun.t6("aa",3,3D,2L,"zz","asd"));
        v.add(Fun.t6("zz",1,2D,2L,"zz","asd"));
        v.add(Fun.t6("zz",2,2D,2L,"zz","asd"));
        v.add(Fun.t6("zz",3,2D,2L,"zz","asd"));

        Object[] vv = (Object[]) TUPLE6.arrayToKeys(v.toArray());
        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();

        TUPLE6.serialize(out, vv);

        DataInput in = new DataIO.DataInputByteArray(out.copyBytes());
        Object[] nn = (Object[]) TUPLE6.deserialize(in,  v.size());

        assertArrayEquals(vv, nn);

    }
    @Test public void tuple6() throws IOException {
        List<Fun.Tuple6<String,Integer,Double, Long,String,String>> v = new ArrayList<Fun.Tuple6<String, Integer,Double,Long,String,String>>();



        String[] ss = new String[]{"aa","bb","oper","zzz","asd"};
        for(String s: ss){
            for(int i = 2;i<1000;i+=i/2){
                for(double d = 2D;i<1000;i+=i/2){
                    for(long l = 3L;i<10000;i+=i/2){
                        for(String s2: ss){
                            for(String s3: ss){
                                v.add(Fun.t6(s,i,d,l,s2,s3));
                            }
                        }
                    }
                }
            }
        }

        Object[] vv = (Object[]) TUPLE6.arrayToKeys(v.toArray());

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        TUPLE6.serialize(out, vv);

        DataInput in = new DataIO.DataInputByteArray(out.copyBytes());
        Object[] nn = (Object[]) TUPLE6.deserialize(in,  v.size());

        assertArrayEquals(vv, nn);

    }


    @Test public void testUUID() throws IOException {
        List<java.util.UUID> ids = new ArrayList<java.util.UUID>();
        for(int i=0;i<100;i++)
            ids.add(java.util.UUID.randomUUID());

        long[] vv = (long[]) UUID.arrayToKeys(ids.toArray());

        int i=0;
        for(java.util.UUID u:ids){
            assertEquals(u.getMostSignificantBits(),vv[i++]);
            assertEquals(u.getLeastSignificantBits(),vv[i++]);
        }

        //clone
        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        UUID.serialize(out, vv);

        DataInput in = new DataIO.DataInputByteArray(out.copyBytes());
        long[] nn = (long[]) UUID.deserialize(in,  ids.size());

        assertArrayEquals(vv, nn);

        //test key addition
        java.util.UUID r = java.util.UUID.randomUUID();
        ids.add(10,r);
        long[] vv2 = (long[]) UUID.putKey(vv,10,r);
        i=0;
        for(java.util.UUID u:ids){
            assertEquals(u.getMostSignificantBits(),vv2[i++]);
            assertEquals(u.getLeastSignificantBits(),vv2[i++]);
        }

        vv2 = (long[]) UUID.deleteKey(vv2,10);

        assertArrayEquals(vv,vv2);
    }

    void randomSerializer(BTreeKeySerializer ser, Fun.Function0 fab){
        Set keys2 = new TreeSet(ser.comparator());

        for(int i=0;i<3;i++){
            keys2.add(fab.run());
        }
        Object keys = ser.arrayToKeys(keys2.toArray());

        for(int i=0;i<1e3;i++){
            Object key = fab.run();
            long[] child = new long[keys2.size()];
            for(int ii=0;ii<child.length;ii++){
                child[ii] = ii+1;
            }


            BTreeMap.BNode node = new BTreeMap.DirNode(keys,false,false,false, child);
            int pos = ser.findChildren(node,key);

            if(keys2.contains(key)){
                //check that it is contained
                int keyPos = ser.findChildren(node,key);
                Object key2 = ser.getKey(keys,keyPos);
                assertTrue(ser.comparator().compare(key,key2)==0);
            }else{
                //add it
                keys = ser.putKey(keys,pos,key);
                keys2.add(key);
            }
            assertArrayEquals(keys2.toArray(),  ser.keysToArray(keys));


            if(i%10==0){
                //test random split
                int split = r.nextInt(keys2.size());

                //first half
                assertArrayEquals(
                        Arrays.copyOf(keys2.toArray(),split),
                        ser.keysToArray(ser.copyOfRange(keys, 0, split))
                );

                //second half
                assertArrayEquals(
                        Arrays.copyOfRange(keys2.toArray(),split, keys2.size()),
                        ser.keysToArray(ser.copyOfRange(keys, split, keys2.size()))
                );
            }

            if(i%9==0){
                //test key removal
                int del = r.nextInt(keys2.size());
                Object kk = ser.getKey(keys, del);
                keys = ser.deleteKey(keys,del);
                keys2.remove(kk);
                assertArrayEquals(keys2.toArray(),  ser.keysToArray(keys));
            }
        }

    }

    Random r = new Random();

    @Test public void positive_Integer(){
        randomSerializer(BTreeKeySerializer.INTEGER, new Fun.Function0() {

            @Override
            public Object run() {
                return Math.abs(r.nextInt());
            }
        });
    }


    @Test public void positive_Long(){
        randomSerializer(BTreeKeySerializer.LONG, new Fun.Function0() {

            @Override
            public Object run() {
                return Math.abs(r.nextLong());
            }
        });
    }


    @Test public void uuid(){
        randomSerializer(BTreeKeySerializer.UUID, new Fun.Function0() {

            @Override
            public Object run() {
                return new UUID(r.nextLong(), r.nextLong());
            }
        });
    }


    @Test public void basic(){
        randomSerializer(BTreeKeySerializer.BASIC, new Fun.Function0() {

            @Override
            public Object run() {
                return new UUID(r.nextLong(), r.nextLong());
            }
        });
    }

    @Test public void string2(){
        randomSerializer(BTreeKeySerializer.STRING2, new Fun.Function0() {

            @Override
            public Object run() {
                int size = r.nextInt(100);
                return UtilsTest.randomString(size);
            }
        });
    }


    @Test public void string(){
        randomSerializer(BTreeKeySerializer.STRING, new Fun.Function0() {

            @Override
            public Object run() {
                int size = r.nextInt(100);
                return UtilsTest.randomString(size);
            }
        });
    }



    @Test public void tuple2_random(){
        randomSerializer(BTreeKeySerializer.TUPLE2, new Fun.Function0() {
            @Override
            public Object run() {
                return new Fun.Tuple2("aa",r.nextInt());
            }
        });
    }

    @Test public void tuple2_random2(){
        randomSerializer(BTreeKeySerializer.TUPLE2, new Fun.Function0() {
            @Override
            public Object run() {
                return new Fun.Tuple2(r.nextInt(),r.nextInt());
            }
        });
    }

    @Test public void tuple3_random(){
        randomSerializer(BTreeKeySerializer.TUPLE3, new Fun.Function0() {
            @Override
            public Object run() {
                return new Fun.Tuple3("aa","bb",r.nextInt());
            }
        });
    }

    @Test public void tuple3_random2(){
        randomSerializer(BTreeKeySerializer.TUPLE3, new Fun.Function0() {
            @Override
            public Object run() {
                return new Fun.Tuple3(r.nextInt(),r.nextInt(),r.nextInt());
            }
        });
    }


    @Test public void tuple3_random3(){
        randomSerializer(BTreeKeySerializer.TUPLE3, new Fun.Function0() {
            @Override
            public Object run() {
                return new Fun.Tuple3("aa",r.nextInt(),"bb");
            }
        });
    }

    @Test public void tuple3_random4(){
        randomSerializer(BTreeKeySerializer.TUPLE3, new Fun.Function0() {
            @Override
            public Object run() {
                return new Fun.Tuple3(r.nextInt(),"aa","bb");
            }
        });
    }

}