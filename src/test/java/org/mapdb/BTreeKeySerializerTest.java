package org.mapdb;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class BTreeKeySerializerTest {

    @Test public void testLong(){
        DB db = DBMaker.newMemoryDB()
                .cacheDisable()
                .make();
        Map m = db.createTreeMap("test",32,false,false,
                BTreeKeySerializer.ZERO_OR_POSITIVE_LONG,
                null, null );

        for(long i = 0; i<1000;i++){
            m.put(i*i,i*i+1);
        }

        for(long i = 0; i<1000;i++){
            assertEquals(i * i + 1, m.get(i * i));
        }
    }


    @Test public void testString(){


        DB db = DBMaker.newMemoryDB()
                .cacheDisable()
                .make();
        Map m = db.createTreeMap("test",32,false, false,
                BTreeKeySerializer.STRING,
                null, null );

        List<String> list = new ArrayList <String>();
        for(long i = 0; i<1000;i++){
            String s = ""+ Math.random()+(i*i*i);
            m.put(s,s+"aa");
        }

        for(String s:list){
            assertEquals(s+"aa",m.get(s));
        }
    }

    final BTreeKeySerializer.Tuple2KeySerializer tuple2_serializer = new BTreeKeySerializer.Tuple2KeySerializer(Utils.COMPARABLE_COMPARATOR,
            Serializer.BASIC_SERIALIZER, Serializer.BASIC_SERIALIZER);

    @Test public void tuple2_simple() throws IOException {
        List<Fun.Tuple2<String,Integer>> v = new ArrayList<Fun.Tuple2<String, Integer>>();

        v.add(null);
        v.add(Fun.t2("aa",1));
        v.add(Fun.t2("aa",2));
        v.add(Fun.t2("aa",3));
        v.add(Fun.t2("bb",3));
        v.add(Fun.t2("zz",1));
        v.add(Fun.t2("zz",2));
        v.add(Fun.t2("zz",3));
        v.add(null);

        DataOutput2 out = new DataOutput2();

        tuple2_serializer.serialize(out, 1, v.size() - 1, v.toArray());

        DataInput2 in = new DataInput2(out.copyBytes());
        Object[] nn = tuple2_serializer.deserialize(in,1,v.size()-1, v.size());

        assertArrayEquals(v.toArray(), nn);

    }
    @Test public void tuple2() throws IOException {
        List<Fun.Tuple2<String,Integer>> v = new ArrayList<Fun.Tuple2<String, Integer>>();

        v.add(null);

        for(String s: new String[]{"aa","bb","oper","zzz"}){
            for(int i = 2;i<1000;i+=i/2){
                v.add(Fun.t2(s,i));
            }
        }

        v.add(null);

        DataOutput2 out = new DataOutput2();
        tuple2_serializer.serialize(out, 1, v.size() - 1, v.toArray());

        DataInput2 in = new DataInput2(out.copyBytes());
        Object[] nn = tuple2_serializer.deserialize(in,1,v.size()-1, v.size());

        assertArrayEquals(v.toArray(), nn);

    }


    final BTreeKeySerializer.Tuple3KeySerializer tuple3_serializer = new BTreeKeySerializer.Tuple3KeySerializer(
            Utils.COMPARABLE_COMPARATOR,Utils.COMPARABLE_COMPARATOR,
            Serializer.BASIC_SERIALIZER, Serializer.BASIC_SERIALIZER, Serializer.BASIC_SERIALIZER);

    @Test public void tuple3_simple() throws IOException {
        List<Fun.Tuple3<String,Integer, Double>> v = new ArrayList<Fun.Tuple3<String, Integer, Double>>();

        v.add(null);
        v.add(Fun.t3("aa",1,1D));
        v.add(Fun.t3("aa",1,2D));
        v.add(Fun.t3("aa",2,2D));
        v.add(Fun.t3("aa",3,2D));
        v.add(Fun.t3("aa",3,3D));
        v.add(Fun.t3("zz",1,2D));
        v.add(Fun.t3("zz",2,2D));
        v.add(Fun.t3("zz",3,2D));
        v.add(null);

        DataOutput2 out = new DataOutput2();

        tuple3_serializer.serialize(out, 1, v.size() - 1, v.toArray());

        DataInput2 in = new DataInput2(out.copyBytes());
        Object[] nn = tuple3_serializer.deserialize(in,1,v.size()-1, v.size());

        assertArrayEquals(v.toArray(), nn);

    }
    @Test public void tuple3() throws IOException {
        List<Fun.Tuple3<String,Integer,Double>> v = new ArrayList<Fun.Tuple3<String, Integer,Double>>();

        v.add(null);

        for(String s: new String[]{"aa","bb","oper","zzz"}){
            for(int i = 2;i<1000;i+=i/2){
                for(double d = 2D;i<1000;i+=i/2){
                    v.add(Fun.t3(s,i,d));
                }
            }
        }

        v.add(null);

        DataOutput2 out = new DataOutput2();
        tuple3_serializer.serialize(out, 1, v.size() - 1, v.toArray());

        DataInput2 in = new DataInput2(out.copyBytes());
        Object[] nn = tuple3_serializer.deserialize(in,1,v.size()-1, v.size());

        assertArrayEquals(v.toArray(), nn);

    }

    final BTreeKeySerializer.Tuple4KeySerializer tuple4_serializer = new BTreeKeySerializer.Tuple4KeySerializer(
            Utils.COMPARABLE_COMPARATOR,Utils.COMPARABLE_COMPARATOR,Utils.COMPARABLE_COMPARATOR,
            Serializer.BASIC_SERIALIZER, Serializer.BASIC_SERIALIZER, Serializer.BASIC_SERIALIZER, Serializer.BASIC_SERIALIZER);

    @Test public void tuple4_simple() throws IOException {
        List<Fun.Tuple4<String,Integer, Double, Long>> v = new ArrayList<Fun.Tuple4<String, Integer, Double,Long>>();

        v.add(null);
        v.add(Fun.t4("aa",1,1D,1L));
        v.add(Fun.t4("aa",1,1D,2L));
        v.add(Fun.t4("aa",1,2D,2L));
        v.add(Fun.t4("aa",2,2D,2L));
        v.add(Fun.t4("aa",3,2D,2L));
        v.add(Fun.t4("aa",3,3D,2L));
        v.add(Fun.t4("zz",1,2D,2L));
        v.add(Fun.t4("zz",2,2D,2L));
        v.add(Fun.t4("zz",3,2D,2L));
        v.add(null);

        DataOutput2 out = new DataOutput2();

        tuple4_serializer.serialize(out, 1, v.size() - 1, v.toArray());

        DataInput2 in = new DataInput2(out.copyBytes());
        Object[] nn = tuple4_serializer.deserialize(in,1,v.size()-1, v.size());

        assertArrayEquals(v.toArray(), nn);

    }
    @Test public void tuple4() throws IOException {
        List<Fun.Tuple4<String,Integer,Double, Long>> v = new ArrayList<Fun.Tuple4<String, Integer,Double,Long>>();

        v.add(null);

        for(String s: new String[]{"aa","bb","oper","zzz"}){
            for(int i = 2;i<1000;i+=i/2){
                for(double d = 2D;i<1000;i+=i/2){
                    for(long l = 3L;i<10000;i+=i/2){
                        v.add(Fun.t4(s,i,d,l));
                    }
                }
            }
        }

        v.add(null);

        DataOutput2 out = new DataOutput2();
        tuple4_serializer.serialize(out, 1, v.size() - 1, v.toArray());

        DataInput2 in = new DataInput2(out.copyBytes());
        Object[] nn = tuple4_serializer.deserialize(in,1,v.size()-1, v.size());

        assertArrayEquals(v.toArray(), nn);

    }

}