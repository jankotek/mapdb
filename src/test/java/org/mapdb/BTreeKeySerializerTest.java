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
        randomSerializer(BTreeKeySerializer.ARRAY2, new Fun.Function0() {
            @Override
            public Object run() {
                return new Object[]{"aa",r.nextInt()};
            }
        });
    }

    @Test public void tuple2_random2(){
        randomSerializer(BTreeKeySerializer.ARRAY2, new Fun.Function0() {
            @Override
            public Object run() {
                return new Object[]{r.nextInt(),r.nextInt()};
            }
        });
    }

    @Test public void tuple3_random(){
        randomSerializer(BTreeKeySerializer.ARRAY3, new Fun.Function0() {
            @Override
            public Object run() {
                return new Object[]{"aa","bb",r.nextInt()};
            }
        });
    }

    @Test public void tuple3_random2(){
        randomSerializer(BTreeKeySerializer.ARRAY3, new Fun.Function0() {
            @Override
            public Object run() {
                return new Object[]{r.nextInt(),r.nextInt(),r.nextInt()};
            }
        });
    }


    @Test public void tuple3_random3(){
        randomSerializer(BTreeKeySerializer.ARRAY3, new Fun.Function0() {
            @Override
            public Object run() {
                return new Object[]{"aa",r.nextInt(),"bb"};
            }
        });
    }

    @Test public void tuple3_random4(){
        randomSerializer(BTreeKeySerializer.ARRAY3, new Fun.Function0() {
            @Override
            public Object run() {
                return new Object[]{r.nextInt(),"aa","bb"};
            }
        });
    }

}