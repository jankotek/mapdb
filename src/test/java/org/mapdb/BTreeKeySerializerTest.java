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
        DB db = DBMaker.memoryDB()
                .transactionDisable()
                .make();
        Map m = db.treeMapCreate("test")
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


        DB db = DBMaker.memoryDB()
                .transactionDisable()
                .make();
        Map m =  db.treeMapCreate("test")
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
            int[] child = new int[keys2.size()];
            for(int ii=0;ii<keys2.size();ii++){
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
                return TT.randomString(size);
            }
        });
    }


    @Test public void string(){
        randomSerializer(BTreeKeySerializer.STRING, new Fun.Function0() {

            @Override
            public Object run() {
                int size = r.nextInt(100);
                return TT.randomString(size);
            }
        });
    }


    @Test public void stringUTF(){
        randomSerializer(BTreeKeySerializer.STRING, new Fun.Function0() {

            @Override
            public Object run() {
                int size = r.nextInt(100);
                return TT.randomString(size)+((char)200);
            }
        });
    }

    @Test public void string2UTF(){
        randomSerializer(BTreeKeySerializer.STRING2, new Fun.Function0() {

            @Override
            public Object run() {
                int size = r.nextInt(100);
                return TT.randomString(size)+((char)200);
            }
        });
    }

    @Test public void stringUTFXX(){
        randomSerializer(BTreeKeySerializer.STRING, new Fun.Function0() {

            @Override
            public Object run() {
                int size = r.nextInt(100);
                return TT.randomString(size)+((char)2222);
            }
        });
    }

    @Test public void string2UTFXX(){
        randomSerializer(BTreeKeySerializer.STRING2, new Fun.Function0() {

            @Override
            public Object run() {
                int size = r.nextInt(100);
                return TT.randomString(size)+((char)2222);
            }
        });
    }


    @Test public void compress_tuple2_random(){
        randomSerializer(new  BTreeKeySerializer.Compress(BTreeKeySerializer.ARRAY2), new Fun.Function0() {
            @Override
            public Object run() {
                return new Object[]{"aa",r.nextInt()};
            }
        });
    }

    @Test public void compress_basic_random(){
        randomSerializer(new  BTreeKeySerializer.Compress(BTreeKeySerializer.BASIC), new Fun.Function0() {
            @Override
            public Object run() {
                return TT.randomString(100);
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



    @Test public void string_formats_compatible() throws IOException {
        ArrayList keys = new ArrayList();
        for(int i=0;i<1000;i++){
            keys.add("common prefix "+ TT.randomString(10 + new Random().nextInt(100)));
        }

        checkStringSerializers(keys);
    }


    @Test public void string_formats_compatible_no_prefix() throws IOException {
        ArrayList keys = new ArrayList();
        for(int i=0;i<1000;i++){
            keys.add(TT.randomString(10 + new Random().nextInt(100)));
        }

        checkStringSerializers(keys);
    }

    @Test public void string_formats_compatible_equal_size() throws IOException {
        ArrayList keys = new ArrayList();
        for(int i=0;i<1000;i++){
            keys.add("common prefix "+ TT.randomString(10));
        }

        checkStringSerializers(keys);
    }



    public void checkStringSerializers(ArrayList keys) throws IOException {
        Collections.sort(keys);
        //first check clone on both
        checkKeyClone(BTreeKeySerializer.STRING,keys.toArray());
        checkKeyClone(BTreeKeySerializer.STRING2,keys.toArray());

        //now serializer and deserialize with other and compare
        {
            DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
            BTreeKeySerializer.STRING.serialize(out, BTreeKeySerializer.STRING.arrayToKeys(keys.toArray()));

            DataIO.DataInputByteArray in = new DataIO.DataInputByteArray(out.buf);
            Object[] keys2 = BTreeKeySerializer.STRING2.keysToArray(BTreeKeySerializer.STRING2.deserialize(in, keys.size()));

            assertArrayEquals(keys.toArray(), keys2);
        }

        {
            DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
            BTreeKeySerializer.STRING2.serialize(out, BTreeKeySerializer.STRING2.arrayToKeys(keys.toArray()));

            DataIO.DataInputByteArray in = new DataIO.DataInputByteArray(out.buf);
            Object[] keys2 = BTreeKeySerializer.STRING.keysToArray(BTreeKeySerializer.STRING.deserialize(in, keys.size()));

            assertArrayEquals(keys.toArray(), keys2);
        }

        //convert to byte[] and check with BYTE_ARRAY serializers
        for(int i=0;i<keys.size();i++){
            keys.set(i,((String)keys.get(i)).getBytes());
        }

        //first check clone on both
        checkKeyClone(BTreeKeySerializer.BYTE_ARRAY,keys.toArray());
        checkKeyClone(BTreeKeySerializer.BYTE_ARRAY2,keys.toArray());

        //now serializer and deserialize with other and compare
        {
            DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
            BTreeKeySerializer.BYTE_ARRAY.serialize(out, BTreeKeySerializer.BYTE_ARRAY.arrayToKeys(keys.toArray()));

            DataIO.DataInputByteArray in = new DataIO.DataInputByteArray(out.buf);
            Object[] keys2 = BTreeKeySerializer.BYTE_ARRAY2.keysToArray(BTreeKeySerializer.BYTE_ARRAY2.deserialize(in, keys.size()));

            assertArrayEquals(keys.toArray(), keys2);
        }

        {
            DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
            BTreeKeySerializer.BYTE_ARRAY2.serialize(out, BTreeKeySerializer.BYTE_ARRAY2.arrayToKeys(keys.toArray()));

            DataIO.DataInputByteArray in = new DataIO.DataInputByteArray(out.buf);
            Object[] keys2 = BTreeKeySerializer.BYTE_ARRAY.keysToArray(BTreeKeySerializer.BYTE_ARRAY.deserialize(in, keys.size()));

            assertArrayEquals(keys.toArray(), keys2);
        }

    }

    @Test public void stringPrefixLen(){
        checkPrefixLen(0, "");
        checkPrefixLen(4, "aaaa");
        checkPrefixLen(2, "aa","aaaa");
        checkPrefixLen(2, "aaaa","aa");
        checkPrefixLen(2, "aa","aabb");
        checkPrefixLen(2, "aaBB","aabb");
        checkPrefixLen(2, "aaBB","aabb","aabbaa");
        checkPrefixLen(2, "aabbaa","aaBB","aabb");
    }

    void checkPrefixLen(int expected, Object... keys){
        StringArrayKeys keys1 = BTreeKeySerializer.STRING.arrayToKeys(keys);
        assertEquals(expected, keys1.commonPrefixLen());

        char[][] keys2 = BTreeKeySerializer.STRING2.arrayToKeys(keys);
        assertEquals(expected, BTreeKeySerializer.commonPrefixLen(keys2));

    }


}