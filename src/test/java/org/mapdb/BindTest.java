package org.mapdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mapdb.Fun.*;

@SuppressWarnings({"rawtypes","unchecked"})
public class BindTest {

    BTreeMap<Integer,String> m;

    @Before
    public void init(){
        m = DBMaker.memoryDB().transactionDisable().make().treeMap("test");
    }


    @After
    public void close(){
        m.engine.close();
    }


    String[] split(String s){
        if(s==null) return null;
        String[] ret = new String[s.length()];
        for(int i=0;i<ret.length;i++)
            ret[i] = ""+s.charAt(i);
        return ret;
    }

    @Test public void secondary_values(){
        m.put(1,"jedna");
        m.put(2,"dve");

        Set<Object[]> sec = new TreeSet(Fun.COMPARABLE_ARRAY_COMPARATOR);

        Bind.secondaryValues(m,sec,new Function2<String[], Integer, String>() {
            @Override
            public String[] run(Integer integer, String s) {
                return split(s);
            }
        });

        //filled if empty
        assertEquals(5+3,sec.size());
        assert(sec.contains(new Object[]{2,"d"}));
        assert(sec.contains(new Object[]{2,"v"}));
        assert(sec.contains(new Object[]{2,"e"}));

        //old values preserved
        m.put(2,"dvea");
        assertEquals(5+4,sec.size());
        assert(sec.contains(new Object[]{2,"d"}));
        assert(sec.contains(new Object[]{2,"v"}));
        assert(sec.contains(new Object[]{2,"e"}));
        assert(sec.contains(new Object[]{2,"a"}));

        //old values deleted
        m.put(2,"dva");
        assertEquals(5+3,sec.size());
        assert(sec.contains(new Object[]{2,"d"}));
        assert(sec.contains(new Object[]{2,"v"}));
        assert(sec.contains(new Object[]{2,"a"}));

        //all removed on delete
        m.remove(2);
        assertEquals(5,sec.size());

        //all added on put
        m.put(2,"dva");
        assertEquals(5+3,sec.size());
        assert(sec.contains(new Object[]{2,"d"}));
        assert(sec.contains(new Object[]{2,"v"}));
        assert(sec.contains(new Object[]{2,"a"}));

    }

    @Test public void secondary_keys(){
        m.put(1,"jedna");
        m.put(2,"dve");

        Set<Object[]> sec = new TreeSet(Fun.COMPARABLE_ARRAY_COMPARATOR);

        Bind.secondaryKeys(m, sec, new Function2<String[], Integer, String>() {
            @Override
            public String[] run(Integer integer, String s) {
                return split(s);
            }
        });

        //filled if empty
        assertEquals(5+3,sec.size());
        assert(sec.contains(new Object[]{"d",2}));
        assert(sec.contains(new Object[]{"v",2}));
        assert(sec.contains(new Object[]{"e",2}));

        //old values preserved
        m.put(2,"dvea");
        assertEquals(5+4,sec.size());
        assert(sec.contains(new Object[]{"d",2}));
        assert(sec.contains(new Object[]{"v",2}));
        assert(sec.contains(new Object[]{"e",2}));
        assert(sec.contains(new Object[]{"a",2}));

        //old values deleted
        m.put(2,"dva");
        assertEquals(5+3,sec.size());
        assert(sec.contains(new Object[]{"d",2}));
        assert(sec.contains(new Object[]{"v",2}));
        assert(sec.contains(new Object[]{"a",2}));

        //all removed on delete
        m.remove(2);
        assertEquals(5,sec.size());

        //all added on put
        m.put(2,"dva");
        assertEquals(5+3,sec.size());
        assert(sec.contains(new Object[]{"d",2}));
        assert(sec.contains(new Object[]{"v",2}));
        assert(sec.contains(new Object[]{"a",2}));

    }

    @Test public void htreemap_listeners(){
        mapListeners(DBMaker.memoryDB().transactionDisable().make().
                hashMapCreate("test").keySerializer(Serializer.INTEGER).valueSerializer(Serializer.INTEGER).make());
    }

    @Test public void btreemap_listeners(){
        mapListeners(DBMaker.memoryDB().transactionDisable().make().
                treeMapCreate("test").keySerializer(Serializer.INTEGER).valueSerializer(Serializer.INTEGER).make());
    }


    void mapListeners(Bind.MapWithModificationListener test) {
        final AtomicReference rkey = new AtomicReference();
        final AtomicReference roldVal = new AtomicReference();
        final AtomicReference rnewVal = new AtomicReference();

        test.modificationListenerAdd(new Bind.MapListener() {
            @Override
            public void update(Object key, Object oldVal, Object newVal) {
                rkey.set(key);
                roldVal.set(oldVal);
                rnewVal.set(newVal);
            }
        });

        int max = (int) Math.min(100,Math.max(1e8,Math.pow(4, TT.scale())));
        Random r = new Random();
        for(int i=0;i<max;i++){
            Integer k = r.nextInt(max/100);
            Integer v = k*1000;
            Integer vold = null;

            if(test.containsKey(k)){
                vold = v*10;
                test.put(k,vold);
            }


            test.put(k,v);
            assertEquals(k, rkey.get());
            assertEquals(v, rnewVal.get());
            assertEquals(vold, roldVal.get());

            final int m = i%20;
            if(m==1){
                test.remove(k);
                assertEquals(k, rkey.get());
                assertEquals(null, rnewVal.get());
                assertEquals(v, roldVal.get());
            }else  if(m==2){
                test.put(k,i*20);
                assertEquals(k, rkey.get());
                assertEquals(i*20, rnewVal.get());
                assertEquals(v, roldVal.get());
            }else if(m==3&& !test.containsKey(i+1)){
                test.putIfAbsent(i + 1, i + 2);
                assertEquals(i+1, rkey.get());
                assertEquals(i+2, rnewVal.get());
                assertEquals(null, roldVal.get());
            }else if(m==4){
                ((ConcurrentMap)test).remove(k,v);
                assertEquals(k, rkey.get());
                assertEquals(null, rnewVal.get());
                assertEquals(v, roldVal.get());
            }else if(m==5){
                ((ConcurrentMap)test).replace(k,v,i*i);
                assertEquals(k, rkey.get());
                assertEquals(i*i, rnewVal.get());
                assertEquals(v, roldVal.get());
            }else if(m==5){
                ((ConcurrentMap)test).replace(k,i*i);
                assertEquals(k, rkey.get());
                assertEquals(i*i, rnewVal.get());
                assertEquals(v, roldVal.get());
            }




        }

    }

    @Test public void issue453_histogram_not_created_on_empty_secondary_set() {
        DB db = DBMaker.memoryDB().transactionDisable().make();

        HTreeMap<Long, Double> map = db.hashMap("map");

        // histogram, category is a key, count is a value
        ConcurrentMap<Integer, Long> histogram = new ConcurrentHashMap<Integer, Long>(); //any map will do

        //insert some random stuff
        for(long key=0;key<1e4;key++){
            map.put(key, Math.random());
        }

        // bind histogram to primary map
        // we need function which returns category for each map entry
        Bind.histogram(map, histogram, new Fun.Function2<Integer, Long, Double>(){
            @Override
            public Integer run(Long key, Double value) {
                if(value<0.25) return 1;
                else if(value<0.5) return 2;
                else if(value<0.75) return 3;
                else return 4;
            }
        });

        for(int i=1;i<=4;i++){
            assertTrue(histogram.containsKey(i));
        }
    }

    @Test public void histogram(){
        DB db = DBMaker.memoryDB().transactionDisable().make();

        HTreeMap<Long, Double> map = db.hashMap("map");

        // histogram, category is a key, count is a value
        ConcurrentMap<Integer, Long> histogram = new ConcurrentHashMap<Integer, Long>(); //any map will do

        // bind histogram to primary map
        // we need function which returns category for each map entry
        Bind.histogram(map, histogram, new Fun.Function2<Integer, Long, Double>(){
            @Override
            public Integer run(Long key, Double value) {
                if(value<0.25) return 1;
                else if(value<0.5) return 2;
                else if(value<0.75) return 3;
                else return 4;
            }
        });

        //insert some random stuff
        for(long key=0;key<1e4;key++){
            map.put(key, Math.random());
        }

        for(int i=1;i<=4;i++){
            assertTrue(histogram.containsKey(i));
        }
    }

}
