package org.mapdb;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MapListenerTest {

        @Test public void hashMap(){
            tt(DBMaker.newMemoryDB().writeAheadLogDisable().make().getHashMap("test"));
        }

        @Test public void treeMap(){
            tt(DBMaker.newMemoryDB().writeAheadLogDisable().make().getTreeMap("test"));
        }


        void tt(Bind.MapWithModificationListener m){
            final AtomicReference key = new AtomicReference(null);
            final AtomicReference newVal = new AtomicReference(null);
            final AtomicReference oldVal = new AtomicReference(null);
            final AtomicInteger counter = new AtomicInteger(0);

            Bind.MapListener listener = new Bind.MapListener(){
                @Override public void update(Object key2, Object oldVal2, Object newVal2) {
                    counter.incrementAndGet();
                    key.set(key2);
                    oldVal.set(oldVal2);
                    newVal.set(newVal2);
                }
            };

            m.addModificationListener(listener);

            //check CRUD
            m.put("aa","bb");
            assertTrue(key.get()=="aa" && newVal.get()=="bb" && oldVal.get()==null && counter.get()==1);

            m.put("aa","cc");
            assertTrue(key.get()=="aa" && newVal.get()=="cc" && oldVal.get()=="bb" && counter.get()==2);

            m.remove("aa");
            assertTrue(key.get()=="aa" && newVal.get()==null && oldVal.get()=="cc" && counter.get()==3);

            //check clear()
            m.put("aa","bb");
            assertTrue(key.get()=="aa" && newVal.get()=="bb" && oldVal.get()==null && counter.get()==4);
            m.clear();
            assertTrue(key.get()=="aa" && newVal.get()==null && oldVal.get()=="bb" && counter.get()==5);


            //check it was unregistered
            counter.set(0);
            m.removeModificationListener(listener);
            m.put("aa","bb");
            assertEquals(0, counter.get());
    }

}
