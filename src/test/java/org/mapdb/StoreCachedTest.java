package org.mapdb;


import org.junit.Test;
import org.mapdb.DBException.DataCorruption;
import org.mapdb.Store.LongObjectMap;


import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@SuppressWarnings({"rawtypes","unchecked"})
public class
        StoreCachedTest<E extends StoreCached> extends StoreDirectTest<E>{

    @Override boolean canRollback(){return false;}


    @Override protected E openEngine() {
        StoreCached e =new StoreCached(f.getPath());
        e.init();
        return (E)e;
    }

    @Test public void put_delete(){
        e = openEngine();
        long recid = e.put(1L, Serializer.LONG);
        int pos = e.lockPos(recid);
        assertEquals(1, e.writeCache[pos].size);
        e.delete(recid,Serializer.LONG);
        assertEquals(1, e.writeCache[pos].size);
    }

    @Test public void put_update_delete(){
        e = openEngine();
        long recid = e.put(1L, Serializer.LONG);
        int pos = e.lockPos(recid);
        assertEquals(1, e.writeCache[pos].size);
        e.update(recid,2L,Serializer.LONG);
        assertEquals(1,e.writeCache[pos].size);
        e.delete(recid,Serializer.LONG);
        assertEquals(1,e.writeCache[pos].size);
    }

    @Test(timeout = 100000)
    public void flush_write_cache(){
        if(TT.scale()==0)
            return;
        for(ScheduledExecutorService E:
                new ScheduledExecutorService[]{
                        null,
                        Executors.newSingleThreadScheduledExecutor()
                }) {
            final int M = 1234;
            StoreCached e = new StoreCached(
                    null,
                    Volume.ByteArrayVol.FACTORY,
                    null,
                    1,
                    0,
                    false,
                    false,
                    null,
                    false,
                    false,
                    false,
                    null,
                    E,
                    0L,
                    0L,
                    false,
                    1024,
                    M
            );
            e.init();

            assertEquals(M, e.writeQueueSize);
            assertEquals(0, e.writeCache[0].size);

            //write some stuff so cache is almost full
            for (int i = 0; i < M ; i++) {
                e.put("aa", Serializer.STRING);
            }

            assertEquals(M, e.writeCache[0].size);

            //one extra item causes overflow
            e.put("bb", Serializer.STRING);


            while(E!=null && e.writeCache[0].size>0){
                LockSupport.parkNanos(1000);
            }

            assertEquals(0, e.writeCache[0].size);

            if(E!=null)
                E.shutdown();

            e.close();
        }
    }
    
	@Test public void test_assertLongStackPage_throws_exception_when_offset_lessthan_page_size() {
		e = openEngine();
		for (long offset = 0; offset < StoreDirect.PAGE_SIZE; offset++) {
			try {
				e.assertLongStackPage(offset, null);
				fail("DataCorruption exception was expected, but not thrown. " + "Offset=" + offset + ", PAGE_SIZE="
						+ StoreDirect.PAGE_SIZE);
			} catch (DBException.DataCorruption dbe) {

			} 
		}
		e.assertLongStackPage(StoreDirect.PAGE_SIZE, new byte[16]);
	}
	
	@Test public void test_assertLongStackPage_throws_exception_when_parameter_length_not_multiple_of_16() {
		e = openEngine();
		for (int parameterLength = 1; parameterLength < 16; parameterLength++) {
			try {
				e.assertLongStackPage(StoreDirect.PAGE_SIZE, new byte[parameterLength]);
				fail("Assertion error was expected but not thrown " + "Parameter length=" + parameterLength);
			} catch (AssertionError ae) {

			}
		}
		e.assertLongStackPage(StoreDirect.PAGE_SIZE, new byte[16]);
	}
	
	@Test(expected = DataCorruption.class)
	public void test_assertLongStackPage_throws_exception_when_parameter_length_is_zero() {
		e = openEngine();
		e.assertLongStackPage(StoreDirect.PAGE_SIZE, new byte[0]);
	}
	
	@Test(expected = DataCorruption.class)
	public void test_assertLongStackPage_throws_exception_when_parameter_length_exceeds_maximum() {
		e = openEngine();
		e.assertLongStackPage(StoreDirect.PAGE_SIZE, new byte[StoreDirect.MAX_REC_SIZE + 1]);
	}
	
	@Test(expected = AssertionError.class)
	public void test_assertNoOverlaps_throws_exception_when_overlaps_exist() {
		e = openEngine();
		LongObjectMap<byte[]> pages = new LongObjectMap<byte[]>();
		pages.put(1, new byte[2]);
		pages.put(3, new byte[2]);
		pages.put(4, new byte[1]);
		e.assertNoOverlaps(pages);
	}

}
