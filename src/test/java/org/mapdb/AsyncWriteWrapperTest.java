package org.mapdb;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static junit.framework.Assert.assertNotNull;

/**
 * @author Jan Kotek
 */
public class AsyncWriteWrapperTest extends TestFile{
    AsyncWriteWrapper recman;

    @Before public void reopenStore() throws IOException {
        assertNotNull(index);
        if(recman!=null)
           recman.close();
        recman =  new AsyncWriteWrapper(new StorageDirect(index,false,false,false,false), true);
    }





    @Test public void write_fetch_update_delete() throws IOException {
        long recid = recman.recordPut("aaa",Serializer.STRING_SERIALIZER);
        Assert.assertEquals("aaa",recman.recordGet(recid, Serializer.STRING_SERIALIZER));
        reopenStore();
        Assert.assertEquals("aaa",recman.recordGet(recid, Serializer.STRING_SERIALIZER));
        recman.recordUpdate(recid, "bbb", Serializer.STRING_SERIALIZER);
        Assert.assertEquals("bbb", recman.recordGet(recid, Serializer.STRING_SERIALIZER));
        reopenStore();
        Assert.assertEquals("bbb",recman.recordGet(recid, Serializer.STRING_SERIALIZER));

    }


    @Test(timeout = 0xFFFF)
     public void concurrent_updates_test() throws InterruptedException, IOException {


        final int threadNum = 16;
        final int updates = 1000;
        final CountDownLatch latch = new CountDownLatch(threadNum);
        final Map<Integer,Long> recids = new ConcurrentHashMap<Integer, Long>();


        for(int i = 0;i<threadNum; i++){
            final int num = i;
            new Thread(new Runnable() {
                @Override public void run() {
                    long recid = recman.recordPut("START-",Serializer.STRING_SERIALIZER);
                    recids.put(num, recid);
                    for(int i = 0;i<updates; i++){
                        String str= recman.recordGet(recid, Serializer.STRING_SERIALIZER);
                        str +=num+",";
                        recman.recordUpdate(recid, str,Serializer.STRING_SERIALIZER);
                    }
                    latch.countDown();
                }
            }).start();
        }


        latch.await();

        reopenStore();


        Assert.assertEquals(recids.size(),threadNum);
        for(int i = 0;i<threadNum; i++){
            long recid = recids.get(i);

            String expectedStr ="START-";
            for(int j=0;j<updates;j++)
                expectedStr +=i+",";

            String v = recman.recordGet(recid, Serializer.STRING_SERIALIZER);
            Assert.assertEquals(expectedStr, v);
        }



    }
}
