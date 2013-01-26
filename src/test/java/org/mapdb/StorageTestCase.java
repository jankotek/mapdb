package org.mapdb;


import org.junit.After;
import org.junit.Before;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * JUnit test case which provides JDBM specific staff
 */
abstract public class StorageTestCase extends TestFile{


    Storage engine;

     @Before
     public void setUp() throws Exception {
        engine = openEngine();
    }

    @After
    public void tearDown() throws Exception {
        if(engine!=null && !engine.isClosed())
            engine.close();
    }


    protected Storage openEngine() {
        return new StorageDirect(fac);
    }


    void reopenStore() {
        engine.close();
        engine = openEngine();
    }


    DataInput2 swap(DataOutput2 d){
        byte[] b = d.copyBytes();
        return new DataInput2(ByteBuffer.wrap(b),0);
    }


    int countIndexRecords(){
        int ret = 0;
        final long indexFileSize = engine.index.getLong(StorageDirect.RECID_CURRENT_INDEX_FILE_SIZE*8);
        for(int pos = StorageDirect.INDEX_OFFSET_START * 8;
            pos<indexFileSize;
            pos+=8){
            if(0!= engine.index.getLong(pos)){
                ret++;
            }
        }
        return ret;
    }

    long getIndexRecord(long recid){
        return engine.index.getLong(recid*8);
    }

    List<Long> getLongStack(long recid){

        ArrayList<Long> ret =new ArrayList<Long>();

        long pagePhysid = engine.index.getLong(recid*8) & StorageDirect.PHYS_OFFSET_MASK;


        while(pagePhysid!=0){
            final byte numberOfRecordsInPage = engine.phys.getByte((int) (pagePhysid% Volume.BUF_SIZE));

            for(int rec = numberOfRecordsInPage; rec>0;rec--){
                final long l = engine.phys.getLong((int) (pagePhysid% Volume.BUF_SIZE+ rec*8));
                ret.add(l);
            }

            //read location of previous page
            pagePhysid = engine.phys.getLong((int)(pagePhysid% Volume.BUF_SIZE)) & StorageDirect.PHYS_OFFSET_MASK;
        }


        return ret;
    }


    final List<Long> arrayList(long... vals){
        ArrayList<Long> ret = new ArrayList<Long>();
        for(Long l:vals){
            ret.add(l);
        }
        return ret;
    }


}
