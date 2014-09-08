package org.mapdb10;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings({"rawtypes","unchecked"})
public class EngineWrapper_ImmutabilityCheckEngine {

    @Test
    public void test(){
        Volume.Factory fab = Volume.memoryFactory(false,0L,CC.VOLUME_CHUNK_SHIFT);
        Engine e = new StoreDirect(fab);
        e = new EngineWrapper.ImmutabilityCheckEngine(e);

        List rec = new ArrayList();
        rec.add("aa");
        long recid = e.put(rec,Serializer.BASIC);
        rec.add("bb");

        try{
            e.update(recid, rec, Serializer.BASIC);
            fail("should throw exception");
        }catch(AssertionError ee){
            assertTrue(ee.getMessage().startsWith("Record instance was modified"));
        }

        try{
            e.close();
            fail("should throw exception");
        }catch(AssertionError ee){
            assertTrue(ee.getMessage().startsWith("Record instance was modified"));
        }
    }

}
