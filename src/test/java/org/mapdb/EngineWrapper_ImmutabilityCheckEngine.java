package org.mapdb;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EngineWrapper_ImmutabilityCheckEngine {

    @Test
    public void test(){
        Volume.Factory fab = Volume.memoryFactory(false);
        Engine e = new StoreDirect(fab);
        e = new EngineWrapper.ImmutabilityCheckEngine(e);

        List rec = new ArrayList();
        rec.add("aa");
        long recid = e.put(rec,Serializer.BASIC_SERIALIZER);
        rec.add("bb");

        try{
            e.update(recid, rec, Serializer.BASIC_SERIALIZER);
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
