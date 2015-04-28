package org.mapdb;

import java.util.Map;

import org.junit.Test;

public class Issue490 extends TestFile {

    /**
     * 
     * <p>This test is a little odd, we depend on the TestFile class
     * to remove the files created and opened here. For this test to
     * be meaningful it needs to be run on windows where if a failure 
     * occurs it will be in TestFile, but because we failed to actually
     * close the files here.</p>
     * <p>On linux you can run this test in debug and assert manually that
     * all files have been closed.</p>
     * 
     */
    @Test
    public void testAsyncThreadInterruptedBeforeClose() {
        try {
            DB db = DBMaker
                .newFileDB(super.index)
                .asyncWriteEnable()
                //Disable these two, the same issue exists in both of these, 
                //but we are looking at async write only.
                .commitFileSyncDisable()
                .transactionDisable()
                .make();
            try {
                try {
                    Map<String, String> map = db.getHashMap("foobar");
                    Thread.currentThread().interrupt();
                    //it seems we have to add a few entries for the exception in the async
                    //thread to appear.
                    for(int i = 0; i < 10000; i++){
                        map.put("mapdb" + i, "very cool!");
                    }
                } finally {
                    //Current thread is interrupted close should close file handles but wont
                    //be able to save everything to disk.
                    db.close();
                }
            } catch (Throwable t) {
                //We don't care about the exception map db throws
            }
        } finally {
            Thread.interrupted(); //make sure the thread is not interrupted, so as not to disrupt 
            //any post test tasks.
        }
    }
}

