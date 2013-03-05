package org.mapdb;

import org.junit.After;

import java.io.File;
import java.io.IOException;

/**
 * provides temporarily test files which are deleted after JVM exits.
 */
abstract public class TestFile {

    protected final File index = Utils.tempDbFile();
    protected final File data = new File(index.getPath()+ StorageDirect.DATA_FILE_EXT);
    protected final File log = new File(index.getPath()+ StorageJournaled.TRANS_LOG_FILE_EXT);

    protected Volume.Factory fac = Volume.fileFactory(false, false, index, data, log);


    @After public void after() throws IOException {
        if(index!=null)
            index.delete();
        if(data!=null)
            data.delete();
        if(log!=null)
            log.delete();
    }
}
