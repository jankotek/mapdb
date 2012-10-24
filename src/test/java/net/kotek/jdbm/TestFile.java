package net.kotek.jdbm;

import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;

/**
 * provides temporarily test files which are deleted after JVM exits.
 */
abstract public class TestFile {

    protected final File index = JdbmUtil.tempDbFile();
    protected final File data = new File(index.getPath()+Storage.DATA_FILE_EXT);
    protected final File log = new File(index.getPath()+StorageTrans.TRANS_LOG_FILE_EXT);


    @After public void after() throws IOException {
        if(index!=null)
            index.delete();
        if(data!=null)
            data.delete();
        if(log!=null)
            log.delete();
    }
}
