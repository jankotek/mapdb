package org.mapdb;

import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.Random;

public class Issue442Test {

    //this tests runs very long and consumes 10GB space, so is ignored
    @Test @Ignore
    public void crash(){
        File f = UtilsTest.tempDbFile();
        DB db = DBMaker
                .newFileDB(f)
                .asyncWriteEnable()
                .cacheLRUEnable()
                .cacheSize(100)
                .mmapFileEnableIfSupported()
                .closeOnJvmShutdown()
                .deleteFilesAfterClose()
                .make();

        //put data until its over 10GB
        long size = (long) 1e10;

        File fp = new File(f.getPath()+".p");
        Random r = new Random();
        while(fp.length()<size){
            db.engine.put(UtilsTest.randomByteArray(r.nextInt(200*1000)),Serializer.BYTE_ARRAY);
            if(r.nextInt(10)==1)
                db.commit();
        }
        db.compact();
        db.close();
    }
}
