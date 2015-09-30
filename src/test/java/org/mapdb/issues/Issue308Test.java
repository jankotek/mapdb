package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.*;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class Issue308Test {

    @Test
    public void test() {
        if(TT.scale()==0)
            return;

        DB db = DBMaker.tempFileDB()
                .mmapFileEnableIfSupported()
                .compressionEnable()
                .transactionDisable()
                .checksumEnable()
                .commitFileSyncDisable()
                .make();
        Iterator<Fun.Pair<Long, String>> newIterator = new Iterator<Fun.Pair<Long, String>>() {
            private AtomicLong value = new AtomicLong(10000000);

            @Override
            public boolean hasNext() {
                return value.get() > 0;
            }

            @Override
            public Fun.Pair<Long, String> next() {
                Long v = value.decrementAndGet();
                return new Fun.Pair<Long, String>(v, v.toString());
            }

            @Override
            public void remove() {

            }
        };
        BTreeMap<Long, String> cubeData = db.treeMapCreate("data").pumpSource(newIterator).make();
    }
}
