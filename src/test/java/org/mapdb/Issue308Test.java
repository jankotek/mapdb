package org.mapdb;

import org.junit.Test;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class Issue308Test {

    @Test
    public void test() {
        DB db = DBMaker.newTempFileDB()
                .mmapFileEnableIfSupported()
                .compressionEnable()
                .transactionDisable()
                .checksumEnable()
                .syncOnCommitDisable()
                .make();
        Iterator<Fun.Tuple2<Long, String>> newIterator = new Iterator<Fun.Tuple2<Long, String>>() {
            private AtomicLong value = new AtomicLong(10000000);

            @Override
            public boolean hasNext() {
                return value.get() > 0;
            }

            @Override
            public Fun.Tuple2<Long, String> next() {
                Long v = value.decrementAndGet();
                return new Fun.Tuple2<Long, String>(v, v.toString());
            }

            @Override
            public void remove() {

            }
        };
        BTreeMap<Long, String> cubeData = db.createTreeMap("data").pumpSource(newIterator).make();
    }
}
