package org.mapdb;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class Issue523Test {

    private static final int NUM_ENTRIES = 1000;

    @Test
    public void  MapDbReadOnlyTest() throws IOException {
        File dbFile = File.createTempFile("mapdb","mapdb");
        testCreate(dbFile);
        testRead(dbFile);
    }

    private void testCreate(File dbFile) {
        DB db = DBMaker.newFileDB(dbFile).transactionDisable().mmapFileEnablePartial().make();

            BTreeMap<Integer, String> map = db.createTreeMap("aa").makeOrGet();
            for (int i = 0; i < NUM_ENTRIES; i++) {
                map.put(i, "value-" + i);
            }


            db.commit();
            db.close();

    }

    private void testRead(File dbFile) {
        DB db = DBMaker.newFileDB(dbFile).transactionDisable().readOnly().mmapFileEnablePartial().make();

            BTreeMap<Integer, String> map = db.createTreeMap("aa").makeOrGet();
            for (int i = 0; i < NUM_ENTRIES; i++) {
                map.get(i);
            }


            db.close();
            // check if the file is still locked
            assertTrue(dbFile.delete()); 

    }
}
