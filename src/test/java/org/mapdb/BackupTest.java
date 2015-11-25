package org.mapdb;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BackupTest {

    @Test
    public void full_backup() {
        DB db = DBMaker.memoryDB().transactionDisable().make();
        Set m = db.hashSet("test");

        for (int i = 0; i < 1000; i++) {
            m.add(TT.randomString(1000, i));
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        Pump.backupFull(db, out);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());

        DB db2 = Pump.backupFullRestore(
                DBMaker.memoryDB().transactionDisable(),
                in);

        Set m2 = db2.hashSet("test");

        assertEquals(1000, m.size());
        assertTrue(m.containsAll(m2));
        assertTrue(m2.containsAll(m));
    }

    @Test
    public void incremental_backup() {
        DB db = DBMaker.memoryDB().transactionDisable().make();
        Map m = db.hashMap("test");
        File dir = TT.tempDbDir();

        List<byte[]> backups = new ArrayList<byte[]>();

        for(int j=0;j<10;j++ ){
            for (int i = 0; i < 1000; i++) {
                m.put(i, TT.randomString(1000, j*1000+i));
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Pump.backupIncremental(db,dir);
            backups.add(out.toByteArray());
        }

        InputStream[] in = new InputStream[backups.size()];
        for(int i=0;i<in.length;i++){
            in[i] = new ByteArrayInputStream(backups.get(i));
        }

        DB db2 = Pump.backupIncrementalRestore(
                DBMaker.memoryDB().transactionDisable(),
                dir);

        Map m2 = db2.hashMap("test");

        assertTrue(m.size() == 1000);
        assertTrue(m.entrySet().containsAll(m2.entrySet()));
        assertTrue(m2.entrySet().containsAll(m.entrySet()));

    }

}