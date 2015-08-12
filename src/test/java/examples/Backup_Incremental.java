package examples;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Pump;

import java.io.File;
import java.io.IOException;
import java.util.Set;

/*
 * Shows how pump can be used to backup and restore live database.
 *
 * This uses incremental backup, first backup file contains full backup,
 * latter backup contain only difference from last backup
 */
public class Backup_Incremental {

    public static void main(String[] args) throws IOException {
        //create database and insert some data
        DB db = DBMaker.memoryDB().transactionDisable().make();
        Set s = db.hashSet("test");
        s.add("one");
        s.add("two");

        //incremental backup requires backup folder
        String tmpdir = System.getProperty("java.io.tmpdir");
        File backupFolder = new File(tmpdir+File.separator+"mapdbTest"+System.currentTimeMillis());
        backupFolder.mkdir();
        
        //make first backup
        Pump.backupIncremental(db, backupFolder);

        //insert some extra data and make second backup
        s.add("three");
        s.add("four");
        Pump.backupIncremental(db, backupFolder);

        //now close database and create new instance with restored content
        db.close();
        DB db2 = Pump.backupIncrementalRestore(
                //configuration used to instantiate empty database
                DBMaker.memoryDB().transactionDisable(),
                //input stream with backup data
                backupFolder);

        Set s2 = db2.hashSet("test");
        System.out.println(s2);
    }
}
