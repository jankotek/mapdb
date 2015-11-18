package examples;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Pump;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Set;

/*
 * Shows how pump can be used to backup and restore live database
 */
public class Backup {

    public static void main(String[] args) throws IOException {
        //create database and insert some data
        DB db = DBMaker.memoryDB().transactionDisable().make();
        Set s = db.hashSet("test");
        s.add("one");
        s.add("two");

        //make full backup
        File backupFile = File.createTempFile("mapdbTest","mapdb");
        FileOutputStream out = new FileOutputStream(backupFile);

        Pump.backupFull(db,out);
        out.flush();
        out.close();

        //now close database and create new instance with restored content
        db.close();
        DB db2 = Pump.backupFullRestore(
                //configuration used to instantiate empty database
                DBMaker.memoryDB().transactionDisable(),
                //input stream with backup data
                new FileInputStream(backupFile));

        Set s2 = db2.hashSet("test");
        System.out.println(s2);
    }
}
