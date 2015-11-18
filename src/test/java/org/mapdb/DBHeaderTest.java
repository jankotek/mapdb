package org.mapdb;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public abstract class DBHeaderTest {

    public static class _StoreDirect extends DBHeaderTest{

        @Override
        DBMaker.Maker maker() {
            return DBMaker.fileDB(file).transactionDisable();
        }
    }

    public static class _StoreWAL extends DBHeaderTest{

        @Override
        DBMaker.Maker maker() {
            return DBMaker.fileDB(file);
        }
    }


    public static class _StoreAppend extends DBHeaderTest{

        @Override
        DBMaker.Maker maker() {
            return DBMaker.appendFileDB(file);
        }
    }

    File file;
    {
        try {
            file = File.createTempFile("mapdbTest","mapdb");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    abstract DBMaker.Maker maker();


    public long getBitField(DB db) {
        Volume v =
                db.getEngine() instanceof StoreDirect ?
                        ((StoreDirect)db.getEngine()).headVol :
                        ((StoreAppend)db.getEngine()).wal.volumes.get(0);

        return v.getLong(8);
    }



    @Test
    public void lzw(){
        DB db = maker()
                .compressionEnable()
                .make();

        db.hashMap("aa").put("aa", "bb");
        db.commit();
        assertEquals(1L<<Store.FEAT_COMP_LZF,getBitField(db));
        db.close();
        try {
            maker().make();
            fail();
        }catch(DBException.WrongConfig e){
            assertEquals("Store was created with compression, but no compression is enabled in config.",e.getMessage());
        }
    }

    @Test
    public void lzw2(){
        DB db = maker()
                .make();

        db.hashMap("aa").put("aa", "bb");
        db.commit();
        assertEquals(0L,getBitField(db));
        db.close();
        try {
            maker().compressionEnable().make();
            fail();
        }catch(DBException.WrongConfig e){
            assertEquals("Compression is set in config, but store was created with compression.",e.getMessage());
        }
    }


    @Test
    public void xtea(){
        DB db = maker()
                .encryptionEnable("password")
                .make();

        db.hashMap("aa").put("aa", "bb");
        db.commit();
        assertEquals(1L<<Store.FEAT_ENC_XTEA,getBitField(db));
        db.close();
        try {
            maker().make();
            fail();
        }catch(DBException.WrongConfig e){
            assertEquals("Store was created with encryption, but no password is set in config.",e.getMessage());
        }
    }

    @Test
    public void xtea2(){
        DB db = maker()
                .make();

        db.hashMap("aa").put("aa", "bb");
        db.commit();
        assertEquals(0L,getBitField(db));
        db.close();
        try {
            maker().encryptionEnable("password").make();
            fail();
        }catch(DBException.WrongConfig e){
            assertEquals("Password is set, but store is not encrypted.",e.getMessage());
        }
    }


    @Test
    public void crc32(){
        DB db = maker()
                .checksumEnable()
                .make();

        db.hashMap("aa").put("aa", "bb");
        db.commit();
        assertEquals(1L<<Store.FEAT_CRC,getBitField(db));
        db.close();
        try {
            maker().make();
            fail();
        }catch(DBException.WrongConfig e){
            assertEquals("Store was created with CRC32 checksum, but it is not enabled in config.",e.getMessage());
        }
    }

    @Test
    public void crc32_(){
        DB db = maker()
                .make();

        db.hashMap("aa").put("aa", "bb");
        db.commit();
        assertEquals(0L,getBitField(db));
        db.close();
        try {
            maker().checksumEnable().make();
            fail();
        }catch(DBException.WrongConfig e){
            e.printStackTrace();
            assertEquals("Checksum us enabled, but store was created without it.",e.getMessage());
        }
    }

    @Test public void fail_on_unknown_bit(){
        DB db = maker()
                .make();

        db.hashMap("aa").put("aa", "bb");
        db.commit();
        assertEquals(0L, getBitField(db));
        db.close();

        //fake bitfield
        Volume r = new Volume.RandomAccessFileVol(file,false,false,0L);
        r.putLong(8, 2L << 32);
        r.sync();
        r.close();


        try {
            maker().make();
            fail();
        }catch(DBException.WrongConfig e){
            assertEquals("Unknown feature #33. Store was created with never MapDB version, this version does not support this feature.",e.getMessage());
        }
    }
}