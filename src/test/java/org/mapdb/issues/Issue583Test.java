package org.mapdb.issues;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mapdb.*;

import java.io.*;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class Issue583Test {

    public static final String MAP = "map";

    private File dbFile;

    @Before
    public void createTempFolder() throws IOException {
        dbFile = TT.tempDbFile();
    }

    @After
    public void deleteTempFolder() {
        dbFile.delete();
    }

    @Test
    public void testGettingFromMemoryMapReturnsNull() {
        DB diskDb = DBMaker.fileDB(dbFile)
                .fileMmapEnable()
                .transactionDisable()
                .closeOnJvmShutdown()
                .deleteFilesAfterClose()
                .make();

        DB memoryDb = DBMaker.memoryDB()
                .transactionDisable()
                .make();

        AtomicInteger serializerCalls = new AtomicInteger();

        HTreeMap<Integer, Value> diskMap = diskDb.hashMapCreate(MAP)
                .keySerializer(Serializer.INTEGER)
                .valueSerializer(new ValueSerializer(serializerCalls))
                .make();

        HTreeMap<Integer, Value> memoryMap = memoryDb.hashMapCreate(MAP)
                .expireMaxSize(1)
                .expireOverflow(diskMap, true)
                .expireTick(0)
                .make();


        for (int i = 0; i < 17; i++) { // 17 is minimal for disk overflow (even with cacheSize=1)
            memoryMap.put(i, new Value(i));
        }
        assertTrue("Expecting overflow to disk, but no serialization happened", serializerCalls.get() > 0);


        Set<Integer> inMemoryKeys = memoryMap.keySet();
        for (Integer inMemoryKey : inMemoryKeys) {
            assertTrue(memoryMap.containsKey(inMemoryKey));
            assertNotNull(memoryMap.get(inMemoryKey));
        }

        Set<Integer> inDiskKeys = diskMap.keySet();
        for (Integer inDiskKey : inDiskKeys) {
            assertTrue(diskMap.containsKey(inDiskKey));
            assertNotNull(diskMap.get(inDiskKey));
        }

        memoryMap.close();
        diskMap.close();
    }


    private static class Value implements Serializable {
        private final int value;

        private Value(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }
    }


    private static class ValueSerializer extends Serializer<Value> {

        private final AtomicInteger called;

        private ValueSerializer(AtomicInteger called) {
            this.called = called;
        }

        @Override
        public void serialize(DataOutput out, Value value) throws IOException {
            called.incrementAndGet();
            out.writeInt(value.value);
        }

        @Override
        public Value deserialize(DataInput in, int available) throws IOException {
            return new Value(in.readInt());
        }

        @Override
        public int fixedSize() {
            return 4;
        }
    }

}