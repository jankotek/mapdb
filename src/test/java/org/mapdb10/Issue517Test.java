package org.mapdb10;

import org.junit.Test;

import java.io.*;

import static org.junit.Assert.*;

public class Issue517Test {

    static class NonSerializableSerializer implements Serializer{

        @Override
        public void serialize(DataOutput out, Object value) throws IOException {

        }

        @Override
        public Object deserialize(DataInput in, int available) throws IOException {
            return null;
        }

        @Override
        public int fixedSize() {
            return -1;
        }
    }


    @Test(timeout = 10000)
    public void secondGet() throws Exception {
        DB db = DBMaker.newMemoryDB().transactionDisable().make();

        for(int i = 0;i<10;i++) {
            try {
                db.createTreeMap("map").valueSerializer(new NonSerializableSerializer()).makeOrGet();
                fail("should throw exception");
            } catch (IllegalArgumentException e) {
                assertTrue(e.getMessage().contains("Not serializable"));
            }
        }
    }
}