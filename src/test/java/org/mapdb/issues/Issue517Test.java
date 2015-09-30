package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Issue517Test {

    static class NonSerializableSerializer extends Serializer {

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
        DB db = DBMaker.memoryDB().transactionDisable().make();

        for(int i = 0;i<10;i++) {
            db.treeMapCreate("map").valueSerializer(new NonSerializableSerializer()).makeOrGet();
        }
    }
}