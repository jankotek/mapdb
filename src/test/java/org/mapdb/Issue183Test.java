package org.mapdb;

import org.junit.Test;

import java.io.*;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class Issue183Test {

    @Test
    public void main(){

        File f = Utils.tempDbFile();

        Map<String, String> map1;

        TxMaker txMaker = DBMaker
                .newFileDB(f)
                .closeOnJvmShutdown()
                .cacheDisable()
                .makeTxMaker();

        DB db = txMaker.makeTx();

        map1 = db.createTreeMap("map1")
                .valueSerializer(new StringSerializer())
                .makeOrGet();

        map1.put("foo", "bar");
        db.commit();
        db.close();
        txMaker.close();


        txMaker = DBMaker
                .newFileDB(f)
                .closeOnJvmShutdown()
                .cacheDisable()
                .makeTxMaker();

        db = txMaker.makeTx();

        map1 = db.createTreeMap("map1")
                .valueSerializer(new StringSerializer())
                .makeOrGet();

        assertEquals("bar", map1.get("foo"));
        map1.put("foo2", "bar2");
        db.commit();
        db.close();
        txMaker.close();

    }

    private static final class StringSerializer implements Serializer<String>, Serializable {

        private static final long serialVersionUID = -8356516782418439492L;

        @Override
        public void serialize(DataOutput out, String value) throws IOException {
            out.writeUTF(value);
        }

        @Override
        public String deserialize(DataInput in, int available) throws IOException {
            return in.readUTF();
        }

        @Override
        public int fixedSize() {
            return -1;
        }

    }
}
