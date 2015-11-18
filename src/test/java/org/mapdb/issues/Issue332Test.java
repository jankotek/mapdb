package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.TT;

import java.io.*;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/*
 * Created by paspi on 26.05.2014.
 */
public class Issue332Test {

    // 4 length bytes will be prepended to this string: 000000ef
    final static  String problem = "76fa135e7d216e829a53845a983469ac1e4edb6120b79667d667e7d4f8560101010100000022bf456901000000230000002102123eeaa90e2f5786ce028e60ec03702706dadecee373a90b09b88a99cc668f46ac3358c8ea6433279c678846fb6e06eeccd82e2fe888f2ac203476d3918cd405790100000038ffffff9e000000be438253be43825301000000109bf45901000000230000002102123eeaa90e2f5786ce028e60ec03702706dadecee373a90b09b88a99cc668f46ac38bf80f10129594a7e949cc43c3bd6f8670ba5ab59874305f6839406738a9cf90100000038ffffff9e00000081bd175381bd1753";
    public static final Serializer.CompressionWrapper<String> VALUE_SERIALIZER = new Serializer.CompressionWrapper<String>(new TestSerializer());

    public static final class TestSerializer extends  Serializer<String> implements Serializable {

        // http://stackoverflow.com/a/140430
        private static byte[] fromHexString(final String encoded) {
            if ((encoded.length() % 2) != 0)
                throw new IllegalArgumentException("Input string must contain an even number of characters");

            final byte result[] = new byte[encoded.length()/2];
            final char enc[] = encoded.toCharArray();
            for (int i = 0; i < enc.length; i += 2) {
                StringBuilder curr = new StringBuilder(2);
                curr.append(enc[i]).append(enc[i + 1]);
                result[i/2] = (byte) Integer.parseInt(curr.toString(), 16);
            }
            return result;
        }

        // http://stackoverflow.com/a/13006907
        private static String bytArrayToHex(byte[] a) {
            StringBuilder sb = new StringBuilder();
            for(byte b: a)
                sb.append(String.format("%02x", b&0xff));
            return sb.toString();
        }


        @Override
        public void serialize(DataOutput out, String value) throws IOException {
            byte [] buf = fromHexString(value);
            out.writeInt(buf.length);
            out.write(buf);
        }

        @Override
        public String deserialize(DataInput in, int available) throws IOException {
            int nsize = in.readInt();
            byte[] buf = new byte[nsize];
            in.readFully(buf);

            return bytArrayToHex(buf);
        }

        @Override
        public int fixedSize() {
            return -1;
        }
    }

    @Test
    public void run() throws IOException {
        File f = File.createTempFile("mapdbTest","mapdb");
        DB db = DBMaker.fileDB(f)
                .closeOnJvmShutdown()
                .make();

        Map<Integer, String> testMap = db.hashMapCreate("testmap")
                .valueSerializer(VALUE_SERIALIZER)
                        //.valueSerializer(new TestSerializer())
                .makeOrGet();

        testMap.put(1, problem);
        db.commit();
        db.close();

        db = null;
        testMap = null;

        //-------------------------
        db = DBMaker.fileDB(f)
                .closeOnJvmShutdown()
                .make();
        testMap = db.hashMapCreate("testmap")
                .valueSerializer(VALUE_SERIALIZER)
                .makeOrGet();
        String deserialized = testMap.get(1);

        db.close();
        assertEquals(problem,deserialized);
    }

    @Test public void test_ser_itself(){
        String other = TT.clone(problem, new TestSerializer());
        assertEquals(problem, other);
    }

    @Test public void test_comp(){
        String other = TT.clone(problem, VALUE_SERIALIZER);
        assertEquals(problem, other);
    }


}