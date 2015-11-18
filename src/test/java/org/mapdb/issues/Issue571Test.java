package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.TxMaker;

import java.io.*;
import java.util.Map;


/**
 *
 * @author gpeche
 */
public class Issue571Test {

    public static void serialize(final Serializable obj, final OutputStream outputStream) throws IOException {
        if (outputStream == null) {
            throw new IllegalArgumentException("The OutputStream must not be null");
        }
        ObjectOutputStream out = null;
        try {
            // stream closed in the finally
            out = new ObjectOutputStream(outputStream);
            out.writeObject(obj);

        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (final IOException ex) { // NOPMD
                // ignore close exception
            }
        }
    }

    public static byte[] serialize(final Serializable obj) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
        serialize(obj, baos);
        return baos.toByteArray();
    }

    public static <T> T deserialize(final InputStream inputStream) throws IOException, ClassNotFoundException {
        if (inputStream == null) {
            throw new IllegalArgumentException("The InputStream must not be null");
        }
        ObjectInputStream in = null;
        try {
            // stream closed in the finally
            in = new ObjectInputStream(inputStream);
            @SuppressWarnings("unchecked") // may fail with CCE if serialised form is incorrect
            final T obj = (T) in.readObject();
            return obj;


        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (final IOException ex) { // NOPMD
                // ignore close exception
            }
        }
    }

    public static <T> T deserialize(final byte[] objectData) throws IOException, ClassNotFoundException {
        if (objectData == null) {
            throw new IllegalArgumentException("The byte[] must not be null");
        }
        return deserialize(new ByteArrayInputStream(objectData));
    }

    // Dummy class for testing
    public static class CustomValueClass implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    // Customs serializer for our dummy class. Must be Serializable so MapDB can store it in the catalog.
    public static class CustomSerializer extends Serializer<CustomValueClass> implements Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public void serialize(DataOutput out, CustomValueClass value) throws IOException {
            byte[] bs = Issue571Test.serialize(value);
            Serializer.BYTE_ARRAY.serialize(out, bs);
        }

        @Override
        public CustomValueClass deserialize(DataInput in, int available) throws IOException {
            byte[] bs = Serializer.BYTE_ARRAY.deserialize(in, available);
            try {
                return (CustomValueClass) Issue571Test.deserialize(bs);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

    }

    private void performTest(DBMaker.Maker m, Object value) throws Exception {
        performTest(m, value, null);
    }

    private void performTest(DBMaker.Maker m, Object value, Serializer<?> vs) throws Exception {
        TxMaker maker = m.makeTxMaker();

        final DB creationTrans = maker.makeTx();
        final DB.BTreeMapMaker mapMaker = creationTrans.treeMapCreate("testIndex");
        if (vs != null) {
            mapMaker.valueSerializer(vs);
        }
        mapMaker.make();
        creationTrans.commit();
        creationTrans.close();

        final DB updateTrans1 = maker.makeTx();
        Map map1 = updateTrans1.treeMap("testIndex");
        map1.put("testKey", value);
        try {
            updateTrans1.commit();
        } catch (IllegalAccessError err) {
            err.printStackTrace();
            throw err;
        } finally {
            if (!updateTrans1.isClosed()) {
                updateTrans1.close();
            }
        }
    }

    @Test
    public void testCommitFailsDueToStaleEngineInCatalogValueSerializer1() throws Exception {
        final DBMaker.Maker m = DBMaker.memoryDB().cacheHardRefEnable();
        performTest(m, new CustomValueClass());
    }

    @Test
    public void testCommitFailsDueToStaleEngineInCatalogValueSerializer2() throws Exception {
        final DBMaker.Maker m = DBMaker.memoryDB().cacheSoftRefEnable();
        performTest(m, new CustomValueClass());
    }

    @Test
    public void testCommitFailsDueToStaleEngineInCatalogValueSerializer3() throws Exception {
        final DBMaker.Maker m = DBMaker.memoryDB().cacheWeakRefEnable();
        performTest(m, new CustomValueClass());
    }

    @Test
    public void testCommitFailsDueToStaleEngineInCatalogValueSerializer4() throws Exception {
        final DBMaker.Maker m = DBMaker.memoryDB().cacheLRUEnable();
        performTest(m, new CustomValueClass());
    }

    @Test
    public void testCommitFailsDueToStaleEngineInCatalogValueSerializer5() throws Exception {
        final DBMaker.Maker m = DBMaker.memoryDB().cacheHashTableEnable();
        performTest(m, new CustomValueClass());
    }

    @Test
    public void testCommitSucceedsWhenNoCachingUsedInCatalogValueSerializer() throws Exception {
        final DBMaker.Maker m = DBMaker.memoryDB();
        performTest(m, new CustomValueClass());
    }

    @Test
    public void testCommitSucceedsWhenNotUsingCustomObjectsAsValues() throws Exception {
        final DBMaker.Maker m = DBMaker.memoryDB().cacheHardRefEnable();
        performTest(m, "This value is not a custom object");
    }

    @Test
    public void testCommitSucceedsWhenUsingCustomValueSerializer() throws Exception {
        final DBMaker.Maker m = DBMaker.memoryDB().cacheSoftRefEnable();
        performTest(m, new CustomValueClass(), new CustomSerializer());
    }
}