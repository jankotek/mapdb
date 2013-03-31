package org.mapdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * https://github.com/jankotek/MapDB/issues/41
 * @author Laurent Pellegrino
 *
 * TODO fully investigate this concurrent issue.
 */
public class Issue41Test {

    private static int NB_OPERATIONS = 1000;

    private File DB_PATH = Utils.tempDbFile();

    private static String MAP_NAME = "mymap";

    private DB db;

    private HTreeMap<Key, Value> map;

    private ExecutorService threadPool;

    private CountDownLatch doneSignal;

    @Before
    public void setUp() {
        db =
                DBMaker.newFileDB(DB_PATH)
                        .cacheSoftRefEnable()
                        .closeOnJvmShutdown()
                        .deleteFilesAfterClose()
                        .writeAheadLogDisable()
                        .make();

        map =
                db.createHashMap(
                        MAP_NAME, false, new Key.Serializer(), new Value.Serializer());

        threadPool = Executors.newFixedThreadPool(16);

        doneSignal = new CountDownLatch(NB_OPERATIONS);


    }

    @Test
    public void test1() throws InterruptedException {
        final Value value = new Value();
        final Key key = new Key(value, "http://www.mapdb.org/");

        for (int i = 0; i < NB_OPERATIONS; i++) {
            final int j = i;

            threadPool.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        map.put(key, value);
                    } finally {
                        doneSignal.countDown();
//                        System.out.println("OP " + j);
                    }
                }
            });
        }
    }

    @Test
    public void test2() throws InterruptedException {
        final ConcurrentMap<Key, Value> alreadyAdded =
                new ConcurrentHashMap<Key, Value>();

        for (int i = 0; i < NB_OPERATIONS; i++) {
            final int j = i;

            threadPool.execute(new Runnable() {

                @Override
                public void run() {
                    try {
                        if (j % 2 == 0) {
                            Value value = new Value();
                            Key key = new Key(value, Integer.toString(j));

                            alreadyAdded.putIfAbsent(key, value);
                            map.putIfAbsent(key, value);
                        } else {
                            Iterator<Key> it = alreadyAdded.keySet().iterator();

                            if (it.hasNext()) {
                                map.get(it.next());
                            }
                        }
                    } finally {
                        doneSignal.countDown();
//                        System.out.println("OP " + j);
                    }
                }
            });
        }
    }

    @After
    public void tearDown() throws InterruptedException {
        doneSignal.await();
        threadPool.shutdown();
        db.close();
    }

    public static class Value implements Serializable {

        private static final long serialVersionUID = 1L;

        public static final Serializer SERIALIZER = new Serializer();

        protected final UUID value;

        public Value() {
            this.value = UUID.randomUUID();
        }

        private Value(UUID uuid) {
            this.value = uuid;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((this.value == null)
                    ? 0 : this.value.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof Value)) {
                return false;
            }
            Value other = (Value) obj;
            if (this.value == null) {
                if (other.value != null) {
                    return false;
                }
            } else if (!this.value.equals(other.value)) {
                return false;
            }
            return true;
        }

        public static final class Serializer implements
                org.mapdb.Serializer<Value>, Serializable {

            private static final long serialVersionUID = 140L;

            @Override
            public void serialize(DataOutput out, Value value)
                    throws IOException {
                out.writeLong(value.value.getMostSignificantBits());
                out.writeLong(value.value.getLeastSignificantBits());
            }

            @Override
            public Value deserialize(DataInput in, int available)
                    throws IOException {
                return new Value(new UUID(in.readLong(), in.readLong()));
            }

        }

    }

    public static class Key implements Serializable {

        private static final long serialVersionUID = 1L;

        protected final Value subscriptionId;

        protected final String eventId;

        public Key(Value subscriptionId, String eventId) {
            this.subscriptionId = subscriptionId;
            this.eventId = eventId;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((this.eventId == null)
                    ? 0 : this.eventId.hashCode());
            result = prime * result + ((this.subscriptionId == null)
                    ? 0 : this.subscriptionId.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (!(obj instanceof Key)) {
                return false;
            }
            Key other = (Key) obj;
            if (this.eventId == null) {
                if (other.eventId != null) {
                    return false;
                }
            } else if (!this.eventId.equals(other.eventId)) {
                return false;
            }
            if (this.subscriptionId == null) {
                if (other.subscriptionId != null) {
                    return false;
                }
            } else if (!this.subscriptionId.equals(other.subscriptionId)) {
                return false;
            }
            return true;
        }

        public static final class Serializer implements
                org.mapdb.Serializer<Key>, Serializable {

            private static final long serialVersionUID = 1L;

            @Override
            public void serialize(DataOutput out, Key notificationId)
                    throws IOException {
                out.writeUTF(notificationId.eventId);

                Value.SERIALIZER.serialize(out, notificationId.subscriptionId);
            }

            @Override
            public Key deserialize(DataInput in, int available)
                    throws IOException {
                String eventId = in.readUTF();

                Value subscriptionId =
                        Value.SERIALIZER.deserialize(in, available);

                return new Key(subscriptionId, eventId);
            }

        }

    }



}
