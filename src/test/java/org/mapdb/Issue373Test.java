package org.mapdb;

import junit.framework.TestCase;

import java.io.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Alexandre Y. Bouchov
 * Date: 27.08.14
 * Time: 11:04
 */
public class Issue373Test extends TestCase {
    SerializerPojo p = new SerializerPojo(new CopyOnWriteArrayList<SerializerPojo.ClassInfo>());

    public static class NotSerializableClass {
    }

    public static class Pojo2 implements Serializable {
        private Class classVal;

        public Pojo2(Class classVal) {
            this.classVal = classVal;
        }
    }

    public static class Pojo implements Serializable {
        private static final int VERSION = 0;
        private transient Pojo2 pojo2Val;

        public Pojo(Pojo2 pojo2Val) {
            this.pojo2Val = pojo2Val;
        }

        private void writeObject(ObjectOutputStream out)
                throws IOException {
            out.defaultWriteObject();
            out.writeInt(VERSION);
            out.writeObject(pojo2Val);
        }

        private void readObject(ObjectInputStream in)
                throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            int ver = in.readInt();
            if (ver == 0) {
                pojo2Val = (Pojo2) in.readObject();
            } else {
                throw new IOException("Unknown class version (" + ver + ')');
            }
        }
    }

    public static class SimplePojo implements Serializable {
        private Pojo2 pojo2Val;

        public SimplePojo(Pojo2 pojo2Val) {
            this.pojo2Val = pojo2Val;
        }

        public Pojo2 getPojo2Val() {
            return pojo2Val;
        }
    }

    public void test_Pojo2() throws Exception {
        serialize(new Pojo2(NotSerializableClass.class));
    }

    public void test_SimplePojo() throws Exception {
        serialize(new SimplePojo(new Pojo2(NotSerializableClass.class)));
    }

    public void test_Pojo() throws Exception {
        serialize(new Pojo(new Pojo2(NotSerializableClass.class)));
    }

    private void serialize(Object i) throws IOException {
        DataOutput in = new DataIO.DataOutputByteArray();
        p.serialize(in, i);
    }
}