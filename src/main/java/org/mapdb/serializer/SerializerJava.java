package org.mapdb.serializer;

import org.mapdb.CC;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerJava extends GroupSerializerObjectArray {

    protected final ClassLoader classLoader;

    public SerializerJava(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    public SerializerJava(){
        this(Thread.currentThread().getContextClassLoader());
    }

    /**
     * This subclass of ObjectInputStream delegates loading of classes to
     * an existing ClassLoader.
     */

    class ObjectInputStreamWithLoader extends ObjectInputStream
    {
        /**
         * Loader must be non-null;
         */

        public ObjectInputStreamWithLoader(InputStream in)
            throws IOException {
            super(in);
        }

        /**
         * Use the given ClassLoader rather than using the system class
         */
        @SuppressWarnings("rawtypes")
        protected Class resolveClass(ObjectStreamClass desc)
            throws IOException, ClassNotFoundException {
            String name = desc.getName();
            try {
                return Class.forName(name, false, classLoader);
            } catch (ClassNotFoundException ex) {
                return super.resolveClass(desc);
            }
        }
    }

    @Override
    public void serialize(DataOutput2 out, Object value) throws IOException {
        ObjectOutputStream out2 = new ObjectOutputStream((OutputStream) out);
        out2.writeObject(value);
        out2.flush();
    }

    @Override
    public Object deserialize(DataInput2 in, int available) throws IOException {
        try {
            ObjectInputStream in2 = new ObjectInputStreamWithLoader(new DataInput2.DataInputToStream(in));
            return in2.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Object[] valueArrayDeserialize(DataInput2 in, int size) throws IOException {
        try {
            ObjectInputStream in2 = new ObjectInputStreamWithLoader(new DataInput2.DataInputToStream(in));
            Object ret =  in2.readObject();
            if(CC.PARANOID && size!=valueArraySize(ret))
                throw new AssertionError();
            return (Object[]) ret;
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, Object vals) throws IOException {
        ObjectOutputStream out2 = new ObjectOutputStream((OutputStream) out);
        out2.writeObject(vals);
        out2.flush();
    }
}
