package org.mapdb.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class IO {

    public static long readLong(DataInputStream in) throws IOException {
        return in.readLong();
    }

    public static int readInt(DataInputStream in) throws IOException {
            return in.readInt();
    }

    public static byte[] readByteArray(InputStream is, int size) throws IOException {
        byte[] buf = new byte[size];
        int read = is.read(buf, 0, size);
        if(read!=size)
            throw new IOException("not fully read"); //TODO read fully
        return buf;
    }

    public static void writeLong(DataOutputStream out, long value) throws IOException {
        out.writeLong(value);
    }

    public static void writeInt(DataOutputStream out, int value) throws IOException {
        out.writeInt(value);
    }

    public static void writeByteArray(DataOutputStream out, byte[] buf) throws IOException {
        out.write(buf);
    }
}
