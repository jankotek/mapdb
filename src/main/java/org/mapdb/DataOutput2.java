/*
 *  Copyright (c) 2012 Jan Kotek
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.mapdb;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
  * Provides {@link DataOutput} implementation on top of growable {@code byte[]}
 * <p/>
 *  {@link java.io.ByteArrayOutputStream} is not used as it requires {@code byte[]} copying
 *
 * @author Jan Kotek
 */
public final class DataOutput2 extends OutputStream implements DataOutput {

    public byte[] buf;
    public int pos;
    public int sizeMask;


    public DataOutput2(){
        pos = 0;
        buf = new byte[16]; //TODO take hint from serializer for initial size
        sizeMask = 0xFFFFFFFF-(buf.length-1);
    }


    public byte[] copyBytes(){
        return Arrays.copyOf(buf, pos);
    }

    /**
     * make sure there will be enough space in buffer to write N bytes
     */
    public void ensureAvail(int n) {

        n+=pos;
        if ((n&sizeMask)!=0) {
            int newSize = buf.length;
            while(newSize<n){
                newSize<<=2;
                sizeMask<<=2;
            }
            buf = Arrays.copyOf(buf, newSize);
        }
    }

    public static int nextPowTwo(final int a)
    {
        int b = 1;
        while (b < a)
        {
            b = b << 1;
        }
        return b;
    }


    @Override
    public void write(final int b) throws IOException {
        ensureAvail(1);
        buf[pos++] = (byte) b;
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        ensureAvail(len);
        System.arraycopy(b, off, buf, pos, len);
        pos += len;
    }

    @Override
    public void writeBoolean(final boolean v) throws IOException {
        ensureAvail(1);
        buf[pos++] = (byte) (v ? 1 : 0);
    }

    @Override
    public void writeByte(final int v) throws IOException {
        ensureAvail(1);
        buf[pos++] = (byte) (v);
    }

    @Override
    public void writeShort(final int v) throws IOException {
        ensureAvail(2);
        buf[pos++] = (byte) (0xff & (v >> 8));
        buf[pos++] = (byte) (0xff & (v));
    }

    @Override
    public void writeChar(final int v) throws IOException {
        // I know: 4 bytes, but char only consumes 2,
        // has to stay here for backward compatibility
        writeInt(v);
    }

    @Override
    public void writeInt(final int v) throws IOException {
        ensureAvail(4);
        buf[pos++] = (byte) (0xff & (v >> 24));
        buf[pos++] = (byte) (0xff & (v >> 16));
        buf[pos++] = (byte) (0xff & (v >> 8));
        buf[pos++] = (byte) (0xff & (v));
    }

    @Override
    public void writeLong(final long v) throws IOException {
        ensureAvail(8);
        buf[pos++] = (byte) (0xff & (v >> 56));
        buf[pos++] = (byte) (0xff & (v >> 48));
        buf[pos++] = (byte) (0xff & (v >> 40));
        buf[pos++] = (byte) (0xff & (v >> 32));
        buf[pos++] = (byte) (0xff & (v >> 24));
        buf[pos++] = (byte) (0xff & (v >> 16));
        buf[pos++] = (byte) (0xff & (v >> 8));
        buf[pos++] = (byte) (0xff & (v));
    }

    @Override
    public void writeFloat(final float v) throws IOException {
        writeInt(Float.floatToIntBits(v));
    }

    @Override
    public void writeDouble(final double v) throws IOException {
        writeLong(Double.doubleToLongBits(v));
    }

    @Override
    public void writeBytes(final String s) throws IOException {
        writeUTF(s);
    }

    @Override
    public void writeChars(final String s) throws IOException {
        writeUTF(s);
    }

    @Override
    public void writeUTF(final String s) throws IOException {
        final int len = s.length();
        packInt(len);
        for (int i = 0; i < len; i++) {
            int c = (int) s.charAt(i);
            packInt(c);
        }
    }

    /* packInt and packLong originally come from Kryo framework and were written by Nathan Sweet.
     * It was modified to fit MapDB purposes.
     * It is relicensed from BSD to Apache 2 with his permission:
     *
     * Date: 27.5.2014 12:44
     *
     *   Hi Jan,
     *
     *   I'm fine with you putting code from the Kryo under Apache 2.0, as long as you keep the copyright and author. :)
     *
     *   Cheers!
     *   -Nate
     *
     * -----------------------------
     *
     *  Copyright (c) 2012 Nathan Sweet
     *
     *  Licensed under the Apache License, Version 2.0 (the "License");
     *  you may not use this file except in compliance with the License.
     *  You may obtain a copy of the License at
     *
     *    http://www.apache.org/licenses/LICENSE-2.0
     *
     *  Unless required by applicable law or agreed to in writing, software
     *  distributed under the License is distributed on an "AS IS" BASIS,
     *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     *  See the License for the specific language governing permissions and
     *  limitations under the License.
     */

    /**
     * Pack  non-negative long into output stream.
     * It will occupy 1-10 bytes depending on value (lower values occupy smaller space)
     *
     * This method originally comes from Kryo Framework, author Nathan Sweet.
     * It was modified to fit MapDB needs.
     *
     * @param out DataOutput to put value into
     * @param value to be serialized, must be non-negative
     * @throws java.io.IOException
     *
     */
    static public void packLong(DataOutput out, long value) throws IOException {
        if(CC.PARANOID && value<0)
            throw new AssertionError("negative value: "+value);

        while ((value & ~0x7FL) != 0) {
            out.write((((int) value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.write((byte) value);
    }





    /**
     * Pack  non-negative long into output stream.
     * It will occupy 1-5 bytes depending on value (lower values occupy smaller space)
     *
     * This method originally comes from Kryo Framework, author Nathan Sweet.
     * It was modified to fit MapDB needs.
     *
     * @param in DataOutput to put value into
     * @param value to be serialized, must be non-negative
     * @throws IOException
     */

    static public void packInt(DataOutput in, int value) throws IOException {
        if(CC.PARANOID && value<0)
            throw new AssertionError("negative value: "+value);

        while ((value & ~0x7F) != 0) {
            in.write(((value & 0x7F) | 0x80));
            value >>>= 7;
        }

        in.write((byte) value);
    }



    protected void packInt(int value) throws IOException {
        if(CC.PARANOID && value<0)
            throw new AssertionError("negative value: "+value);

        while ((value & ~0x7F) != 0) {
            ensureAvail(1);
            buf[pos++]= (byte) ((value & 0x7F) | 0x80);
            value >>>= 7;
        }
        ensureAvail(1);
        buf[pos++]= (byte) value;
    }

}
