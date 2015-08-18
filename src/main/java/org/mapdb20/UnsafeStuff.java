package org.mapdb20;


import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.Long.rotateLeft;
import static org.mapdb20.DataIO.PRIME64_1;
import static org.mapdb20.DataIO.PRIME64_2;
import static org.mapdb20.DataIO.PRIME64_3;
import static org.mapdb20.DataIO.PRIME64_4;
import static org.mapdb20.DataIO.PRIME64_5;


/**
 * Contains classes which use {@code sun.misc.Unsafe}.
 * This class will fail to compile on Android, to proceed just delete it and associated unit test.
 * It is not referenced directly, is only instantiated indirectly with reflection,
 * and MapDB will use other option.
 *
 */
class UnsafeStuff {

    static final Logger LOG = Logger.getLogger(UnsafeStuff.class.getName());

    static final sun.misc.Unsafe UNSAFE = getUnsafe();

    @SuppressWarnings("restriction")
    private static sun.misc.Unsafe getUnsafe() {
        if(ByteOrder.nativeOrder()!=ByteOrder.LITTLE_ENDIAN){
            LOG.log(Level.WARNING,"This is not Little Endian platform. Unsafe optimizations are disabled.");
            return null;
        }
        try {
            java.lang.reflect.Field singleoneInstanceField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            singleoneInstanceField.setAccessible(true);
            sun.misc.Unsafe ret =  (sun.misc.Unsafe)singleoneInstanceField.get(null);
            return ret;
        } catch (Throwable e) {
            LOG.log(Level.WARNING,"Could not instantiate sun.misc.Unsafe. Fall back to DirectByteBuffer and other alternatives.",e);
            return null;
        }
    }

    private static final long BYTE_ARRAY_OFFSET;
    private static final int BYTE_ARRAY_SCALE;
    private static final long INT_ARRAY_OFFSET;
    private static final int INT_ARRAY_SCALE;
    private static final long SHORT_ARRAY_OFFSET;
    private static final int SHORT_ARRAY_SCALE;
    private static final long CHAR_ARRAY_OFFSET;
    private static final int CHAR_ARRAY_SCALE;

    static {
        BYTE_ARRAY_OFFSET = UNSAFE==null?-1:UNSAFE.arrayBaseOffset(byte[].class);
        BYTE_ARRAY_SCALE = UNSAFE==null?-1:UNSAFE.arrayIndexScale(byte[].class);
        INT_ARRAY_OFFSET = UNSAFE==null?-1:UNSAFE.arrayBaseOffset(int[].class);
        INT_ARRAY_SCALE = UNSAFE==null?-1:UNSAFE.arrayIndexScale(int[].class);
        SHORT_ARRAY_OFFSET = UNSAFE==null?-1:UNSAFE.arrayBaseOffset(short[].class);
        SHORT_ARRAY_SCALE = UNSAFE==null?-1:UNSAFE.arrayIndexScale(short[].class);
        CHAR_ARRAY_OFFSET = UNSAFE==null?-1:UNSAFE.arrayBaseOffset(char[].class);
        CHAR_ARRAY_SCALE = UNSAFE==null?-1:UNSAFE.arrayIndexScale(char[].class);
    }


    public static boolean unsafeAvailable(){
        return UNSAFE !=null;
    }


    static final class UnsafeVolume extends Volume {



        // Cached array base offset
        private static final long ARRAY_BASE_OFFSET = UNSAFE ==null?-1 : UNSAFE.arrayBaseOffset(byte[].class);;

        public static final VolumeFactory FACTORY = new VolumeFactory() {
            @Override
            public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisable, int sliceShift, long initSize, boolean fixedSize) {
                return new UnsafeVolume(0,sliceShift, initSize);
            }
        };

        public static boolean unsafeAvailable(){
            return UNSAFE !=null;
        }


        // This number limits the number of bytes to copy per call to Unsafe's
        // copyMemory method. A limit is imposed to allow for safepoint polling
        // during a large copy
        static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;


        static void copyFromArray(byte[] src, long srcPos,
                                  long dstAddr, long length)
        {
            long offset = ARRAY_BASE_OFFSET + srcPos;
            while (length > 0) {
                long size = (length > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : length;
                UNSAFE.copyMemory(src, offset, null, dstAddr, size);
                length -= size;
                offset += size;
                dstAddr += size;
            }
        }


        static void copyToArray(long srcAddr, byte[] dst, long dstPos,
                                long length)
        {
            long offset = ARRAY_BASE_OFFSET + dstPos;
            while (length > 0) {
                long size = (length > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : length;
                UNSAFE.copyMemory(null, srcAddr, dst, offset, size);
                length -= size;
                srcAddr += size;
                offset += size;
            }
        }



        protected volatile long[] addresses= new long[0];
        protected volatile sun.nio.ch.DirectBuffer[] buffers = new sun.nio.ch.DirectBuffer[0];

        protected final long sizeLimit;
        protected final boolean hasLimit;
        protected final int sliceShift;
        protected final int sliceSizeModMask;
        protected final int sliceSize;

        protected final ReentrantLock growLock = new ReentrantLock(CC.FAIR_LOCKS);


        public UnsafeVolume() {
            this(0, CC.VOLUME_PAGE_SHIFT,0L);
        }

        public UnsafeVolume(long sizeLimit, int sliceShift, long initSize) {
            this.sizeLimit = sizeLimit;
            this.hasLimit = sizeLimit>0;
            this.sliceShift = sliceShift;
            this.sliceSize = 1<< sliceShift;
            this.sliceSizeModMask = sliceSize -1;
            if(initSize!=0)
                ensureAvailable(initSize);
        }


        @Override
        public void ensureAvailable(long offset) {
            offset=Fun.roundUp(offset,1L<<sliceShift);

            //*LOG*/ System.err.printf("tryAvailabl: offset:%d\n",offset);
            //*LOG*/ System.err.flush();
            if(hasLimit && offset>sizeLimit) {
                //return false;
                throw new IllegalAccessError("too big"); //TODO size limit here
            }


            int slicePos = (int) (offset >>> sliceShift);

            //check for most common case, this is already mapped
            if (slicePos < addresses.length){
                return;
            }

            growLock.lock();
            try{
                //check second time
                if(slicePos<= addresses.length)
                    return; //already enough space

                int oldSize = addresses.length;
                long[] addresses2 = addresses;
                sun.nio.ch.DirectBuffer[] buffers2 = buffers;

                int newSize = slicePos;
                addresses2 = Arrays.copyOf(addresses2, newSize);
                buffers2 = Arrays.copyOf(buffers2, newSize);

                for(int pos=oldSize;pos<addresses2.length;pos++) {
                    //take address from DirectByteBuffer so allocated memory can be released by GC
                    sun.nio.ch.DirectBuffer buf = (sun.nio.ch.DirectBuffer) ByteBuffer.allocateDirect(sliceSize);
                    long address = buf.address();

                    //TODO is cleanup necessary here?
                    //TODO speedup  by copying an array
                    for(long i=0;i<sliceSize;i+=8) {
                        UNSAFE.putLong(address + i, 0L);
                    }

                    buffers2[pos]=buf;
                    addresses2[pos]=address;
                }

                addresses = addresses2;
                buffers = buffers2;
            }finally{
                growLock.unlock();
            }
        }


        @Override
        public void truncate(long size) {
            //TODO support truncate
        }

        @Override
        public void putLong(long offset, long value) {
            //*LOG*/ System.err.printf("putLong: offset:%d, value:%d\n",offset,value);
            //*LOG*/ System.err.flush();
            value = Long.reverseBytes(value);
            final long address = addresses[((int) (offset >>> sliceShift))];
            offset = offset & sliceSizeModMask;
            UNSAFE.putLong(address + offset, value);
        }

        @Override
        public void putInt(long offset, int value) {
            //*LOG*/ System.err.printf("putInt: offset:%d, value:%d\n",offset,value);
            //*LOG*/ System.err.flush();
            value = Integer.reverseBytes(value);
            final long address = addresses[((int) (offset >>> sliceShift))];
            offset = offset & sliceSizeModMask;
            UNSAFE.putInt(address + offset, value);
        }

        @Override
        public void putByte(long offset, byte value) {
            //*LOG*/ System.err.printf("putByte: offset:%d, value:%d\n",offset,value);
            //*LOG*/ System.err.flush();
            final long address = addresses[((int) (offset >>> sliceShift))];
            offset = offset & sliceSizeModMask;
            UNSAFE.putByte(address + offset, value);
        }

        @Override
        public void putData(long offset, byte[] src, int srcPos, int srcSize) {
//        for(int pos=srcPos;pos<srcPos+srcSize;pos++){
//            UNSAFE.putByte(address+offset+pos,src[pos]);
//        }
            //*LOG*/ System.err.printf("putData: offset:%d, srcLen:%d, srcPos:%d, srcSize:%d\n",offset, src.length, srcPos, srcSize);
            //*LOG*/ System.err.flush();
            final long address = addresses[((int) (offset >>> sliceShift))];
            offset = offset & sliceSizeModMask;

            copyFromArray(src, srcPos, address+offset, srcSize);
        }

        @Override
        public void putData(long offset, ByteBuffer buf) {
            //*LOG*/ System.err.printf("putData: offset:%d, bufPos:%d, bufLimit:%d:\n",offset,buf.position(), buf.limit());
            //*LOG*/ System.err.flush();
            final long address = addresses[((int) (offset >>> sliceShift))];
            offset = offset & sliceSizeModMask;

            for(int pos=buf.position();pos<buf.limit();pos++){
                UNSAFE.putByte(address + offset + pos, buf.get(pos));
            }

        }

        @Override
        public long getLong(long offset) {
            //*LOG*/ System.err.printf("getLong: offset:%d \n",offset);
            //*LOG*/ System.err.flush();
            final long address = addresses[((int) (offset >>> sliceShift))];
            offset = offset & sliceSizeModMask;
            long l =  UNSAFE.getLong(address +offset);
            return Long.reverseBytes(l);
        }

        @Override
        public int getInt(long offset) {
            //*LOG*/ System.err.printf("getInt: offset:%d\n",offset);
            //*LOG*/ System.err.flush();
            final long address = addresses[((int) (offset >>> sliceShift))];
            offset = offset & sliceSizeModMask;
            int i =  UNSAFE.getInt(address +offset);
            return Integer.reverseBytes(i);
        }

        @Override
        public byte getByte(long offset) {
            //*LOG*/ System.err.printf("getByte: offset:%d\n",offset);
            //*LOG*/ System.err.flush();
            final long address = addresses[((int) (offset >>> sliceShift))];
            offset = offset & sliceSizeModMask;

            return UNSAFE.getByte(address +offset);
        }

        @Override
        public DataInput getDataInput(long offset, int size) {
            final long address = addresses[((int) (offset >>> sliceShift))];
            offset = offset & sliceSizeModMask;
            return new DataInputUnsafe(address, (int) offset);
        }

        @Override
        public void getData(long offset, byte[] bytes, int bytesPos, int size) {
            final long address = addresses[((int) (offset >>> sliceShift))];
            offset = offset & sliceSizeModMask;
            copyToArray(address+offset,bytes, bytesPos,size);
        }

//        @Override
//        public DataInput2 getDataInput(long offset, int size) {
//            //*LOG*/ System.err.printf("getDataInput: offset:%d, size:%d\n",offset,size);
//            //*LOG*/ System.err.flush();
//            byte[] dst = new byte[size];
////        for(int pos=0;pos<size;pos++){
////            dst[pos] = UNSAFE.getByte(address +offset+pos);
////        }
//
//            final long address = addresses[((int) (offset >>> sliceShift))];
//            offset = offset & sliceSizeModMask;
//
//            copyToArray(address+offset, dst, ARRAY_BASE_OFFSET,
//                    0,
//                    size);
//
//            return new DataInput2(dst);
//        }



        @Override
        public void putDataOverlap(long offset, byte[] data, int pos, int len) {
            boolean overlap = (offset>>>sliceShift != (offset+len)>>>sliceShift);

            if(overlap){
                while(len>0){
                    long addr = addresses[((int) (offset >>> sliceShift))];
                    long pos2 = offset&sliceSizeModMask;

                    long toPut = Math.min(len,sliceSize - pos2);

                    //System.arraycopy(data, pos, b, pos2, toPut);
                    copyFromArray(data,pos,addr+pos2,toPut);

                    pos+=toPut;
                    len -=toPut;
                    offset+=toPut;
                }
            }else{
                putData(offset,data,pos,len);
            }
        }

        @Override
        public DataInput getDataInputOverlap(long offset, int size) {
            boolean overlap = (offset>>>sliceShift != (offset+size)>>>sliceShift);
            if(overlap){
                byte[] bb = new byte[size];
                final int origLen = size;
                while(size>0){
                    long addr = addresses[((int) (offset >>> sliceShift))];
                    long pos = offset&sliceSizeModMask;
                    long toPut = Math.min(size,sliceSize - pos);

                    //System.arraycopy(b, pos, bb, origLen - size, toPut);
                    copyToArray(addr+pos,bb,origLen-size,toPut);

                    size -=toPut;
                    offset+=toPut;
                }
                return new DataIO.DataInputByteArray(bb);
            }else{
                //return mapped buffer
                return getDataInput(offset,size);
            }
        }



        @Override
        public void close() {
            closed = true;
            sun.nio.ch.DirectBuffer[] buf2 = buffers;
            buffers=null;
            addresses = null;
            for(sun.nio.ch.DirectBuffer buf:buf2){
                buf.cleaner().clean();
            }
        }

        @Override
        public void sync() {
        }

        @Override
        public int sliceSize() {
            return sliceSize;
        }


        @Override
        public boolean isSliced() {
            return true;
        }

        @Override
        public long length() {
            return 1L*addresses.length*sliceSize;
        }

        @Override
        public File getFile() {
            return null;
        }

        @Override
        public boolean getFileLocked() {
            return false;
        }

        @Override
        public void clear(long startOffset, long endOffset) {
            while(startOffset<endOffset){
                putByte(startOffset++, (byte) 0); //TODO use batch copy
            }
        }

        public static final class DataInputUnsafe implements DataIO.DataInputInternal{

            protected final long baseAdress;
            protected long pos2;

            public DataInputUnsafe(long baseAdress, int pos) {
                this.baseAdress = baseAdress;
                this.pos2 = baseAdress+pos;
            }


            @Override
            public int getPos() {
                return (int) (pos2-baseAdress);
            }

            @Override
            public void setPos(int pos) {
                this.pos2 = baseAdress+pos;
            }

            @Override
            public byte[] internalByteArray() {
                return null;
            }

            @Override
            public ByteBuffer internalByteBuffer() {
                return null;
            }

            @Override
            public void close() {

            }

            @Override
            public long unpackLong() throws IOException {
                sun.misc.Unsafe UNSAFE = UnsafeStuff.UNSAFE;
                long pos = pos2;
                long ret = 0;
                byte v;
                do{
                    //$DELAY$
                    v = UNSAFE.getByte(pos++);
                    ret = (ret<<7 ) | (v & 0x7F);
                }while(v<0);
                pos2 = pos;
                return ret;

            }

            @Override
            public int unpackInt() throws IOException {
                sun.misc.Unsafe UNSAFE = UnsafeStuff.UNSAFE;
                long pos = pos2;
                int ret = 0;
                byte v;
                do{
                    //$DELAY$
                    v = UNSAFE.getByte(pos++);
                    ret = (ret<<7 ) | (v & 0x7F);
                }while(v<0);
                pos2 = pos;
                return ret;

            }


            @Override
            public long[] unpackLongArrayDeltaCompression(final int size) throws IOException {
                sun.misc.Unsafe UNSAFE = UnsafeStuff.UNSAFE;
                long[] ret = new long[size];
                long pos2_ = pos2;
                long prev=0;
                byte v;
                for(int i=0;i<size;i++){
                    long r = 0;
                    do {
                        //$DELAY$
                        v = UNSAFE.getByte(pos2_++);
                        r = (r << 7) | (v & 0x7F);
                    } while (v < 0);
                    prev+=r;
                    ret[i]=prev;
                }
                pos2 = pos2_;
                return ret;
            }

            @Override
            public void unpackLongArray(long[] array, int start, int end) {
                sun.misc.Unsafe UNSAFE = UnsafeStuff.UNSAFE;
                long pos2_ = pos2;
                long ret;
                byte v;
                for(;start<end;start++) {
                    ret = 0;
                    do {
                        //$DELAY$
                        v = UNSAFE.getByte(pos2_++);
                        ret = (ret << 7) | (v & 0x7F);
                    } while (v < 0);
                    array[start] = ret;
                }
                pos2 = pos2_;
            }

            @Override
            public void unpackIntArray(int[] array, int start, int end) {
                sun.misc.Unsafe UNSAFE = UnsafeStuff.UNSAFE;
                long pos2_ = pos2;
                int ret;
                byte v;
                for(;start<end;start++) {
                    ret = 0;
                    do {
                        //$DELAY$
                        v = UNSAFE.getByte(pos2_++);
                        ret = (ret << 7) | (v & 0x7F);
                    } while (v < 0);
                    array[start]=ret;
                }
                pos2 = pos2_;
            }

            @Override
            public void readFully(byte[] b) throws IOException {
                copyToArray(pos2, b, 0, b.length);
                pos2+=b.length;
            }

            @Override
            public void readFully(byte[] b, int off, int len) throws IOException {
                copyToArray(pos2,b,off,len);
                pos2+=len;
            }

            @Override
            public int skipBytes(int n) throws IOException {
                pos2+=n;
                return n;
            }

            @Override
            public boolean readBoolean() throws IOException {
                return readByte()==1;
            }

            @Override
            public byte readByte() throws IOException {
                return UnsafeStuff.UNSAFE.getByte(pos2++);
            }

            @Override
            public int readUnsignedByte() throws IOException {
                return UnsafeStuff.UNSAFE.getByte(pos2++) & 0xFF;
            }

            @Override
            public short readShort() throws IOException {
                //$DELAY$
                return (short)((readByte() << 8) | (readByte() & 0xff));
            }

            @Override
            public int readUnsignedShort() throws IOException {
                //$DELAY$
                return readChar();
            }

            @Override
            public char readChar() throws IOException {
                //$DELAY$
                return (char)(
                        ((readByte() & 0xff) << 8) |
                        ((readByte() & 0xff)));
            }

            @Override
            public int readInt() throws IOException {
                int ret = UnsafeStuff.UNSAFE.getInt(pos2);
                pos2+=4;
                return Integer.reverseBytes(ret);
            }

            @Override
            public long readLong() throws IOException {
                long ret = UnsafeStuff.UNSAFE.getLong(pos2);
                pos2+=8;
                return Long.reverseBytes(ret);
            }

            @Override
            public float readFloat() throws IOException {
                return Float.intBitsToFloat(readInt());
            }

            @Override
            public double readDouble() throws IOException {
                return Double.longBitsToDouble(readLong());
            }

            @Override
            public String readLine() throws IOException {
                return readUTF();
            }

            @Override
            public String readUTF() throws IOException {
                final int len = unpackInt();
                char[] b = new char[len];
                for (int i = 0; i < len; i++)
                    //$DELAY$
                    b[i] = (char) unpackInt();
                return new String(b);
            }

        }
    }


    /**
     * <p>
     * Calculates XXHash64 from given {@code byte[]} buffer.
     * </p><p>
     * This code comes from <a href="https://github.com/jpountz/lz4-java">LZ4-Java</a> created
     * by Adrien Grand.
     * </p>
     *
     * @param buf to calculate hash from
     * @param off offset to start calculation from
     * @param len length of data to calculate hash
     * @param seed  hash seed
     * @return XXHash.
     */
    public static long hash(byte[] buf, int off, int len, long seed) {
        if (UNSAFE==null){
            return DataIO.hash(buf,off,len,seed);
        }

        if (len < 0) {
            throw new IllegalArgumentException("lengths must be >= 0");
        }
        if(off<0 || off>buf.length || off+len<0 || off+len>buf.length){
            throw new IndexOutOfBoundsException();
        }

        final int end = off + len;
        long h64;

        if (len >= 32) {
            final int limit = end - 32;
            long v1 = seed + PRIME64_1 + PRIME64_2;
            long v2 = seed + PRIME64_2;
            long v3 = seed + 0;
            long v4 = seed - PRIME64_1;
            do {
                v1 += readLongLE(buf, off) * PRIME64_2;
                v1 = rotateLeft(v1, 31);
                v1 *= PRIME64_1;
                off += 8;

                v2 += readLongLE(buf, off) * PRIME64_2;
                v2 = rotateLeft(v2, 31);
                v2 *= PRIME64_1;
                off += 8;

                v3 += readLongLE(buf, off) * PRIME64_2;
                v3 = rotateLeft(v3, 31);
                v3 *= PRIME64_1;
                off += 8;

                v4 += readLongLE(buf, off) * PRIME64_2;
                v4 = rotateLeft(v4, 31);
                v4 *= PRIME64_1;
                off += 8;
            } while (off <= limit);

            h64 = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);

            v1 *= PRIME64_2; v1 = rotateLeft(v1, 31); v1 *= PRIME64_1; h64 ^= v1;
            h64 = h64 * PRIME64_1 + PRIME64_4;

            v2 *= PRIME64_2; v2 = rotateLeft(v2, 31); v2 *= PRIME64_1; h64 ^= v2;
            h64 = h64 * PRIME64_1 + PRIME64_4;

            v3 *= PRIME64_2; v3 = rotateLeft(v3, 31); v3 *= PRIME64_1; h64 ^= v3;
            h64 = h64 * PRIME64_1 + PRIME64_4;

            v4 *= PRIME64_2; v4 = rotateLeft(v4, 31); v4 *= PRIME64_1; h64 ^= v4;
            h64 = h64 * PRIME64_1 + PRIME64_4;
        } else {
            h64 = seed + PRIME64_5;
        }

        h64 += len;

        while (off <= end - 8) {
            long k1 = readLongLE(buf, off);
            k1 *= PRIME64_2; k1 = rotateLeft(k1, 31); k1 *= PRIME64_1; h64 ^= k1;
            h64 = rotateLeft(h64, 27) * PRIME64_1 + PRIME64_4;
            off += 8;
        }

        if (off <= end - 4) {
            h64 ^= (readIntLE(buf, off) & 0xFFFFFFFFL) * PRIME64_1;
            h64 = rotateLeft(h64, 23) * PRIME64_2 + PRIME64_3;
            off += 4;
        }

        while (off < end) {
            h64 ^= (buf[off] & 0xFF) * PRIME64_5;
            h64 = rotateLeft(h64, 11) * PRIME64_1;
            ++off;
        }

        h64 ^= h64 >>> 33;
        h64 *= PRIME64_2;
        h64 ^= h64 >>> 29;
        h64 *= PRIME64_3;
        h64 ^= h64 >>> 32;

        return h64;
    }


    public static long readLongLE(byte[] src, int srcOff) {
        return UNSAFE.getLong(src, BYTE_ARRAY_OFFSET + srcOff);
    }


    public static int readIntLE(byte[] src, int srcOff) {
        return UNSAFE.getInt(src, BYTE_ARRAY_OFFSET + srcOff);
    }


    /**
     * <p>
     * Calculates XXHash64 from given {@code char[]} buffer.
     * </p><p>
     * This code comes from <a href="https://github.com/jpountz/lz4-java">LZ4-Java</a> created
     * by Adrien Grand.
     * </p>
     *
     * @param buf to calculate hash from
     * @param off offset to start calculation from
     * @param len length of data to calculate hash
     * @param seed  hash seed
     * @return XXHash.
     */
    public static long hash(char[] buf, int off, int len, long seed) {
        if (UNSAFE==null){
            return DataIO.hash(buf,off,len,seed);
        }

        if (len < 0) {
            throw new IllegalArgumentException("lengths must be >= 0");
        }
        if(off<0 || off>buf.length || off+len<0 || off+len>buf.length){
            throw new IndexOutOfBoundsException();
        }

        final int end = off + len;
        long h64;

        if (len >= 16) {
            final int limit = end - 16;
            long v1 = seed + PRIME64_1 + PRIME64_2;
            long v2 = seed + PRIME64_2;
            long v3 = seed + 0;
            long v4 = seed - PRIME64_1;
            do {
                v1 += readLongLE(buf, off) * PRIME64_2;
                v1 = rotateLeft(v1, 31);
                v1 *= PRIME64_1;
                off += 4;

                v2 += readLongLE(buf, off) * PRIME64_2;
                v2 = rotateLeft(v2, 31);
                v2 *= PRIME64_1;
                off += 4;

                v3 += readLongLE(buf, off) * PRIME64_2;
                v3 = rotateLeft(v3, 31);
                v3 *= PRIME64_1;
                off += 4;

                v4 += readLongLE(buf, off) * PRIME64_2;
                v4 = rotateLeft(v4, 31);
                v4 *= PRIME64_1;
                off += 4;
            } while (off <= limit);

            h64 = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);

            v1 *= PRIME64_2; v1 = rotateLeft(v1, 31); v1 *= PRIME64_1; h64 ^= v1;
            h64 = h64 * PRIME64_1 + PRIME64_4;

            v2 *= PRIME64_2; v2 = rotateLeft(v2, 31); v2 *= PRIME64_1; h64 ^= v2;
            h64 = h64 * PRIME64_1 + PRIME64_4;

            v3 *= PRIME64_2; v3 = rotateLeft(v3, 31); v3 *= PRIME64_1; h64 ^= v3;
            h64 = h64 * PRIME64_1 + PRIME64_4;

            v4 *= PRIME64_2; v4 = rotateLeft(v4, 31); v4 *= PRIME64_1; h64 ^= v4;
            h64 = h64 * PRIME64_1 + PRIME64_4;
        } else {
            h64 = seed + PRIME64_5;
        }

        h64 += len;

        while (off <= end - 4) {
            long k1 = readLongLE(buf, off);
            k1 *= PRIME64_2; k1 = rotateLeft(k1, 31); k1 *= PRIME64_1; h64 ^= k1;
            h64 = rotateLeft(h64, 27) * PRIME64_1 + PRIME64_4;
            off += 4;
        }

        if (off <= end - 2) {
            h64 ^= (readIntLE(buf, off) & 0xFFFFFFFFL) * PRIME64_1;
            h64 = rotateLeft(h64, 23) * PRIME64_2 + PRIME64_3;
            off += 2;
        }

        while (off < end) {
            h64 ^= (readCharLE(buf,off) & 0xFFFF) * PRIME64_5;
            h64 = rotateLeft(h64, 11) * PRIME64_1;
            ++off;
        }

        h64 ^= h64 >>> 33;
        h64 *= PRIME64_2;
        h64 ^= h64 >>> 29;
        h64 *= PRIME64_3;
        h64 ^= h64 >>> 32;

        return h64;
    }

    public static long readLongLE(char[] src, int srcOff) {
        return UNSAFE.getLong(src, CHAR_ARRAY_OFFSET + srcOff * CHAR_ARRAY_SCALE);
    }


    public static int readIntLE(char[] src, int srcOff) {
        return UNSAFE.getInt(src, CHAR_ARRAY_OFFSET + srcOff * CHAR_ARRAY_SCALE);
    }

    public static char readCharLE(char[] src, int srcOff) {
        return UNSAFE.getChar(src, CHAR_ARRAY_OFFSET + srcOff*CHAR_ARRAY_SCALE);
    }
}
