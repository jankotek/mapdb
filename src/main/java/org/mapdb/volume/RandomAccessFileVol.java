package org.mapdb.volume;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.CC;
import org.mapdb.DBException;
import org.mapdb.DBUtil;
import org.mapdb.DataInput2;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;

import static java.lang.Long.rotateLeft;
import static org.mapdb.DBUtil.*;

/**
 * Created by jan on 2/29/16.
 */
public final class RandomAccessFileVol extends Volume {


    public static final VolumeFactory FACTORY = new VolumeFactory() {
        @Override
        public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisable, int sliceShift, long initSize, boolean fixedSize) {
            //TODO allocate initSize
            return new org.mapdb.volume.RandomAccessFileVol(new File(file), readOnly, fileLockDisable, initSize);
        }

        @NotNull
        @Override
        public boolean exists(@Nullable String file) {
            return new File(file).exists();
        }

    };
    protected final File file;
    protected final RandomAccessFile raf;
    protected final FileLock fileLock;


    public RandomAccessFileVol(File file, boolean readOnly, boolean fileLockDisable, long initSize) {
        this.file = file;
        try {
            this.raf = new RandomAccessFile(file, readOnly ? "r" : "rw"); //TODO rwd, rws? etc
            this.fileLock = Volume.lockFile(file, raf, readOnly, fileLockDisable);

            //grow file if needed
            if (initSize != 0 && !readOnly) {
                long oldLen = raf.length();
                if (initSize > raf.length()) {
                    raf.setLength(initSize);
                    clear(oldLen, initSize);
                }
            }
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }
    }

    @Override
    public synchronized void ensureAvailable(long offset) {
        try {
            if (raf.length() < offset)
                raf.setLength(offset);
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }
    }

    @Override
    public synchronized void truncate(long size) {
        try {
            raf.setLength(size);
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }
    }

    @Override
    public synchronized void putLong(long offset, long value) {
        if (CC.VOLUME_PRINT_STACK_AT_OFFSET != 0 && CC.VOLUME_PRINT_STACK_AT_OFFSET >= offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset + 8) {
            new IOException("VOL STACK:").printStackTrace();
        }

        try {
            raf.seek(offset);
            raf.writeLong(value);
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }
    }


    @Override
    public synchronized void putInt(long offset, int value) {
        if (CC.VOLUME_PRINT_STACK_AT_OFFSET != 0 && CC.VOLUME_PRINT_STACK_AT_OFFSET >= offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset + 4) {
            new IOException("VOL STACK:").printStackTrace();
        }

        try {
            raf.seek(offset);
            raf.writeInt(value);
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }

    }

    @Override
    public synchronized void putByte(long offset, byte value) {
        if (CC.VOLUME_PRINT_STACK_AT_OFFSET != 0 && CC.VOLUME_PRINT_STACK_AT_OFFSET == offset) {
            new IOException("VOL STACK:").printStackTrace();
        }

        try {
            raf.seek(offset);
            raf.writeByte(value);
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }

    }

    @Override
    public synchronized void putData(long offset, byte[] src, int srcPos, int srcSize) {
        if (CC.VOLUME_PRINT_STACK_AT_OFFSET != 0 && CC.VOLUME_PRINT_STACK_AT_OFFSET >= offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset + srcSize) {
            new IOException("VOL STACK:").printStackTrace();
        }

        try {
            raf.seek(offset);
            raf.write(src, srcPos, srcSize);
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }
    }

    @Override
    public synchronized void putData(long offset, ByteBuffer buf) {
        byte[] bb = buf.array();
        int pos = buf.position();
        int size = buf.limit() - pos;
        if (CC.VOLUME_PRINT_STACK_AT_OFFSET != 0 && CC.VOLUME_PRINT_STACK_AT_OFFSET >= offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset + size) {
            new IOException("VOL STACK:").printStackTrace();
        }

        if (bb == null) {
            bb = new byte[size];
            buf.get(bb);
            pos = 0;
        }
        putData(offset, bb, pos, size);
    }

    @Override
    public synchronized long getLong(long offset) {
        try {
            raf.seek(offset);
            return raf.readLong();
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }
    }

    @Override
    public synchronized int getInt(long offset) {
        try {
            raf.seek(offset);
            return raf.readInt();
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }

    }

    @Override
    public synchronized byte getByte(long offset) {
        try {
            raf.seek(offset);
            return raf.readByte();
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }
    }

    @Override
    public synchronized DataInput2 getDataInput(long offset, int size) {
        try {
            raf.seek(offset);
            byte[] b = new byte[size];
            raf.readFully(b);
            return new DataInput2.ByteArray(b);
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }
    }

    @Override
    public synchronized void getData(long offset, byte[] bytes, int bytesPos, int size) {
        try {
            raf.seek(offset);
            raf.readFully(bytes, bytesPos, size);
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }
    }

    @Override
    public synchronized void close() {
        if (closed)
            return;

        closed = true;
        try {
            if (fileLock != null && fileLock.isValid()) {
                fileLock.release();
            }
            raf.close();
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }
    }

    @Override
    public synchronized void sync() {
        try {
            raf.getFD().sync();
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }
    }

    @Override
    public int sliceSize() {
        return 0;
    }

    @Override
    public boolean isSliced() {
        return false;
    }

    @Override
    public synchronized long length() {
        try {
            return raf.length();
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }
    }

    @Override
    public File getFile() {
        return file;
    }

    @Override
    public synchronized boolean getFileLocked() {
        return fileLock != null && fileLock.isValid();
    }

    @Override
    public synchronized void clear(long startOffset, long endOffset) {
        try {
            clearRAF(raf, startOffset, endOffset);
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }
    }

    protected static void clearRAF(RandomAccessFile raf, long startOffset, long endOffset) throws IOException {
        raf.seek(startOffset);
        while (startOffset < endOffset) {
            long remaining = Math.min(CLEAR.length, endOffset - startOffset);
            raf.write(CLEAR, 0, (int) remaining);
            startOffset += CLEAR.length;
        }
    }

    @Override
    public synchronized void putUnsignedShort(long offset, int value) {
        try {
            raf.seek(offset);
            raf.write(value >> 8);
            raf.write(value);
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }
    }

    @Override
    public synchronized int getUnsignedShort(long offset) {
        try {
            raf.seek(offset);
            return (raf.readUnsignedByte() << 8) |
                    raf.readUnsignedByte();

        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }
    }

    @Override
    public synchronized long getSixLong(long offset) {
        try {
            raf.seek(offset);
            return
                    (((long) raf.readUnsignedByte()) << 40) |
                            (((long) raf.readUnsignedByte()) << 32) |
                            (((long) raf.readUnsignedByte()) << 24) |
                            (raf.readUnsignedByte() << 16) |
                            (raf.readUnsignedByte() << 8) |
                            raf.readUnsignedByte();
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }
    }

    @Override
    public synchronized void putSixLong(long pos, long value) {
        if (CC.ASSERT && (value >>> 48 != 0))
            throw new DBException.DataCorruption("six long out of range");
        try {
            raf.seek(pos);

            raf.write((int) (value >>> 40));
            raf.write((int) (value >>> 32));
            raf.write((int) (value >>> 24));
            raf.write((int) (value >>> 16));
            raf.write((int) (value >>> 8));
            raf.write((int) (value));
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }

    }

    @Override
    public int putPackedLong(long pos, long value) {
        try {
            raf.seek(pos);

            //$DELAY$
            int ret = 1;
            int shift = 63 - Long.numberOfLeadingZeros(value);
            shift -= shift % 7; // round down to nearest multiple of 7
            while (shift != 0) {
                ret++;
                raf.write((int) (((value >>> shift) & 0x7F) | 0x80));
                //$DELAY$
                shift -= 7;
            }
            raf.write((int) (value & 0x7F));
            return ret;
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }

    }


    @Override
    public long getPackedLong(long pos) {
        try {
            raf.seek(pos);

            long ret = 0;
            long pos2 = 0;
            byte v;
            do {
                pos2++;
                v = raf.readByte();
                ret = (ret << 7) | (v & 0x7F);
            } while (v < 0);

            return (pos2 << 60) | ret;
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }

    }

    @Override
    public synchronized long hash(long off, long len, long seed) {
        if (len < 0) {
            throw new IllegalArgumentException("lengths must be >= 0");
        }
        if (len == 0)
            return seed;
        long bufLen = length();
        if (off < 0 || off >= bufLen || off + len < 0 || off + len > bufLen) {
            throw new IndexOutOfBoundsException();
        }
        try {
            raf.seek(off);

            while ((off & 0x7) != 0 && len > 0) {
                //scroll until offset is not dividable by 8
                seed = (seed << 8) | raf.readUnsignedByte();
                off++;
                len--;
            }

            final long end = off + len;
            long h64;

            if (len >= 32) {
                final long limit = end - 32;
                long v1 = seed + PRIME64_1 + PRIME64_2;
                long v2 = seed + PRIME64_2;
                long v3 = seed + 0;
                long v4 = seed - PRIME64_1;
                byte[] buf = new byte[32];
                do {
                    raf.readFully(buf); //reading single byte[] is faster than 4xreadLong
                    v1 += Long.reverseBytes(DBUtil.getLong(buf, 0)) * PRIME64_2;
                    v1 = rotateLeft(v1, 31);
                    v1 *= PRIME64_1;
                    off += 8;

                    v2 += Long.reverseBytes(DBUtil.getLong(buf, 8)) * PRIME64_2;
                    v2 = rotateLeft(v2, 31);
                    v2 *= PRIME64_1;
                    off += 8;

                    v3 += Long.reverseBytes(DBUtil.getLong(buf, 16)) * PRIME64_2;
                    v3 = rotateLeft(v3, 31);
                    v3 *= PRIME64_1;
                    off += 8;

                    v4 += Long.reverseBytes(DBUtil.getLong(buf, 24)) * PRIME64_2;
                    v4 = rotateLeft(v4, 31);
                    v4 *= PRIME64_1;
                    off += 8;
                } while (off <= limit);

                h64 = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);

                v1 *= PRIME64_2;
                v1 = rotateLeft(v1, 31);
                v1 *= PRIME64_1;
                h64 ^= v1;
                h64 = h64 * PRIME64_1 + PRIME64_4;

                v2 *= PRIME64_2;
                v2 = rotateLeft(v2, 31);
                v2 *= PRIME64_1;
                h64 ^= v2;
                h64 = h64 * PRIME64_1 + PRIME64_4;

                v3 *= PRIME64_2;
                v3 = rotateLeft(v3, 31);
                v3 *= PRIME64_1;
                h64 ^= v3;
                h64 = h64 * PRIME64_1 + PRIME64_4;

                v4 *= PRIME64_2;
                v4 = rotateLeft(v4, 31);
                v4 *= PRIME64_1;
                h64 ^= v4;
                h64 = h64 * PRIME64_1 + PRIME64_4;
            } else {
                h64 = seed + PRIME64_5;
            }

            h64 += len;

            while (off <= end - 8) {
                long k1 = Long.reverseBytes(raf.readLong());
                k1 *= PRIME64_2;
                k1 = rotateLeft(k1, 31);
                k1 *= PRIME64_1;
                h64 ^= k1;
                h64 = rotateLeft(h64, 27) * PRIME64_1 + PRIME64_4;
                off += 8;
            }

            if (off <= end - 4) {
                h64 ^= (Integer.reverseBytes(raf.readInt()) & 0xFFFFFFFFL) * PRIME64_1;
                h64 = rotateLeft(h64, 23) * PRIME64_2 + PRIME64_3;
                off += 4;
            }

            while (off < end) {
                h64 ^= (raf.readByte() & 0xFF) * PRIME64_5;
                h64 = rotateLeft(h64, 11) * PRIME64_1;
                ++off;
            }

            h64 ^= h64 >>> 33;
            h64 *= PRIME64_2;
            h64 ^= h64 >>> 29;
            h64 *= PRIME64_3;
            h64 ^= h64 >>> 32;

            return h64;
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }
    }

}
