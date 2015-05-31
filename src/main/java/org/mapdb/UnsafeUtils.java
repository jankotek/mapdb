package org.mapdb;

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static org.mapdb.Utils.NATIVE_BYTE_ORDER;

import java.lang.reflect.Field;
import java.nio.ByteOrder;

import sun.misc.Unsafe;

enum UnsafeUtils {
  ;

  private static final Unsafe UNSAFE;
  private static final long BYTE_ARRAY_OFFSET;
  private static final int BYTE_ARRAY_SCALE;
  private static final long INT_ARRAY_OFFSET;
  private static final int INT_ARRAY_SCALE;
  private static final long SHORT_ARRAY_OFFSET;
  private static final int SHORT_ARRAY_SCALE;
  
  static {
    try {
      Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
      theUnsafe.setAccessible(true);
      UNSAFE = (Unsafe) theUnsafe.get(null);
      BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
      BYTE_ARRAY_SCALE = UNSAFE.arrayIndexScale(byte[].class);
      INT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(int[].class);
      INT_ARRAY_SCALE = UNSAFE.arrayIndexScale(int[].class);
      SHORT_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(short[].class);
      SHORT_ARRAY_SCALE = UNSAFE.arrayIndexScale(short[].class);
    } catch (IllegalAccessException e) {
      throw new ExceptionInInitializerError("Cannot access Unsafe");
    } catch (NoSuchFieldException e) {
      throw new ExceptionInInitializerError("Cannot access Unsafe");
    } catch (SecurityException e) {
      throw new ExceptionInInitializerError("Cannot access Unsafe");
    }
  }

  public static void checkRange(byte[] buf, int off) {
    SafeUtils.checkRange(buf, off);
  }

  public static void checkRange(byte[] buf, int off, int len) {
    SafeUtils.checkRange(buf, off, len);
  }

  public static void checkLength(int len) {
    SafeUtils.checkLength(len);
  }

  public static byte readByte(byte[] src, int srcOff) {
    return UNSAFE.getByte(src, BYTE_ARRAY_OFFSET + BYTE_ARRAY_SCALE * srcOff);
  }

  public static void writeByte(byte[] src, int srcOff, byte value) {
    UNSAFE.putByte(src, BYTE_ARRAY_OFFSET + BYTE_ARRAY_SCALE * srcOff, (byte) value);
  }

  public static void writeByte(byte[] src, int srcOff, int value) {
    writeByte(src, srcOff, (byte) value);
  }

  public static long readLong(byte[] src, int srcOff) {
    return UNSAFE.getLong(src, BYTE_ARRAY_OFFSET + srcOff);
  }

  public static long readLongLE(byte[] src, int srcOff) {
    long i = readLong(src, srcOff);
    if (NATIVE_BYTE_ORDER == ByteOrder.BIG_ENDIAN) {
      i = Long.reverseBytes(i);
    }
    return i;
  }

  public static void writeLong(byte[] dest, int destOff, long value) {
    UNSAFE.putLong(dest, BYTE_ARRAY_OFFSET + destOff, value);
  }

  public static int readInt(byte[] src, int srcOff) {
    return UNSAFE.getInt(src, BYTE_ARRAY_OFFSET + srcOff);
  }

  public static int readIntLE(byte[] src, int srcOff) {
    int i = readInt(src, srcOff);
    if (NATIVE_BYTE_ORDER == ByteOrder.BIG_ENDIAN) {
      i = Integer.reverseBytes(i);
    }
    return i;
  }

  public static void writeInt(byte[] dest, int destOff, int value) {
    UNSAFE.putInt(dest, BYTE_ARRAY_OFFSET + destOff, value);
  }

  public static short readShort(byte[] src, int srcOff) {
    return UNSAFE.getShort(src, BYTE_ARRAY_OFFSET + srcOff);
  }

  public static int readShortLE(byte[] src, int srcOff) {
    short s = readShort(src, srcOff);
    if (NATIVE_BYTE_ORDER == ByteOrder.BIG_ENDIAN) {
      s = Short.reverseBytes(s);
    }
    return s & 0xFFFF;
  }

  public static void writeShort(byte[] dest, int destOff, short value) {
    UNSAFE.putShort(dest, BYTE_ARRAY_OFFSET + destOff, value);
  }

  public static void writeShortLE(byte[] buf, int off, int v) {
    writeByte(buf, off, (byte) v);
    writeByte(buf, off + 1, (byte) (v >>> 8));
  }

  public static int readInt(int[] src, int srcOff) {
    return UNSAFE.getInt(src, INT_ARRAY_OFFSET + INT_ARRAY_SCALE * srcOff);
  }

  public static void writeInt(int[] dest, int destOff, int value) {
    UNSAFE.putInt(dest, INT_ARRAY_OFFSET + INT_ARRAY_SCALE * destOff, value);
  }

  public static int readShort(short[] src, int srcOff) {
    return UNSAFE.getShort(src, SHORT_ARRAY_OFFSET + SHORT_ARRAY_SCALE * srcOff) & 0xFFFF;
  }

  public static void writeShort(short[] dest, int destOff, int value) {
    UNSAFE.putShort(dest, SHORT_ARRAY_OFFSET + SHORT_ARRAY_SCALE * destOff, (short) value);
  }
}
