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

import static org.mapdb.LZ4Constants.*;
import static org.mapdb.LZ4Constants.LAST_LITERALS;
import static org.mapdb.LZ4Constants.ML_BITS;
import static org.mapdb.LZ4Constants.ML_MASK;
import static org.mapdb.LZ4Constants.RUN_MASK;
import static org.mapdb.UnsafeUtils.readByte;
import static org.mapdb.UnsafeUtils.readInt;
import static org.mapdb.UnsafeUtils.readLong;
import static org.mapdb.UnsafeUtils.readShort;
import static org.mapdb.UnsafeUtils.writeByte;
import static org.mapdb.UnsafeUtils.writeInt;
import static org.mapdb.UnsafeUtils.writeLong;
import static org.mapdb.UnsafeUtils.writeShort;
import static org.mapdb.Utils.NATIVE_BYTE_ORDER;

import org.mapdb.DBException.LZ4Exception;

import java.nio.ByteOrder;

enum LZ4UnsafeUtils {
  ;

  static void safeArraycopy(byte[] src, int srcOff, byte[] dest, int destOff, int len) {
    final int fastLen = len & 0xFFFFFFF8;
    wildArraycopy(src, srcOff, dest, destOff, fastLen);
    for (int i = 0, slowLen = len & 0x7; i < slowLen; i += 1) {
      writeByte(dest, destOff + fastLen + i, readByte(src, srcOff + fastLen + i));
    }
  }

  static void wildArraycopy(byte[] src, int srcOff, byte[] dest, int destOff, int len) {
    for (int i = 0; i < len; i += 8) {
      writeLong(dest, destOff + i, readLong(src, srcOff + i));
    }
  }

  static void wildIncrementalCopy(byte[] dest, int matchOff, int dOff, int matchCopyEnd) {
    if (dOff - matchOff < 4) {
      for (int i = 0; i < 4; ++i) {
        writeByte(dest, dOff+i, readByte(dest, matchOff+i));
      }
      dOff += 4;
      matchOff += 4;
      int dec = 0;
      assert dOff >= matchOff && dOff - matchOff < 8;
      switch (dOff - matchOff) {
      case 1:
        matchOff -= 3;
        break;
      case 2:
        matchOff -= 2;
        break;
      case 3:
        matchOff -= 3;
        dec = -1;
        break;
      case 5:
        dec = 1;
        break;
      case 6:
        dec = 2;
        break;
      case 7:
        dec = 3;
        break;
      default:
        break;
      }
      writeInt(dest, dOff, readInt(dest, matchOff));
      dOff += 4;
      matchOff -= dec;
    } else if (dOff - matchOff < COPY_LENGTH) {
      writeLong(dest, dOff, readLong(dest, matchOff));
      dOff += dOff - matchOff;
    }
    while (dOff < matchCopyEnd) {
      writeLong(dest, dOff, readLong(dest, matchOff));
      dOff += 8;
      matchOff += 8;
    }
  }

  static void safeIncrementalCopy(byte[] dest, int matchOff, int dOff, int matchLen) {
    for (int i = 0; i < matchLen; ++i) {
      dest[dOff + i] = dest[matchOff + i];
      writeByte(dest, dOff + i, readByte(dest, matchOff + i));
    }
  }

  static int readShortLittleEndian(byte[] src, int srcOff) {
    short s = readShort(src, srcOff);
    if (NATIVE_BYTE_ORDER == ByteOrder.BIG_ENDIAN) {
      s = Short.reverseBytes(s);
    }
    return s & 0xFFFF;
  }

  static void writeShortLittleEndian(byte[] dest, int destOff, int value) {
    short s = (short) value;
    if (NATIVE_BYTE_ORDER == ByteOrder.BIG_ENDIAN) {
      s = Short.reverseBytes(s);
    }
    writeShort(dest, destOff, s);
  }

  static boolean readIntEquals(byte[] src, int ref, int sOff) {
    return readInt(src, ref) == readInt(src, sOff);
  }

  static int commonBytes(byte[] src, int ref, int sOff, int srcLimit) {
    int matchLen = 0;
    while (sOff <= srcLimit - 8) {
      if (readLong(src, sOff) == readLong(src, ref)) {
        matchLen += 8;
        ref += 8;
        sOff += 8;
      } else {
        final int zeroBits;
        if (NATIVE_BYTE_ORDER == ByteOrder.BIG_ENDIAN) {
          zeroBits = Long.numberOfLeadingZeros(readLong(src, sOff) ^ readLong(src, ref));
        } else {
          zeroBits = Long.numberOfTrailingZeros(readLong(src, sOff) ^ readLong(src, ref));
        }
        return matchLen + (zeroBits >>> 3);
      }
    }
    while (sOff < srcLimit && readByte(src, ref++) == readByte(src, sOff++)) {
      ++matchLen;
    }
    return matchLen;
  }

  static int writeLen(int len, byte[] dest, int dOff) {
    while (len >= 0xFF) {
      writeByte(dest, dOff++, 0xFF);
      len -= 0xFF;
    }
    writeByte(dest, dOff++, len);
    return dOff;
  }

  static int encodeSequence(byte[] src, int anchor, int matchOff, int matchRef, int matchLen, byte[] dest, int dOff, int destEnd) {
    final int runLen = matchOff - anchor;
    final int tokenOff = dOff++;
    int token;

    if (runLen >= RUN_MASK) {
      token = (byte) (RUN_MASK << ML_BITS);
      dOff = writeLen(runLen - RUN_MASK, dest, dOff);
    } else {
      token = runLen << ML_BITS;
    }

    // copy literals
    wildArraycopy(src, anchor, dest, dOff, runLen);
    dOff += runLen;

    // encode offset
    final int matchDec = matchOff - matchRef;
    dest[dOff++] = (byte) matchDec;
    dest[dOff++] = (byte) (matchDec >>> 8);

    // encode match len
    matchLen -= 4;
    if (dOff + (1 + LAST_LITERALS) + (matchLen >>> 8) > destEnd) {
      throw new LZ4Exception("maxDestLen is too small");
    }
    if (matchLen >= ML_MASK) {
      token |= ML_MASK;
      dOff = writeLen(matchLen - RUN_MASK, dest, dOff);
    } else {
      token |= matchLen;
    }

    dest[tokenOff] = (byte) token;

    return dOff;
  }

  static int commonBytesBackward(byte[] b, int o1, int o2, int l1, int l2) {
    int count = 0;
    while (o1 > l1 && o2 > l2 && readByte(b, --o1) == readByte(b, --o2)) {
      ++count;
    }
    return count;
  }

  static int lastLiterals(byte[] src, int sOff, int srcLen, byte[] dest, int dOff, int destEnd) {
    return LZ4SafeUtils.lastLiterals(src, sOff, srcLen, dest, dOff, destEnd);
  }

}
