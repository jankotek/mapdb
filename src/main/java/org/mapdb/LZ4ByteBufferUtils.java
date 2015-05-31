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

import static org.mapdb.LZ4Constants.COPY_LENGTH;
import static org.mapdb.LZ4Constants.LAST_LITERALS;
import static org.mapdb.LZ4Constants.ML_BITS;
import static org.mapdb.LZ4Constants.ML_MASK;
import static org.mapdb.LZ4Constants.RUN_MASK;
import static org.mapdb.ByteBufferUtils.readByte;
import static org.mapdb.ByteBufferUtils.readInt;
import static org.mapdb.ByteBufferUtils.readLong;
import static org.mapdb.ByteBufferUtils.writeByte;
import static org.mapdb.ByteBufferUtils.writeInt;
import static org.mapdb.ByteBufferUtils.writeLong;
import org.mapdb.DBException.LZ4Exception;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

enum LZ4ByteBufferUtils {
  ;
  static int hash(ByteBuffer buf, int i) {
    return LZ4Utils.hash(readInt(buf, i));
  }

  static int hash64k(ByteBuffer buf, int i) {
    return LZ4Utils.hash64k(readInt(buf, i));
  }

  static boolean readIntEquals(ByteBuffer buf, int i, int j) {
    return buf.getInt(i) == buf.getInt(j);
  }

  static void safeIncrementalCopy(ByteBuffer dest, int matchOff, int dOff, int matchLen) {
    for (int i = 0; i < matchLen; ++i) {
      dest.put(dOff + i, dest.get(matchOff + i));
    }
  }

  static void wildIncrementalCopy(ByteBuffer dest, int matchOff, int dOff, int matchCopyEnd) {
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

  static int commonBytes(ByteBuffer src, int ref, int sOff, int srcLimit) {
    int matchLen = 0;
    while (sOff <= srcLimit - 8) {
      if (readLong(src, sOff) == readLong(src, ref)) {
        matchLen += 8;
        ref += 8;
        sOff += 8;
      } else {
        final int zeroBits;
        if (src.order() == ByteOrder.BIG_ENDIAN) {
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

  static int commonBytesBackward(ByteBuffer b, int o1, int o2, int l1, int l2) {
    int count = 0;
    while (o1 > l1 && o2 > l2 && b.get(--o1) == b.get(--o2)) {
      ++count;
    }
    return count;
  }

  static void safeArraycopy(ByteBuffer src, int sOff, ByteBuffer dest, int dOff, int len) {
    for (int i = 0; i < len; ++i) {
      dest.put(dOff + i, src.get(sOff + i));
    }
  }

  static void wildArraycopy(ByteBuffer src, int sOff, ByteBuffer dest, int dOff, int len) {
    assert src.order().equals(dest.order());
    try {
      for (int i = 0; i < len; i += 8) {
        dest.putLong(dOff + i, src.getLong(sOff + i));
      }
    } catch (IndexOutOfBoundsException e) {
      throw new LZ4Exception("Malformed input at offset " + sOff);
    }
  }

  static int encodeSequence(ByteBuffer src, int anchor, int matchOff, int matchRef, int matchLen, ByteBuffer dest, int dOff, int destEnd) {
    final int runLen = matchOff - anchor;
    final int tokenOff = dOff++;

    if (dOff + runLen + (2 + 1 + LAST_LITERALS) + (runLen >>> 8) > destEnd) {
      throw new LZ4Exception("maxDestLen is too small");
    }

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
    dest.put(dOff++, (byte) matchDec);
    dest.put(dOff++, (byte) (matchDec >>> 8));

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

    dest.put(tokenOff, (byte) token);

    return dOff;
  }

  static int lastLiterals(ByteBuffer src, int sOff, int srcLen, ByteBuffer dest, int dOff, int destEnd) {
    final int runLen = srcLen;

    if (dOff + runLen + 1 + (runLen + 255 - RUN_MASK) / 255 > destEnd) {
      throw new LZ4Exception();
    }

    if (runLen >= RUN_MASK) {
      dest.put(dOff++, (byte) (RUN_MASK << ML_BITS));
      dOff = writeLen(runLen - RUN_MASK, dest, dOff);
    } else {
      dest.put(dOff++, (byte) (runLen << ML_BITS));
    }
    // copy literals
    safeArraycopy(src, sOff, dest, dOff, runLen);
    dOff += runLen;

    return dOff;
  }

  static int writeLen(int len, ByteBuffer dest, int dOff) {
    while (len >= 0xFF) {
      dest.put(dOff++, (byte) 0xFF);
      len -= 0xFF;
    }
    dest.put(dOff++, (byte) len);
    return dOff;
  }

  static class Match {
    int start, ref, len;

    void fix(int correction) {
      start += correction;
      ref += correction;
      len -= correction;
    }

    int end() {
      return start + len;
    }
  }

  static void copyTo(Match m1, Match m2) {
    m2.len = m1.len;
    m2.start = m1.start;
    m2.ref = m1.ref;
  }

}
