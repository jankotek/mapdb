// Auto-generated: DO NOT EDIT

package org.mapdb;

import static org.mapdb.LZ4Constants.*;
import org.mapdb.DBException.LZ4Exception;

import java.nio.ByteBuffer;

/**
 * Decompressor.
 */
final class SafeLZ4Decompressor extends LZ4SafeDecompressor {

  @Override
  public int decompress(byte[] src, final int srcOff, final int srcLen , byte[] dest, final int destOff, int destLen) {


    SafeUtils.checkRange(src, srcOff, srcLen);
    SafeUtils.checkRange(dest, destOff, destLen);

    if (destLen == 0) {
      if (srcLen != 1 || SafeUtils.readByte(src, srcOff) != 0) {
        throw new LZ4Exception("Output buffer too small");
      }
      return 0;
    }

    final int srcEnd = srcOff + srcLen;


    final int destEnd = destOff + destLen;

    int sOff = srcOff;
    int dOff = destOff;

    while (true) {
      final int token = SafeUtils.readByte(src, sOff) & 0xFF;
      ++sOff;

      // literals
      int literalLen = token >>> ML_BITS;
      if (literalLen == RUN_MASK) {
        byte len = (byte) 0xFF;
        while (sOff < srcEnd &&(len = SafeUtils.readByte(src, sOff++)) == (byte) 0xFF) {
          literalLen += 0xFF;
        }
        literalLen += len & 0xFF;
      }

      final int literalCopyEnd = dOff + literalLen;

      if (literalCopyEnd > destEnd - COPY_LENGTH || sOff + literalLen > srcEnd - COPY_LENGTH) {
        if (literalCopyEnd > destEnd) {
          throw new LZ4Exception();
        } else if (sOff + literalLen != srcEnd) {
          throw new LZ4Exception("Malformed input at " + sOff);

        } else {
          LZ4SafeUtils.safeArraycopy(src, sOff, dest, dOff, literalLen);
          sOff += literalLen;
          dOff = literalCopyEnd;
          break; // EOF
        }
      }

      LZ4SafeUtils.wildArraycopy(src, sOff, dest, dOff, literalLen);
      sOff += literalLen;
      dOff = literalCopyEnd;

      // matchs
      final int matchDec = SafeUtils.readShortLE(src, sOff);
      sOff += 2;
      int matchOff = dOff - matchDec;

      if (matchOff < destOff) {
        throw new LZ4Exception("Malformed input at " + sOff);
      }

      int matchLen = token & ML_MASK;
      if (matchLen == ML_MASK) {
        byte len = (byte) 0xFF;
        while (sOff < srcEnd &&(len = SafeUtils.readByte(src, sOff++)) == (byte) 0xFF) {
          matchLen += 0xFF;
        }
        matchLen += len & 0xFF;
      }
      matchLen += MIN_MATCH;

      final int matchCopyEnd = dOff + matchLen;

      if (matchCopyEnd > destEnd - COPY_LENGTH) {
        if (matchCopyEnd > destEnd) {
          throw new LZ4Exception("Malformed input at " + sOff);
        }
        LZ4SafeUtils.safeIncrementalCopy(dest, matchOff, dOff, matchLen);
      } else {
        LZ4SafeUtils.wildIncrementalCopy(dest, matchOff, dOff, matchCopyEnd);
      }
      dOff = matchCopyEnd;
    }


    return dOff - destOff;

  }

  @Override
  public int decompress(ByteBuffer src, final int srcOff, final int srcLen , ByteBuffer dest, final int destOff, int destLen) {

    if (src.hasArray() && dest.hasArray()) {
      return decompress(src.array(), srcOff + src.arrayOffset(), srcLen, dest.array(), destOff + dest.arrayOffset(), destLen);
    }
    src = ByteBufferUtils.inNativeByteOrder(src);
    dest = ByteBufferUtils.inNativeByteOrder(dest);


    ByteBufferUtils.checkRange(src, srcOff, srcLen);
    ByteBufferUtils.checkRange(dest, destOff, destLen);

    if (destLen == 0) {
      if (srcLen != 1 || ByteBufferUtils.readByte(src, srcOff) != 0) {
        throw new LZ4Exception("Output buffer too small");
      }
      return 0;
    }

    final int srcEnd = srcOff + srcLen;


    final int destEnd = destOff + destLen;

    int sOff = srcOff;
    int dOff = destOff;

    while (true) {
      final int token = ByteBufferUtils.readByte(src, sOff) & 0xFF;
      ++sOff;

      // literals
      int literalLen = token >>> ML_BITS;
      if (literalLen == RUN_MASK) {
        byte len = (byte) 0xFF;
        while (sOff < srcEnd &&(len = ByteBufferUtils.readByte(src, sOff++)) == (byte) 0xFF) {
          literalLen += 0xFF;
        }
        literalLen += len & 0xFF;
      }

      final int literalCopyEnd = dOff + literalLen;

      if (literalCopyEnd > destEnd - COPY_LENGTH || sOff + literalLen > srcEnd - COPY_LENGTH) {
        if (literalCopyEnd > destEnd) {
          throw new LZ4Exception();
        } else if (sOff + literalLen != srcEnd) {
          throw new LZ4Exception("Malformed input at " + sOff);

        } else {
          LZ4ByteBufferUtils.safeArraycopy(src, sOff, dest, dOff, literalLen);
          sOff += literalLen;
          dOff = literalCopyEnd;
          break; // EOF
        }
      }

      LZ4ByteBufferUtils.wildArraycopy(src, sOff, dest, dOff, literalLen);
      sOff += literalLen;
      dOff = literalCopyEnd;

      // matchs
      final int matchDec = ByteBufferUtils.readShortLE(src, sOff);
      sOff += 2;
      int matchOff = dOff - matchDec;

      if (matchOff < destOff) {
        throw new LZ4Exception("Malformed input at " + sOff);
      }

      int matchLen = token & ML_MASK;
      if (matchLen == ML_MASK) {
        byte len = (byte) 0xFF;
        while (sOff < srcEnd &&(len = ByteBufferUtils.readByte(src, sOff++)) == (byte) 0xFF) {
          matchLen += 0xFF;
        }
        matchLen += len & 0xFF;
      }
      matchLen += MIN_MATCH;

      final int matchCopyEnd = dOff + matchLen;

      if (matchCopyEnd > destEnd - COPY_LENGTH) {
        if (matchCopyEnd > destEnd) {
          throw new LZ4Exception("Malformed input at " + sOff);
        }
        LZ4ByteBufferUtils.safeIncrementalCopy(dest, matchOff, dOff, matchLen);
      } else {
        LZ4ByteBufferUtils.wildIncrementalCopy(dest, matchOff, dOff, matchCopyEnd);
      }
      dOff = matchCopyEnd;
    }


    return dOff - destOff;

  }


}

