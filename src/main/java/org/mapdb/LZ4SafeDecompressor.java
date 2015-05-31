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

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * LZ4 decompressor that requires the size of the compressed data to be known.
 * <p>
 * Implementations of this class are usually a little slower than those of
 * {@link LZ4FastDecompressor} but do not require the size of the original data to
 * be known.
 */
abstract class LZ4SafeDecompressor {

  /**
   * Decompress <code>src[srcOff:srcLen]</code> into
   * <code>dest[destOff:destOff+maxDestLen]</code> and returns the number of
   * decompressed bytes written into <code>dest</code>.
   *
   * @param srcLen the exact size of the compressed stream
   * @return the original input size
   * @throws LZ4Exception if maxDestLen is too small
   */
  public abstract int decompress(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff, int maxDestLen);

  /**
   * Uncompress <code>src[srcOff:srcLen]</code> into
   * <code>dest[destOff:destOff+maxDestLen]</code> and returns the number of
   * decompressed bytes written into <code>dest</code>.
   *
   * @param srcLen the exact size of the compressed stream
   * @return the original input size
   * @throws LZ4Exception if maxDestLen is too small
   */
  public abstract int decompress(ByteBuffer src, int srcOff, int srcLen, ByteBuffer dest, int destOff, int maxDestLen);

  /**
   * Convenience method, equivalent to calling
   * {@link #decompress(byte[], int, int, byte[], int, int) decompress(src, srcOff, srcLen, dest, destOff, dest.length - destOff)}.
   */
  public final int decompress(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff) {
    return decompress(src, srcOff, srcLen, dest, destOff, dest.length - destOff);
  }

  /**
   * Convenience method, equivalent to calling
   * {@link #decompress(byte[], int, int, byte[], int) decompress(src, 0, src.length, dest, 0)}
   */
  public final int decompress(byte[] src, byte[] dest) {
    return decompress(src, 0, src.length, dest, 0);
  }

  /**
   * Convenience method which returns <code>src[srcOff:srcOff+srcLen]</code>
   * decompressed.
   * <p><b><span style="color:red">Warning</span></b>: this method has an
   * important overhead due to the fact that it needs to allocate a buffer to
   * decompress into, and then needs to resize this buffer to the actual
   * decompressed length.</p>
   * <p>Here is how this method is implemented:</p>
   * <pre>
   * byte[] decompressed = new byte[maxDestLen];
   * final int decompressedLength = decompress(src, srcOff, srcLen, decompressed, 0, maxDestLen);
   * if (decompressedLength != decompressed.length) {
   *   decompressed = Arrays.copyOf(decompressed, decompressedLength);
   * }
   * return decompressed;
   * </pre>
   */
  public final byte[] decompress(byte[] src, int srcOff, int srcLen, int maxDestLen) {
    byte[] decompressed = new byte[maxDestLen];
    final int decompressedLength = decompress(src, srcOff, srcLen, decompressed, 0, maxDestLen);
    if (decompressedLength != decompressed.length) {
      decompressed = Arrays.copyOf(decompressed, decompressedLength);
    }
    return decompressed;
  }

  /**
   * Convenience method, equivalent to calling
   * {@link #decompress(byte[], int, int, int) decompress(src, 0, src.length, maxDestLen)}.
   */
  public final byte[] decompress(byte[] src, int maxDestLen) {
    return decompress(src, 0, src.length, maxDestLen);
  }

  /**
   * Decompress <code>src</code> into <code>dest</code>. <code>src</code>'s
   * {@link ByteBuffer#remaining()} must be exactly the size of the compressed
   * data. This method moves the positions of the buffers.
   */
  public final void decompress(ByteBuffer src, ByteBuffer dest) {
    final int decompressed = decompress(src, src.position(), src.remaining(), dest, dest.position(), dest.remaining());
    src.position(src.limit());
    dest.position(dest.position() + decompressed);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
