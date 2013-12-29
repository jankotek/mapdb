/*
 * Re-licensed under Apache 2 license with Thomas Mueller permission
 *
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.mapdb;


import java.util.Arrays;

/**
 * An implementation of the EncryptionXTEA block cipher algorithm.
 * <p>
 * This implementation uses 32 rounds.
 * The best attack reported as of 2009 is 36 rounds (Wikipedia).
 * <p/>
 * It requires 32 byte long encryption key, so SHA256 password hash is used.
 */
public final class EncryptionXTEA{

    /**
     * Blocks sizes are always multiples of this number.
     */
    public static final int ALIGN = 16;

    private static final int DELTA = 0x9E3779B9;
    private final int k0, k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13, k14, k15;
    private final int k16, k17, k18, k19, k20, k21, k22, k23, k24, k25, k26, k27, k28, k29, k30, k31;


    public EncryptionXTEA(byte[] password) {
        byte[] b = getHash(password, false);
        int[] key = new int[4];
        for (int i = 0; i < 16;) {
            key[i / 4] = (b[i++] << 24) + ((b[i++] & 255) << 16) + ((b[i++] & 255) << 8) + (b[i++] & 255);
        }
        int[] r = new int[32];
        for (int i = 0, sum = 0; i < 32;) {
            r[i++] = sum + key[sum & 3];
            sum += DELTA;
            r[i++] = sum + key[ (sum >>> 11) & 3];
        }
        k0 = r[0]; k1 = r[1]; k2 = r[2]; k3 = r[3]; k4 = r[4]; k5 = r[5]; k6 = r[6]; k7 = r[7];
        k8 = r[8]; k9 = r[9]; k10 = r[10]; k11 = r[11]; k12 = r[12]; k13 = r[13]; k14 = r[14]; k15 = r[15];
        k16 = r[16]; k17 = r[17]; k18 = r[18]; k19 = r[19]; k20 = r[20]; k21 = r[21]; k22 = r[22]; k23 = r[23];
        k24 = r[24]; k25 = r[25]; k26 = r[26]; k27 = r[27]; k28 = r[28]; k29 = r[29]; k30 = r[30]; k31 = r[31];
    }


    public void encrypt(byte[] bytes, int off, int len) {
        assert(len % ALIGN == 0):("unaligned len " + len);

        for (int i = off; i < off + len; i += 8) {
            encryptBlock(bytes, bytes, i);
        }
    }

    public void decrypt(byte[] bytes, int off, int len) {
        assert(len % ALIGN == 0):("unaligned len " + len);

        for (int i = off; i < off + len; i += 8) {
            decryptBlock(bytes, bytes, i);
        }
    }

    private void encryptBlock(byte[] in, byte[] out, int off) {
        int y = (in[off] << 24) | ((in[off+1] & 255) << 16) | ((in[off+2] & 255) << 8) | (in[off+3] & 255);
        int z = (in[off+4] << 24) | ((in[off+5] & 255) << 16) | ((in[off+6] & 255) << 8) | (in[off+7] & 255);
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k0; z += (((y >>> 5) ^ (y << 4)) + y) ^ k1;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k2; z += (((y >>> 5) ^ (y << 4)) + y) ^ k3;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k4; z += (((y >>> 5) ^ (y << 4)) + y) ^ k5;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k6; z += (((y >>> 5) ^ (y << 4)) + y) ^ k7;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k8; z += (((y >>> 5) ^ (y << 4)) + y) ^ k9;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k10; z += (((y >>> 5) ^ (y << 4)) + y) ^ k11;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k12; z += (((y >>> 5) ^ (y << 4)) + y) ^ k13;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k14; z += (((y >>> 5) ^ (y << 4)) + y) ^ k15;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k16; z += (((y >>> 5) ^ (y << 4)) + y) ^ k17;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k18; z += (((y >>> 5) ^ (y << 4)) + y) ^ k19;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k20; z += (((y >>> 5) ^ (y << 4)) + y) ^ k21;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k22; z += (((y >>> 5) ^ (y << 4)) + y) ^ k23;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k24; z += (((y >>> 5) ^ (y << 4)) + y) ^ k25;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k26; z += (((y >>> 5) ^ (y << 4)) + y) ^ k27;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k28; z += (((y >>> 5) ^ (y << 4)) + y) ^ k29;
        y += (((z << 4) ^ (z >>> 5)) + z) ^ k30; z += (((y >>> 5) ^ (y << 4)) + y) ^ k31;
        out[off] = (byte) (y >> 24); out[off+1] = (byte) (y >> 16); out[off+2] = (byte) (y >> 8); out[off+3] = (byte) y;
        out[off+4] = (byte) (z >> 24); out[off+5] = (byte) (z >> 16); out[off+6] = (byte) (z >> 8); out[off+7] = (byte) z;
    }

    private void decryptBlock(byte[] in, byte[] out, int off) {
        int y = (in[off] << 24) | ((in[off+1] & 255) << 16) | ((in[off+2] & 255) << 8) | (in[off+3] & 255);
        int z = (in[off+4] << 24) | ((in[off+5] & 255) << 16) | ((in[off+6] & 255) << 8) | (in[off+7] & 255);
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k31; y -= (((z << 4) ^ (z >>> 5)) + z) ^ k30;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k29; y -= (((z << 4) ^ (z >>> 5)) + z) ^ k28;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k27; y -= (((z << 4) ^ (z >>> 5)) + z) ^ k26;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k25; y -= (((z << 4) ^ (z >>> 5)) + z) ^ k24;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k23; y -= (((z << 4) ^ (z >>> 5)) + z) ^ k22;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k21; y -= (((z << 4) ^ (z >>> 5)) + z) ^ k20;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k19; y -= (((z << 4) ^ (z >>> 5)) + z) ^ k18;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k17; y -= (((z << 4) ^ (z >>> 5)) + z) ^ k16;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k15; y -= (((z << 4) ^ (z >>> 5)) + z) ^ k14;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k13; y -= (((z << 4) ^ (z >>> 5)) + z) ^ k12;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k11; y -= (((z << 4) ^ (z >>> 5)) + z) ^ k10;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k9; y -= (((z << 4) ^ (z >>> 5)) + z) ^ k8;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k7; y -= (((z << 4) ^ (z >>> 5)) + z) ^ k6;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k5; y -= (((z << 4) ^ (z >>> 5)) + z) ^ k4;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k3; y -= (((z << 4) ^ (z >>> 5)) + z) ^ k2;
        z -= (((y >>> 5) ^ (y << 4)) + y) ^ k1; y -= (((z << 4) ^ (z >>> 5)) + z) ^ k0;
        out[off] = (byte) (y >> 24); out[off+1] = (byte) (y >> 16); out[off+2] = (byte) (y >> 8); out[off+3] = (byte) y;
        out[off+4] = (byte) (z >> 24); out[off+5] = (byte) (z >> 16); out[off+6] = (byte) (z >> 8); out[off+7] = (byte) z;
    }


    /**
     * The first 32 bits of the fractional parts of the cube roots of the first
     * sixty-four prime numbers. Used for SHA256 password hash
     */
    private static final int[] K = { 0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5,
            0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5, 0xd807aa98,
            0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
            0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6,
            0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
            0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3,
            0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138,
            0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e,
            0x92722c85, 0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3,
            0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
            0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
            0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814,
            0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2 };


    /**
     * Calculate the hash code for the given data.
     *
     * @param data the data to hash
     * @param nullData if the data should be filled with zeros after calculating the hash code
     * @return the hash code
     */
    public static byte[] getHash(byte[] data, boolean nullData) {
        int byteLen = data.length;
        int intLen = ((byteLen + 9 + 63) / 64) * 16;
        byte[] bytes = new byte[intLen * 4];
        System.arraycopy(data, 0, bytes, 0, byteLen);
        if (nullData) {
            Arrays.fill(data, (byte) 0);
        }
        bytes[byteLen] = (byte) 0x80;
        int[] buff = new int[intLen];
        for (int i = 0, j = 0; j < intLen; i += 4, j++) {
            buff[j] = readInt(bytes, i);
        }
        buff[intLen - 2] = byteLen >>> 29;
        buff[intLen - 1] = byteLen << 3;
        int[] w = new int[64];
        int[] hh = { 0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19 };
        for (int block = 0; block < intLen; block += 16) {
            for (int i = 0; i < 16; i++) {
                w[i] = buff[block + i];
            }
            for (int i = 16; i < 64; i++) {
                int x = w[i - 2];
                int theta1 = rot(x, 17) ^ rot(x, 19) ^ (x >>> 10);
                x = w[i - 15];
                int theta0 = rot(x, 7) ^ rot(x, 18) ^ (x >>> 3);
                w[i] = theta1 + w[i - 7] + theta0 + w[i - 16];
            }

            int a = hh[0], b = hh[1], c = hh[2], d = hh[3];
            int e = hh[4], f = hh[5], g = hh[6], h = hh[7];

            for (int i = 0; i < 64; i++) {
                int t1 = h + (rot(e, 6) ^ rot(e, 11) ^ rot(e, 25))
                        + ((e & f) ^ ((~e) & g)) + K[i] + w[i];
                int t2 = (rot(a, 2) ^ rot(a, 13) ^ rot(a, 22))
                        + ((a & b) ^ (a & c) ^ (b & c));
                h = g;
                g = f;
                f = e;
                e = d + t1;
                d = c;
                c = b;
                b = a;
                a = t1 + t2;
            }
            hh[0] += a;
            hh[1] += b;
            hh[2] += c;
            hh[3] += d;
            hh[4] += e;
            hh[5] += f;
            hh[6] += g;
            hh[7] += h;
        }
        byte[] result = new byte[32];
        for (int i = 0; i < 8; i++) {
            writeInt(result, i * 4, hh[i]);
        }
        Arrays.fill(w, 0);
        Arrays.fill(buff, 0);
        Arrays.fill(hh, 0);
        Arrays.fill(bytes, (byte) 0);
        return result;
    }

    private static int rot(int i, int count) {
        return (i << (32 - count)) | (i >>> count);
    }

    private static int readInt(byte[] b, int i) {
        return ((b[i] & 0xff) << 24) + ((b[i + 1] & 0xff) << 16)
                + ((b[i + 2] & 0xff) << 8) + (b[i + 3] & 0xff);
    }

    private static void writeInt(byte[] b, int i, int value) {
        b[i] = (byte) (value >> 24);
        b[i + 1] = (byte) (value >> 16);
        b[i + 2] = (byte) (value >> 8);
        b[i + 3] = (byte) value;
    }


}
