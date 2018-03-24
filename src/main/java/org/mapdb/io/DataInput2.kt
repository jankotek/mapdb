package org.mapdb.io

import java.io.DataInput
import java.io.IOException

abstract class DataInput2: DataInput {

    /**
     * How many bytes are available for read (`total size - current position`).
     * Returns `Integer.MIN_VALUE` if this information is not available 
     */
    @Throws(IOException::class)
    abstract fun available():Int


    @Throws(IOException::class)
    abstract fun readPackedInt():Int

    @Throws(IOException::class)
    abstract fun readPackedLong():Long


    @Throws(IOException::class)
    override fun readFloat(): Float {
        return java.lang.Float.intBitsToFloat(readInt())
    }

    @Throws(IOException::class)
    override fun readDouble(): Double {
        return java.lang.Double.longBitsToDouble(readLong())
    }

    @Throws(IOException::class)
    override fun readLine(): String {
        return readUTF()
    }

    @Throws(IOException::class)
    override fun readUTF(): String {
        //TODO is it better then DataOutputStream.readUTF?
        val len = readPackedInt()
        val b = CharArray(len)
        for (i in 0 until len)
            b[i] = readPackedInt().toChar()
        return String(b)
    }

    @Throws(IOException::class)
    override fun readUnsignedShort(): Int {
        return readChar().toInt()
    }


    @Throws(IOException::class)
    override fun readFully(b: ByteArray) {
        readFully(b, 0, b.size)
    }

}
