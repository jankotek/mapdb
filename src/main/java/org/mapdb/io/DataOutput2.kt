package org.mapdb.io

import java.io.DataOutput
import java.io.IOException


interface DataOutput2: DataOutput {

    /**
     * Give a hint to DataOutput about total size of serialized data.
     * This may prevent `byte[]` resize. And huge records might be placed into temporary file
     */
    @Throws(IOException::class)
    fun sizeHint(size:Int)

    @Throws(IOException::class)
    fun writePackedInt(value:Int)

    @Throws(IOException::class)
    fun writePackedLong(value:Long)

    @Throws(IOException::class)
    fun copyBytes(): ByteArray
}
