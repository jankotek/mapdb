package org.mapdb.serializer

import org.mapdb.io.DataInput2
import org.mapdb.io.DataOutput2

/** Turns object instance into binary form and vice versa */
interface Serializer<K>{

    fun serialize(out: DataOutput2, k:K)

    fun deserialize(input: DataInput2):K
}

