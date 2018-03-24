package org.mapdb.serializer

import java.io.DataInput
import java.io.DataOutput

/** Turns object instance into binary form and vice versa */
interface Serializer<K>{

    fun serialize(k:K, out: DataOutput)

    fun deserialize(input:DataInput):K
}