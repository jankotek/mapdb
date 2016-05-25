package org.mapdb.VolumeAccess

import org.fest.reflect.core.Reflection
import org.mapdb.*
import org.mapdb.volume.*

val Volume.sliceShift: Int
    get() = Reflection.field("sliceShift").ofType(Int::class.java).`in`(this).get()

