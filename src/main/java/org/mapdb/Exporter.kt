package org.mapdb

import org.mapdb.io.DataOutput2

interface Exporter{

    fun exportToDataOutput2(out: DataOutput2)
}
