package org.mapdb.util;

import org.jetbrains.annotations.NotNull;
import org.mapdb.io.DataOutput2ByteArray;

public interface Exporter {

    void exportToDataOutput2(@NotNull DataOutput2ByteArray output);
}
