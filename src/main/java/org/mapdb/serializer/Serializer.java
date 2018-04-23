package org.mapdb.serializer;

import org.jetbrains.annotations.NotNull;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

/** Turns object instance into binary form and vice versa */
public interface Serializer<K>{

    void serialize(@NotNull K k, @NotNull DataOutput2 out);

    K deserialize(@NotNull DataInput2 input);

    default boolean equals(K k1, K k2){
        return k1==k2 || k1.equals(k2);
    }

    default int hashCode(K k){
        return k.hashCode();
    }

}

