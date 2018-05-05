package org.mapdb.serializer;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

/** Turns object instance into binary form and vice versa */
public interface Serializer<K>{

    void serialize(@NotNull K k, @NotNull DataOutput2 out);

    K deserialize(@NotNull DataInput2 input);

    Class serializedType();

    default boolean equals(@Nullable K k1, @Nullable K k2){
        return k1==k2 || (k1!=null && k1.equals(k2));
    }


    //TODO long test that hashCode is well distributed for random values
    default int hashCode(@NotNull K k){
        return k.hashCode();
    }

}

