package org.mapdb.ser;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataInput2ByteArray;
import org.mapdb.io.DataOutput2;
import org.mapdb.io.DataOutput2ByteArray;

import java.io.IOError;
import java.io.IOException;
import java.util.Comparator;

/** Turns object instance into binary form and vice versa */
public interface Serializer<K> extends Comparator<K> {  //TODO deatach comparator from serializer???

    void serialize(@NotNull DataOutput2 out, @NotNull K k);

    K deserialize(@NotNull DataInput2 input);

    @Nullable Class serializedType();

    default boolean equals(@Nullable K k1, @Nullable K k2){
        return k1==k2 || (k1!=null && k1.equals(k2));
    }


    default int hashCode(@NotNull K k){
        return k.hashCode(); //TODO better hash
    }


    default int hashCode(@NotNull K k, int hashSeed){
        return hashSeed + k.hashCode(); //TODO better mixing
    }

    default K deserialize(DataInput2 in, int i){
        if(i!=-1)
            throw new AssertionError(); //FIXME temp method for compatibility

        return deserialize(in);
    }

    @Deprecated
    default boolean isTrusted(){
        return true;
    }

    default int compare(K k1, K k2){
        return ((Comparable)k1).compareTo(k2); //TODO comparators
    }

    default int fixedSize() {
        return -1;
    }

    /** Creates binary copy of given object. If the datatype is immutable the same instance might be returned */
    default K clone(K value){
        DataOutput2 out = new DataOutput2ByteArray();
        serialize(out, value);
        DataInput2 in2 = new DataInput2ByteArray(out.copyBytes());
        return deserialize(in2);
    }

}

