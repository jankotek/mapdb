package org.mapdb.kotlin;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;
import org.mapdb.ser.Serializer;

import static org.junit.Assert.assertEquals;

public class Issue888_serializer_override {


    static class AlternateHashSer implements Serializer<Integer> {
        @Override
        public int hashCode(Integer integer) {
            return -integer;
        }

        @Override
        public void serialize(@NotNull DataOutput2 out, Integer integer) {

        }

        @Override
        public Integer deserialize(@NotNull DataInput2 input) {
            return null;
        }

        @Override
        public Class serializedType() {
            return Integer.class;
        }
    }

    @Test public void ser_default_override(){
        assertEquals(-10, new AlternateHashSer().hashCode(10));
    }
}
