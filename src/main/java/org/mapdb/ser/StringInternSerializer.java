package org.mapdb.ser;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;

/**
 * Created by jan on 2/28/16.
 */
public class StringInternSerializer extends DefaultGroupSerializer<String> {
    @Override
    public void serialize(DataOutput2 out, String value) {
        out.writeUTF(value);
    }

    @Override
    public String deserialize(DataInput2 in) {
        return in.readUTF().intern();
    }

    @Nullable
    @Override
    public Class serializedType() {
        return String.class;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public int hashCode(@NotNull String s, int seed) {
        return Serializers.STRING.hashCode(s, seed);
    }


}
