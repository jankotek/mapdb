package org.mapdb.serializer;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerStringIntern extends GroupSerializerObjectArray<String> {
    @Override
    public void serialize(DataOutput2 out, String value) throws IOException {
        out.writeUTF(value);
    }

    @Override
    public String deserialize(DataInput2 in, int available) throws IOException {
        return in.readUTF().intern();
    }

    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public int hashCode(@NotNull String s, int seed) {
        return STRING.hashCode(s, seed);
    }


}
