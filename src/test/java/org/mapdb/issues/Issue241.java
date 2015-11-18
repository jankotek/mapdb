package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TT;

import java.io.File;
import java.io.Serializable;
import java.util.Map;

public class Issue241
{
    @Test
    public void main()
    {
        DB db = getDb();
        final String mapName = "map"; //$NON-NLS-1$
        Map<Long, CustomClass> map = db.treeMapCreate(mapName).make();
//                db.createTreeMap(mapName)
//                .valueSerializer(new CustomSerializer()).make();
        map.put(1L, new CustomClass("aString", 1001L)); //$NON-NLS-1$
        db.commit();
        db.close();

        db = getDb();
        map = db.treeMap(mapName);
        map.get(1L);
    }

    private static DB getDb()
    {
        final File dbFile = TT.tempDbFile();
        return DBMaker.appendFileDB(dbFile).make();
    }

    private static final class CustomClass implements Serializable
    {
        private final String aString;
        private final Long aLong;

        private CustomClass(String aString, Long aLong)
        {
            this.aString = aString;
            this.aLong = aLong;
        }

        private String getaString()
        {
            return aString;
        }

        private Long getaLong()
        {
            return aLong;
        }
    }

//    public static final class CustomSerializer implements Serializer<CustomClass>, Serializable
//    {
//        @Override
//        public void serialize(DataOutput out, CustomClass value) throws IOException
//        {
//            out.writeLong(value.getaLong());
//            final byte[] stringBytes = value.getaString().getBytes();
//            out.writeInt(stringBytes.length);
//            out.write(stringBytes);
//        }
//
//        @Override
//        public CustomClass deserialize(DataInput in, int available) throws IOException
//        {
//            final Long theLong = in.readLong();
//            final int stringBytesLength = in.readInt();
//            final byte[] stringBytes = new byte[stringBytesLength];
//            in.readFully(stringBytes);
//            return new CustomClass(new String(stringBytes), theLong);
//        }
//    }
}
