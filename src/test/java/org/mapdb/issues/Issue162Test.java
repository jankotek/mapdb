package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.TT;

import java.io.*;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class Issue162Test {

    public static class MyValue implements Serializable {
        private String string;

        public MyValue(String string) {
            this.string = string;
        }

        @Override
        public String toString() {
            return "MyValue{" + "string='" + string + '\'' + '}';
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MyValue)) return false;

            MyValue myValue = (MyValue) o;
            if (!string.equals(myValue.string)) return false;
            return true;
        }


        @Override
        public int hashCode() {
            return string.hashCode();
        }
    }

    public static class MyValueSerializer extends Serializer<MyValue> implements Serializable {

        @Override
        public void serialize(DataOutput out, MyValue value) throws IOException {
            assertTrue(value != null);
            System.out.println("Custom serializer called with '" + value + "'");
            out.writeUTF(value.string);
        }

        @Override
        public MyValue deserialize(DataInput in, int available) throws IOException {
            String s = in.readUTF();
            return new MyValue(s);
        }

    }

    private static void printEntries(Map<Long, MyValue> map) {
        System.out.println("Reading back data");
        for (Map.Entry<Long, MyValue> entry : map.entrySet()) {
            System.out.println("Entry id = " + entry.getKey() + ", contents = " + entry.getValue().toString());
        }

        assertEquals("one",map.get(1L).string);
        assertEquals("two",map.get(2L).string);
    }

    File path = TT.tempDbFile();

    @Test public void testHashMap() {
        System.out.println("--- Testing HashMap with custom serializer");

        DB db = DBMaker.fileDB(path).make();
        Map<Long, MyValue> map = db.hashMapCreate("map")
                .valueSerializer(new MyValueSerializer())
                .make();
        db.commit();

        System.out.println("Putting and committing data");
        map.put(1L, new MyValue("one"));
        map.put(2L, new MyValue("two"));
        db.commit();

        System.out.println("Closing and reopening db");
        db.close();
        map = null;

        db = DBMaker.fileDB(path).make();
        map = db.hashMap("map");

        printEntries(map);
    }

    @Test public void testBTreeMap() {
        System.out.println("--- Testing BTreeMap with custom serializer");

        DB db = DBMaker.fileDB(path).make();
        Map<Long, MyValue> map = db.treeMapCreate("map")
                .valueSerializer(new MyValueSerializer())
                .make();
        db.commit();

        System.out.println("Putting and committing data");
        map.put(1L, new MyValue("one"));
        map.put(2L, new MyValue("two"));
        db.commit();

        System.out.println("Closing and reopening db");
        db.close();
        map = null;

        db = DBMaker.fileDB(path).make();
        map = db.treeMap("map");

        printEntries(map);
    }

}
