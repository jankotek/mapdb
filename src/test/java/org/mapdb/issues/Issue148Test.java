package org.mapdb.issues;




import org.junit.Test;
import org.mapdb.*;

import java.io.*;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class Issue148Test {

    @Test public void repeated_update(){
        File mapdbFile = TT.tempDbFile();

        String str = TT.randomString(1000);
        Engine engine = DBMaker.appendFileDB(mapdbFile).closeOnJvmShutdown().makeEngine();
        long recid = engine.put(str, Serializer.STRING_NOSIZE);
        engine.commit();
        engine.close();

        for(int i=10;i<100;i++){
            engine = DBMaker.appendFileDB(mapdbFile).closeOnJvmShutdown().makeEngine();
            assertEquals(str, engine.get(recid, Serializer.STRING_NOSIZE));
            str = TT.randomString(i);
            engine.update(recid,str,Serializer.STRING_NOSIZE);
            assertEquals(str, engine.get(recid, Serializer.STRING_NOSIZE));
            engine.commit();
            engine.close();
        }


    }

    @Test
    public void test(){

        // 1 : Create HTreeMap, put some values , Commit and Close;
        File mapdbFile = TT.tempDbFile();
        DB mapdb = DBMaker.appendFileDB(mapdbFile).closeOnJvmShutdown().make();

        Serializer<CustomValue> valueSerializer = new CustomValueSerializer();
        HTreeMap<String, CustomValue> users = mapdb.hashMapCreate("users").counterEnable().make();
        users.put("jhon", new CustomValue("jhon",  32));
        users.put("mike", new CustomValue("mike",  30));
        mapdb.commit();

        System.out.println("Create and Fisrt Put [\"jhon\"->32, \"mike\"->30]");
        dumpUserDB(users);

        users.replace("mike", new CustomValue("mike",  33));
        mapdb.commit();

        System.out.println("Replace Before Close : [\"mike\"->33] looks as works");
        dumpUserDB(users);

        mapdb.close();


        // 2 : Open HTreeMap, replace some values , Commit and Close;
        mapdb = DBMaker.appendFileDB(mapdbFile).closeOnJvmShutdown().make();
        users = mapdb.hashMap("users");

        System.out.println("Just Reopen : all values ar good");
        dumpUserDB(users);

        CustomValue old = users.replace("jhon", new CustomValue("jhon", 31));
        assertEquals(32, old.age);
        assertEquals("jhon", old.name);

        assertEquals(31, users.get("jhon").age);
        assertEquals("jhon", users.get("jhon").name);

        mapdb.commit();
        assertEquals(31, users.get("jhon").age);
        assertEquals("jhon", users.get("jhon").name);

        System.out.println("Do Replacement on Reopen : [\"jhon\"->31] looks as works");
        dumpUserDB(users);
        mapdb.close();


        // 3 : Open HTreeMap, Dump
        mapdb = DBMaker.appendFileDB(mapdbFile).closeOnJvmShutdown().make();
        users = mapdb.hashMap("users");

        System.out.println("But final value is not changed");
        dumpUserDB(users);
        assertEquals(31, users.get("jhon").age);
        assertEquals("jhon", users.get("jhon").name);

        mapdb.close();
    }

    public static void dumpUserDB(HTreeMap<String, CustomValue> users){

        Set<String> keyset = users.keySet();
        if(keyset==null){
            return;
        }

        for( String key : keyset ){
            CustomValue cv = users.get(key);
            System.out.format("%s(%b) : %d%n", key, key.equals(cv.name), cv.age);
        }

        System.out.println("");
    }

    /* Custom Value and Serializer **/

    public static class CustomValue implements Serializable {

        private static final long serialVersionUID = -7585177565368493580L;
        final String name;
        final int age;

        public CustomValue(String name, int age){

            this.name = name;
            this.age = age;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + age;
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            CustomValue other = (CustomValue) obj;
            if (age != other.age)
                return false;
            if (name == null) {
                if (other.name != null)
                    return false;
            } else if (!name.equals(other.name))
                return false;
            return true;
        }
    }

    public static class CustomValueSerializer extends Serializer<CustomValue> implements Serializable {

        private static final long serialVersionUID = -6987588810823227467L;

        public void serialize(DataOutput out, CustomValue value) throws IOException {

            out.writeUTF(value.name);
            out.writeInt(value.age);
        }

        public CustomValue deserialize(DataInput in, int available)
                throws IOException {

            return new CustomValue( in.readUTF(), in.readInt() );
        }

    }


}
