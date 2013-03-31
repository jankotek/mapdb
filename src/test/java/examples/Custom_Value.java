package examples;

import java.io.*;
import java.util.Map;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.Utils;

/*
 *	Demonstrates HashMaps with non-standard types of objects as key or value.
 */
public class Custom_Value {


    /**
     * MapDB uses custom serialization which stores class metadata at single place.
     * Thanks to it is 10x more efficient than standard Java serialization.
     *
     * Using custom values in MapDB has three conditions:
     *
     *   1)  classes should be immutable. There is instance cache, background serialization etc
     *         Modifing your classes after they were inserted into MapDB may leed to unexpected things.
     *
     *   2) You should implement `Serializable` marker interface. MapDB tries to stay compatible
     *         with standard Java serialization.
     *
     *   3) Even your values should implement equalsTo method for CAS (compare-and-swap) operations.
     *
     */
    public static class Person implements Serializable{
        final String name;
        final String city;

        public Person(String n, String c){
            super();
            this.name = n;
            this.city = c;
        }

        public String getName() {
            return name;
        }

        public String getCity() {
            return city;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Person person = (Person) o;

            if (city != null ? !city.equals(person.city) : person.city != null) return false;
            if (name != null ? !name.equals(person.name) : person.name != null) return false;

            return true;
        }

    }

    public static void main(String[] args) {

        // Open db in temp directory
        File f = Utils.tempDbFile();
		DB db = DBMaker.newFileDB(f)
				.make();
		
		// Open or create table
		Map<String,Person> dbMap = db.getTreeMap("personAndCity");
		
		// Add data
		Person bilbo = new Person("Bilbo","The Shire");
		Person sauron = new Person("Sauron","Mordor");
		Person radagast = new Person("Radagast","Crazy Farm");
		
		dbMap.put("west",bilbo);
		dbMap.put("south",sauron);
		dbMap.put("mid",radagast);

		// Commit and close
		db.commit();
		db.close();


        //
        // Second option for using cystom values is to use your own serializer.
        // This usually leads to better performance as MapDB does not have to
        // analyze the class structure.
        //

        class CustomSerializer implements Serializer<Person>, Serializable{

            @Override
            public void serialize(DataOutput out, Person value) throws IOException {
                out.writeUTF(value.getName());
                out.writeUTF(value.getCity());
            }

            @Override
            public Person deserialize(DataInput in, int available) throws IOException {
                return new Person(in.readUTF(), in.readUTF());
            }
        }

        Serializer<Person> serializer = new CustomSerializer();

        DB db2 = DBMaker.newTempFileDB().make();

        Map<String,Person> map2 = db2.createHashMap("map", false, null, serializer); //key serializer is null (use default)

        map2.put("North", new Person("Yet another dwarf","Somewhere"));

        db2.close();


	}


}
	

	


