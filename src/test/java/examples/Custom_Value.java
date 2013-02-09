import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.mapdb.BTreeKeySerializer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

/*
	Demonstrates HashMaps with non-standard types of objects as key or value.
	
	@author Pedro Alves
*/

// Remember to always implement Serializable in classes that are used as key or value
final class Person implements Serializable{
	private String name;
	private String city;
	
	public Person(String n, String c){
		super();
		this.name = n;
		this.city = c;
	}

	String getName() {
		return name;
	}

	String getCity() {
		return city;
	}

}
// MapDB has its own space efficient serialization tightly optimized for minimal CPU overhead. 
// MapDB serialization tries to mimic Standard Java Serialization behavior. So you may use Serializable 
// marker interface, customize binary format with Externalizable methods and so on.
// MapDB serialization framework is completely transparent and activated by default. 
// In most cases you wont even realize it is there. But for best performance you may also bypass it 
// and use custom binary format by implementing Serializer.

final class PersonSerializer implements Serializer<Person>, Serializable{
	@Override
	public void serialize(DataOutput out, Person value) throws IOException {	
		out.writeBytes(value.getName());
		out.writeBytes(value.getCity());		
	}
	
	@Override
	public Person deserialize(DataInput in, int available) throws IOException {
		if(available != -1 && available != 0){
			String name = in.readUTF();
			String city = in.readUTF();
			
			return new Person(name,city);
		}else{
			return null;
		}
	}
}

public class People {

	public static void main(String[] args) {
		boolean valuesStoredOutsideNodes = true;
		
		// Open or create db file
		DB db = DBMaker.newTempFileDB()
				.asyncWriteDisable()
				.make();

 		// Open or create table.
		// Need this for custom Serializer
		Map<String,Person> dbMap = db.createTreeMap("personAndCity", 
						32, 
						valuesStoredOutsideNodes,
						BTreeKeySerializer.STRING,
						new PersonSerializer(),
						null);
		
		// Create data
		Person bilbo = new Person("Bilbo","The Shire");
		Person sauron = new Person("Sauron","Mordor");
		Person radagast = new Person("Radagast","Crazy Farm");
		
		// Add data
		dbMap.put(bilbo.getName(),bilbo);
		dbMap.put(sauron.getName(),sauron);
		dbMap.put(radagast.getName(),radagast);		
		
		
		// Read data from dbMap
		Iterator<String> iterator = dbMap.keySet().iterator();		
		System.out.println("Data contained on map: \n");
		while(iterator.hasNext()){
			Person p = dbMap.get(iterator.next());
			System.out.println("\tName: "+p.getName());
			System.out.println("\tCity: "+p.getCity()+"\n");
		}
		
	}
}
	

	


