import java.io.File;
import java.io.Serializable;
import java.util.Map;

import org.mapdb.DB;
import org.mapdb.DBMaker;

/*
	Demonstrates HashMaps with non-standard types of objects as key or value.
	
	@author Pedro Alves
*/

// Remember to always implement Serializable in classes that are used as key or value
class Person implements Serializable{
	String name;
	String city;
	
	public Person(String n, String c){
		super();
		this.name = n;
		this.city = c;
	}
}

public class People {

	public static void main(String[] args) {
		
		// Open or create db file
		DB db = DBMaker.newFileDB(new File("dbCustomValue"))
				.asyncWriteDisable()
				.make();
		
		// Open or create table
		Map<String,Person> dbMap = db.getTreeMap("personAndCity");
		
		// Add data
		Person bilbo = new Person("Bilbo","The Shire");
		Person sauron = new Person("Sauron","Mordor");
		Person radagast = new Person("Radagast","Crazy Farm");
		
		dbMap.put(bilbo.name,bilbo);
		dbMap.put(sauron.name,sauron);
		dbMap.put(radagast.name,radagast);
		
		
		// Commit and close
		db.commit();
		db.close();
	}
}
	

	


